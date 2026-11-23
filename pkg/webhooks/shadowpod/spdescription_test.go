// Copyright 2019-2026 The Liqo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shadowpod

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	offloadingv1beta1 "github.com/liqotech/liqo/apis/offloading/v1beta1"
	"github.com/liqotech/liqo/pkg/consts"
	"github.com/liqotech/liqo/pkg/utils/testutil"
)

// rl builds a ResourceList from "<name>=<quantity>" pairs, keeping the test expectations readable.
func rl(pairs ...string) corev1.ResourceList {
	result := corev1.ResourceList{}
	for _, pair := range pairs {
		fields := strings.SplitN(pair, "=", 2)
		ExpectWithOffset(1, fields).To(HaveLen(2), "invalid resource pair %q", pair)
		result[corev1.ResourceName(fields[0])] = resource.MustParse(fields[1])
	}
	return result
}

// forgedContainer is a compact description of a container to forge in tests.
type forgedContainer struct {
	name          string
	requests      corev1.ResourceList
	limits        corev1.ResourceList
	restartPolicy *corev1.ContainerRestartPolicy
}

func sidecar(name string, requests corev1.ResourceList, limits corev1.ResourceList) forgedContainer {
	always := corev1.ContainerRestartPolicyAlways
	return forgedContainer{name: name, requests: requests, limits: limits, restartPolicy: &always}
}

func forgeShadowPodWithContainers(containers, initContainers []forgedContainer, overhead corev1.ResourceList) *offloadingv1beta1.ShadowPod {
	toContainers := func(in []forgedContainer) []corev1.Container {
		out := make([]corev1.Container, len(in))
		for i := range in {
			out[i] = corev1.Container{
				Name:      in[i].name,
				Image:     "test-image",
				Resources: corev1.ResourceRequirements{Requests: in[i].requests, Limits: in[i].limits},
			}
			if in[i].restartPolicy != nil {
				out[i].RestartPolicy = in[i].restartPolicy
			}
		}
		return out
	}

	return &offloadingv1beta1.ShadowPod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testShadowPodName,
			Namespace: testNamespace,
			UID:       testShadowPodUID,
			Labels: map[string]string{
				"liqo.io/originID":     string(clusterID),
				consts.CreatorLabelKey: userName,
			},
		},
		Spec: offloadingv1beta1.ShadowPodSpec{
			Pod: corev1.PodSpec{
				Containers:     toContainers(containers),
				InitContainers: toContainers(initContainers),
				Overhead:       overhead,
			},
		},
	}
}

// expectResourceList asserts semantic equality of two ResourceLists (same keys, same values), which
// is independent of the internal Quantity representation.
func expectResourceList(actual, expected corev1.ResourceList) {
	ExpectWithOffset(1, actual).To(HaveLen(len(expected)))
	for name, want := range expected {
		got, found := actual[name]
		ExpectWithOffset(1, found).To(BeTrue(), "resource %q is missing", name)
		ExpectWithOffset(1, got.Cmp(want)).To(Equal(0), "resource %q: got %s, want %s", name, got.String(), want.String())
	}
}

var _ = Describe("ShadowPod Description", func() {
	var (
		spValidator       *Validator
		err               error
		errTest           error
		shadowPod         *offloadingv1beta1.ShadowPod
		spDescription     *Description
		spDescriptionTest *Description
		spNamespacedName  types.NamespacedName
		fakeNamespace     *corev1.Namespace
		piForDescription  *peeringInfo
		fakeClient        client.Client
		quota             *corev1.ResourceList
		containers        []containerResource
		initContainers    []containerResource
	)

	BeforeEach(func() {

		fakeNamespace = testutil.FakeNamespaceWithClusterID(clusterID, tenantNamespace)

		fakeClient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(fakeNamespace).Build()

		spValidator = webhook.Admission{Handler: NewValidator(fakeClient, false)}.Handler.(*Validator)

		piForDescription = createPeeringInfo(userName, *resourceQuota)

		spNamespacedName = types.NamespacedName{Name: testShadowPodName, Namespace: testNamespace}
	})

	Describe("Get or Create a ShadowPod Description", func() {
		JustBeforeEach(func() {
			containers = append(containers, containerResource{cpu: 100, memory: 100})
			spDescription, err = piForDescription.getOrCreateShadowPodDescription(ctx, spValidator.client,
				forgeShadowPodWithResourceRequests(containers, nil), offloadingv1beta1.SoftLimitsEnforcement)
		})

		When("The ShadowPod Description does not exist", func() {
			BeforeEach(func() {
				delete(piForDescription.shadowPods, testShadowPodName)
			})
			It("should return a new one without errors", func() {
				Expect(spDescription.running).To(BeTrue())
				Expect(err).To(BeNil())
			})
		})
		When("The ShadowPod Description exists in cache and is running", func() {
			BeforeEach(func() {
				spDescriptionTest = createShadowPodDescription(testShadowPodName, testNamespace, testShadowPodUID, *forgeResourceList(100, 100))
				errTest = fmt.Errorf("ShadowPod %s is already running", spDescriptionTest.namespacedName.Name)
				err = nil
				piForDescription.shadowPods[spNamespacedName.String()] = spDescriptionTest
			})
			It("should return an error", func() {
				Expect(spDescription).To(BeNil())
				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(Equal(errTest.Error()))
			})
		})
		When("The ShadowPod Description exists in cache and is not running. The Shadow Pod exists in system snapshot", func() {
			BeforeEach(func() {
				spDescriptionTest = createShadowPodDescription(testShadowPodName, testNamespace, testShadowPodUID, *forgeResourceList(100, 100))
				spDescriptionTest.terminate()
				errTest = fmt.Errorf("ShadowPod still exists in the system")
				piForDescription.shadowPods[spNamespacedName.String()] = spDescriptionTest
				containers = append(containers, containerResource{cpu: 100, memory: 100})
				Expect(fakeClient.Create(ctx, forgeShadowPodWithResourceRequests(containers, nil))).ToNot(HaveOccurred())
			})
			It("should return an error", func() {
				Expect(spDescription).To(BeNil())
				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(Equal(errTest.Error()))
			})
		})
		When("The ShadowPod Description exists in cache but is not running and the Shadow Pod does not exist in system snapshot anymore", func() {
			BeforeEach(func() {
				spDescriptionTest = createShadowPodDescription(testShadowPodName, testNamespace, testShadowPodUID, *forgeResourceList(100, 100))
				spDescriptionTest.terminate()
				piForDescription.shadowPods[spNamespacedName.String()] = spDescriptionTest
			})
			It("should return a new running one without errors", func() {
				Expect(spDescription.running).To(BeTrue())
				Expect(err).To(BeNil())
			})
		})
	})

	Describe("Get a ShadowPod Description", func() {
		JustBeforeEach(func() {
			containers = append(containers, containerResource{cpu: 100, memory: 100})
			spDescription, err = piForDescription.getShadowPodDescription(forgeShadowPodWithResourceRequests(containers, nil))
		})

		When("The ShadowPod Description exists", func() {
			BeforeEach(func() {
				spDescriptionTest = createShadowPodDescription(testShadowPodName, testNamespace, testShadowPodUID, *forgeResourceList(100, 100))
				piForDescription.shadowPods[spNamespacedName.String()] = spDescriptionTest
			})
			It("should return it without errors", func() {
				Expect(spDescription).ToNot(BeNil())
				Expect(err).To(BeNil())
			})
		})
		When("The ShadowPod Description does not exist", func() {
			BeforeEach(func() {
				errTest = fmt.Errorf("ShadowPod %s not found (Maybe Cache problem)", testShadowPodName)
				delete(piForDescription.shadowPods, testShadowPodName)
			})
			It("should return an error", func() {
				Expect(spDescription).To(BeNil())
				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(Equal(errTest.Error()))
			})
		})
		When("The ShadowPod Description exists but with a UID mismatch", func() {
			BeforeEach(func() {
				spDescriptionTest = createShadowPodDescription(testShadowPodName, testNamespace, testShadowPodUIDInvalid, *forgeResourceList(100, 100))
				errTest = fmt.Errorf("ShadowPod %s: UID mismatch", testShadowPodName)
				piForDescription.shadowPods[spNamespacedName.String()] = spDescriptionTest
			})
			It("should return an error", func() {
				Expect(spDescription).To(BeNil())
				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(Equal(errTest.Error()))
			})
		})
	})

	Describe("Get Quota from a ShadowPod", func() {
		JustBeforeEach(func() {
			quota, err = getQuotaFromShadowPod(shadowPod, *resourceQuota, offloadingv1beta1.SoftLimitsEnforcement)
		})

		When("at least one ShadowPod container has not cpu or memory requests defined", func() {
			BeforeEach(func() {
				containers = []containerResource{{cpu: 100}}
				initContainers = nil
				shadowPod = forgeShadowPodWithResourceRequests(containers, initContainers)
				errTest = fmt.Errorf(`ShadowPod %s: container "test-container": request for resource "memory" is not set`,
					testShadowPodName)
			})
			It("should return an error", func() {
				Expect(quota).To(BeNil())
				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(Equal(errTest.Error()))
			})
		})
		When("at least one ShadowPod initContainer has not cpu or memory requests defined", func() {
			BeforeEach(func() {
				containers = []containerResource{{cpu: 100, memory: 100}}
				initContainers = []containerResource{{cpu: 100}}
				shadowPod = forgeShadowPodWithResourceRequests(containers, initContainers)
				errTest = fmt.Errorf(`ShadowPod %s: initContainer "test-init-container": request for resource "memory" is not set`,
					testShadowPodName)
			})
			It("should return an error", func() {
				Expect(quota).To(BeNil())
				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(Equal(errTest.Error()))
			})
		})
		When("The ShadowPod has not any container defined", func() {
			BeforeEach(func() {
				containers = nil
				initContainers = nil
				shadowPod = forgeShadowPodWithResourceRequests(containers, initContainers)
				errTest = fmt.Errorf("ShadowPod %s has no containers defined", shadowPod.GetName())
			})
			It("should return an error", func() {
				Expect(quota).To(BeNil())
				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(Equal(errTest.Error()))
			})
		})
		When("The Shadow has some valid containers and init containers", func() {
			BeforeEach(func() {
				containers = []containerResource{{cpu: 100, memory: 100}, {cpu: 200, memory: 100}}
				initContainers = []containerResource{{cpu: 100, memory: 300}}
				shadowPod = forgeShadowPodWithResourceRequests(containers, initContainers)
			})
			It("should return a ResourceList which is the Max between the sum of all containers resources and the Max of initContainers", func() {
				// running = (100+200) cpu, (100+100) memory = 300 cpu, 200 memory
				// init peak = 100 cpu, 300 memory
				// max = 300 cpu, 300 memory
				Expect(quota).To(Equal(forgeResourceList(300, 300)))
				Expect(err).To(BeNil())
			})
		})
	})

	Describe("Compute the effective requests of a ShadowPod", func() {
		var (
			got *corev1.ResourceList
		)

		compute := func(sp *offloadingv1beta1.ShadowPod, quotaResources corev1.ResourceList,
			mode offloadingv1beta1.LimitsEnforcement) {
			got, err = getQuotaFromShadowPod(sp, quotaResources, mode)
		}

		When("there are only regular containers", func() {
			It("sums their requests: 1+2 cpu, 1Gi+2Gi memory = 3 cpu, 3Gi", func() {
				sp := forgeShadowPodWithContainers([]forgedContainer{
					{name: "c1", requests: rl("cpu=1", "memory=1Gi")},
					{name: "c2", requests: rl("cpu=2", "memory=2Gi")},
				}, nil, nil)
				compute(sp, rl("cpu=100", "memory=100Gi"), offloadingv1beta1.NoLimitsEnforcement)
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=3", "memory=3Gi"))
			})
		})

		When("there are several regular init containers", func() {
			It("uses the per-resource init peak, not the sum: max(4cpu/1Gi, 1cpu/8Gi) = 4 cpu, 8Gi", func() {
				sp := forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", requests: rl("cpu=1", "memory=1Gi")}},
					[]forgedContainer{
						{name: "i1", requests: rl("cpu=4", "memory=1Gi")},
						{name: "i2", requests: rl("cpu=1", "memory=8Gi")},
					}, nil)
				compute(sp, rl("cpu=100", "memory=100Gi"), offloadingv1beta1.NoLimitsEnforcement)
				// running = 1 cpu, 1Gi; init peak = max((4,1Gi),(1,8Gi)) = (4,8Gi); max = 4 cpu, 8Gi.
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=4", "memory=8Gi"))
			})
		})

		When("there is a resident init sidecar (restartPolicy: Always)", func() {
			It("adds the sidecar to the running total: 1+1 cpu, 1Gi+1Gi = 2 cpu, 2Gi", func() {
				sp := forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", requests: rl("cpu=1", "memory=1Gi")}},
					[]forgedContainer{sidecar("sidecar", rl("cpu=1", "memory=1Gi"), nil)},
					nil)
				compute(sp, rl("cpu=100", "memory=100Gi"), offloadingv1beta1.NoLimitsEnforcement)
				// running = regular(1,1Gi) + sidecar(1,1Gi) = (2,2Gi); init peak = (1,1Gi); max = (2,2Gi).
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=2", "memory=2Gi"))
			})
		})

		When("a regular init container follows a resident sidecar", func() {
			It("includes the already started sidecar in the init peak: max(2cpu/2Gi running, 3cpu/1.5Gi peak) = 3 cpu, 2Gi", func() {
				sp := forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", requests: rl("cpu=1", "memory=1Gi")}},
					[]forgedContainer{
						sidecar("sidecar", rl("cpu=1", "memory=1Gi"), nil),
						{name: "i2", requests: rl("cpu=2", "memory=500Mi")},
					}, nil)
				compute(sp, rl("cpu=100", "memory=100Gi"), offloadingv1beta1.NoLimitsEnforcement)
				// running = (1,1Gi) + sidecar (1,1Gi) = (2,2Gi)
				// init peak = max(sidecar (1,1Gi), sidecar+i2 (1+2, 1Gi+500Mi)) = (3, 1.5Gi)
				// result = max((2,2Gi),(3,1.5Gi)) = (3, 2Gi)
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=3", "memory=2Gi"))
			})
		})

		When("the ShadowPod declares an overhead", func() {
			It("adds the overhead to the effective requests: 1+1 cpu, 1Gi+1Gi = 2 cpu, 2Gi", func() {
				sp := forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", requests: rl("cpu=1", "memory=1Gi")}},
					nil, rl("cpu=1", "memory=1Gi"))
				compute(sp, rl("cpu=100", "memory=100Gi"), offloadingv1beta1.NoLimitsEnforcement)
				// containers = (1,1Gi); overhead = (1,1Gi); total = (2,2Gi).
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=2", "memory=2Gi"))
			})

			It("does not take part in the Hard requests==limits check: container 1==1 passes, total = 1+1 = 2 cpu", func() {
				sp := forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", requests: rl("cpu=1"), limits: rl("cpu=1")}},
					nil, rl("cpu=1"))
				compute(sp, rl("cpu=100"), offloadingv1beta1.HardLimitsEnforcement)
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=2"))
			})
		})

		When("the container declares only limits (not yet defaulted)", func() {
			It("defaults the requests to the limits, per resource: 1 cpu, 1Gi", func() {
				sp := forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", limits: rl("cpu=1", "memory=1Gi")}}, nil, nil)
				compute(sp, rl("cpu=100", "memory=100Gi"), offloadingv1beta1.SoftLimitsEnforcement)
				// Like the API server on Pod admission: request = limit for each resource without a request.
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=1", "memory=1Gi"))
			})

			It("satisfies Hard enforcement, since the effective requests equal the limits", func() {
				sp := forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", limits: rl("cpu=1", "memory=1Gi")}}, nil, nil)
				compute(sp, rl("cpu=100", "memory=100Gi"), offloadingv1beta1.HardLimitsEnforcement)
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=1", "memory=1Gi"))
			})
		})

		When("the requests were already defaulted by the API server", func() {
			It("produces the same quota as the not-yet-defaulted input: 1 cpu, 1Gi", func() {
				sp := forgeShadowPodWithContainers([]forgedContainer{{
					name:     "c1",
					requests: rl("cpu=1", "memory=1Gi"),
					limits:   rl("cpu=1", "memory=1Gi"),
				}}, nil, nil)
				compute(sp, rl("cpu=100", "memory=100Gi"), offloadingv1beta1.SoftLimitsEnforcement)
				// SetDefaults_Pod copies each limit into the missing requests, so the already-defaulted
				// object (requests == limits) yields the same quota as the limits-only object above.
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=1", "memory=1Gi"))
			})
		})

		When("a container declares an extended resource not listed in the quota", func() {
			It("does not account nor restrict it: only cpu and memory are returned", func() {
				sp := forgeShadowPodWithContainers([]forgedContainer{{
					name:     "c1",
					requests: rl("cpu=1", "memory=1Gi", "nvidia.com/gpu=4"),
				}}, nil, nil)
				compute(sp, rl("cpu=100", "memory=100Gi"), offloadingv1beta1.SoftLimitsEnforcement)
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=1", "memory=1Gi"))
			})
		})

		When("the quota lists an extended resource", func() {
			It("accounts and restricts it: 2 gpu", func() {
				sp := forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", requests: rl("cpu=1", "memory=1Gi", "nvidia.com/gpu=2")}},
					[]forgedContainer{{name: "i1", requests: rl("cpu=1", "memory=1Gi", "nvidia.com/gpu=1")}}, nil)
				compute(sp, rl("cpu=100", "memory=100Gi", "nvidia.com/gpu=10"), offloadingv1beta1.SoftLimitsEnforcement)
				// running = (1,1Gi,2 gpu); init peak = (1,1Gi,1 gpu); result = 2 gpu.
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=1", "memory=1Gi", "nvidia.com/gpu=2"))
			})
		})

		When("the quota lists ephemeral-storage", func() {
			It("accounts it: 1Gi + 2Gi = 3Gi", func() {
				sp := forgeShadowPodWithContainers([]forgedContainer{
					{name: "c1", requests: rl("ephemeral-storage=1Gi")},
					{name: "c2", requests: rl("ephemeral-storage=2Gi")},
				}, nil, nil)
				compute(sp, rl("ephemeral-storage=100Gi"), offloadingv1beta1.NoLimitsEnforcement)
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("ephemeral-storage=3Gi"))
			})
		})

		When("the quota does not list memory and a container omits it", func() {
			It("does not require it: only cpu is accounted", func() {
				sp := forgeShadowPodWithContainers([]forgedContainer{{name: "c1", requests: rl("cpu=1")}}, nil, nil)
				compute(sp, rl("cpu=100"), offloadingv1beta1.SoftLimitsEnforcement)
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=1"))
			})
		})

		When("the limits enforcement is unknown", func() {
			It("returns a diagnostic naming the mode", func() {
				sp := forgeShadowPodWithContainers([]forgedContainer{{name: "c1", requests: rl("cpu=1")}}, nil, nil)
				compute(sp, rl("cpu=100"), offloadingv1beta1.LimitsEnforcement("Bogus"))
				Expect(got).To(BeNil())
				Expect(err).To(MatchError(ContainSubstring(`unknown limits enforcement mode "Bogus"`)))
			})
		})

		When("the limits enforcement mode is lowercase, as produced by the controller-manager flag", func() {
			It("is normalized: 'none' behaves like None, accounting the requests 3 cpu", func() {
				sp := forgeShadowPodWithContainers([]forgedContainer{{
					name: "c1", requests: rl("cpu=3"), limits: rl("cpu=2"),
				}}, nil, nil)
				compute(sp, rl("cpu=100"), offloadingv1beta1.LimitsEnforcement("none"))
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=3"))
			})
		})
	})

	Describe("Limits enforcement", func() {
		var (
			got  *corev1.ResourceList
			mode offloadingv1beta1.LimitsEnforcement
		)

		JustBeforeEach(func() {
			got, err = getQuotaFromShadowPod(shadowPod, rl("cpu=100", "memory=100Gi"), mode)
		})

		When("Hard and a regular init sidecar has requests != limits", func() {
			BeforeEach(func() {
				mode = offloadingv1beta1.HardLimitsEnforcement
				shadowPod = forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", requests: rl("cpu=1", "memory=1Gi"), limits: rl("cpu=1", "memory=1Gi")}},
					[]forgedContainer{sidecar("sidecar", rl("cpu=1", "memory=1Gi"), rl("cpu=2", "memory=1Gi"))}, nil)
			})
			It("denies, identifying the sidecar container and the resource", func() {
				Expect(got).To(BeNil())
				Expect(err).To(MatchError(ContainSubstring(`initContainer "sidecar": resource "cpu"`)))
				Expect(err).To(MatchError(ContainSubstring("Hard limits enforcement")))
			})
		})

		When("Hard and a regular container has requests == limits", func() {
			BeforeEach(func() {
				mode = offloadingv1beta1.HardLimitsEnforcement
				shadowPod = forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", requests: rl("cpu=2", "memory=2Gi"), limits: rl("cpu=2", "memory=2Gi")}},
					nil, nil)
			})
			It("admits with the effective requests: 2 cpu, 2Gi", func() {
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=2", "memory=2Gi"))
			})
		})

		When("Hard and a container requests a tracked resource without declaring the limit", func() {
			BeforeEach(func() {
				mode = offloadingv1beta1.HardLimitsEnforcement
				shadowPod = forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", requests: rl("cpu=1", "memory=1Gi"), limits: rl("memory=1Gi")}},
					nil, nil)
			})
			It("denies, identifying the container and the resource", func() {
				Expect(got).To(BeNil())
				Expect(err).To(MatchError(ContainSubstring(`container "c1": resource "cpu"`)))
				Expect(err).To(MatchError(ContainSubstring("without limits")))
			})
		})

		When("Soft and requests are lower than limits", func() {
			BeforeEach(func() {
				mode = offloadingv1beta1.SoftLimitsEnforcement
				shadowPod = forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", requests: rl("cpu=1", "memory=1Gi"), limits: rl("cpu=2", "memory=2Gi")}},
					nil, nil)
			})
			It("admits, accounting the requests: 1 cpu, 1Gi", func() {
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=1", "memory=1Gi"))
			})
		})

		When("Soft and requests exceed limits", func() {
			BeforeEach(func() {
				mode = offloadingv1beta1.SoftLimitsEnforcement
				shadowPod = forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", requests: rl("cpu=3", "memory=1Gi"), limits: rl("cpu=2", "memory=1Gi")}},
					nil, nil)
			})
			It("denies, identifying the container and the resource", func() {
				Expect(got).To(BeNil())
				Expect(err).To(MatchError(ContainSubstring(`container "c1": resource "cpu"`)))
				Expect(err).To(MatchError(ContainSubstring("exceed limits")))
			})
		})

		When("None and requests exceed limits", func() {
			BeforeEach(func() {
				mode = offloadingv1beta1.NoLimitsEnforcement
				shadowPod = forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", requests: rl("cpu=3", "memory=1Gi"), limits: rl("cpu=2", "memory=1Gi")}},
					nil, nil)
			})
			It("admits without enforcing the limit, accounting the requests: 3 cpu, 1Gi", func() {
				Expect(err).ToNot(HaveOccurred())
				expectResourceList(*got, rl("cpu=3", "memory=1Gi"))
			})
		})

		When("Soft and a regular container omits a tracked CPU or memory request without a limit to default from", func() {
			BeforeEach(func() {
				mode = offloadingv1beta1.SoftLimitsEnforcement
				shadowPod = forgeShadowPodWithContainers(
					[]forgedContainer{{name: "c1", requests: rl("memory=1Gi")}}, nil, nil)
			})
			It("denies, identifying the container and the missing resource", func() {
				Expect(got).To(BeNil())
				Expect(err).To(MatchError(ContainSubstring(`container "c1": request for resource "cpu" is not set`)))
			})
		})

		When("the ShadowPod declares pod-level resources", func() {
			BeforeEach(func() {
				mode = offloadingv1beta1.NoLimitsEnforcement
				shadowPod = forgeShadowPodWithContainers([]forgedContainer{{name: "c1", requests: rl("cpu=1")}}, nil, nil)
				shadowPod.Spec.Pod.Resources = &corev1.ResourceRequirements{
					Requests: rl("cpu=10", "memory=10Gi"),
				}
			})
			It("rejects them explicitly, instead of silently ignoring them", func() {
				Expect(got).To(BeNil())
				Expect(err).To(MatchError(ContainSubstring("pod-level resources")))
				Expect(err).To(MatchError(ContainSubstring("spec.resources")))
			})
		})

		When("a container declares a negative quantity for a tracked resource", func() {
			BeforeEach(func() {
				mode = offloadingv1beta1.NoLimitsEnforcement
				shadowPod = forgeShadowPodWithContainers([]forgedContainer{{
					name: "c1", requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("-1")},
				}}, nil, nil)
			})
			It("denies, identifying the container and the resource", func() {
				Expect(got).To(BeNil())
				Expect(err).To(MatchError(ContainSubstring(`container "c1"`)))
				Expect(err).To(MatchError(ContainSubstring(`resource "cpu"`)))
				Expect(err).To(MatchError(ContainSubstring("must not be negative")))
			})
		})
	})

	Describe("Cache rebuild with the new accounting", func() {
		var (
			quotaObject  *offloadingv1beta1.Quota
			newValidator *Validator
			cacheClient  client.Client
			rebuilt      *peeringInfo
		)

		BeforeEach(func() {
			quotaObject = &offloadingv1beta1.Quota{
				ObjectMeta: metav1.ObjectMeta{Name: userName, Namespace: tenantNamespace},
				Spec: offloadingv1beta1.QuotaSpec{
					User:              userName,
					LimitsEnforcement: offloadingv1beta1.NoLimitsEnforcement,
					Resources:         rl("cpu=10", "memory=10Gi"),
				},
			}

			// A regular container + a resident sidecar => 2+1 = 3 cpu, 2Gi+1Gi = 3Gi.
			running := forgeShadowPodWithContainers(
				[]forgedContainer{{name: "c1", requests: rl("cpu=2", "memory=2Gi")}},
				[]forgedContainer{sidecar("sidecar", rl("cpu=1", "memory=1Gi"), nil)},
				rl("cpu=1"))
			running.Name = "running-1"
			running.UID = testShadowPodUID
			// 2+1+1 overhead = 4 cpu, 3Gi.

			// A ShadowPod that would violate Hard requests==limits: it must still be accounted,
			// since the cache rebuild must not evict nor reject already running objects.
			nonCompliant := forgeShadowPodWithContainers(
				[]forgedContainer{{name: "c1", requests: rl("cpu=1", "memory=1Gi"), limits: rl("cpu=2", "memory=2Gi")}},
				nil, nil)
			nonCompliant.Name = "non-compliant"
			nonCompliant.UID = testShadowPodUID2

			// A ShadowPod carrying pod-level resources: not supported for accounting, but it must not
			// be dropped from the cache.
			withPodLevel := forgeShadowPodWithContainers(
				[]forgedContainer{{name: "c1", requests: rl("cpu=1")}}, nil, nil)
			withPodLevel.Name = "pod-level"
			withPodLevel.UID = testShadowPodUID3
			withPodLevel.Spec.Pod.Resources = &corev1.ResourceRequirements{Requests: rl("cpu=10")}

			cacheClient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(
				quotaObject, running, nonCompliant, withPodLevel).Build()

			newValidator = webhook.Admission{Handler: NewValidator(cacheClient, true)}.Handler.(*Validator)
		})

		JustBeforeEach(func() {
			Expect(newValidator.initializeCache(ctx)).To(Succeed())
			pi, found := newValidator.PeeringCache.getPeeringInfo(userName)
			Expect(found).To(BeTrue())
			rebuilt = pi
		})

		It("keeps all the running ShadowPods and accounts them with the effective requests", func() {
			Expect(rebuilt.shadowPods).To(HaveLen(3))
			Expect(rebuilt.shadowPods[testNamespace+"/running-1"]).ToNot(BeNil())
			Expect(rebuilt.shadowPods[testNamespace+"/non-compliant"]).ToNot(BeNil())
			Expect(rebuilt.shadowPods[testNamespace+"/pod-level"]).ToNot(BeNil())

			// running-1: max(regular+sidecar = (3,3Gi), sidecar peak (1,1Gi)) + overhead (1,0)
			//            = (3,3Gi) + (1,0) = 4 cpu, 3Gi
			// non-compliant: requests (1,1Gi) (the mutation of the input is irrelevant, requests win)
			// pod-level: (1,0), pod-level resources ignored
			// total = (4+1+1) cpu, (3Gi+1Gi) = 6 cpu, 4Gi
			expectResourceList(rebuilt.usedQuota, rl("cpu=6", "memory=4Gi"))
		})

		It("does not evict the running ShadowPods on a cache refresh", func() {
			_, err := newValidator.refreshCache(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(rebuilt.shadowPods).To(HaveLen(3))
			expectResourceList(rebuilt.usedQuota, rl("cpu=6", "memory=4Gi"))
		})

		It("limits only the subsequent admissions when the rebuilt usage exceeds the quota", func() {
			// Shrink the quota so that the rebuilt usage (6 cpu) exceeds it.
			rebuilt.updateQuotas(rl("cpu=5", "memory=10Gi"))
			free := rebuilt.getFreeQuota()[corev1.ResourceCPU]
			// free = quota (5) - used (6) = -1 cpu
			Expect(free.Cmp(resource.MustParse("-1"))).To(Equal(0))

			// The already running ShadowPods are still accounted and present.
			Expect(rebuilt.shadowPods).To(HaveLen(3))

			// A new ShadowPod is denied, since it would push the usage further over the quota.
			incoming := forgeShadowPodWithContainers(
				[]forgedContainer{{name: "c1", requests: rl("cpu=1", "memory=1Gi")}}, nil, nil)
			incoming.Name = "incoming"
			err := rebuilt.testAndUpdateCreation(ctx, cacheClient, incoming, offloadingv1beta1.NoLimitsEnforcement, false)
			Expect(err).To(MatchError(ContainSubstring("quota usage exceeded")))
			Expect(rebuilt.shadowPods[testNamespace+"/incoming"]).To(BeNil())
		})

		It("recomputes the quota of a running ShadowPod when the accounting semantics change", func() {
			// Simulate a Description created with the previous accounting (sum of regular containers
			// only, ignoring the sidecar and the overhead): 2 cpu, 2Gi instead of the current 4 cpu, 3Gi.
			key := testNamespace + "/running-1"
			Expect(rebuilt.shadowPods[key]).ToNot(BeNil())
			rebuilt.subUsedResources(rebuilt.shadowPods[key].quota)
			rebuilt.shadowPods[key].quota = rl("cpu=2", "memory=2Gi")
			rebuilt.addUsedResources(rebuilt.shadowPods[key].quota)
			// used = previous total (6 cpu, 4Gi) - 4 cpu/3Gi + 2 cpu/2Gi = 4 cpu, 3Gi
			expectResourceList(rebuilt.usedQuota, rl("cpu=4", "memory=3Gi"))

			// A refresh recomputes the running ShadowPod quota with the new semantics, without evicting it.
			_, refreshErr := newValidator.refreshCache(ctx)
			Expect(refreshErr).ToNot(HaveOccurred())
			Expect(rebuilt.shadowPods).To(HaveLen(3))
			expectResourceList(rebuilt.usedQuota, rl("cpu=6", "memory=4Gi"))

			// The recomputed usage is visible, and only the subsequent admissions are affected.
			rebuilt.updateQuotas(rl("cpu=5", "memory=10Gi"))
			free := rebuilt.getFreeQuota()[corev1.ResourceCPU]
			Expect(free.Cmp(resource.MustParse("-1"))).To(Equal(0))
		})
	})
})
