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
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	resourcehelpers "k8s.io/component-helpers/resource"
	klog "k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	offloadingv1beta1 "github.com/liqotech/liqo/apis/offloading/v1beta1"
)

// Description is a struct that contains the main informations about a shadow pod.
type Description struct {
	namespacedName    types.NamespacedName
	uid               types.UID
	quota             corev1.ResourceList
	running           bool
	creationTimestamp time.Time
}

func createShadowPodDescription(name, namespace string, uid types.UID, resources corev1.ResourceList) *Description {
	return &Description{
		namespacedName:    types.NamespacedName{Name: name, Namespace: namespace},
		uid:               uid,
		quota:             resources,
		running:           true,
		creationTimestamp: time.Now(),
	}
}

func (pi *peeringInfo) getOrCreateShadowPodDescription(ctx context.Context, c client.Client,
	sp *offloadingv1beta1.ShadowPod, limitsEnforcement offloadingv1beta1.LimitsEnforcement) (*Description, error) {
	nsname := types.NamespacedName{Name: sp.Name, Namespace: sp.Namespace}
	spQuota, err := getQuotaFromShadowPod(sp, pi.totalQuota, limitsEnforcement)
	if err != nil {
		return nil, err
	}
	klog.V(4).Infof("ShadowPod %s quota %s", nsname.String(), quotaFormatter(*spQuota))
	spd, found := pi.shadowPods[nsname.String()]
	if found {
		if spd.running {
			return nil, fmt.Errorf("ShadowPod %s is already running", sp.GetName())
		}
		err := checkShadowPodExistence(ctx, c, nsname)
		if err != nil {
			return nil, err
		}
		// Removing the old ShadowPodDescription from the cache and creating a new one
		// Cache refreshing has not deleted it from cache
		pi.removeShadowPod(spd)
	}
	return createShadowPodDescription(sp.GetName(), sp.GetNamespace(), sp.GetUID(), *spQuota), nil
}

func (pi *peeringInfo) getShadowPodDescription(sp *offloadingv1beta1.ShadowPod) (*Description, error) {
	nsname := types.NamespacedName{Name: sp.Name, Namespace: sp.Namespace}
	spd, found := pi.shadowPods[nsname.String()]
	if !found {
		// If the shadow pod is not found, it means that it is already terminated and removed from the cache
		// A new deleting request should never be received for the same shadow pod
		// Anyway, in this case, an error is returned to anticipate the webhook response and avoid the cache quota usage update
		return nil, fmt.Errorf("ShadowPod %s not found (Maybe Cache problem)", sp.GetName())
	} else if spd.uid != sp.GetUID() {
		return nil, fmt.Errorf("ShadowPod %s: UID mismatch", sp.GetName())
	}
	return spd, nil
}

func (spd *Description) terminate() {
	spd.running = false
}

func checkShadowPodExistence(ctx context.Context, spvclient client.Client, namespacedName types.NamespacedName) error {
	sp := &offloadingv1beta1.ShadowPod{}
	err := spvclient.Get(ctx, namespacedName, sp)
	if err != nil {
		return client.IgnoreNotFound(err)
	}
	return fmt.Errorf("ShadowPod still exists in the system")
}

// containerGroup identifies the kind of container in diagnostics.
type containerGroup string

const (
	regularContainers containerGroup = "container"
	initContainers    containerGroup = "initContainer"
)

// getQuotaFromShadowPod computes the resources accounted against the user quota for a ShadowPod
// admitted through the webhook, enforcing the configured limits mode.
//
// It returns an error when the ShadowPod cannot be accounted consistently with the effective requests
// Kubernetes computes for the derived Pod, or when it violates the limits enforcement mode. The error
// always identifies the offending container and resource.
func getQuotaFromShadowPod(shadowpod *offloadingv1beta1.ShadowPod, quotaResources corev1.ResourceList,
	limitsEnforcement offloadingv1beta1.LimitsEnforcement) (*corev1.ResourceList, error) {
	// At least one container is required
	if shadowpod.Spec.Pod.Containers == nil {
		return nil, fmt.Errorf("ShadowPod %s has no containers defined", shadowpod.GetName())
	}

	if err := validatePodLevelResources(shadowpod); err != nil {
		return nil, err
	}

	// Work on a deep copy: the defaulting must not alter the object received by the webhook.
	pod := &corev1.Pod{Spec: *shadowpod.Spec.Pod.DeepCopy()}
	defaultContainerRequests(pod.Spec.Containers)
	defaultContainerRequests(pod.Spec.InitContainers)

	if err := validateShadowPodQuantities(shadowpod.GetName(), &pod.Spec, quotaResources); err != nil {
		return nil, err
	}
	if err := validateShadowPodLimits(shadowpod.GetName(), &pod.Spec, quotaResources, limitsEnforcement); err != nil {
		return nil, err
	}

	return computeShadowPodQuota(&pod.Spec, quotaResources), nil
}

// computeShadowPodQuota computes the effective requests of a ShadowPod for the resources listed in
// quotaResources, without applying any policy.
//
// It is used on admission (after validation) and by the cache refresher: the latter must keep
// accounting the already running ShadowPods even if they no longer satisfy the current limits
// enforcement mode, so recomputing the cache never evicts them nor rejects their deletion.
//
// For every tracked resource, the result is the effective requests of the Pod, as computed by
// Kubernetes for scheduling and admission (see k8s.io/component-helpers/resource.PodRequests):
//
//   - running requests = sum(requests of the regular containers) + sum(requests of the restartable
//     init containers, i.e. init containers with restartPolicy: Always);
//   - init peak = max over the init containers i of (sum(requests of restartable init containers
//     before i) + requests of i);
//   - container requests = max(running requests, init peak);
//   - effective requests = container requests + spec.overhead.
//
// Resources not listed in quotaResources are neither accounted nor restricted, and overhead is not a
// container, so it takes part in the effective requests but never in the limits/requests checks.
func computeShadowPodQuota(podSpec *corev1.PodSpec, quotaResources corev1.ResourceList) *corev1.ResourceList {
	// Work on a deep copy: defaulting must not alter the stored object.
	pod := &corev1.Pod{Spec: *podSpec.DeepCopy()}
	defaultContainerRequests(pod.Spec.Containers)
	defaultContainerRequests(pod.Spec.InitContainers)

	// Pod-level resources (spec.resources) are not propagated by Liqo when forging the Pod derived
	// from the ShadowPod, hence they must not be accounted here. On admission such ShadowPods are
	// rejected (see validatePodLevelResources); on cache rebuild they are kept but without counting
	// those values, which the provider would never see.
	pod.Spec.Resources = nil

	// Reuse the upstream Kubernetes implementation, so that the accounting matches the effective
	// requests computed for the real Pod created from this ShadowPod.
	podRequests := resourcehelpers.PodRequests(pod, resourcehelpers.PodResourcesOptions{})
	result := corev1.ResourceList{}
	for name, quantity := range podRequests {
		if _, tracked := quotaResources[name]; !tracked {
			// Resources not listed in the Quota are not accounted and not restricted.
			klog.V(5).Infof("ShadowPod quota: resource %q not listed in the quota, skipping it", name)
			continue
		}
		result[name] = quantity.DeepCopy()
	}
	return &result
}

// defaultContainerRequests emulates the API-server defaulting of container requests:
// for each resource with a limit and no request, the request is set to the limit.
// It matches the behavior of the Pod admission on the cluster hosting the Pod.
func defaultContainerRequests(containers []corev1.Container) {
	for i := range containers {
		if len(containers[i].Resources.Limits) == 0 {
			continue
		}
		if containers[i].Resources.Requests == nil {
			containers[i].Resources.Requests = corev1.ResourceList{}
		}
		for name, limit := range containers[i].Resources.Limits {
			if _, found := containers[i].Resources.Requests[name]; !found {
				containers[i].Resources.Requests[name] = limit.DeepCopy()
			}
		}
	}
}

// validatePodLevelResources rejects pod-level resources (spec.resources). That field is gated by the
// alpha PodLevelResources feature, whose Kubernetes support covers only cpu, memory and hugepages-*,
// and Liqo neither enables nor propagates it when forging the Pod derived from a ShadowPod. Hence the
// pod-level requests/limits would never be enforced on the provider: rather than silently ignoring
// them and claiming the accounting matches the effective requests, the ShadowPod is rejected with an
// explicit diagnostic. Names the feature does not support are rejected by the API server itself.
func validatePodLevelResources(shadowpod *offloadingv1beta1.ShadowPod) error {
	podResources := shadowpod.Spec.Pod.Resources
	if podResources == nil {
		return nil
	}
	names := map[string]struct{}{}
	for _, list := range []corev1.ResourceList{podResources.Requests, podResources.Limits} {
		for name := range list {
			if resourcehelpers.IsSupportedPodLevelResource(name) {
				names[string(name)] = struct{}{}
			}
		}
	}
	if len(names) == 0 {
		return nil
	}
	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	slices.Sort(sorted)
	return fmt.Errorf("ShadowPod %s: pod-level resources %v in spec.resources are not supported by the "+
		"ShadowPod quota accounting (alpha PodLevelResources feature, not propagated by Liqo)",
		shadowpod.GetName(), sorted)
}

// validateShadowPodQuantities checks that the quantities of the resources tracked by the quota are
// valid (i.e. not negative). The check is performed regardless of the limits enforcement mode, since
// an invalid quantity would corrupt the quota accounting.
func validateShadowPodQuantities(shadowpodName string, podSpec *corev1.PodSpec, quotaResources corev1.ResourceList) error {
	groups := []struct {
		kind       containerGroup
		containers []corev1.Container
	}{
		{regularContainers, podSpec.Containers},
		{initContainers, podSpec.InitContainers},
	}
	for _, group := range groups {
		for i := range group.containers {
			c := &group.containers[i]
			for _, list := range []corev1.ResourceList{c.Resources.Requests, c.Resources.Limits} {
				// Iterate deterministically, to produce a stable diagnostic.
				for _, name := range trackedResourceNames(list, nil, quotaResources) {
					quantity := list[name]
					if quantity.Sign() < 0 {
						return fmt.Errorf("ShadowPod %s: %s %q: invalid quantity %q for resource %q: "+
							"it must not be negative", shadowpodName, group.kind, c.Name, quantity.String(), name)
					}
				}
			}
		}
	}
	for _, name := range trackedResourceNames(podSpec.Overhead, nil, quotaResources) {
		quantity := podSpec.Overhead[name]
		if quantity.Sign() < 0 {
			return fmt.Errorf("ShadowPod %s: pod overhead: invalid quantity %q for resource %q: "+
				"it must not be negative", shadowpodName, quantity.String(), name)
		}
	}
	return nil
}

// validateShadowPodLimits applies the limits enforcement mode to the containers of the ShadowPod,
// based on the effective requests and limits (i.e. after the API-server defaulting of requests from
// limits), which are the values the kubelet will enforce on the provider:
//   - Hard: requests must be equal to limits for every tracked resource;
//   - Soft: requests must not exceed limits for every tracked resource;
//   - None: no check is performed.
//
// Sidecar containers (restartable init containers) follow the very same rules as regular and init
// containers. Under Soft and Hard, every regular container and every init container must have a
// request for the CPU and memory resources when those resources are tracked by the quota; a container
// declaring only the limit is accepted, since Kubernetes defaults its requests to the limits and the
// resulting effective requests are therefore strict.
func validateShadowPodLimits(shadowpodName string, podSpec *corev1.PodSpec,
	quotaResources corev1.ResourceList, limitsEnforcement offloadingv1beta1.LimitsEnforcement) error {
	mode := normalizeLimitsEnforcement(limitsEnforcement)
	switch mode {
	case offloadingv1beta1.NoLimitsEnforcement:
		return nil
	case offloadingv1beta1.SoftLimitsEnforcement, offloadingv1beta1.HardLimitsEnforcement:
	default:
		return fmt.Errorf("ShadowPod %s: unknown limits enforcement mode %q", shadowpodName, limitsEnforcement)
	}

	groups := []struct {
		kind       containerGroup
		containers []corev1.Container
	}{
		{regularContainers, podSpec.Containers},
		{initContainers, podSpec.InitContainers},
	}
	for _, group := range groups {
		for i := range group.containers {
			if err := validateContainerLimits(shadowpodName, group.kind, &group.containers[i],
				quotaResources, mode); err != nil {
				return err
			}
		}
	}
	return nil
}

// normalizeLimitsEnforcement maps the limits enforcement mode to its canonical value. The mode comes
// from the Quota field, which the CRD neither defaults nor normalizes (and the controller-manager flag
// accepts lowercase values), so both the empty value and a case-insensitive match are accepted.
func normalizeLimitsEnforcement(limitsEnforcement offloadingv1beta1.LimitsEnforcement) offloadingv1beta1.LimitsEnforcement {
	for _, mode := range []offloadingv1beta1.LimitsEnforcement{
		offloadingv1beta1.HardLimitsEnforcement,
		offloadingv1beta1.SoftLimitsEnforcement,
		offloadingv1beta1.NoLimitsEnforcement,
	} {
		if strings.EqualFold(string(limitsEnforcement), string(mode)) {
			return mode
		}
	}
	if limitsEnforcement == "" {
		// Zero value of the Quota field: the CRD does not default it. Use the documented default.
		return offloadingv1beta1.NoLimitsEnforcement
	}
	return ""
}

// validateContainerLimits checks a single container against the enforcement mode. It receives the
// container after the API-server defaulting of requests from limits, so the requests are the effective
// ones the kubelet will enforce.
func validateContainerLimits(shadowpodName string, kind containerGroup, c *corev1.Container,
	quotaResources corev1.ResourceList, limitsEnforcement offloadingv1beta1.LimitsEnforcement) error {
	// Requests are mandatory for the CPU and memory resources, when they are tracked by the quota.
	// A container declaring only the limits is compliant: Kubernetes defaults its requests to the
	// limits, and the effective requests are therefore set (see defaultContainerRequests).
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		if _, tracked := quotaResources[name]; !tracked {
			continue
		}
		if _, found := c.Resources.Requests[name]; !found {
			return fmt.Errorf("ShadowPod %s: %s %q: request for resource %q is not set",
				shadowpodName, kind, c.Name, name)
		}
	}

	// Compare requests and limits only for the resources tracked by the quota. Overhead is not part
	// of the container resources, hence it never takes part in these checks.
	for _, name := range trackedResourceNames(c.Resources.Requests, c.Resources.Limits, quotaResources) {
		request, requestFound := c.Resources.Requests[name]
		limit, limitFound := c.Resources.Limits[name]
		if !requestFound && !limitFound {
			// Neither request nor limit: nothing to enforce for this resource.
			continue
		}
		if !requestFound {
			// After defaulting this can only happen with a zero limit.
			request = *resource.NewQuantity(0, limit.Format)
		}
		if !limitFound {
			// No limit: only a zero request is compatible with Hard enforcement, since the limit is
			// implicitly zero.
			if limitsEnforcement == offloadingv1beta1.HardLimitsEnforcement && !request.IsZero() {
				return fmt.Errorf("ShadowPod %s: %s %q: resource %q: request (%s) without limits "+
					"under Hard limits enforcement", shadowpodName, kind, c.Name, name, request.String())
			}
			continue
		}
		if err := compareRequestAndLimit(shadowpodName, kind, c.Name, name, request, limit, limitsEnforcement); err != nil {
			return err
		}
	}
	return nil
}

// compareRequestAndLimit applies the Soft or Hard comparison between the effective requests and the
// limits of a single container for one tracked resource.
func compareRequestAndLimit(shadowpodName string, kind containerGroup, containerName string,
	name corev1.ResourceName, request, limit resource.Quantity,
	limitsEnforcement offloadingv1beta1.LimitsEnforcement) error {
	if limitsEnforcement == offloadingv1beta1.SoftLimitsEnforcement {
		if request.Cmp(limit) > 0 {
			return fmt.Errorf("ShadowPod %s: %s %q: resource %q: requests (%s) exceed limits (%s) "+
				"under Soft limits enforcement",
				shadowpodName, kind, containerName, name, request.String(), limit.String())
		}
		return nil
	}
	if request.Cmp(limit) != 0 {
		return fmt.Errorf("ShadowPod %s: %s %q: resource %q: requests (%s) differ from limits (%s) "+
			"under Hard limits enforcement",
			shadowpodName, kind, containerName, name, request.String(), limit.String())
	}
	return nil
}

// trackedResourceNames returns the sorted set of resource names appearing in the given lists and
// tracked by the quota, to iterate deterministically and produce stable diagnostics.
func trackedResourceNames(requests, limits, quotaResources corev1.ResourceList) []corev1.ResourceName {
	seen := map[corev1.ResourceName]struct{}{}
	for _, list := range []corev1.ResourceList{requests, limits} {
		for name := range list {
			if _, tracked := quotaResources[name]; tracked {
				seen[name] = struct{}{}
			}
		}
	}
	names := make([]corev1.ResourceName, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func quotaFormatter(quota corev1.ResourceList) string {
	// Print every resource tracked by the quota (including extended resources and ephemeral-storage),
	// sorted for stable logs, so that the accounting can be diagnosed.
	names := make([]string, 0, len(quota))
	for name := range quota {
		names = append(names, string(name))
	}
	slices.Sort(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		quantity := quota[corev1.ResourceName(name)]
		parts = append(parts, fmt.Sprintf("%s: %s", name, quantity.String()))
	}
	return fmt.Sprintf("[ %s ]", strings.Join(parts, ", "))
}
