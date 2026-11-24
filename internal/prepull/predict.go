package prepull

import (
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
)

// Taints under these prefixes are set by the kubelet, the cloud provider or
// Karpenter while a node starts, and are gone by the time pods schedule.
var transientTaintPrefixes = []string{
	"node.kubernetes.io/",
	"node.cloudprovider.kubernetes.io/",
	"karpenter.sh/",
}

// Waiting reasons the kubelet reports when it cannot get an image.
var imagePullFailures = map[string]bool{
	"ErrImagePull":      true,
	"ImagePullBackOff":  true,
	"InvalidImageName":  true,
	"ErrImageNeverPull": true,
}

// Prediction is the set of images the pull pod fetches on a new node.
type Prediction struct {
	// Images are ordered by how many containers use them, most used first.
	Images []string
	// Truncated counts images dropped by the max-images limit.
	Truncated int
	// NeedSecrets counts pods skipped because they use imagePullSecrets,
	// which the pull pod cannot read from another namespace.
	NeedSecrets int
	// Unpullable counts images skipped because a pod on a disrupted node
	// already fails to pull them. Such a pull would hold the node until the
	// timeout for no gain.
	Unpullable int
}

// Predict returns the images of the pods that could move from the disrupted
// nodes to target and that target does not hold yet.
func Predict(target *corev1.Node, pods []corev1.Pod, ignoreTaintKeys map[string]bool, maxImages int) Prediction {
	var p Prediction
	present := map[string]bool{}
	for _, img := range target.Status.Images {
		for _, name := range img.Names {
			present[name] = true
		}
	}

	uses := map[string]int{}
	unpullable := map[string]bool{}
	for i := range pods {
		pod := &pods[i]
		if !movable(pod) || !fits(pod, target, ignoreTaintKeys) {
			continue
		}
		if len(pod.Spec.ImagePullSecrets) > 0 {
			p.NeedSecrets++
			continue
		}
		failing := map[string]bool{}
		for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
			for _, status := range statuses {
				if w := status.State.Waiting; w != nil && imagePullFailures[w.Reason] {
					failing[status.Name] = true
				}
			}
		}
		for _, containers := range [][]corev1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
			for _, c := range containers {
				uses[c.Image]++
				if failing[c.Name] {
					unpullable[c.Image] = true
				}
			}
		}
	}

	for image := range uses {
		switch {
		case image == "" || present[image]:
		case unpullable[image]:
			p.Unpullable++
		default:
			p.Images = append(p.Images, image)
		}
	}
	sort.Slice(p.Images, func(i, j int) bool {
		a, b := p.Images[i], p.Images[j]
		if uses[a] != uses[b] {
			return uses[a] > uses[b]
		}
		return a < b
	})
	if maxImages > 0 && len(p.Images) > maxImages {
		p.Truncated = len(p.Images) - maxImages
		p.Images = p.Images[:maxImages]
	}
	return p
}

// movable reports whether a drain evicts the pod so that it is created again
// on another node.
func movable(pod *corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return false
	}
	if _, mirror := pod.Annotations[corev1.MirrorPodAnnotationKey]; mirror {
		return false
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "DaemonSet" || owner.Kind == "Node" {
			return false
		}
	}
	return true
}

func fits(pod *corev1.Pod, node *corev1.Node, ignoreTaintKeys map[string]bool) bool {
	if ok, err := nodeaffinity.GetRequiredNodeAffinity(pod).Match(node); err != nil || !ok {
		return false
	}
	for _, taint := range node.Spec.Taints {
		if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		if ignoreTaintKeys[taint.Key] || transient(taint.Key) {
			continue
		}
		if !tolerated(pod.Spec.Tolerations, taint) {
			return false
		}
	}
	return true
}

func transient(key string) bool {
	for _, prefix := range transientTaintPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func tolerated(tolerations []corev1.Toleration, taint corev1.Taint) bool {
	for _, t := range tolerations {
		if t.Effect != "" && t.Effect != taint.Effect {
			continue
		}
		switch t.Operator {
		case corev1.TolerationOpExists:
			if t.Key == "" || t.Key == taint.Key {
				return true
			}
		case "", corev1.TolerationOpEqual:
			if t.Key == taint.Key && t.Value == taint.Value {
				return true
			}
		}
	}
	return false
}
