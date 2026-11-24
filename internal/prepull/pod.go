package prepull

import (
	"fmt"
	"hash/fnv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

const (
	// ManagedByLabel marks the pull pods this controller owns.
	ManagedByLabel = "app.kubernetes.io/managed-by"
	// ManagedByValue is the value of ManagedByLabel on pull pods.
	ManagedByValue = "karpenter-prepull"
	// SourceLabel holds the UID of the draining node a pull pod works for.
	SourceLabel = "karpenter-prepull.goldsky.com/source"

	noopVolume = "noop"
	noopDir    = "/karpenter-prepull"
	noopPath   = noopDir + "/noop"
	// The pull pod never runs as root, whatever user the image declares.
	nonRootID = 65532
)

// PodOptions configures the pull pod.
type PodOptions struct {
	Namespace string
	// NoopImage holds the /noop binary for every node architecture in use.
	NoopImage         string
	HostNetwork       bool
	PriorityClassName string
	// DeadlineSeconds bounds the time the pod may spend on the node.
	DeadlineSeconds int64
}

// PodName is the name of the pod that pulls, on node, the images of the pods
// of the draining node source.
func PodName(node, source *corev1.Node) string {
	h := fnv.New64a()
	h.Write([]byte(node.UID))
	h.Write([]byte(source.UID))
	return fmt.Sprintf("prepull-%x", h.Sum64())
}

// BuildPod returns a pod bound to node with one container per image. Each
// container runs the noop binary, so the pod succeeds once the kubelet has
// pulled every image.
func BuildPod(node, source *corev1.Node, images []string, opts PodOptions) *corev1.Pod {
	security := &corev1.SecurityContext{
		RunAsUser:                ptr.To[int64](nonRootID),
		RunAsGroup:               ptr.To[int64](nonRootID),
		RunAsNonRoot:             ptr.To(true),
		AllowPrivilegeEscalation: ptr.To(false),
		ReadOnlyRootFilesystem:   ptr.To(true),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}

	containers := make([]corev1.Container, len(images))
	for i, image := range images {
		containers[i] = corev1.Container{
			Name:            fmt.Sprintf("image-%d", i),
			Image:           image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         []string{noopPath},
			SecurityContext: security,
			VolumeMounts:    []corev1.VolumeMount{{Name: noopVolume, MountPath: noopDir, ReadOnly: true}},
		}
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: opts.Namespace,
			Name:      PodName(node, source),
			Labels:    map[string]string{ManagedByLabel: ManagedByValue, SourceLabel: string(source.UID)},
			// The pod goes away with the drained node, when its result is no longer needed.
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       source.Name,
				UID:        source.UID,
			}},
		},
		Spec: corev1.PodSpec{
			NodeName:                      node.Name,
			RestartPolicy:                 corev1.RestartPolicyNever,
			HostNetwork:                   opts.HostNetwork,
			PriorityClassName:             opts.PriorityClassName,
			ActiveDeadlineSeconds:         ptr.To(opts.DeadlineSeconds),
			TerminationGracePeriodSeconds: ptr.To[int64](0),
			AutomountServiceAccountToken:  ptr.To(false),
			EnableServiceLinks:            ptr.To(false),
			Tolerations:                   []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			SecurityContext: &corev1.PodSecurityContext{
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Volumes: []corev1.Volume{{
				Name:         noopVolume,
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}},
			InitContainers: []corev1.Container{{
				Name:            "install",
				Image:           opts.NoopImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"/noop", "install", noopPath},
				SecurityContext: security,
				VolumeMounts:    []corev1.VolumeMount{{Name: noopVolume, MountPath: noopDir}},
			}},
			Containers: containers,
		},
	}
}
