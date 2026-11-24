package prepull

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const startupKey = "karpenter-prepull.goldsky.com/pending"

func testPod(name string, mutate func(*corev1.Pod), images ...string) corev1.Pod {
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "apps"}}
	pod.Spec.NodeName = "old"
	pod.Spec.Tolerations = []corev1.Toleration{{Key: "pool", Operator: corev1.TolerationOpEqual, Value: "pipelines"}}
	for _, image := range images {
		pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "c", Image: image})
	}
	if mutate != nil {
		mutate(&pod)
	}
	return pod
}

func targetNode() *corev1.Node {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "new",
		UID:    "new-uid",
		Labels: map[string]string{"pool": "pipelines", corev1.LabelOSStable: "linux"},
	}}
	node.Spec.Taints = []corev1.Taint{
		{Key: "pool", Value: "pipelines", Effect: corev1.TaintEffectNoSchedule},
		{Key: startupKey, Effect: corev1.TaintEffectNoSchedule},
		{Key: "node.kubernetes.io/not-ready", Effect: corev1.TaintEffectNoExecute},
	}
	return node
}

func TestPredictKeepsOnlyPodsThatCanMoveToTheNode(t *testing.T) {
	pods := []corev1.Pod{
		testPod("moves", nil, "app:1"),
		testPod("init", func(p *corev1.Pod) {
			p.Spec.InitContainers = []corev1.Container{{Name: "i", Image: "loader:1"}}
		}, "app:1"),
		testPod("daemon", func(p *corev1.Pod) {
			p.OwnerReferences = []metav1.OwnerReference{{Kind: "DaemonSet", Name: "agent"}}
		}, "agent:1"),
		testPod("done", func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded }, "job:1"),
		testPod("static", func(p *corev1.Pod) {
			p.Annotations = map[string]string{corev1.MirrorPodAnnotationKey: "x"}
		}, "static:1"),
		testPod("other-pool", func(p *corev1.Pod) {
			p.Spec.NodeSelector = map[string]string{"pool": "web"}
		}, "web:1"),
		testPod("no-toleration", func(p *corev1.Pod) { p.Spec.Tolerations = nil }, "plain:1"),
		testPod("private", func(p *corev1.Pod) {
			p.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "regcred"}}
		}, "private:1"),
	}

	got := Predict(targetNode(), pods, map[string]bool{startupKey: true}, 10)

	if want := []string{"app:1", "loader:1"}; !reflect.DeepEqual(got.Images, want) {
		t.Errorf("images = %v, want %v", got.Images, want)
	}
	if got.NeedSecrets != 1 {
		t.Errorf("NeedSecrets = %d, want 1", got.NeedSecrets)
	}
}

func TestPredictBlocksOnAForeignStartupTaintUnlessIgnored(t *testing.T) {
	node := targetNode()
	node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{Key: "cni.example.com/not-ready", Effect: corev1.TaintEffectNoSchedule})
	pods := []corev1.Pod{testPod("moves", nil, "app:1")}

	if got := Predict(node, pods, map[string]bool{startupKey: true}, 10); len(got.Images) != 0 {
		t.Errorf("images = %v, want none while the foreign taint is not ignored", got.Images)
	}
	ignore := map[string]bool{startupKey: true, "cni.example.com/not-ready": true}
	if got := Predict(node, pods, ignore, 10); len(got.Images) != 1 {
		t.Errorf("images = %v, want app:1 once the foreign taint is ignored", got.Images)
	}
}

func TestPredictSkipsPresentImagesAndKeepsTheMostUsed(t *testing.T) {
	node := targetNode()
	node.Status.Images = []corev1.ContainerImage{{Names: []string{"repo/cached@sha256:abc", "cached:1"}}}
	pods := []corev1.Pod{
		testPod("a", nil, "rare:1", "common:1", "cached:1"),
		testPod("b", nil, "common:1", "second:1"),
		testPod("c", nil, "common:1", "second:1"),
	}

	got := Predict(node, pods, map[string]bool{startupKey: true}, 2)

	if want := []string{"common:1", "second:1"}; !reflect.DeepEqual(got.Images, want) {
		t.Errorf("images = %v, want %v", got.Images, want)
	}
	if got.Truncated != 1 {
		t.Errorf("Truncated = %d, want 1", got.Truncated)
	}
}
