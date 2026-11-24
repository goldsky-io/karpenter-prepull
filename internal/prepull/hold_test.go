package prepull

import (
	"context"
	"net/http"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const pullNamespace = "karpenter-prepull"

type harness struct {
	t      *testing.T
	client client.Client
	clock  *clocktesting.FakeClock
	r      *Reconciler
	hold   *EvictionHold
}

// newHarness builds a cluster in which Karpenter drains node "old" and node
// "new" has just registered. reason is the DisruptionReason condition of the
// NodeClaim of "old"; empty means Karpenter drains it without a replacement.
func newHarness(t *testing.T, reason string, pods ...corev1.Pod) *harness {
	t.Helper()
	now := time.Now().Truncate(time.Second)
	fakeClock := clocktesting.NewFakeClock(now)

	target := targetNode()
	target.CreationTimestamp = metav1.NewTime(now.Add(-time.Minute))

	source := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:              "old",
		UID:               "old-uid",
		Labels:            map[string]string{"pool": "pipelines"},
		CreationTimestamp: metav1.NewTime(now.Add(-24 * time.Hour)),
		OwnerReferences:   []metav1.OwnerReference{{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: "old-claim", UID: "c"}},
	}}
	source.Spec.Taints = []corev1.Taint{{Key: "karpenter.sh/disrupted", Effect: corev1.TaintEffectNoSchedule}}

	claim := &unstructured.Unstructured{}
	claim.SetGroupVersionKind(NodeClaimGVK)
	claim.SetName("old-claim")
	if reason != "" {
		conditions := []any{map[string]any{"type": "DisruptionReason", "status": "True", "reason": reason}}
		if err := unstructured.SetNestedSlice(claim.Object, conditions, "status", "conditions"); err != nil {
			t.Fatal(err)
		}
	}

	objects := []client.Object{target, source, claim}
	for i := range pods {
		objects = append(objects, &pods[i])
	}
	c := fake.NewClientBuilder().
		WithScheme(clientgoscheme.Scheme).
		WithObjects(objects...).
		WithStatusSubresource(&corev1.Pod{}).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		Build()
	planner := &Planner{Client: c, APIReader: c, Clock: fakeClock, Options: Options{
		DisruptionTaintKey: "karpenter.sh/disrupted",
		Timeout:            2 * time.Minute,
		MaxImages:          30,
		TargetMaxAge:       15 * time.Minute,
		IgnoreTaintKeys:    []string{startupKey},
		Pod:                PodOptions{Namespace: pullNamespace, NoopImage: "noop:1", DeadlineSeconds: 120},
	}}
	return &harness{t: t, client: c, clock: fakeClock,
		r:    &Reconciler{Planner: planner, Recorder: events.NewFakeRecorder(100)},
		hold: &EvictionHold{Planner: planner},
	}
}

func (h *harness) node(name string) *corev1.Node {
	h.t.Helper()
	node := &corev1.Node{}
	if err := h.client.Get(context.Background(), types.NamespacedName{Name: name}, node); err != nil {
		h.t.Fatal(err)
	}
	return node
}

func (h *harness) reconcile() {
	h.t.Helper()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "old"}}
	if _, err := h.r.Reconcile(context.Background(), req); err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
}

func (h *harness) pullPods() []corev1.Pod {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.client.List(context.Background(), pods, client.InNamespace(pullNamespace)); err != nil {
		h.t.Fatal(err)
	}
	return pods.Items
}

// startDrain marks "old" as deleted, which is how Karpenter starts a drain.
func (h *harness) startDrain() {
	h.t.Helper()
	node := h.node("old")
	node.Finalizers = []string{"karpenter.sh/termination"}
	if err := h.client.Update(context.Background(), node); err != nil {
		h.t.Fatal(err)
	}
	if err := h.client.Delete(context.Background(), node); err != nil {
		h.t.Fatal(err)
	}
	// The fake client stamps the deletion with the wall clock.
	h.clock.SetTime(h.node("old").DeletionTimestamp.Time)
}

func (h *harness) finishPulls(phase corev1.PodPhase) {
	h.t.Helper()
	for _, pod := range h.pullPods() {
		pod.Status.Phase = phase
		if err := h.client.Status().Update(context.Background(), &pod); err != nil {
			h.t.Fatal(err)
		}
	}
}

// evict returns the HTTP status the API server would answer for an eviction.
func (h *harness) evict(pod string) int32 {
	h.t.Helper()
	resp := h.hold.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Namespace:   "apps",
		Name:        pod,
		SubResource: "eviction",
	}})
	if resp.Allowed {
		return http.StatusOK
	}
	return resp.Result.Code
}

func TestPullStartsOnTheNewNodeForADriftedNode(t *testing.T) {
	h := newHarness(t, "Drifted", testPod("app", nil, "app:1", "sidecar:1"))

	h.reconcile()

	pods := h.pullPods()
	if len(pods) != 1 {
		t.Fatalf("pull pods = %d, want 1", len(pods))
	}
	if pods[0].Spec.NodeName != "new" {
		t.Errorf("pull pod is bound to %q, want the new node", pods[0].Spec.NodeName)
	}
	if got := len(pods[0].Spec.Containers); got != 2 {
		t.Errorf("pull pod has %d image containers, want 2", got)
	}
}

func TestNoPullWhenKarpenterDrainsWithoutAReplacement(t *testing.T) {
	// A spot interruption or an expiry: the node is tainted, with no DisruptionReason.
	h := newHarness(t, "", testPod("app", nil, "app:1"))

	h.reconcile()

	if pods := h.pullPods(); len(pods) != 0 {
		t.Errorf("pull pods = %d, want none", len(pods))
	}
}

func TestNodeThatAppearsDuringTheDrainIsNotATarget(t *testing.T) {
	h := newHarness(t, "Drifted", testPod("app", nil, "app:1"))
	h.startDrain()
	late := targetNode()
	late.Name, late.UID = "late", "late-uid"
	late.CreationTimestamp = metav1.NewTime(h.clock.Now().Add(30 * time.Second))
	if err := h.client.Create(context.Background(), late); err != nil {
		t.Fatal(err)
	}
	h.clock.Step(time.Minute)

	h.reconcile()

	for _, pod := range h.pullPods() {
		if pod.Spec.NodeName == "late" {
			t.Error("a pull pod runs on a node that appeared after the drain started")
		}
	}
	if got := h.evict("app"); got != http.StatusTooManyRequests {
		t.Errorf("eviction while the pull on the first new node runs = %d, want 429", got)
	}
	h.finishPulls(corev1.PodSucceeded)
	if got := h.evict("app"); got != http.StatusOK {
		t.Errorf("eviction after that pull = %d, want it allowed without a wait for the late node", got)
	}
}

func TestNoPullOnANodeThatIsNotNew(t *testing.T) {
	h := newHarness(t, "Drifted", testPod("app", nil, "app:1"))
	h.clock.Step(time.Hour)

	h.reconcile()

	if pods := h.pullPods(); len(pods) != 0 {
		t.Errorf("pull pods = %d, want none on a node older than the limit", len(pods))
	}
}

func TestPullPodsAreRemovedWhenKarpenterGivesUpTheDisruption(t *testing.T) {
	h := newHarness(t, "Drifted", testPod("app", nil, "app:1"))
	h.reconcile()

	node := h.node("old")
	node.Spec.Taints = nil
	if err := h.client.Update(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	h.reconcile()

	if pods := h.pullPods(); len(pods) != 0 {
		t.Errorf("pull pods = %d, want none after the taint is gone", len(pods))
	}
}

func TestEvictionIsHeldOnlyWhileThePullRuns(t *testing.T) {
	h := newHarness(t, "Drifted", testPod("app", nil, "app:1"))
	h.reconcile()

	if got := h.evict("app"); got != http.StatusOK {
		t.Errorf("eviction before the drain = %d, want it allowed", got)
	}

	h.startDrain()
	if got := h.evict("app"); got != http.StatusTooManyRequests {
		t.Errorf("eviction while the pull runs = %d, want 429", got)
	}

	h.finishPulls(corev1.PodSucceeded)
	if got := h.evict("app"); got != http.StatusOK {
		t.Errorf("eviction after the pull = %d, want it allowed", got)
	}
}

func TestEvictionIsAllowedWhenThePullFails(t *testing.T) {
	h := newHarness(t, "Drifted", testPod("app", nil, "app:1"))
	h.reconcile()
	h.startDrain()

	h.finishPulls(corev1.PodFailed)

	if got := h.evict("app"); got != http.StatusOK {
		t.Errorf("eviction after a failed pull = %d, want it allowed", got)
	}
}

func TestEvictionIsAllowedAfterTheTimeout(t *testing.T) {
	h := newHarness(t, "Drifted", testPod("app", nil, "app:1"))
	h.reconcile()
	h.startDrain()

	h.clock.Step(2 * time.Minute)

	if got := h.evict("app"); got != http.StatusOK {
		t.Errorf("eviction after the timeout = %d, want it allowed", got)
	}
}

func TestEvictionWaitsOnlyBrieflyForAPullPodThatDoesNotExist(t *testing.T) {
	// Karpenter can drain in the same second it taints, before the controller acts.
	h := newHarness(t, "Drifted", testPod("app", nil, "app:1"))
	h.startDrain()

	if got := h.evict("app"); got != http.StatusTooManyRequests {
		t.Errorf("eviction at the start of the drain = %d, want 429", got)
	}

	// The controller is down, or its pull pod was rejected.
	h.clock.Step(pullStartGrace)
	if got := h.evict("app"); got != http.StatusOK {
		t.Errorf("eviction with no pull pod after the grace = %d, want it allowed", got)
	}
}

func TestEvictionOfAPodThatNeedsNoPullIsNotHeld(t *testing.T) {
	other := testPod("other-pool", func(p *corev1.Pod) {
		p.Spec.NodeSelector = map[string]string{"pool": "web"}
	}, "web:1")
	h := newHarness(t, "Drifted", testPod("app", nil, "app:1"), other)
	h.reconcile()
	h.startDrain()

	if got := h.evict("other-pool"); got != http.StatusOK {
		t.Errorf("eviction of a pod that cannot move to the new node = %d, want it allowed", got)
	}
}

func TestEvictionIsNotHeldForAnImageThePullPodLeavesOut(t *testing.T) {
	h := newHarness(t, "Drifted", testPod("a", nil, "a:1"), testPod("b", nil, "a:1"), testPod("z", nil, "z:1"))
	h.r.Options.MaxImages = 1
	h.reconcile()
	h.startDrain()
	h.clock.Step(pullStartGrace)

	if got := h.evict("z"); got != http.StatusOK {
		t.Errorf("eviction of a pod whose image is not in the pull pod = %d, want it allowed", got)
	}
	if got := h.evict("a"); got != http.StatusTooManyRequests {
		t.Errorf("eviction of a pod whose image is in the pull pod = %d, want 429", got)
	}
}

func TestUnhealthyNodeIsNeitherWarmedForNorHeld(t *testing.T) {
	h := newHarness(t, "Unhealthy", testPod("app", nil, "app:1"))

	h.reconcile()
	h.startDrain()

	if pods := h.pullPods(); len(pods) != 0 {
		t.Errorf("pull pods = %d, want none for an unhealthy node", len(pods))
	}
	if got := h.evict("app"); got != http.StatusOK {
		t.Errorf("eviction from an unhealthy node = %d, want it allowed", got)
	}
}

func TestEvictionIsAllowedOnceThePodsOwnImagesArePulled(t *testing.T) {
	h := newHarness(t, "Drifted", testPod("a", nil, "a:1"), testPod("b", nil, "a:1"), testPod("z", nil, "z:1"))
	h.reconcile()
	h.startDrain()

	// The pull pod has fetched a:1 and still fetches z:1.
	pull := h.pullPods()[0]
	pull.Status.Phase = corev1.PodRunning
	for _, c := range pull.Spec.Containers {
		status := corev1.ContainerStatus{Name: c.Name}
		if c.Image == "a:1" {
			status.State.Terminated = &corev1.ContainerStateTerminated{}
		} else {
			status.State.Waiting = &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}
		}
		pull.Status.ContainerStatuses = append(pull.Status.ContainerStatuses, status)
	}
	if err := h.client.Status().Update(context.Background(), &pull); err != nil {
		t.Fatal(err)
	}

	if got := h.evict("a"); got != http.StatusOK {
		t.Errorf("eviction of a pod whose image is pulled = %d, want it allowed", got)
	}
	if got := h.evict("z"); got != http.StatusTooManyRequests {
		t.Errorf("eviction of a pod whose image still pulls = %d, want 429", got)
	}
}
