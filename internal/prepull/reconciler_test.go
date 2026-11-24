package prepull

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const pullNamespace = "karpenter-prepull"

type harness struct {
	t      *testing.T
	client client.Client
	clock  *clocktesting.FakeClock
	r      *Reconciler
	// createErr makes every create fail, as a rejecting admission policy does.
	createErr error
}

func newHarness(t *testing.T, objects ...client.Object) *harness {
	t.Helper()
	// Kubernetes timestamps have a resolution of one second.
	fakeClock := clocktesting.NewFakeClock(time.Now().Truncate(time.Second))
	for _, o := range objects {
		o.SetCreationTimestamp(metav1.NewTime(fakeClock.Now()))
	}
	h := &harness{t: t, clock: fakeClock}
	c := fake.NewClientBuilder().
		WithScheme(clientgoscheme.Scheme).
		WithObjects(objects...).
		WithStatusSubresource(&corev1.Pod{}).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		// The API server stamps the creation time; the fake client does not.
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.CreateOption) error {
				if h.createErr != nil {
					return h.createErr
				}
				o.SetCreationTimestamp(metav1.NewTime(fakeClock.Now()))
				return c.Create(ctx, o, opts...)
			},
		}).
		Build()
	h.client = c
	h.r = &Reconciler{
		Client:    c,
		APIReader: c,
		Recorder:  events.NewFakeRecorder(100),
		Clock:     fakeClock,
		Options: Options{
			StartupTaintKey:    startupKey,
			DisruptionTaintKey: "karpenter.sh/disrupted",
			Timeout:            2 * time.Minute,
			MaxImages:          30,
			Pod:                PodOptions{Namespace: pullNamespace, NoopImage: "noop:1", DeadlineSeconds: 150},
		},
	}
	return h
}

func (h *harness) reconcile() ctrl.Result {
	h.t.Helper()
	result, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "new"}})
	if err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
	return result
}

func (h *harness) taintKeys() []string {
	h.t.Helper()
	node := &corev1.Node{}
	if err := h.client.Get(context.Background(), types.NamespacedName{Name: "new"}, node); err != nil {
		h.t.Fatal(err)
	}
	var keys []string
	for _, taint := range node.Spec.Taints {
		keys = append(keys, taint.Key)
	}
	return keys
}

func (h *harness) pullPod() *corev1.Pod {
	h.t.Helper()
	pod := &corev1.Pod{}
	key := types.NamespacedName{Namespace: pullNamespace, Name: PodName(targetNode())}
	if err := h.client.Get(context.Background(), key, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		h.t.Fatal(err)
	}
	return pod
}

func disruptedNode() *corev1.Node {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "old"}}
	node.Spec.Taints = []corev1.Taint{{Key: "karpenter.sh/disrupted", Effect: corev1.TaintEffectNoSchedule}}
	return node
}

var releasedTaints = []string{"pool", "node.kubernetes.io/not-ready"}

func TestScaleUpNodeIsReleasedAtOnce(t *testing.T) {
	// A pod on a node that Karpenter is not disrupting must not hold the new node.
	steady := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "old"}}
	pod := testPod("app", nil, "app:1")
	h := newHarness(t, targetNode(), steady, &pod)

	h.reconcile()

	if got := h.taintKeys(); !reflect.DeepEqual(got, releasedTaints) {
		t.Errorf("taints = %v, want %v", got, releasedTaints)
	}
	if h.pullPod() != nil {
		t.Error("a pull pod exists for a node with nothing to pull")
	}
}

func TestReplacementNodeIsHeldUntilThePullSucceeds(t *testing.T) {
	app := testPod("app", nil, "app:1", "sidecar:1")
	h := newHarness(t, targetNode(), disruptedNode(), &app)

	h.reconcile()

	pod := h.pullPod()
	if pod == nil {
		t.Fatal("no pull pod for a replacement node")
	}
	if pod.Spec.NodeName != "new" {
		t.Errorf("pull pod is bound to %q, want the new node", pod.Spec.NodeName)
	}
	var images []string
	for _, c := range pod.Spec.Containers {
		images = append(images, c.Image)
	}
	if want := []string{"app:1", "sidecar:1"}; !reflect.DeepEqual(images, want) {
		t.Errorf("pull pod images = %v, want %v", images, want)
	}
	if got := h.taintKeys(); len(got) != 3 {
		t.Fatalf("taints = %v, want the startup taint to stay while the pull runs", got)
	}

	h.clock.Step(30 * time.Second)
	if result := h.reconcile(); result.RequeueAfter != 90*time.Second {
		t.Errorf("RequeueAfter = %s, want the 90s left until the timeout", result.RequeueAfter)
	}
	if got := h.taintKeys(); len(got) != 3 {
		t.Fatalf("taints = %v, want the startup taint to stay while the pull runs", got)
	}

	pod.Status.Phase = corev1.PodSucceeded
	if err := h.client.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	h.reconcile()

	if got := h.taintKeys(); !reflect.DeepEqual(got, releasedTaints) {
		t.Errorf("taints = %v, want %v", got, releasedTaints)
	}
	if h.pullPod() != nil {
		t.Error("the pull pod outlived the release")
	}
}

func TestStuckPullReleasesTheNodeAtTheTimeout(t *testing.T) {
	app := testPod("app", nil, "missing:1")
	h := newHarness(t, targetNode(), disruptedNode(), &app)
	h.reconcile()

	h.clock.Step(2*time.Minute + time.Second)
	h.reconcile()

	if got := h.taintKeys(); !reflect.DeepEqual(got, releasedTaints) {
		t.Errorf("taints = %v, want %v", got, releasedTaints)
	}
	if h.pullPod() != nil {
		t.Error("the pull pod outlived the release")
	}
}

func TestFailedPullPodReleasesTheNode(t *testing.T) {
	app := testPod("app", nil, "app:1")
	h := newHarness(t, targetNode(), disruptedNode(), &app)
	h.reconcile()

	pod := h.pullPod()
	pod.Status.Phase = corev1.PodFailed
	pod.Status.Reason = "Evicted"
	if err := h.client.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	h.reconcile()

	if got := h.taintKeys(); !reflect.DeepEqual(got, releasedTaints) {
		t.Errorf("taints = %v, want %v", got, releasedTaints)
	}
}

func TestPullPodIsRemovedWhenTheTaintIsGone(t *testing.T) {
	app := testPod("app", nil, "app:1")
	h := newHarness(t, targetNode(), disruptedNode(), &app)
	h.reconcile()

	node := &corev1.Node{}
	if err := h.client.Get(context.Background(), types.NamespacedName{Name: "new"}, node); err != nil {
		t.Fatal(err)
	}
	node.Spec.Taints = nil
	if err := h.client.Update(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	h.reconcile()

	if h.pullPod() != nil {
		t.Error("the pull pod stayed on a node that is no longer gated")
	}
}

func TestRejectedPullPodReleasesTheNode(t *testing.T) {
	app := testPod("app", nil, "app:1")
	h := newHarness(t, targetNode(), disruptedNode(), &app)
	h.createErr = apierrors.NewForbidden(corev1.Resource("pods"), "prepull", errors.New("denied by policy"))

	h.reconcile()

	if got := h.taintKeys(); !reflect.DeepEqual(got, releasedTaints) {
		t.Errorf("taints = %v, want %v", got, releasedTaints)
	}
}

func TestNodeGatedPastTheTimeoutIsReleasedWithoutAPull(t *testing.T) {
	// The controller was down while the node waited.
	app := testPod("app", nil, "app:1")
	h := newHarness(t, targetNode(), disruptedNode(), &app)
	h.clock.Step(10 * time.Minute)

	h.reconcile()

	if got := h.taintKeys(); !reflect.DeepEqual(got, releasedTaints) {
		t.Errorf("taints = %v, want %v", got, releasedTaints)
	}
	if h.pullPod() != nil {
		t.Error("a pull started for a node that is past its deadline")
	}
}
