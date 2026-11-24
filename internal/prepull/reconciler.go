// Package prepull holds new Karpenter nodes behind a startup taint until they
// have the images of the pods that Karpenter is about to move onto them.
package prepull

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Results of a release, used as the "result" metric label.
const (
	ResultPulled  = "pulled"
	ResultNothing = "nothing_to_pull"
	ResultTimeout = "timeout"
	ResultFailed  = "failed"
)

var (
	releases = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "karpenter_prepull_releases_total",
		Help: "Nodes released from the startup taint, by result.",
	}, []string{"result"})
	pullSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "karpenter_prepull_pull_duration_seconds",
		Help:    "Time from the creation of the pull pod to the release of the node.",
		Buckets: []float64{5, 10, 20, 30, 45, 60, 90, 120, 180, 300, 600},
	}, []string{"result"})
	imagesRequested = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "karpenter_prepull_images_requested_total",
		Help: "Images that pull pods were asked to pull.",
	})
)

func init() {
	metrics.Registry.MustRegister(releases, pullSeconds, imagesRequested)
}

// Options configures the Reconciler.
type Options struct {
	// StartupTaintKey is the taint that gates a new node. NodePools opt in by
	// listing it under startupTaints.
	StartupTaintKey string
	// DisruptionTaintKey is the taint Karpenter puts on a node it will drain.
	DisruptionTaintKey string
	// Timeout is the longest time a node keeps the startup taint, counted from
	// its registration. The node is released when it passes, whatever the
	// state of the pull.
	Timeout time.Duration
	// MaxImages limits the images of one pull pod.
	MaxImages int
	// IgnoreTaintKeys are startup taints of other components. They do not
	// stop a pod from counting as a future tenant of the new node.
	IgnoreTaintKeys []string
	Pod             PodOptions
}

// Reconciler gates nodes that carry the startup taint.
type Reconciler struct {
	Client client.Client
	// APIReader reads pods of all namespaces without caching them.
	APIReader client.Reader
	Recorder  events.EventRecorder
	Clock     clock.PassiveClock
	Options   Options
}

// SetupWithManager registers the reconciler. The manager's cache must hold
// the pods of Options.Pod.Namespace.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	gated := predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return r.gated(e.Object) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return r.gated(e.ObjectOld) || r.gated(e.ObjectNew) },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(e event.GenericEvent) bool { return r.gated(e.Object) },
	}
	pullPod := predicate.NewPredicateFuncs(func(o client.Object) bool {
		return o.GetNamespace() == r.Options.Pod.Namespace && o.GetLabels()[ManagedByLabel] == ManagedByValue
	})
	toNode := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
		pod, ok := o.(*corev1.Pod)
		if !ok || pod.Spec.NodeName == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: pod.Spec.NodeName}}}
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named("prepull").
		For(&corev1.Node{}, builder.WithPredicates(gated)).
		Watches(&corev1.Pod{}, toNode, builder.WithPredicates(pullPod)).
		// A short backoff keeps a failing reconcile inside the gate's deadline.
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](time.Second, 10*time.Second),
		}).
		Complete(r)
}

func (r *Reconciler) gated(o client.Object) bool {
	node, ok := o.(*corev1.Node)
	return ok && hasTaint(node, r.Options.StartupTaintKey)
}

// Reconcile moves one node through the gate: predict, pull, release.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	node := &corev1.Node{}
	if err := r.Client.Get(ctx, req.NamespacedName, node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	pod := &corev1.Pod{}
	podKey := types.NamespacedName{Namespace: r.Options.Pod.Namespace, Name: PodName(node)}
	if err := r.Client.Get(ctx, podKey, pod); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		pod = nil
	}

	if !hasTaint(node, r.Options.StartupTaintKey) || !node.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.deletePod(ctx, pod)
	}

	// One deadline covers the whole gate, so no failure can hold a node longer.
	left := node.CreationTimestamp.Add(r.Options.Timeout).Sub(r.Clock.Now())
	pulled, total := progress(pod)
	switch {
	case pod != nil && pod.Status.Phase == corev1.PodSucceeded:
		return ctrl.Result{}, r.release(ctx, node, pod, ResultPulled, fmt.Sprintf("pulled %d images", total))
	case pod != nil && pod.Status.Phase == corev1.PodFailed && pod.Status.Reason != "DeadlineExceeded":
		note := fmt.Sprintf("pull pod failed (%s) after %d of %d images", pod.Status.Reason, pulled, total)
		return ctrl.Result{}, r.release(ctx, node, pod, ResultFailed, note)
	case left <= 0 && pod == nil:
		note := fmt.Sprintf("gated for more than %s with no pull started", r.Options.Timeout)
		return ctrl.Result{}, r.release(ctx, node, nil, ResultTimeout, note)
	case left <= 0:
		note := fmt.Sprintf("timed out after %s with %d of %d images pulled", r.Options.Timeout, pulled, total)
		return ctrl.Result{}, r.release(ctx, node, pod, ResultTimeout, note)
	case pod == nil:
		return r.start(ctx, node, left)
	}
	return ctrl.Result{RequeueAfter: left}, nil
}

func (r *Reconciler) start(ctx context.Context, node *corev1.Node, left time.Duration) (ctrl.Result, error) {
	if node.Labels[corev1.LabelOSStable] != "" && node.Labels[corev1.LabelOSStable] != "linux" {
		return ctrl.Result{}, r.release(ctx, node, nil, ResultNothing, "not a Linux node")
	}

	pods, sources, err := r.disruptedPods(ctx, node)
	if err != nil {
		return ctrl.Result{}, err
	}
	ignore := map[string]bool{r.Options.StartupTaintKey: true}
	for _, key := range r.Options.IgnoreTaintKeys {
		ignore[key] = true
	}
	prediction := Predict(node, pods, ignore, r.Options.MaxImages)
	if len(prediction.Images) == 0 {
		note := "no node is waiting to drain onto this node"
		if sources > 0 {
			note = fmt.Sprintf("the pods of %d disrupted nodes need no image this node lacks", sources)
		}
		return ctrl.Result{}, r.release(ctx, node, nil, ResultNothing, note)
	}

	pod := BuildPod(node, prediction.Images, r.Options.Pod)
	if err := r.Client.Create(ctx, pod); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// The cache lags behind a pod this controller created a moment ago.
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		// An admission policy that rejects the pull pod must not hold the node.
		return ctrl.Result{}, r.release(ctx, node, nil, ResultFailed, fmt.Sprintf("could not create the pull pod: %v", err))
	}
	imagesRequested.Add(float64(len(prediction.Images)))
	log.FromContext(ctx).Info("started pull", "pod", pod.Name, "images", prediction.Images,
		"disruptedNodes", sources, "truncated", prediction.Truncated, "podsNeedingPullSecrets", prediction.NeedSecrets)
	r.Recorder.Eventf(node, pod, corev1.EventTypeNormal, "PrepullStarted", "Prepull",
		"pulling %d images used by pods on %d disrupted nodes", len(prediction.Images), sources)
	return ctrl.Result{RequeueAfter: left}, nil
}

// disruptedPods returns the pods on the nodes Karpenter has marked for
// disruption, and how many such nodes there are.
func (r *Reconciler) disruptedPods(ctx context.Context, target *corev1.Node) ([]corev1.Pod, int, error) {
	nodes := &corev1.NodeList{}
	if err := r.Client.List(ctx, nodes); err != nil {
		return nil, 0, err
	}
	var pods []corev1.Pod
	sources := 0
	for i := range nodes.Items {
		source := &nodes.Items[i]
		if source.Name == target.Name || !hasTaint(source, r.Options.DisruptionTaintKey) {
			continue
		}
		sources++
		list := &corev1.PodList{}
		if err := r.APIReader.List(ctx, list, client.MatchingFields{"spec.nodeName": source.Name}); err != nil {
			return nil, 0, err
		}
		pods = append(pods, list.Items...)
	}
	return pods, sources, nil
}

func (r *Reconciler) release(ctx context.Context, node *corev1.Node, pod *corev1.Pod, result, note string) error {
	// A node changes often while it starts, so read it live and retry on conflict.
	// The lock keeps a taint that another writer adds between read and patch.
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		live := &corev1.Node{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(node), live); err != nil {
			return err
		}
		updated := live.DeepCopy()
		updated.Spec.Taints = nil
		for _, taint := range live.Spec.Taints {
			if taint.Key != r.Options.StartupTaintKey {
				updated.Spec.Taints = append(updated.Spec.Taints, taint)
			}
		}
		return r.Client.Patch(ctx, updated, client.MergeFromWithOptions(live, client.MergeFromWithOptimisticLock{}))
	})
	if err != nil {
		return client.IgnoreNotFound(err)
	}

	releases.WithLabelValues(result).Inc()
	if pod != nil {
		pullSeconds.WithLabelValues(result).Observe(r.Clock.Since(pod.CreationTimestamp.Time).Seconds())
	}
	eventType := corev1.EventTypeNormal
	if result == ResultTimeout || result == ResultFailed {
		eventType = corev1.EventTypeWarning
	}
	log.FromContext(ctx).Info("released node", "result", result, "note", note)
	r.Recorder.Eventf(node, nil, eventType, "PrepullReleased", "Release", "%s", note)
	return r.deletePod(ctx, pod)
}

func (r *Reconciler) deletePod(ctx context.Context, pod *corev1.Pod) error {
	if pod == nil || !pod.DeletionTimestamp.IsZero() {
		return nil
	}
	return client.IgnoreNotFound(r.Client.Delete(ctx, pod, client.GracePeriodSeconds(0)))
}

func hasTaint(node *corev1.Node, key string) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Key == key {
			return true
		}
	}
	return false
}

// progress counts the containers whose image is on the node.
func progress(pod *corev1.Pod) (pulled, total int) {
	if pod == nil {
		return 0, 0
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Running != nil || status.State.Terminated != nil {
			pulled++
		}
	}
	return pulled, len(pod.Spec.Containers)
}
