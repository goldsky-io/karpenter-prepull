package prepull

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var (
	pullPodsCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "karpenter_prepull_pull_pods_created_total",
		Help: "Pull pods created.",
	})
	imagesRequested = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "karpenter_prepull_images_requested_total",
		Help: "Images that pull pods were asked to pull.",
	})
	evictions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "karpenter_prepull_evictions_total",
		Help: "Evictions from nodes Karpenter drains by choice, by decision.",
	}, []string{"decision"})
)

func init() {
	metrics.Registry.MustRegister(pullPodsCreated, imagesRequested, evictions)
}

// Reconciler starts pull pods for every node Karpenter is about to drain.
type Reconciler struct {
	*Planner
	Recorder events.EventRecorder
}

// SetupWithManager registers the reconciler. The manager's cache must hold
// the pods of Options.Pod.Namespace.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	// A new node can be a target of any draining node, so each node event
	// reconciles all of them. There are few at a time.
	draining := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, _ client.Object) []reconcile.Request {
		nodes := &corev1.NodeList{}
		if err := r.Client.List(ctx, nodes); err != nil {
			return nil
		}
		var requests []reconcile.Request
		for i := range nodes.Items {
			if hasTaint(&nodes.Items[i], r.Options.DisruptionTaintKey) {
				requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: nodes.Items[i].Name}})
			}
		}
		return requests
	})
	// Karpenter sets the NodeClaim condition after it taints the node.
	claimNode := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
		claim, ok := o.(*unstructured.Unstructured)
		if !ok {
			return nil
		}
		name, _, _ := unstructured.NestedString(claim.Object, "status", "nodeName")
		if name == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: name}}}
	})
	claims := &unstructured.Unstructured{}
	claims.SetGroupVersionKind(NodeClaimGVK)
	return ctrl.NewControllerManagedBy(mgr).
		Named("prepull").
		Watches(&corev1.Node{}, draining).
		Watches(claims, claimNode).
		Complete(r)
}

// Reconcile starts the pull pods that one draining node needs.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	source := &corev1.Node{}
	if err := r.Client.Get(ctx, req.NamespacedName, source); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	byChoice, err := r.DisruptedByChoice(ctx, source)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !byChoice {
		// Karpenter gave up the disruption, or it never was one.
		return ctrl.Result{}, r.deletePullPods(ctx, source)
	}

	targets, err := r.Targets(ctx, source)
	if err != nil || len(targets) == 0 {
		return ctrl.Result{}, err
	}
	pods := &corev1.PodList{}
	if err := r.APIReader.List(ctx, pods, client.MatchingFields{"spec.nodeName": source.Name}); err != nil {
		return ctrl.Result{}, err
	}

	for i := range targets {
		target := &targets[i]
		key := types.NamespacedName{Namespace: r.Options.Pod.Namespace, Name: PodName(target, source)}
		if err := r.Client.Get(ctx, key, &corev1.Pod{}); err == nil {
			continue
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		prediction := r.Missing(target, pods.Items, r.Options.MaxImages)
		if len(prediction.Images) == 0 {
			continue
		}
		pod := BuildPod(target, source, prediction.Images, r.Options.Pod)
		if err := r.Client.Create(ctx, pod); err != nil {
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			// The webhook holds evictions only while a pull pod runs, so a
			// rejected pull pod costs the warm-up and nothing else.
			log.FromContext(ctx).Error(err, "could not create the pull pod", "target", target.Name)
			r.Recorder.Eventf(source, target, corev1.EventTypeWarning, "PrepullFailed", "Prepull",
				"could not create the pull pod for node %s: %v", target.Name, err)
			continue
		}
		pullPodsCreated.Inc()
		imagesRequested.Add(float64(len(prediction.Images)))
		log.FromContext(ctx).Info("started pull", "target", target.Name, "pod", pod.Name, "images", prediction.Images,
			"truncated", prediction.Truncated, "podsNeedingPullSecrets", prediction.NeedSecrets,
			"unpullableImages", prediction.Unpullable)
		r.Recorder.Eventf(source, target, corev1.EventTypeNormal, "PrepullStarted", "Prepull",
			"pulling %d images on node %s", len(prediction.Images), target.Name)
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) deletePullPods(ctx context.Context, source *corev1.Node) error {
	pods := &corev1.PodList{}
	err := r.Client.List(ctx, pods, client.InNamespace(r.Options.Pod.Namespace),
		client.MatchingLabels{ManagedByLabel: ManagedByValue, SourceLabel: string(source.UID)})
	if err != nil {
		return err
	}
	for i := range pods.Items {
		if err := r.Client.Delete(ctx, &pods.Items[i], client.GracePeriodSeconds(0)); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}
