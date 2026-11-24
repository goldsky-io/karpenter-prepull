package prepull

import (
	"context"
	"fmt"
	"net/http"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// Decisions of the eviction webhook, used as the "decision" metric label.
const (
	DecisionAllowed = "allowed"
	DecisionHeld    = "held"
	DecisionTimeout = "timeout"
)

// pullStartGrace is how long after the start of a drain the webhook waits for
// a pull pod that does not exist yet.
const pullStartGrace = 15 * time.Second

// EvictionHold is a validating webhook for pod evictions. It refuses the
// eviction of a pod from a node Karpenter drains by choice while a pull pod
// still fetches that pod's images on a node the pod can move to.
//
// Every doubt resolves to "allow". With failurePolicy Ignore on the webhook
// configuration, an outage of this webhook leaves Karpenter as it is without it.
// An outage of the controller alone holds a drain for pullStartGrace at most.
type EvictionHold struct {
	*Planner
}

// Handle decides one eviction.
func (h *EvictionHold) Handle(ctx context.Context, req admission.Request) admission.Response {
	if req.SubResource != "eviction" {
		return admission.Allowed("")
	}
	pod := &corev1.Pod{}
	if err := h.APIReader.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, pod); err != nil {
		return admission.Allowed("")
	}
	if pod.Spec.NodeName == "" {
		return admission.Allowed("")
	}
	source := &corev1.Node{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, source); err != nil {
		return admission.Allowed("")
	}
	// Karpenter starts a drain by deleting the node, so the deletion time is
	// the start of the drain.
	if source.DeletionTimestamp.IsZero() {
		return admission.Allowed("")
	}
	if byChoice, err := h.DisruptedByChoice(ctx, source); err != nil || !byChoice {
		return admission.Allowed("")
	}

	waiting, err := h.waitingOn(ctx, source, pod)
	if err != nil || waiting == "" {
		evictions.WithLabelValues(DecisionAllowed).Inc()
		return admission.Allowed("")
	}
	if h.Clock.Since(source.DeletionTimestamp.Time) >= h.Options.Timeout {
		evictions.WithLabelValues(DecisionTimeout).Inc()
		log.FromContext(ctx).Info("hold timed out", "pod", req.Namespace+"/"+req.Name, "node", source.Name, "waitingOn", waiting)
		return admission.Allowed("")
	}
	evictions.WithLabelValues(DecisionHeld).Inc()
	// Karpenter and kubectl treat 429 as "try again", as they do for a
	// PodDisruptionBudget.
	return admission.Response{AdmissionResponse: admissionv1.AdmissionResponse{
		Allowed: false,
		Result: &metav1.Status{
			Code:    http.StatusTooManyRequests,
			Reason:  metav1.StatusReasonTooManyRequests,
			Message: fmt.Sprintf("karpenter-prepull is pulling the images of this pod on node %s", waiting),
		},
	}}
}

// waitingOn returns the name of a node that still pulls images of pod, or ""
// when no pull is in progress.
func (h *EvictionHold) waitingOn(ctx context.Context, source *corev1.Node, pod *corev1.Pod) (string, error) {
	targets, err := h.Targets(ctx, source)
	if err != nil {
		return "", err
	}
	// Karpenter can taint and drain a node in the same second, before the pull pods exist.
	early := h.Clock.Since(source.DeletionTimestamp.Time) < pullStartGrace
	for i := range targets {
		target := &targets[i]
		if len(h.Missing(target, []corev1.Pod{*pod}, 0).Images) == 0 {
			continue
		}
		pull := &corev1.Pod{}
		key := types.NamespacedName{Namespace: h.Options.Pod.Namespace, Name: PodName(target, source)}
		if err := h.Client.Get(ctx, key, pull); err != nil {
			if !apierrors.IsNotFound(err) {
				return "", err
			}
			if early {
				return target.Name, nil
			}
			continue
		}
		if !terminal(pull) && pull.DeletionTimestamp.IsZero() {
			return target.Name, nil
		}
	}
	return "", nil
}
