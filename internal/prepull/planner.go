// Package prepull pulls images onto the nodes that will receive the pods of a
// node Karpenter is about to drain, and holds the evictions until the pulls
// are done.
package prepull

import (
	"context"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NodeClaimGVK is the Karpenter resource that owns a node.
var NodeClaimGVK = schema.GroupVersionKind{Group: "karpenter.sh", Version: "v1", Kind: "NodeClaim"}

// Karpenter sets this NodeClaim condition when it disrupts a node by choice,
// which is when it starts replacement nodes first.
const disruptionReasonCondition = "DisruptionReason"

// An unhealthy node is also disrupted by choice, but it must drain without delay.
var chosenDisruptions = map[string]bool{"Drifted": true, "Underutilized": true, "Empty": true}

// Options configures the controller and the webhook.
type Options struct {
	// DisruptionTaintKey is the taint Karpenter puts on a node it will drain.
	DisruptionTaintKey string
	// Timeout is the longest time evictions are held, counted from the start
	// of the drain. It is also the deadline of a pull pod.
	Timeout time.Duration
	// MaxImages limits the images of one pull pod.
	MaxImages int
	// TargetMaxAge limits the nodes that are warmed to those younger than
	// this. Zero warms every node the pods fit.
	TargetMaxAge time.Duration
	// IgnoreTaintKeys are startup taints of other components. They do not
	// stop a pod from counting as a future tenant of a new node.
	IgnoreTaintKeys []string
	// NodePools limits the nodes whose drains are warmed for and held to
	// those of the named Karpenter NodePools. Empty means every NodePool.
	NodePools []string
	Pod       PodOptions
}

// Planner holds the rules the controller and the webhook share.
type Planner struct {
	// Client reads nodes, NodeClaims and pull pods from the cache.
	Client client.Client
	// APIReader reads pods of all namespaces without caching them.
	APIReader client.Reader
	Clock     clock.PassiveClock
	Options   Options
}

// Eligible reports whether to act on the node: it is in scope, and Karpenter
// marked it for a disruption in which it starts the replacement nodes before
// the drain. Spot interruption and expiry drain at once, so there is nothing
// to wait for.
func (p *Planner) Eligible(ctx context.Context, node *corev1.Node) (bool, error) {
	if !hasTaint(node, p.Options.DisruptionTaintKey) {
		return false, nil
	}
	if len(p.Options.NodePools) > 0 && !slices.Contains(p.Options.NodePools, node.Labels[NodePoolLabel]) {
		return false, nil
	}
	for _, owner := range node.OwnerReferences {
		if owner.Kind != NodeClaimGVK.Kind {
			continue
		}
		claim := &unstructured.Unstructured{}
		claim.SetGroupVersionKind(schema.FromAPIVersionAndKind(owner.APIVersion, owner.Kind))
		if err := p.Client.Get(ctx, client.ObjectKey{Name: owner.Name}, claim); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		conditions, _, _ := unstructured.NestedSlice(claim.Object, "status", "conditions")
		for _, c := range conditions {
			condition, ok := c.(map[string]any)
			if !ok || condition["type"] != disruptionReasonCondition || condition["status"] != "True" {
				continue
			}
			reason, _ := condition["reason"].(string)
			return chosenDisruptions[reason], nil
		}
	}
	return false, nil
}

// Targets returns the nodes that can receive pods of the draining node source
// and are young enough to have an empty image cache.
func (p *Planner) Targets(ctx context.Context, source *corev1.Node) ([]corev1.Node, error) {
	nodes := &corev1.NodeList{}
	if err := p.Client.List(ctx, nodes); err != nil {
		return nil, err
	}
	var targets []corev1.Node
	for _, node := range nodes.Items {
		switch {
		case node.Name == source.Name:
		// Karpenter waits for the replacement before the drain, so a node that
		// appears during the drain is not one. Waiting for it would extend the hold.
		case !source.DeletionTimestamp.IsZero() && node.CreationTimestamp.After(source.DeletionTimestamp.Time):
		case !node.DeletionTimestamp.IsZero() || node.Spec.Unschedulable:
		case hasTaint(&node, p.Options.DisruptionTaintKey):
		case node.Labels[corev1.LabelOSStable] != "" && node.Labels[corev1.LabelOSStable] != "linux":
		case p.Options.TargetMaxAge > 0 && p.Clock.Since(node.CreationTimestamp.Time) > p.Options.TargetMaxAge:
		default:
			targets = append(targets, node)
		}
	}
	return targets, nil
}

// Missing returns the images that pods need and target lacks.
func (p *Planner) Missing(target *corev1.Node, pods []corev1.Pod, maxImages int) Prediction {
	ignore := map[string]bool{}
	for _, key := range p.Options.IgnoreTaintKeys {
		ignore[key] = true
	}
	return Predict(target, pods, ignore, maxImages)
}

func hasTaint(node *corev1.Node, key string) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Key == key {
			return true
		}
	}
	return false
}

func terminal(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}
