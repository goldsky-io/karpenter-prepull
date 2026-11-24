package prepull

import (
	"context"
	"os"

	promclient "github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	corev1 "k8s.io/api/core/v1"
)

// NodePoolLabel is the node label Karpenter sets to the name of the NodePool.
const NodePoolLabel = "karpenter.sh/nodepool"

var meter = otel.Meter("github.com/goldsky-io/karpenter-prepull")

// The Prometheus endpoint shows these names with "." as "_", and counters with a "_total" suffix.
var (
	pullPodsCreated, _ = meter.Int64Counter("karpenter_prepull.pull_pods.created",
		metric.WithDescription("Pull pods created."))
	imagesRequested, _ = meter.Int64Counter("karpenter_prepull.images.requested",
		metric.WithDescription("Images that pull pods were asked to pull."))
	evictions, _ = meter.Int64Counter("karpenter_prepull.evictions",
		metric.WithDescription("Evictions from nodes Karpenter drains by choice, by decision."))
	evictionWait, _ = meter.Float64Histogram("karpenter_prepull.eviction.wait",
		metric.WithUnit("s"),
		metric.WithDescription("Time from the start of a drain to an eviction that was let through."),
		metric.WithExplicitBucketBoundaries(1, 5, 10, 20, 30, 45, 60, 90, 120, 180))
)

func nodePoolAttr(node *corev1.Node) attribute.KeyValue {
	return attribute.String("nodepool", node.Labels[NodePoolLabel])
}

// SetupMetrics serves the metrics of this package on the Prometheus registry.
// It also pushes them over OTLP/HTTP when an OTLP endpoint is set through the
// standard OTEL_EXPORTER_OTLP_* environment variables. The returned function
// flushes and stops the export.
func SetupMetrics(ctx context.Context, registry promclient.Registerer) (func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName("karpenter-prepull")),
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, err
	}
	scrape, err := prometheus.New(prometheus.WithRegisterer(registry), prometheus.WithoutScopeInfo(), prometheus.WithoutTargetInfo())
	if err != nil {
		return nil, err
	}
	opts := []sdkmetric.Option{sdkmetric.WithResource(res), sdkmetric.WithReader(scrape)}

	// Without this check the exporter would push to localhost.
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") != "" {
		push, err := otlpmetrichttp.New(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(push)))
	}

	provider := sdkmetric.NewMeterProvider(opts...)
	otel.SetMeterProvider(provider)
	return provider.Shutdown, nil
}
