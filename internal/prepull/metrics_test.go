package prepull

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	promclient "github.com/prometheus/client_golang/prometheus"
)

func TestMetricsReachPrometheusAndOTLP(t *testing.T) {
	var mu sync.Mutex
	var pushed []byte
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/metrics" {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			pushed = append(pushed, body...)
			mu.Unlock()
		}
	}))
	defer collector.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)

	registry := promclient.NewRegistry()
	stop, err := SetupMetrics(context.Background(), registry)
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t, "Drifted", testPod("app", nil, "app:1"))
	h.reconcile()
	h.startDrain()
	h.evict("app")
	h.finishPulls("Succeeded")
	h.evict("app")

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	scraped := map[string]bool{}
	for _, family := range families {
		scraped[family.GetName()] = true
	}
	for _, name := range []string{
		"karpenter_prepull_pull_pods_created_total",
		"karpenter_prepull_images_requested_total",
		"karpenter_prepull_evictions_total",
		"karpenter_prepull_eviction_wait_seconds",
	} {
		if !scraped[name] {
			t.Errorf("the Prometheus endpoint lacks %s", name)
		}
	}

	if err := stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, name := range []string{"karpenter_prepull.evictions", "karpenter_prepull.eviction.wait", "karpenter-prepull"} {
		if !bytes.Contains(pushed, []byte(name)) {
			t.Errorf("the OTLP push lacks %q", name)
		}
	}
}
