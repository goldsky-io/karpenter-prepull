// Command controller runs karpenter-prepull.
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/goldsky-io/karpenter-prepull/internal/prepull"
)

func main() {
	var (
		opts         prepull.Options
		ignoreTaints string
		metricsAddr  string
		probeAddr    string
		certDir      string
		webhookPort  int
		leaderElect  bool
	)
	flag.StringVar(&opts.DisruptionTaintKey, "disruption-taint-key", "karpenter.sh/disrupted",
		"Key of the taint Karpenter puts on a node it will drain.")
	flag.DurationVar(&opts.Timeout, "timeout", 2*time.Minute,
		"Longest time evictions are held after a drain starts. Also the deadline of a pull pod.")
	flag.IntVar(&opts.MaxImages, "max-images", 30, "Most images one pull pod pulls.")
	flag.DurationVar(&opts.TargetMaxAge, "target-node-max-age", 15*time.Minute,
		"Warm only nodes younger than this. Zero warms every node the pods of a draining node fit.")
	flag.StringVar(&ignoreTaints, "ignore-taint-keys", "",
		"Comma-separated startup taint keys of other components, ignored when matching pods to a new node.")
	flag.StringVar(&opts.Pod.Namespace, "namespace", os.Getenv("POD_NAMESPACE"), "Namespace of the pull pods.")
	flag.StringVar(&opts.Pod.NoopImage, "noop-image", os.Getenv("NOOP_IMAGE"),
		"Image that holds /noop for every node architecture. Normally the image of this controller.")
	flag.BoolVar(&opts.Pod.HostNetwork, "host-network", false,
		"Run pull pods on the host network, so they do not wait for a pod IP on a node that is still starting.")
	flag.StringVar(&opts.Pod.PriorityClassName, "priority-class-name", "", "Priority class of the pull pods.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address of the metrics endpoint.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address of the health probes.")
	flag.IntVar(&webhookPort, "webhook-port", 9443, "Port of the eviction webhook.")
	flag.StringVar(&certDir, "webhook-cert-dir", "/etc/karpenter-prepull/tls", "Directory with tls.crt and tls.key of the webhook.")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Run one active controller at a time. The webhook serves on every replica.")
	zapOpts := zap.Options{}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	log := ctrl.Log.WithName("setup")

	if opts.Pod.Namespace == "" || opts.Pod.NoopImage == "" {
		log.Info("--namespace and --noop-image are required")
		os.Exit(2)
	}
	if ignoreTaints != "" {
		opts.IgnoreTaintKeys = strings.Split(ignoreTaints, ",")
	}
	opts.Pod.DeadlineSeconds = int64(opts.Timeout.Seconds())

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		log.Error(err, "build scheme")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                        scheme,
		Metrics:                       metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:        probeAddr,
		WebhookServer:                 webhook.NewServer(webhook.Options{Port: webhookPort, CertDir: certDir}),
		LeaderElection:                leaderElect,
		LeaderElectionID:              "karpenter-prepull",
		LeaderElectionNamespace:       opts.Pod.Namespace,
		LeaderElectionReleaseOnCancel: true,
		Cache: cache.Options{
			DefaultTransform: cache.TransformStripManagedFields(),
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Pod{}: {Namespaces: map[string]cache.Config{opts.Pod.Namespace: {}}},
			},
		},
		// NodeClaims are read as unstructured objects, from the cache.
		Client: client.Options{Cache: &client.CacheOptions{Unstructured: true}},
	})
	if err != nil {
		log.Error(err, "create manager")
		os.Exit(1)
	}

	planner := &prepull.Planner{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Clock:     clock.RealClock{},
		Options:   opts,
	}
	reconciler := &prepull.Reconciler{Planner: planner, Recorder: mgr.GetEventRecorder("karpenter-prepull")}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		log.Error(err, "set up controller")
		os.Exit(1)
	}
	mgr.GetWebhookServer().Register("/evictions", &webhook.Admission{Handler: &prepull.EvictionHold{Planner: planner}})

	warm := &cacheWarmer{cache: mgr.GetCache()}
	if err := mgr.Add(warm); err != nil {
		log.Error(err, "add cache warmer")
		os.Exit(1)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "add health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("webhook", mgr.GetWebhookServer().StartedChecker()); err != nil {
		log.Error(err, "add ready check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("cache", warm.ready); err != nil {
		log.Error(err, "add ready check")
		os.Exit(1)
	}

	log.Info("starting", "timeout", opts.Timeout.String(), "targetNodeMaxAge", opts.TargetMaxAge.String())
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "run manager")
		os.Exit(1)
	}
}

// cacheWarmer fills the caches the webhook reads on every replica. Without it
// a replica that is not the leader would start them on its first eviction.
type cacheWarmer struct {
	cache  cache.Cache
	synced atomic.Bool
}

func (w *cacheWarmer) NeedLeaderElection() bool { return false }

func (w *cacheWarmer) Start(ctx context.Context) error {
	claims := &unstructured.Unstructured{}
	claims.SetGroupVersionKind(prepull.NodeClaimGVK)
	for _, o := range []client.Object{&corev1.Node{}, &corev1.Pod{}, claims} {
		if _, err := w.cache.GetInformer(ctx, o); err != nil {
			return err
		}
	}
	w.synced.Store(true)
	<-ctx.Done()
	return nil
}

func (w *cacheWarmer) ready(*http.Request) error {
	if !w.synced.Load() {
		return errors.New("caches are not synced")
	}
	return nil
}
