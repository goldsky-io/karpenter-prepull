// Command controller runs karpenter-prepull.
package main

import (
	"flag"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/goldsky-io/karpenter-prepull/internal/prepull"
)

func main() {
	var (
		opts         prepull.Options
		ignoreTaints string
		metricsAddr  string
		probeAddr    string
		leaderElect  bool
	)
	flag.StringVar(&opts.StartupTaintKey, "startup-taint-key", "karpenter-prepull.goldsky.com/pending",
		"Key of the startup taint that gates a node. List it under startupTaints of each NodePool to gate.")
	flag.StringVar(&opts.DisruptionTaintKey, "disruption-taint-key", "karpenter.sh/disrupted",
		"Key of the taint Karpenter puts on a node it will drain.")
	flag.DurationVar(&opts.Timeout, "timeout", 2*time.Minute,
		"Longest time a pull may hold a node. The node is released when it passes.")
	flag.IntVar(&opts.MaxImages, "max-images", 30, "Most images one pull pod pulls.")
	flag.StringVar(&ignoreTaints, "ignore-taint-keys", "",
		"Comma-separated startup taint keys of other components, ignored when matching pods to the new node.")
	flag.StringVar(&opts.Pod.Namespace, "namespace", os.Getenv("POD_NAMESPACE"), "Namespace of the pull pods.")
	flag.StringVar(&opts.Pod.NoopImage, "noop-image", os.Getenv("NOOP_IMAGE"),
		"Image that holds /noop for every node architecture. Normally the image of this controller.")
	flag.BoolVar(&opts.Pod.HostNetwork, "host-network", false,
		"Run pull pods on the host network, so they do not wait for a pod IP on a node that is still starting.")
	flag.StringVar(&opts.Pod.PriorityClassName, "priority-class-name", "", "Priority class of the pull pods.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address of the metrics endpoint.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address of the health probes.")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Run one active replica at a time.")
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
	// The kubelet stops the pod a little after the controller would have.
	opts.Pod.DeadlineSeconds = int64(opts.Timeout.Seconds()) + 30

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		log.Error(err, "build scheme")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                        scheme,
		Metrics:                       metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:        probeAddr,
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
	})
	if err != nil {
		log.Error(err, "create manager")
		os.Exit(1)
	}

	reconciler := &prepull.Reconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Recorder:  mgr.GetEventRecorder("karpenter-prepull"),
		Clock:     clock.RealClock{},
		Options:   opts,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		log.Error(err, "set up controller")
		os.Exit(1)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "add health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "add ready check")
		os.Exit(1)
	}

	log.Info("starting", "startupTaintKey", opts.StartupTaintKey, "timeout", opts.Timeout.String())
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "run manager")
		os.Exit(1)
	}
}
