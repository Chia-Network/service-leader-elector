package election

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/chia-network/go-modules/pkg/slogs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 2 * time.Second

	serviceAccountNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

// Run contends for a namespaced Lease and blocks until ctx is cancelled.
// Namespace is always the current pod namespace; identity and timings use fixed defaults.
func Run(ctx context.Context, leaseName string) {
	config, err := rest.InClusterConfig()
	if err != nil {
		slogs.Logr.Fatal("building in-cluster config", "error", err)
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		slogs.Logr.Fatal("creating kubernetes client", "error", err)
	}

	namespace := currentNamespace()
	identity := identity()

	slogs.Logr.Info("starting leader election",
		"lease", leaseName,
		"namespace", namespace,
		"identity", identity,
	)

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: namespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				slogs.Logr.Info("became leader",
					"lease", leaseName,
					"namespace", namespace,
					"identity", identity,
				)
				<-ctx.Done()
			},
			OnStoppedLeading: func() {
				slogs.Logr.Warn("stopped leading",
					"lease", leaseName,
					"namespace", namespace,
					"identity", identity,
				)
			},
			OnNewLeader: func(current string) {
				if current == identity {
					return
				}
				slogs.Logr.Info("new leader observed",
					"lease", leaseName,
					"namespace", namespace,
					"leader", current,
					"identity", identity,
				)
			},
		},
	})
}

func currentNamespace() string {
	if ns := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); ns != "" {
		return ns
	}

	data, err := os.ReadFile(serviceAccountNamespacePath)
	if err != nil {
		slogs.Logr.Fatal("resolving current namespace",
			"error", err,
			"path", serviceAccountNamespacePath,
		)
	}

	ns := strings.TrimSpace(string(data))
	if ns == "" {
		slogs.Logr.Fatal("resolving current namespace: namespace file is empty",
			"path", serviceAccountNamespacePath,
		)
	}
	return ns
}

func identity() string {
	if id := strings.TrimSpace(os.Getenv("POD_NAME")); id != "" {
		return id
	}
	if id := strings.TrimSpace(os.Getenv("HOSTNAME")); id != "" {
		return id
	}

	hostname, err := os.Hostname()
	if err != nil {
		slogs.Logr.Fatal("resolving identity", "error", err)
	}
	if hostname == "" {
		slogs.Logr.Fatal("resolving identity: hostname is empty")
	}
	return hostname
}
