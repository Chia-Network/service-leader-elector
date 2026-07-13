package election

import (
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chia-network/go-modules/pkg/slogs"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 2 * time.Second

	labelClearAttempts = 5

	serviceAccountNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

	// LeaderLabelKey is set to LeaderLabelValue on the elected leader pod so
	// Services can select only that pod.
	LeaderLabelKey = "leader"
	// LeaderLabelValue is the value applied to LeaderLabelKey on the leader pod.
	LeaderLabelValue = "true"
)

var leading atomic.Bool

// IsLeader reports whether this process currently holds the lease.
func IsLeader() bool {
	return leading.Load()
}

// Run contends for a namespaced Lease and blocks until ctx is cancelled.
// Namespace is always the current pod namespace; identity and timings use fixed defaults.
// While leading, the pod is labeled leader=true for Service routing.
// If the lease is lost, demotion cleanup runs and election is re-entered until ctx ends.
func Run(ctx context.Context, cfg Config) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
		"lease", cfg.LeaseName,
		"namespace", namespace,
		"identity", identity,
		"on_stopped_leading_process", cfg.OnStoppedLeadingProcess,
	)

	// Clear any stale leader label from a previous run before contending.
	// Failure is fatal: a leftover leader=true would keep this pod in the leader Service.
	mustClearLeaderLabel(ctx, client, namespace, identity, "startup")

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      cfg.LeaseName,
			Namespace: namespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}

	// heldLeadership tracks whether we successfully became leader so demote
	// signaling runs at most once per leadership term and only after real leadership.
	var heldLeadership atomic.Bool
	// leadingTerm counts an in-flight OnStartedLeading callback (including its
	// demotion cleanup). client-go runs that callback in a goroutine and does not
	// wait for it before Run returns, so OnStoppedLeading and the re-entry loop
	// must Wait to avoid overlapping terms.
	var leadingTerm sync.WaitGroup

	lec := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				leadingTerm.Add(1)
				defer leadingTerm.Done()

				// Clear label before returning so ReleaseOnCancel does not hand
				// off the lease while this pod is still selected by the Service.
				defer func() {
					mustClearLeaderLabel(context.Background(), client, namespace, identity, "after leading")
					if heldLeadership.Swap(false) {
						signalOnStoppedLeading(cfg)
					}
					leading.Store(false)
				}()

				// Set the Service selector label before advertising leadership.
				// On failure, cancel election and return so the lease is released
				// instead of Fatal (which would skip cleanup).
				if err := setLeaderLabel(ctx, client, namespace, identity); err != nil {
					slogs.Logr.Error("setting leader label",
						"error", err,
						"namespace", namespace,
						"pod", identity,
					)
					cancel()
					return
				}
				leading.Store(true)
				heldLeadership.Store(true)

				slogs.Logr.Info("became leader",
					"lease", cfg.LeaseName,
					"namespace", namespace,
					"identity", identity,
				)
				<-ctx.Done()
			},
			OnStoppedLeading: func() {
				// Run cancels OnStartedLeading then calls this without waiting;
				// block until that callback's demotion cleanup finishes.
				leadingTerm.Wait()

				mustClearLeaderLabel(context.Background(), client, namespace, identity, "stopped leading")
				if heldLeadership.Swap(false) {
					signalOnStoppedLeading(cfg)
				}
				leading.Store(false)

				slogs.Logr.Warn("stopped leading",
					"lease", cfg.LeaseName,
					"namespace", namespace,
					"identity", identity,
				)
			},
			OnNewLeader: func(current string) {
				if current == identity {
					return
				}
				slogs.Logr.Info("new leader observed",
					"lease", cfg.LeaseName,
					"namespace", namespace,
					"leader", current,
					"identity", identity,
				)
			},
		},
	}

	// client-go's Run returns once this instance stops holding the lease; it does
	// not re-acquire. Loop until the parent context is cancelled so demotion
	// (label clear + optional co-process signal) is followed by renewed contention.
	for {
		if ctx.Err() != nil {
			return
		}
		leaderelection.RunOrDie(ctx, lec)
		// Belt-and-suspenders with OnStoppedLeading's Wait: do not contend again
		// until any OnStartedLeading cleanup from this term is finished.
		leadingTerm.Wait()
		if ctx.Err() != nil {
			return
		}
		slogs.Logr.Info("lease lost; re-entering leader election",
			"lease", cfg.LeaseName,
			"namespace", namespace,
			"identity", identity,
		)
	}
}

func setLeaderLabel(ctx context.Context, client kubernetes.Interface, namespace, podName string) error {
	patch := []byte(`{"metadata":{"labels":{"` + LeaderLabelKey + `":"` + LeaderLabelValue + `"}}}`)
	_, err := client.CoreV1().Pods(namespace).Patch(ctx, podName, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return err
	}
	slogs.Logr.Info("set leader label",
		"namespace", namespace,
		"pod", podName,
		"label", LeaderLabelKey+"="+LeaderLabelValue,
	)
	return nil
}

func clearLeaderLabel(ctx context.Context, client kubernetes.Interface, namespace, podName string) error {
	patch := []byte(`{"metadata":{"labels":{"` + LeaderLabelKey + `":null}}}`)
	_, err := client.CoreV1().Pods(namespace).Patch(ctx, podName, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		// Pod already gone — it cannot remain selected by the leader Service.
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	slogs.Logr.Info("cleared leader label",
		"namespace", namespace,
		"pod", podName,
		"label", LeaderLabelKey,
	)
	return nil
}

// mustClearLeaderLabel retries clearing the leader label and Fatals if it cannot,
// so a former leader cannot keep receiving traffic via the leader Service.
func mustClearLeaderLabel(ctx context.Context, client kubernetes.Interface, namespace, podName, reason string) {
	var err error
	for attempt := 1; attempt <= labelClearAttempts; attempt++ {
		err = clearLeaderLabel(ctx, client, namespace, podName)
		if err == nil {
			return
		}
		slogs.Logr.Warn("clearing leader label",
			"error", err,
			"attempt", attempt,
			"reason", reason,
			"namespace", namespace,
			"pod", podName,
		)
		if attempt == labelClearAttempts {
			break
		}
		timer := time.NewTimer(retryPeriod)
		select {
		case <-ctx.Done():
			timer.Stop()
			// Keep retrying with a detached context so demotion still clears the label.
			ctx = context.Background()
		case <-timer.C:
		}
	}
	slogs.Logr.Fatal("clearing leader label",
		"error", err,
		"reason", reason,
		"namespace", namespace,
		"pod", podName,
	)
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
