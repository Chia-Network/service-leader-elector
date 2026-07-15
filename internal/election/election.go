package election

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
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

// Config controls leader election and optional demote signaling.
type Config struct {
	LeaseName string

	// OnStoppedLeadingProcess, if non-empty, enables signaling processes whose
	// /proc/<pid>/cmdline contains this substring when this instance stops leading.
	OnStoppedLeadingProcess string
	// OnStoppedLeadingSignal is the signal to send (default SIGTERM).
	OnStoppedLeadingSignal syscall.Signal
}

const (
	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 2 * time.Second

	// Cap exponential backoff for label-clear retries at retryPeriod<<4 (32s).
	maxLabelFailShift = 4
	// Wait after demotion before re-contending so the co-process can exit
	// and restart before this pod may become leader again.
	demoteSignalBackoff = leaseDuration

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
	// Failure is fatal: a leftover leader=true would keep this pod in the leader Service
	// and there is no retry loop to fall back on at startup.
	mustClearLeaderLabel(ctx, client, namespace, identity)

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

	for {
		if ctx.Err() != nil {
			return
		}

		done := make(chan struct{})
		termCtx, termCancel := context.WithCancel(ctx)

		// Set in OnStartedLeading's defer when shutdown occurs while leading.
		// Signals the main loop to wait for a new leader before clearing the label,
		// so the leader Service never has zero endpoints during a rollout.
		var needsDeferredCleanup bool

		lec := leaderelection.LeaderElectionConfig{
			Lock:            lock,
			LeaseDuration:   leaseDuration,
			RenewDeadline:   renewDeadline,
			RetryPeriod:     retryPeriod,
			ReleaseOnCancel: true,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(leCtx context.Context) {
					defer close(done)

					var labelSet bool
					defer func() {
						leading.Store(false)
						if !labelSet {
							return
						}
						// On shutdown (SIGTERM), keep the label so the leader
						// Service retains endpoints while another pod acquires
						// the lease. The main loop handles cleanup after
						// waiting for a new leader.
						if ctx.Err() != nil {
							needsDeferredCleanup = true
							return
						}
						clearLeaderLabelUntilSuccess(ctx, client, namespace, identity)
						signalOnStoppedLeading(cfg)
					}()

					if err := setLeaderLabel(leCtx, client, namespace, identity); err != nil {
						slogs.Logr.Error("setting leader label",
							"error", err,
							"namespace", namespace,
							"pod", identity,
						)
						// Cancel the term so the lease is released (ReleaseOnCancel)
						// and other candidates can acquire it.
						termCancel()
						return
					}
					labelSet = true
					leading.Store(true)

					slogs.Logr.Info("became leader",
						"lease", cfg.LeaseName,
						"namespace", namespace,
						"identity", identity,
					)
					<-leCtx.Done()
				},
				OnStoppedLeading: func() {
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

		leaderelection.RunOrDie(termCtx, lec)

		// On shutdown, wait for OnStartedLeading's defer to finish so
		// needsDeferredCleanup is set before we read it. The defer is fast
		// on shutdown (just sets a bool), so the timeout handles the case
		// where OnStartedLeading never ran this term.
		if ctx.Err() != nil {
			select {
			case <-done:
			case <-time.After(retryPeriod):
			}
			termCancel()

			if needsDeferredCleanup {
				slogs.Logr.Info("lease resigned, waiting for new leader before clearing label",
					"lease", cfg.LeaseName,
					"timeout", leaseDuration,
				)
				if waitForNewLeader(client, namespace, cfg.LeaseName, identity, leaseDuration) {
					slogs.Logr.Info("new leader confirmed, clearing label",
						"lease", cfg.LeaseName,
					)
				} else {
					slogs.Logr.Warn("timed out waiting for new leader, clearing label anyway",
						"lease", cfg.LeaseName,
						"timeout", leaseDuration,
					)
				}
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				clearLeaderLabelUntilSuccess(cleanupCtx, client, namespace, identity)
				cleanupCancel()
				signalOnStoppedLeading(cfg)
			}
			return
		}

		wasLeader := waitForDone(ctx, done)
		termCancel()

		if wasLeader {
			slogs.Logr.Info("backing off before re-entering election after demotion",
				"backoff", demoteSignalBackoff,
				"lease", cfg.LeaseName,
			)
			sleepWithContext(ctx, demoteSignalBackoff)
		} else {
			sleepWithContext(ctx, retryPeriod)
		}

		slogs.Logr.Info("re-entering leader election",
			"lease", cfg.LeaseName,
			"namespace", namespace,
			"identity", identity,
		)
	}
}

// waitForDone blocks until OnStartedLeading finishes (done is closed) or a
// timeout elapses indicating OnStartedLeading never ran this term.
func waitForDone(ctx context.Context, done <-chan struct{}) (wasLeader bool) {
	select {
	case <-done:
		return true
	case <-time.After(retryPeriod):
		return false
	case <-ctx.Done():
		return false
	}
}

// waitForNewLeader polls the Lease until a different pod holds it or timeout
// expires. Returns true if a new leader was observed.
func waitForNewLeader(client kubernetes.Interface, namespace, leaseName, selfIdentity string, timeout time.Duration) bool {
	deadline := time.After(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			return false
		case <-ticker.C:
			lease, err := client.CoordinationV1().Leases(namespace).Get(
				context.Background(), leaseName, metav1.GetOptions{},
			)
			if err != nil {
				slogs.Logr.Debug("polling lease for new leader", "error", err)
				continue
			}
			if lease.Spec.HolderIdentity != nil &&
				*lease.Spec.HolderIdentity != "" &&
				*lease.Spec.HolderIdentity != selfIdentity {
				return true
			}
		}
	}
}

// sleepWithContext blocks for d or until ctx is cancelled.
func sleepWithContext(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	select {
	case <-ctx.Done():
		timer.Stop()
	case <-timer.C:
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

// clearLeaderLabelUntilSuccess retries clearing the leader label with capped
// exponential backoff until it succeeds or ctx is cancelled. On context
// cancellation, one last-ditch attempt is made with a background context.
func clearLeaderLabelUntilSuccess(ctx context.Context, client kubernetes.Interface, namespace, podName string) {
	for attempt := 0; ; attempt++ {
		err := clearLeaderLabel(ctx, client, namespace, podName)
		if err == nil {
			return
		}
		slogs.Logr.Warn("clearing leader label, will retry",
			"error", err,
			"attempt", attempt+1,
			"namespace", namespace,
			"pod", podName,
		)
		shift := attempt
		if shift > maxLabelFailShift {
			shift = maxLabelFailShift
		}
		backoff := retryPeriod << shift
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			// Last-ditch attempt so the label doesn't outlive the process.
			_ = clearLeaderLabel(context.Background(), client, namespace, podName)
			return
		case <-timer.C:
		}
	}
}

// mustClearLeaderLabel clears a potentially stale leader label at startup.
// Retries a bounded number of times and Fatals on failure, since at startup
// there is no retry loop to fall back on.
func mustClearLeaderLabel(ctx context.Context, client kubernetes.Interface, namespace, podName string) {
	const attempts = 5
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		err = clearLeaderLabel(ctx, client, namespace, podName)
		if err == nil {
			return
		}
		slogs.Logr.Warn("clearing stale leader label at startup",
			"error", err,
			"attempt", attempt,
			"namespace", namespace,
			"pod", podName,
		)
		if attempt == attempts {
			break
		}
		sleepWithContext(ctx, retryPeriod)
	}
	slogs.Logr.Fatal("clearing leader label at startup",
		"error", err,
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
