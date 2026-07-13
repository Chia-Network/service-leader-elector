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
	// Cap exponential backoff after setLeaderLabel failures at retryPeriod<<4 (32s).
	maxLabelFailShift = 4
	// Wait after demote signal before re-contending so the co-process can exit
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
	// consecutiveLabelFails backs off re-entry after setLeaderLabel failures so
	// permanent errors (e.g. RBAC) do not tight-loop against the API server.
	var consecutiveLabelFails atomic.Int32
	// demoteSignaled means this term sent a co-process demote signal; re-entry
	// must wait so we do not reclaim leadership while that process is restarting.
	var demoteSignaled atomic.Bool

	// term is the in-flight election round. WaitGroup.Add(1) happens on this
	// goroutine before RunOrDie so Wait cannot race a zero-counter Add from the
	// OnStartedLeading goroutine.
	var term atomic.Pointer[electionTerm]

	lec := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leCtx context.Context) {
				t := term.Load()
				t.started.Store(true)
				defer t.finish()

				// Demotion cleanup: signal co-process even if label clear Fatals.
				defer func() {
					wasLeader := heldLeadership.Swap(false)
					leading.Store(false)

					clearErr := clearLeaderLabelRetries(context.Background(), client, namespace, identity, "after leading")
					if wasLeader && signalOnStoppedLeading(cfg) {
						demoteSignaled.Store(true)
					}
					if clearErr != nil && wasLeader {
						// Only Fatal if we actually held leadership — a stale
						// label is dangerous. If we never set the label
						// (wasLeader=false), let the retry loop handle it.
						slogs.Logr.Fatal("clearing leader label",
							"error", clearErr,
							"reason", "after leading",
							"namespace", namespace,
							"pod", identity,
						)
					} else if clearErr != nil {
						slogs.Logr.Error("clearing leader label (never held leadership, will retry)",
							"error", clearErr,
							"namespace", namespace,
							"pod", identity,
						)
					}
				}()

				// Set the Service selector label before advertising leadership.
				// On failure, cancel only this term so the outer loop can retry.
				if err := setLeaderLabel(leCtx, client, namespace, identity); err != nil {
					fails := consecutiveLabelFails.Add(1)
					slogs.Logr.Error("setting leader label",
						"error", err,
						"namespace", namespace,
						"pod", identity,
						"consecutive_failures", fails,
					)
					t.cancel()
					return
				}
				consecutiveLabelFails.Store(0)
				leading.Store(true)
				heldLeadership.Store(true)

				slogs.Logr.Info("became leader",
					"lease", cfg.LeaseName,
					"namespace", namespace,
					"identity", identity,
				)
				<-leCtx.Done()
			},
			OnStoppedLeading: func() {
				t := term.Load()
				// Run cancels OnStartedLeading then calls this without waiting.
				// Wait for that callback's demotion cleanup; do not repeat it
				// (redundant clear can Fatal after the first already succeeded).
				waitForElectionTerm(ctx, t)
				if t != nil {
					t.wg.Wait()
				}

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

		termCtx, termCancel := context.WithCancel(ctx)
		t := newElectionTerm(termCancel)
		term.Store(t)

		leaderelection.RunOrDie(termCtx, lec)
		waitForElectionTerm(ctx, t)
		t.wg.Wait()
		termCancel()

		if ctx.Err() != nil {
			return
		}

		if demoteSignaled.Swap(false) {
			slogs.Logr.Info("backing off before re-entering election after demote signal",
				"backoff", demoteSignalBackoff,
				"lease", cfg.LeaseName,
			)
			timer := time.NewTimer(demoteSignalBackoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		} else if fails := consecutiveLabelFails.Load(); fails > 0 {
			shift := fails - 1
			if shift > maxLabelFailShift {
				shift = maxLabelFailShift
			}
			backoff := retryPeriod << shift
			slogs.Logr.Warn("backing off before re-entering election after label failure",
				"backoff", backoff,
				"consecutive_failures", fails,
				"lease", cfg.LeaseName,
			)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}

		slogs.Logr.Info("lease lost; re-entering leader election",
			"lease", cfg.LeaseName,
			"namespace", namespace,
			"identity", identity,
		)
	}
}

// electionTerm synchronizes one client-go Run() cycle with OnStartedLeading cleanup.
type electionTerm struct {
	wg         sync.WaitGroup
	finishOnce sync.Once
	started    atomic.Bool
	cancel     context.CancelFunc
}

func newElectionTerm(cancel context.CancelFunc) *electionTerm {
	t := &electionTerm{cancel: cancel}
	t.wg.Add(1)
	return t
}

func (t *electionTerm) finish() {
	t.finishOnce.Do(t.wg.Done)
}

// waitForElectionTerm blocks until OnStartedLeading has claimed the term or it
// is clear the callback will never run (acquire failed), then ensures finish().
func waitForElectionTerm(ctx context.Context, t *electionTerm) {
	if t == nil {
		return
	}
	deadline := time.Now().Add(retryPeriod)
	for !t.started.Load() {
		if ctx.Err() != nil {
			t.finish()
			return
		}
		if time.Now().After(deadline) {
			break
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.finish()
			return
		case <-timer.C:
		}
	}
	if !t.started.Load() {
		// OnStartedLeading never ran (or did not claim in time); release Add(1).
		t.finish()
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

// clearLeaderLabelRetries retries clearing the leader label and returns the last error.
func clearLeaderLabelRetries(ctx context.Context, client kubernetes.Interface, namespace, podName, reason string) error {
	var err error
	for attempt := 1; attempt <= labelClearAttempts; attempt++ {
		err = clearLeaderLabel(ctx, client, namespace, podName)
		if err == nil {
			return nil
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
	return err
}

// mustClearLeaderLabel retries clearing the leader label and Fatals if it cannot,
// so a former leader cannot keep receiving traffic via the leader Service.
func mustClearLeaderLabel(ctx context.Context, client kubernetes.Interface, namespace, podName, reason string) {
	if err := clearLeaderLabelRetries(ctx, client, namespace, podName, reason); err != nil {
		slogs.Logr.Fatal("clearing leader label",
			"error", err,
			"reason", reason,
			"namespace", namespace,
			"pod", podName,
		)
	}
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
