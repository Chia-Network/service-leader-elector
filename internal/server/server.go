package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/chia-network/go-modules/pkg/slogs"
	"github.com/chia-network/service-leader-elector/internal/election"
)

// Run starts an HTTP status server (GET /leader for debugging).
// Service routing uses the leader pod label, not this endpoint.
// It blocks until ctx is cancelled, then shuts down gracefully.
func Run(ctx context.Context, port int) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/leader", leaderHandler)

	addr := fmt.Sprintf(":%d", port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		slogs.Logr.Info("starting status server", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutting down status server: %w", err)
		}
		return nil
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("status server failed: %w", err)
		}
		return nil
	}
}

func leaderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if election.IsLeader() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("leader\n"))
		return
	}

	http.Error(w, "not leader", http.StatusServiceUnavailable)
}
