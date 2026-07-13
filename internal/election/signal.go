package election

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/chia-network/go-modules/pkg/slogs"
	"golang.org/x/sys/unix"
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

func (c Config) demoteSignal() syscall.Signal {
	if c.OnStoppedLeadingSignal == 0 {
		return syscall.SIGTERM
	}
	return c.OnStoppedLeadingSignal
}

// ParseSignal converts names like SIGTERM/TERM/15 into a signal.
func ParseSignal(s string) (syscall.Signal, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	s = strings.TrimPrefix(s, "SIG")
	switch s {
	case "", "TERM":
		return syscall.SIGTERM, nil
	case "INT":
		return syscall.SIGINT, nil
	case "HUP":
		return syscall.SIGHUP, nil
	case "KILL":
		return syscall.SIGKILL, nil
	case "QUIT":
		return syscall.SIGQUIT, nil
	case "USR1":
		return syscall.SIGUSR1, nil
	case "USR2":
		return syscall.SIGUSR2, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("unknown signal %q", s)
	}
	return syscall.Signal(n), nil
}

// signalOnStoppedLeading sends the configured signal to matching co-processes.
// Returns true if at least one signal was successfully delivered.
func signalOnStoppedLeading(cfg Config) bool {
	match := strings.TrimSpace(cfg.OnStoppedLeadingProcess)
	if match == "" {
		return false
	}

	sig := cfg.demoteSignal()
	selfTGID := threadGroupID("self")
	pids, err := findPIDsByCmdline(match)
	if err != nil {
		slogs.Logr.Error("scanning processes for demote signal",
			"error", err,
			"match", match,
		)
		return false
	}

	targets := pids[:0]
	for _, pid := range pids {
		if selfTGID > 0 && pid == selfTGID {
			continue
		}
		targets = append(targets, pid)
	}
	if len(targets) == 0 {
		slogs.Logr.Error("no processes matched for demote signal",
			"match", match,
			"signal", sig.String(),
			"self_tgid", selfTGID,
			"matched_only_self", len(pids) > 0,
		)
		return false
	}

	delivered := false
	for _, pid := range targets {
		if err := unix.Kill(pid, sig); err != nil {
			slogs.Logr.Error("sending demote signal",
				"error", err,
				"pid", pid,
				"signal", sig.String(),
				"match", match,
			)
			continue
		}
		delivered = true
		slogs.Logr.Info("sent demote signal",
			"pid", pid,
			"signal", sig.String(),
			"match", match,
		)
	}
	return delivered
}

func findPIDsByCmdline(substr string) ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}

	// /proc/*/cmdline is NUL-separated args; treat spaces in the match like
	// argument boundaries (same idea as pgrep -f with a multi-word pattern).
	needle := cmdlineNeedle(substr)
	var pids []int
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(ent.Name())
		if err != nil {
			continue
		}
		// /proc may expose thread TIDs; only signal thread-group leaders once.
		if !isThreadGroupLeader(ent.Name()) {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", ent.Name(), "cmdline"))
		if err != nil {
			continue
		}
		if !bytes.Contains(cmdline, needle) {
			continue
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

func cmdlineNeedle(substr string) []byte {
	fields := strings.Fields(substr)
	if len(fields) == 0 {
		return nil
	}
	return []byte(strings.Join(fields, "\x00"))
}

// isThreadGroupLeader reports whether /proc/<id> is a process (TGID) rather than
// a non-leader thread TID.
func isThreadGroupLeader(procID string) bool {
	pid, tgid := pidAndTgid(procID)
	return pid > 0 && pid == tgid
}

func threadGroupID(procID string) int {
	_, tgid := pidAndTgid(procID)
	return tgid
}

func pidAndTgid(procID string) (pid, tgid int) {
	data, err := os.ReadFile(filepath.Join("/proc", procID, "status"))
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		switch {
		case strings.HasPrefix(line, "Pid:"):
			pid, _ = strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "Pid:")))
		case strings.HasPrefix(line, "Tgid:"):
			tgid, _ = strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "Tgid:")))
		}
	}
	return pid, tgid
}
