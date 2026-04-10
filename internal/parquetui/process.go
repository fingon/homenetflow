package parquetui

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gofrs/flock"
)

const (
	processLockRetryDelay      = 100 * time.Millisecond
	processLockTimeout         = 30 * time.Second
	processReplaceGracePeriod  = 5 * time.Second
	processReplaceKillWait     = 2 * time.Second
	processReplacePollInterval = 100 * time.Millisecond
)

type processGuard struct {
	lock    *flock.Flock
	pidFile string
}

func acquireProcessGuard(pidFile string) (*processGuard, error) {
	if pidFile == "" {
		return nil, nil
	}

	lock := flock.New(pidFile + ".lock")
	lockCtx, cancel := context.WithTimeout(context.Background(), processLockTimeout)
	defer cancel()

	locked, err := lock.TryLockContext(lockCtx, processLockRetryDelay)
	if err != nil {
		return nil, fmt.Errorf("lock pid file: %w", err)
	}
	if !locked {
		return nil, fmt.Errorf("lock pid file: timed out waiting for %s", pidFile)
	}

	return &processGuard{
		lock:    lock,
		pidFile: pidFile,
	}, nil
}

func (g *processGuard) Close() error {
	if g == nil || g.lock == nil {
		return nil
	}
	if err := g.lock.Unlock(); err != nil {
		return fmt.Errorf("unlock pid file: %w", err)
	}
	return nil
}

func (g *processGuard) replaceRunningProcess() error {
	if g == nil {
		return nil
	}

	pid, err := readPIDFile(g.pidFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		slog.Warn("remove invalid pid file", "pid_file", g.pidFile, "err", err)
		if removeErr := os.Remove(g.pidFile); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return fmt.Errorf("remove invalid pid file: %w", removeErr)
		}
		return nil
	}

	running, err := processExists(pid)
	if err != nil {
		return fmt.Errorf("check running process: %w", err)
	}
	if !running {
		slog.Debug("remove stale pid file", "pid_file", g.pidFile, "pid", pid)
		if err := os.Remove(g.pidFile); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale pid file: %w", err)
		}
		return nil
	}

	slog.Info("replace running server", "pid_file", g.pidFile, "pid", pid)
	if err := signalProcess(pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("signal previous process: %w", err)
	}
	if err := waitForProcessExit(pid, processReplaceGracePeriod); err == nil {
		return nil
	} else if !errors.Is(err, errProcessStillRunning) {
		return fmt.Errorf("wait for previous process exit: %w", err)
	}

	slog.Warn("force kill previous server", "pid_file", g.pidFile, "pid", pid)
	if err := signalProcess(pid, syscall.SIGKILL); err != nil {
		return fmt.Errorf("force kill previous process: %w", err)
	}
	if err := waitForProcessExit(pid, processReplaceKillWait); err != nil {
		return fmt.Errorf("wait for killed process exit: %w", err)
	}

	return nil
}

func writePIDFile(pidFile string, pid int) error {
	if pidFile == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(pidFile), 0o755); err != nil {
		return fmt.Errorf("create pid file directory: %w", err)
	}

	pidFileTemp, err := os.CreateTemp(filepath.Dir(pidFile), filepath.Base(pidFile)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create pid file temp: %w", err)
	}

	tempPath := pidFileTemp.Name()
	defer func() {
		if removeErr := os.Remove(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			slog.Warn("remove pid file temp failed", "path", tempPath, "err", removeErr)
		}
	}()

	if _, err := pidFileTemp.WriteString(strconv.Itoa(pid) + "\n"); err != nil {
		_ = pidFileTemp.Close()
		return fmt.Errorf("write pid file temp: %w", err)
	}
	if err := pidFileTemp.Close(); err != nil {
		return fmt.Errorf("close pid file temp: %w", err)
	}
	if err := os.Rename(tempPath, pidFile); err != nil {
		return fmt.Errorf("rename pid file temp: %w", err)
	}

	return nil
}

func removePIDFileIfCurrent(pidFile string, pid int) error {
	if pidFile == "" {
		return nil
	}

	guard, err := acquireProcessGuard(pidFile)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := guard.Close(); closeErr != nil {
			slog.Warn("unlock pid file failed", "pid_file", pidFile, "err", closeErr)
		}
	}()

	recordedPID, err := readPIDFile(pidFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		slog.Warn("skip pid file cleanup", "pid_file", pidFile, "err", err)
		return nil
	}
	if recordedPID != pid {
		return nil
	}
	if err := os.Remove(pidFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove pid file: %w", err)
	}

	return nil
}

func readPIDFile(pidFile string) (int, error) {
	pidBytes, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, err
	}

	pidText := strings.TrimSpace(string(pidBytes))
	pid, err := strconv.Atoi(pidText)
	if err != nil {
		return 0, fmt.Errorf("parse pid file %s: %w", pidFile, errInvalidPIDFile)
	}
	if pid <= 0 {
		return 0, fmt.Errorf("parse pid file %s: %w", pidFile, errInvalidPIDFile)
	}

	return pid, nil
}

var (
	errInvalidPIDFile      = errors.New("invalid pid file")
	errProcessStillRunning = errors.New("process still running")
)

func processExists(pid int) (bool, error) {
	err := syscall.Kill(pid, syscall.Signal(0))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	if errors.Is(err, syscall.EPERM) {
		return true, nil
	}
	return false, fmt.Errorf("check pid %d: %w", pid, err)
}

func signalProcess(pid int, signal syscall.Signal) error {
	err := syscall.Kill(pid, signal)
	if err == nil || errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return fmt.Errorf("signal pid %d: %w", pid, err)
}

func waitForProcessExit(pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		running, err := processExists(pid)
		if err != nil {
			return err
		}
		if !running {
			return nil
		}
		if time.Now().After(deadline) {
			return errProcessStillRunning
		}
		time.Sleep(processReplacePollInterval)
	}
}
