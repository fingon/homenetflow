package parquetui

import (
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

const helperProcessEnvKey = "GO_WANT_PARQUETUI_HELPER_PROCESS"

func TestProcessHelper(_ *testing.T) {
	if os.Getenv(helperProcessEnvKey) != "1" {
		return
	}

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM)
	<-signalChannel
	os.Exit(0)
}

func TestConfigValidateRequiresPIDFileForReplaceRunning(t *testing.T) {
	cfg := Config{
		ReplaceRunning: true,
		SrcParquetPath: "data/parquet",
		ReloadInterval: time.Minute,
		Port:           8080,
	}

	err := cfg.Validate()
	assert.ErrorContains(t, err, "pid file is required")
}

func TestReplaceRunningProcessStopsExistingProcess(t *testing.T) {
	t.Parallel()

	cmd := startHelperProcess(t)
	pidFile := filepath.Join(t.TempDir(), "parquetflowui.pid")
	assert.NilError(t, writePIDFile(pidFile, cmd.Process.Pid))

	waitChannel := make(chan error, 1)
	go func() {
		waitChannel <- cmd.Wait()
	}()

	guard, err := acquireProcessGuard(pidFile)
	assert.NilError(t, err)
	defer func() {
		assert.NilError(t, guard.Close())
	}()

	assert.NilError(t, guard.replaceRunningProcess())

	select {
	case <-waitChannel:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for helper process exit")
	}
}

func TestReplaceRunningProcessRemovesStalePIDFile(t *testing.T) {
	t.Parallel()

	pidFile := filepath.Join(t.TempDir(), "parquetflowui.pid")
	assert.NilError(t, os.WriteFile(pidFile, []byte("999999\n"), 0o600))

	guard, err := acquireProcessGuard(pidFile)
	assert.NilError(t, err)
	defer func() {
		assert.NilError(t, guard.Close())
	}()

	assert.NilError(t, guard.replaceRunningProcess())

	_, statErr := os.Stat(pidFile)
	assert.Assert(t, os.IsNotExist(statErr))
}

func TestRemovePIDFileIfCurrentKeepsNewerPID(t *testing.T) {
	t.Parallel()

	pidFile := filepath.Join(t.TempDir(), "parquetflowui.pid")
	assert.NilError(t, os.WriteFile(pidFile, []byte("456\n"), 0o600))

	assert.NilError(t, removePIDFileIfCurrent(pidFile, 123))

	pidBytes, err := os.ReadFile(pidFile)
	assert.NilError(t, err)
	assert.Equal(t, string(pidBytes), "456\n")
}

func startHelperProcess(t *testing.T) *exec.Cmd {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=TestProcessHelper")
	cmd.Env = append(os.Environ(), helperProcessEnvKey+"=1")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	assert.NilError(t, cmd.Start())
	t.Cleanup(func() {
		if cmd.Process == nil {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_, _ = cmd.Process.Wait()
	})

	return cmd
}

func TestReadPIDFileRejectsInvalidData(t *testing.T) {
	t.Parallel()

	pidFile := filepath.Join(t.TempDir(), "parquetflowui.pid")
	assert.NilError(t, os.WriteFile(pidFile, []byte("abc\n"), 0o600))

	_, err := readPIDFile(pidFile)
	assert.ErrorContains(t, err, errInvalidPIDFile.Error())
}

func TestWritePIDFileWritesCurrentPID(t *testing.T) {
	t.Parallel()

	pidFile := filepath.Join(t.TempDir(), "parquetflowui.pid")
	pid := os.Getpid()

	assert.NilError(t, writePIDFile(pidFile, pid))

	pidBytes, err := os.ReadFile(pidFile)
	assert.NilError(t, err)
	assert.Equal(t, string(pidBytes), strconv.Itoa(pid)+"\n")
}
