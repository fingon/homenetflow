package scan

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestSourceTreeBuckets(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	mustWriteFile(t, filepath.Join(tempDir, "2026", "01", "08", "03", "nfcapd.202601080320"))
	mustWriteFile(t, filepath.Join(tempDir, "2026", "03", "29", "10", "nfcapd.202603291000"))
	mustWriteFile(t, filepath.Join(tempDir, "2026", "03", "30", "03", "nfcapd.202603300320"))

	periodSourceFiles, err := SourceTree(tempDir, time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC))
	assert.NilError(t, err)
	assert.Equal(t, len(periodSourceFiles), 3)
}

func TestSourceTreeIgnoresMalformedHierarchy(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	mustWriteFile(t, filepath.Join(tempDir, "2026", "03", "30", "nfcapd.202603300320"))
	mustWriteFile(t, filepath.Join(tempDir, "2026", "bad", "30", "00", "nfcapd.202603300000"))
	mustWriteFile(t, filepath.Join(tempDir, "misc", "nfcapd.ignored"))

	periodSourceFiles, err := SourceTree(tempDir, time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC))
	assert.NilError(t, err)
	assert.Equal(t, len(periodSourceFiles), 0)
}

func TestSourceTreeRejectsFutureInput(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	mustWriteFile(t, filepath.Join(tempDir, "2026", "03", "31", "00", "nfcapd.202603310000"))

	_, err := SourceTree(tempDir, time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC))
	assert.ErrorContains(t, err, "future-dated input")
}

func mustWriteFile(t *testing.T, path string) {
	t.Helper()

	assert.NilError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	assert.NilError(t, os.WriteFile(path, []byte("fixture"), 0o600))
}
