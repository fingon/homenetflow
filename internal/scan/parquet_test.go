package scan

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fingon/homenetflow/internal/model"
	"gotest.tools/v3/assert"
)

func TestFlatParquetTree(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	mustWriteFlatFile(t, filepath.Join(tempDir, "nfcap_202601.parquet"))
	mustWriteFlatFile(t, filepath.Join(tempDir, "nfcap_20260329.parquet"))
	mustWriteFlatFile(t, filepath.Join(tempDir, "nfcap_2026033003.parquet"))
	mustWriteFlatFile(t, filepath.Join(tempDir, "ignored.txt"))

	periodSourceFiles, err := FlatParquetTree(tempDir)
	assert.NilError(t, err)
	assert.Equal(t, len(periodSourceFiles), 3)
	assert.Assert(t, periodSourceFiles[model.Period{Kind: model.PeriodMonth, Start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}].RelPath == "nfcap_202601.parquet")
}

func TestLogTree(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	mustWriteFlatFile(t, filepath.Join(tempDir, "2026-04-01.jsonl"))
	mustWriteFlatFile(t, filepath.Join(tempDir, "2026-04-02.jsonl"))
	mustWriteFlatFile(t, filepath.Join(tempDir, "ignore.log"))

	logFiles, err := LogTree(tempDir)
	assert.NilError(t, err)
	assert.Equal(t, len(logFiles), 2)
	assert.Equal(t, logFiles[0].Period.Start, time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC))
}

func TestPeriodForParquetName(t *testing.T) {
	t.Parallel()

	period, ok, err := periodForParquetName("nfcap_2026033003.parquet")
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.DeepEqual(t, period, model.Period{Kind: model.PeriodHour, Start: time.Date(2026, 3, 30, 3, 0, 0, 0, time.UTC)})
}

func TestPeriodForParquetNameRejectsMalformedInput(t *testing.T) {
	t.Parallel()

	_, ok, err := periodForParquetName("nfcap_2026033.parquet")
	assert.NilError(t, err)
	assert.Assert(t, !ok)
}

func mustWriteFlatFile(t *testing.T, path string) {
	t.Helper()

	assert.NilError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	assert.NilError(t, os.WriteFile(path, []byte("fixture"), 0o600))
}
