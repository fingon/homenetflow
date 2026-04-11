package refresh

import (
	"os"
	"testing"
	"time"

	"github.com/fingon/homenetflow/internal/model"
	"gotest.tools/v3/assert"
)

func TestNeedsEnrichmentRebuildIgnoresDeletedLogs(t *testing.T) {
	t.Parallel()

	sourceFile := model.SourceFile{
		RelPath:  "nfcap_20260329.parquet",
		SizeByte: 10,
		ModTime:  time.Unix(1, 0).UTC(),
	}
	currentLogFiles := []model.SourceFile{}

	rebuild, err := NeedsEnrichmentRebuild("unused", sourceFile, currentLogFiles, false, func(string) (model.EnrichmentManifest, error) {
		return model.NewEnrichmentManifest(sourceFile, []model.SourceFile{{
			RelPath:  "2026-03-29.jsonl",
			SizeByte: 20,
			ModTime:  time.Unix(2, 0).UTC(),
		}}, false), nil
	})
	assert.Assert(t, rebuild || err != nil)
}

func TestNeedsEnrichmentRebuildOnLogicVersionMismatch(t *testing.T) {
	t.Parallel()

	sourceFile := model.SourceFile{
		RelPath:  "nfcap_20260329.parquet",
		SizeByte: 10,
		ModTime:  time.Unix(1, 0).UTC(),
	}

	rebuild, err := NeedsEnrichmentRebuild("unused", sourceFile, nil, false, func(string) (model.EnrichmentManifest, error) {
		manifest := model.NewEnrichmentManifest(sourceFile, nil, false)
		manifest.LogicVersion--
		return manifest, nil
	})
	assert.NilError(t, err)
	assert.Assert(t, rebuild)
}

func TestNeedsEnrichmentRebuildUpgradesSkippedDNSLookups(t *testing.T) {
	t.Parallel()

	sourceFile := model.SourceFile{
		RelPath:  "nfcap_20260329.parquet",
		SizeByte: 10,
		ModTime:  time.Unix(1, 0).UTC(),
	}
	dstPath := t.TempDir() + "/nfcap_20260329.parquet"
	assert.NilError(t, os.WriteFile(dstPath, []byte("fixture"), 0o600))

	rebuild, err := NeedsEnrichmentRebuild(dstPath, sourceFile, nil, false, func(string) (model.EnrichmentManifest, error) {
		return model.NewEnrichmentManifest(sourceFile, nil, true), nil
	})
	assert.NilError(t, err)
	assert.Assert(t, rebuild)
}

func TestNeedsEnrichmentRebuildDoesNotDowngradeFullDNSLookups(t *testing.T) {
	t.Parallel()

	sourceFile := model.SourceFile{
		RelPath:  "nfcap_20260329.parquet",
		SizeByte: 10,
		ModTime:  time.Unix(1, 0).UTC(),
	}
	dstPath := t.TempDir() + "/nfcap_20260329.parquet"
	assert.NilError(t, os.WriteFile(dstPath, []byte("fixture"), 0o600))

	rebuild, err := NeedsEnrichmentRebuild(dstPath, sourceFile, nil, true, func(string) (model.EnrichmentManifest, error) {
		return model.NewEnrichmentManifest(sourceFile, nil, false), nil
	})
	assert.NilError(t, err)
	assert.Assert(t, !rebuild)
}
