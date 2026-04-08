package refresh

import (
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

	rebuild, err := NeedsEnrichmentRebuild("unused", sourceFile, currentLogFiles, func(string) (model.EnrichmentManifest, error) {
		return model.NewEnrichmentManifest(sourceFile, []model.SourceFile{{
			RelPath:  "2026-03-29.jsonl",
			SizeByte: 20,
			ModTime:  time.Unix(2, 0).UTC(),
		}}), nil
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

	rebuild, err := NeedsEnrichmentRebuild("unused", sourceFile, nil, func(string) (model.EnrichmentManifest, error) {
		manifest := model.NewEnrichmentManifest(sourceFile, nil)
		manifest.LogicVersion--
		return manifest, nil
	})
	assert.NilError(t, err)
	assert.Assert(t, rebuild)
}
