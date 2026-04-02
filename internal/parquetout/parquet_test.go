package parquetout

import (
	"testing"
	"time"

	"github.com/fingon/homenetflow/internal/model"
	"gotest.tools/v3/assert"
)

func TestManifestRoundTrip(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := tempDir + "/nfcap_202603.parquet"
	manifest := model.NewRefreshManifest([]model.SourceFile{{
		RelPath:  "2026/03/01/00/nfcapd.202603010000",
		SizeByte: 123,
		ModTime:  time.Unix(10, 11).UTC(),
	}})

	writer, finalize, err := Create(path, manifest)
	assert.NilError(t, err)
	assert.NilError(t, writer.Write(model.FlowRecord{
		Bytes:       1,
		DurationNs:  2,
		DstIP:       "198.51.100.1",
		DstPort:     443,
		Packets:     3,
		Protocol:    6,
		SrcIP:       "192.0.2.1",
		SrcPort:     12345,
		TimeEndNs:   20,
		TimeStartNs: 10,
	}))
	assert.NilError(t, finalize())

	readManifest, err := ReadManifest(path)
	assert.NilError(t, err)
	assert.DeepEqual(t, readManifest, manifest)
}

func TestReadFileRoundTripIncludesEnrichmentColumns(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := tempDir + "/nfcap_202603.parquet"
	srcHost := "www.fingon.iki.fi"
	src2LD := "iki.fi"
	srcTLD := "fi"

	writer, finalize, err := CreateEnriched(path, model.EnrichmentManifest{
		Source:  model.SourceManifest{Path: "nfcap_202603.parquet", SizeByte: 123, ModTimeNs: 456},
		Version: 1,
	})
	assert.NilError(t, err)
	assert.NilError(t, writer.Write(model.FlowRecord{
		Bytes:       1,
		DurationNs:  2,
		DstIP:       "198.51.100.1",
		DstPort:     443,
		Packets:     3,
		Protocol:    6,
		Src2LD:      &src2LD,
		SrcHost:     &srcHost,
		SrcIP:       "192.0.2.1",
		SrcPort:     12345,
		SrcTLD:      &srcTLD,
		TimeEndNs:   20,
		TimeStartNs: 10,
	}))
	assert.NilError(t, finalize())

	var records []model.FlowRecord
	assert.NilError(t, ReadFile(path, func(record model.FlowRecord) error {
		records = append(records, record)
		return nil
	}))

	assert.Equal(t, len(records), 1)
	assert.Equal(t, *records[0].SrcHost, srcHost)
	assert.Equal(t, *records[0].Src2LD, src2LD)
	assert.Equal(t, *records[0].SrcTLD, srcTLD)
}
