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
		IPVersion:   model.IPVersion4,
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
	dstHost := "example.net"

	manifest := model.EnrichmentManifest{
		LogicVersion:   model.EnrichmentLogicVersion,
		SkipDNSLookups: true,
		Source:         model.SourceManifest{Path: "nfcap_202603.parquet", SizeByte: 123, ModTimeNs: 456},
		Version:        model.EnrichmentManifestVersion,
	}
	writer, finalize, err := CreateEnriched(path, manifest)
	assert.NilError(t, err)
	assert.NilError(t, writer.Write(model.FlowRecord{
		Bytes:        1,
		DurationNs:   2,
		DstIP:        "198.51.100.1",
		DstHost:      &dstHost,
		DstIsPrivate: true,
		DstPort:      443,
		IPVersion:    model.IPVersion6,
		Packets:      3,
		Protocol:     6,
		Src2LD:       &src2LD,
		SrcHost:      &srcHost,
		SrcIP:        "192.0.2.1",
		SrcIsPrivate: false,
		SrcPort:      12345,
		SrcTLD:       &srcTLD,
		TimeEndNs:    20,
		TimeStartNs:  10,
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
	assert.Equal(t, *records[0].DstHost, dstHost)
	assert.Equal(t, records[0].IPVersion, model.IPVersion6)
	assert.Assert(t, records[0].DstIsPrivate)
	assert.Assert(t, !records[0].SrcIsPrivate)

	readManifest, err := ReadEnrichmentManifest(path)
	assert.NilError(t, err)
	assert.DeepEqual(t, readManifest, manifest)
}

func TestDNSLookupManifestRoundTripIncludesSkipDNSLookups(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := tempDir + "/dns_lookups_202603.parquet"
	manifest := model.EnrichmentManifest{
		LogicVersion:   model.EnrichmentLogicVersion,
		SkipDNSLookups: true,
		Source:         model.SourceManifest{Path: "nfcap_202603.parquet", SizeByte: 123, ModTimeNs: 456},
		Version:        model.EnrichmentManifestVersion,
	}

	writer, finalize, err := CreateDNSLookups(path, manifest)
	assert.NilError(t, err)
	assert.NilError(t, writer.Write(model.DNSLookupRecord{
		ClientIP:        "192.0.2.1",
		ClientIPVersion: model.IPVersion4,
		Lookups:         1,
		QueryName:       "example.net",
		QueryType:       "A",
		TimeStartNs:     10,
	}))
	assert.NilError(t, finalize())

	readManifest, err := ReadDNSLookupManifest(path)
	assert.NilError(t, err)
	assert.DeepEqual(t, readManifest, manifest)
}

func TestReadFileRoundTripPreservesDirection(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		direction int32
	}{
		{name: "ingress", direction: 0},
		{name: "egress", direction: 1},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			tempDir := t.TempDir()
			path := tempDir + "/nfcap_202603.parquet"

			writer, finalize, err := Create(path, model.RefreshManifest{Version: model.RefreshManifestVersion})
			assert.NilError(t, err)
			assert.NilError(t, writer.Write(model.FlowRecord{
				Bytes:       1,
				Direction:   &testCase.direction,
				DurationNs:  2,
				DstIP:       "198.51.100.1",
				DstPort:     443,
				IPVersion:   model.IPVersion4,
				Packets:     3,
				Protocol:    6,
				SrcIP:       "192.0.2.1",
				SrcPort:     12345,
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
			assert.Assert(t, records[0].Direction != nil)
			assert.Equal(t, *records[0].Direction, testCase.direction)
		})
	}
}
