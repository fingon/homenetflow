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
	srcDeviceID := "mac:aa:bb:cc:dd:ee:01"
	srcDeviceLabel := "laptop.lan"
	srcDeviceSource := "mac"
	dstDeviceID := "host:example.net"
	dstDeviceLabel := "example.net"
	dstDeviceSource := "host"
	inSrcMAC := "aa:bb:cc:dd:ee:01"
	outDstMAC := "aa:bb:cc:dd:ee:02"

	manifest := model.EnrichmentManifest{
		LogicVersion:   model.EnrichmentLogicVersion,
		SkipDNSLookups: true,
		Source:         model.SourceManifest{Path: "nfcap_202603.parquet", SizeByte: 123, ModTimeNs: 456},
		Version:        model.EnrichmentManifestVersion,
	}
	writer, finalize, err := CreateEnriched(path, manifest)
	assert.NilError(t, err)
	assert.NilError(t, writer.Write(model.FlowRecord{
		Bytes:           1,
		DurationNs:      2,
		DstIP:           "198.51.100.1",
		DstDeviceID:     &dstDeviceID,
		DstDeviceLabel:  &dstDeviceLabel,
		DstDeviceSource: &dstDeviceSource,
		DstHost:         &dstHost,
		DstIsPrivate:    true,
		DstPort:         443,
		IPVersion:       model.IPVersion6,
		InSrcMAC:        &inSrcMAC,
		OutDstMAC:       &outDstMAC,
		Packets:         3,
		Protocol:        6,
		SrcDeviceID:     &srcDeviceID,
		SrcDeviceLabel:  &srcDeviceLabel,
		SrcDeviceMAC:    &inSrcMAC,
		SrcDeviceSource: &srcDeviceSource,
		Src2LD:          &src2LD,
		SrcHost:         &srcHost,
		SrcIP:           "192.0.2.1",
		SrcIsPrivate:    false,
		SrcPort:         12345,
		SrcTLD:          &srcTLD,
		TimeEndNs:       20,
		TimeStartNs:     10,
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
	assert.Equal(t, *records[0].SrcDeviceID, srcDeviceID)
	assert.Equal(t, *records[0].SrcDeviceLabel, srcDeviceLabel)
	assert.Equal(t, *records[0].SrcDeviceMAC, inSrcMAC)
	assert.Equal(t, *records[0].SrcDeviceSource, srcDeviceSource)
	assert.Equal(t, *records[0].DstDeviceID, dstDeviceID)
	assert.Equal(t, *records[0].DstDeviceLabel, dstDeviceLabel)
	assert.Equal(t, *records[0].DstDeviceSource, dstDeviceSource)
	assert.Equal(t, *records[0].InSrcMAC, inSrcMAC)
	assert.Equal(t, *records[0].OutDstMAC, outDstMAC)
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
	clientDeviceID := "host:phone.lan"
	clientDeviceLabel := "phone.lan"
	clientDeviceSource := "host"
	assert.NilError(t, writer.Write(model.DNSLookupRecord{
		Answer:             model.DNSAnswerNXDOMAIN,
		ClientDeviceID:     &clientDeviceID,
		ClientDeviceLabel:  &clientDeviceLabel,
		ClientDeviceSource: &clientDeviceSource,
		ClientIP:           "192.0.2.1",
		ClientIPVersion:    model.IPVersion4,
		Lookups:            1,
		QueryName:          "example.net",
		QueryType:          "A",
		TimeStartNs:        10,
	}))
	assert.NilError(t, finalize())

	readManifest, err := ReadDNSLookupManifest(path)
	assert.NilError(t, err)
	assert.DeepEqual(t, readManifest, manifest)

	var records []model.DNSLookupRecord
	assert.NilError(t, ReadDNSLookupFile(path, func(record model.DNSLookupRecord) error {
		records = append(records, record)
		return nil
	}))
	assert.Equal(t, len(records), 1)
	assert.Equal(t, records[0].Answer, model.DNSAnswerNXDOMAIN)
	assert.Equal(t, *records[0].ClientDeviceID, clientDeviceID)
	assert.Equal(t, *records[0].ClientDeviceLabel, clientDeviceLabel)
	assert.Equal(t, *records[0].ClientDeviceSource, clientDeviceSource)
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
