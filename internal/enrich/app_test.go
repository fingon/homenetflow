package enrich

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fingon/homenetflow/internal/model"
	"github.com/fingon/homenetflow/internal/parquetout"
	"gotest.tools/v3/assert"
)

func TestRunEnrichesParquetFromLogs(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()

	stubReverseLookup(t, nil)

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, model.FlowRecord{
		SrcIP:       "192.0.2.10",
		DstIP:       "198.51.100.20",
		SrcPort:     123,
		DstPort:     443,
		IPVersion:   model.IPVersion4,
		Protocol:    6,
		Packets:     1,
		Bytes:       2,
		TimeStartNs: time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC).UnixNano(),
		TimeEndNs:   time.Date(2026, 4, 1, 12, 30, 1, 0, time.UTC).UnixNano(),
		DurationNs:  int64(time.Second),
	})

	logPath := filepath.Join(srcLogDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"192.0.2.10\\\"],\\\"query_name\\\":\\\"www.fingon.iki.fi\\\",\\\"timestamp_end\\\":\\\"2026-04-01T12:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T12:00:00Z\"}\n" +
		"{\"line\":\"{\\\"message\\\":\\\"1 192.168.1.1/123 reply example.net is 198.51.100.20\\\"}\",\"timestamp\":\"2026-04-01T12:10:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, *rows[0].SrcHost, "www.fingon.iki.fi")
	assert.Equal(t, *rows[0].Src2LD, "iki.fi")
	assert.Equal(t, *rows[0].SrcTLD, "fi")
	assert.Equal(t, *rows[0].DstHost, "example.net")
	assert.Equal(t, *rows[0].Dst2LD, "example.net")
	assert.Equal(t, *rows[0].DstTLD, "net")
	assert.Assert(t, !rows[0].SrcIsPrivate)
	assert.Assert(t, !rows[0].DstIsPrivate)
}

func TestRunWritesDNSLookupParquet(t *testing.T) {
	t.Parallel()

	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())

	logPath := filepath.Join(srcLogDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"NXDOMAIN\\\"],\\\"client_ip\\\":\\\"192.168.1.10\\\",\\\"query_name\\\":\\\"Missing.Example.\\\",\\\"query_type\\\":\\\"A\\\",\\\"timestamp_end\\\":\\\"2026-04-01T12:30:00Z\\\"}\",\"timestamp\":\"2026-04-01T12:30:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SkipDNSLookups: true,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	var records []model.DNSLookupRecord
	assert.NilError(t, parquetout.ReadDNSLookupFile(filepath.Join(dstDir, "dns_lookups_2026040112.parquet"), func(record model.DNSLookupRecord) error {
		records = append(records, record)
		return nil
	}))

	assert.Equal(t, len(records), 1)
	assert.Equal(t, records[0].Answer, model.DNSAnswerNXDOMAIN)
	assert.Equal(t, records[0].ClientIP, "192.168.1.10")
	assert.Equal(t, records[0].QueryName, "missing.example")
	assert.Equal(t, records[0].QueryType, "A")
	assert.Equal(t, records[0].Lookups, int64(1))
	assert.Assert(t, records[0].ClientIsPrivate)
}

func TestRunUsesLocalIPv4ReverseDNSForRFC1918Addresses(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, model.FlowRecord{
		SrcIP:       "192.168.1.10",
		DstIP:       "10.0.0.2",
		SrcPort:     123,
		DstPort:     443,
		IPVersion:   model.IPVersion4,
		Protocol:    6,
		Packets:     1,
		Bytes:       2,
		TimeStartNs: time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC).UnixNano(),
		TimeEndNs:   time.Date(2026, 4, 1, 12, 30, 1, 0, time.UTC).UnixNano(),
		DurationNs:  int64(time.Second),
	})

	var lookupCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCount.Add(1)
		return []string{"phone.lan."}, nil
	})

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, *rows[0].SrcHost, "phone.lan")
	assert.Equal(t, *rows[0].Src2LD, "phone")
	assert.Equal(t, *rows[0].SrcTLD, localEntityTLD)
	assert.Equal(t, *rows[0].DstHost, "phone.lan")
	assert.Equal(t, lookupCount.Load(), int32(2))
}

func TestRunUsesLocalIPv4PlaceholderWhenRFC1918ReverseDNSMisses(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()
	stubReverseLookup(t, nil)

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, model.FlowRecord{
		SrcIP:       "192.168.1.10",
		DstIP:       "198.51.100.20",
		SrcPort:     123,
		DstPort:     443,
		IPVersion:   model.IPVersion4,
		Protocol:    6,
		Packets:     1,
		Bytes:       2,
		TimeStartNs: time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC).UnixNano(),
		TimeEndNs:   time.Date(2026, 4, 1, 12, 30, 1, 0, time.UTC).UnixNano(),
		DurationNs:  int64(time.Second),
	})

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, *rows[0].SrcHost, localIPv4Host)
	assert.Equal(t, *rows[0].Src2LD, localIPv4Host)
	assert.Equal(t, *rows[0].SrcTLD, localIPv4Host)
}

func TestRunUsesLocalGroupsForNamedRFC1918Addresses(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()
	stubReverseLookup(t, nil)

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, model.FlowRecord{
		SrcIP:       "192.168.1.10",
		DstIP:       "198.51.100.20",
		SrcPort:     123,
		DstPort:     443,
		IPVersion:   model.IPVersion4,
		Protocol:    6,
		Packets:     1,
		Bytes:       2,
		TimeStartNs: time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC).UnixNano(),
		TimeEndNs:   time.Date(2026, 4, 1, 12, 30, 1, 0, time.UTC).UnixNano(),
		DurationNs:  int64(time.Second),
	})

	logPath := filepath.Join(srcLogDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"192.168.1.10\\\"],\\\"query_name\\\":\\\"phone.lan\\\",\\\"timestamp_end\\\":\\\"2026-04-01T12:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T12:00:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, *rows[0].SrcHost, "phone.lan")
	assert.Equal(t, *rows[0].Src2LD, "phone")
	assert.Equal(t, *rows[0].SrcTLD, localEntityTLD)
	assert.Assert(t, rows[0].SrcIsPrivate)
}

func TestRunDoesNotRebuildWhenRelevantLogIsDeleted(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()
	stubReverseLookup(t, nil)

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())

	logPath := filepath.Join(srcLogDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"192.0.2.10\\\"],\\\"query_name\\\":\\\"www.fingon.iki.fi\\\",\\\"timestamp_end\\\":\\\"2026-04-01T12:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T12:00:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))
	outputPath := filepath.Join(dstDir, "nfcap_2026040112.parquet")
	firstInfo, err := os.Stat(outputPath)
	assert.NilError(t, err)

	assert.NilError(t, os.Remove(logPath))
	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))

	secondInfo, err := os.Stat(outputPath)
	assert.NilError(t, err)
	assert.Equal(t, secondInfo.ModTime(), firstInfo.ModTime())
}

func TestRunDeletesOutputWhenSourceParquetDisappears(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()
	stubReverseLookup(t, nil)

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())
	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))

	assert.NilError(t, os.Remove(sourcePath))
	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))
	_, err := os.Stat(filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Assert(t, os.IsNotExist(err))
}

func TestRunCachesReverseLookupsAcrossRuns(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())

	var lookupCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCount.Add(1)
		return []string{"router.home.arpa."}, nil
	})

	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))
	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))

	assert.Equal(t, lookupCount.Load(), int32(2))
	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, *rows[0].SrcHost, "router.home.arpa")
}

func TestRunIgnoresMalformedPTRResponses(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())

	stubReverseLookup(t, func(ipAddress string) ([]string, error) {
		if ipAddress == "192.0.2.10" {
			return nil, &net.DNSError{
				Err:  invalidPTRNameErrorFragment,
				Name: ipAddress,
			}
		}

		return []string{"example.net."}, nil
	})

	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, len(rows), 1)
	assert.Assert(t, rows[0].SrcHost == nil)
	assert.Assert(t, rows[0].Src2LD == nil)
	assert.Assert(t, rows[0].SrcTLD == nil)
	assert.Equal(t, *rows[0].DstHost, "example.net")
	assert.Equal(t, *rows[0].Dst2LD, "example.net")
	assert.Equal(t, *rows[0].DstTLD, "net")
}

func TestRunSkipsLiveDNSLookupsButUsesExistingCache(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())

	cachePath := filepath.Join(dstDir, reverseDNSCacheFilename)
	cacheContents := []byte("{\"host\":\"cached.example.net\",\"ip\":\"192.0.2.10\",\"resolvedAtNs\":1775044800000000000}\n")
	assert.NilError(t, os.WriteFile(cachePath, cacheContents, 0o600))

	var lookupCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCount.Add(1)
		return []string{"live.example.net."}, nil
	})

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SkipDNSLookups: true,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, *rows[0].SrcHost, "cached.example.net")
	assert.Assert(t, rows[0].DstHost == nil)
	assert.Equal(t, lookupCount.Load(), int32(0))
}

func TestRunSeedsReverseCacheFromStructuredLogsBeforePTR(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())

	logPath := filepath.Join(srcLogDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"192.0.2.10\\\"],\\\"query_name\\\":\\\"seeded.example.net\\\",\\\"query_type\\\":\\\"A\\\",\\\"timestamp_end\\\":\\\"2026-04-01T08:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T08:00:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	var lookupCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCount.Add(1)
		return []string{"live.example.net."}, nil
	})

	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, *rows[0].SrcHost, "seeded.example.net")
	assert.Equal(t, *rows[0].Src2LD, "example.net")
	assert.Equal(t, *rows[0].SrcTLD, "net")
	assert.Equal(t, *rows[0].DstHost, "live.example.net")
	assert.Equal(t, lookupCount.Load(), int32(1))

	cacheBytes, err := os.ReadFile(filepath.Join(dstDir, reverseDNSCacheFilename))
	assert.NilError(t, err)
	assert.Assert(t, strings.Contains(string(cacheBytes), "{\"host\":\"seeded.example.net\",\"ip\":\"192.0.2.10\",\"resolvedAtNs\":1775046600000000000}"))
}

func TestRunSeedsReverseCacheFromStructuredPTRLogsBeforeLiveLookup(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())

	logPath := filepath.Join(srcLogDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"seeded.example.net.\\\"],\\\"query_name\\\":\\\"10.2.0.192.in-addr.arpa\\\",\\\"query_type\\\":\\\"PTR\\\",\\\"timestamp_end\\\":\\\"2026-04-01T08:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T08:00:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	var lookupCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCount.Add(1)
		return []string{"live.example.net."}, nil
	})

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SkipDNSLookups: true,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, *rows[0].SrcHost, "seeded.example.net")
	assert.Assert(t, rows[0].DstHost == nil)
	assert.Equal(t, lookupCount.Load(), int32(0))

	cacheBytes, err := os.ReadFile(filepath.Join(dstDir, reverseDNSCacheFilename))
	assert.NilError(t, err)
	assert.Assert(t, strings.Contains(string(cacheBytes), "{\"host\":\"seeded.example.net\",\"ip\":\"192.0.2.10\",\"resolvedAtNs\":1775046600000000000}"))
}

func TestRunSeedsNegativeReverseCacheFromStructuredPTRNXDOMAINLogs(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())

	logPath := filepath.Join(srcLogDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"NXDOMAIN\\\"],\\\"query_name\\\":\\\"10.2.0.192.in-addr.arpa\\\",\\\"query_type\\\":\\\"PTR\\\",\\\"timestamp_end\\\":\\\"2026-04-01T08:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T08:00:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	var lookupCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCount.Add(1)
		return []string{"live.example.net."}, nil
	})

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SkipDNSLookups: true,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, len(rows), 1)
	assert.Assert(t, rows[0].SrcHost == nil)
	assert.Assert(t, rows[0].DstHost == nil)
	assert.Equal(t, lookupCount.Load(), int32(0))

	cacheBytes, err := os.ReadFile(filepath.Join(dstDir, reverseDNSCacheFilename))
	assert.NilError(t, err)
	assert.Assert(t, strings.Contains(string(cacheBytes), "{\"ip\":\"192.0.2.10\",\"miss\":true,\"resolvedAtNs\":1775046600000000000}"))
}

func TestRunPromotesNegativeCacheEntryFromLaterLogObservation(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()

	writeSourceParquet(t, filepath.Join(srcParquetDir, "nfcap_2026040112.parquet"), model.FlowRecord{
		SrcIP:       "192.0.2.10",
		DstIP:       "198.51.100.20",
		SrcPort:     123,
		DstPort:     443,
		IPVersion:   model.IPVersion4,
		Protocol:    6,
		Packets:     1,
		Bytes:       2,
		TimeStartNs: time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC).UnixNano(),
		TimeEndNs:   time.Date(2026, 4, 1, 12, 30, 1, 0, time.UTC).UnixNano(),
		DurationNs:  int64(time.Second),
	})

	var lookupCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCount.Add(1)
		return nil, &net.DNSError{IsNotFound: true}
	})

	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))
	assert.Equal(t, lookupCount.Load(), int32(2))

	writeSourceParquet(t, filepath.Join(srcParquetDir, "nfcap_2026040114.parquet"), model.FlowRecord{
		SrcIP:       "192.0.2.10",
		DstIP:       "198.51.100.20",
		SrcPort:     123,
		DstPort:     443,
		IPVersion:   model.IPVersion4,
		Protocol:    6,
		Packets:     1,
		Bytes:       2,
		TimeStartNs: time.Date(2026, 4, 1, 14, 30, 0, 0, time.UTC).UnixNano(),
		TimeEndNs:   time.Date(2026, 4, 1, 14, 30, 1, 0, time.UTC).UnixNano(),
		DurationNs:  int64(time.Second),
	})

	logPath := filepath.Join(srcLogDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"192.0.2.10\\\"],\\\"query_name\\\":\\\"promoted.example.net\\\",\\\"query_type\\\":\\\"A\\\",\\\"timestamp_end\\\":\\\"2026-04-01T13:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T13:00:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))
	assert.Equal(t, lookupCount.Load(), int32(2))

	secondRows := readRows(t, filepath.Join(dstDir, "nfcap_2026040114.parquet"))
	assert.Equal(t, len(secondRows), 1)
	assert.Equal(t, *secondRows[0].SrcHost, "promoted.example.net")

	cacheBytes, err := os.ReadFile(filepath.Join(dstDir, reverseDNSCacheFilename))
	assert.NilError(t, err)
	assert.Assert(t, strings.Contains(string(cacheBytes), "{\"ip\":\"192.0.2.10\",\"miss\":true,\"resolvedAtNs\":1775046600000000000}"))
	assert.Assert(t, strings.Contains(string(cacheBytes), "{\"host\":\"promoted.example.net\",\"ip\":\"192.0.2.10\",\"resolvedAtNs\":1775053800000000000}"))
}

func TestRunRebuildsSkippedDNSLookupOutputWhenLiveDNSIsEnabled(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())

	var lookupCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCount.Add(1)
		return []string{"live.example.net."}, nil
	})

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SkipDNSLookups: true,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	outputPath := filepath.Join(dstDir, "nfcap_2026040112.parquet")
	manifest, err := parquetout.ReadEnrichmentManifest(outputPath)
	assert.NilError(t, err)
	assert.Assert(t, manifest.SkipDNSLookups)
	rows := readRows(t, outputPath)
	assert.Equal(t, len(rows), 1)
	assert.Assert(t, rows[0].SrcHost == nil)
	assert.Assert(t, rows[0].DstHost == nil)
	assert.Equal(t, lookupCount.Load(), int32(0))

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	manifest, err = parquetout.ReadEnrichmentManifest(outputPath)
	assert.NilError(t, err)
	assert.Assert(t, !manifest.SkipDNSLookups)
	rows = readRows(t, outputPath)
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, *rows[0].SrcHost, "live.example.net")
	assert.Equal(t, *rows[0].DstHost, "live.example.net")
	assert.Equal(t, lookupCount.Load(), int32(2))
}

func TestRunResolvesOlderIPv6FlowThroughNeighbourMappedIPv4LogName(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()
	stubReverseLookup(t, nil)

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, model.FlowRecord{
		SrcIP:       "2001:db8::10",
		DstIP:       "198.51.100.20",
		SrcPort:     123,
		DstPort:     443,
		IPVersion:   model.IPVersion6,
		Protocol:    6,
		Packets:     1,
		Bytes:       2,
		TimeStartNs: time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC).UnixNano(),
		TimeEndNs:   time.Date(2026, 4, 1, 12, 30, 1, 0, time.UTC).UnixNano(),
		DurationNs:  int64(time.Second),
	})

	logPath := filepath.Join(srcLogDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"192.168.1.10\\\"],\\\"query_name\\\":\\\"mapped.example.net\\\",\\\"timestamp_end\\\":\\\"2026-04-01T12:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T12:00:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	neighbourLogPath := filepath.Join(srcLogDir, "2026-04-10.jsonl")
	neighbourLogContents := []byte("{\"line\":\"{\\\"dst\\\":\\\"192.168.1.10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:00Z\"}\n" +
		"{\"line\":\"{\\\"dst\\\":\\\"2001:db8::10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:01Z\"}\n")
	assert.NilError(t, os.WriteFile(neighbourLogPath, neighbourLogContents, 0o600))

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, *rows[0].SrcHost, "mapped.example.net")
	assert.Equal(t, *rows[0].Src2LD, "mapped")
	assert.Equal(t, *rows[0].SrcTLD, localEntityTLD)
}

func TestRunResolvesOlderIPv6FlowThroughSameFileMACMappedIPv4LogName(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()
	stubReverseLookup(t, nil)

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	srcMAC := "aa:bb:cc:dd:ee:ff"
	writeSourceParquetRecords(t, sourcePath, []model.FlowRecord{
		{
			SrcIP:       "192.168.1.10",
			DstIP:       "198.51.100.20",
			SrcPort:     123,
			DstPort:     443,
			IPVersion:   model.IPVersion4,
			Protocol:    6,
			Packets:     1,
			Bytes:       2,
			InSrcMAC:    &srcMAC,
			TimeStartNs: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC).UnixNano(),
			TimeEndNs:   time.Date(2026, 4, 1, 12, 0, 1, 0, time.UTC).UnixNano(),
			DurationNs:  int64(time.Second),
		},
		{
			SrcIP:       "2001:db8::10",
			DstIP:       "198.51.100.20",
			SrcPort:     123,
			DstPort:     443,
			IPVersion:   model.IPVersion6,
			Protocol:    6,
			Packets:     1,
			Bytes:       2,
			OutDstMAC:   &srcMAC,
			TimeStartNs: time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC).UnixNano(),
			TimeEndNs:   time.Date(2026, 4, 1, 12, 30, 1, 0, time.UTC).UnixNano(),
			DurationNs:  int64(time.Second),
		},
	})

	logPath := filepath.Join(srcLogDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"192.168.1.10\\\"],\\\"query_name\\\":\\\"mapped.example.net\\\",\\\"timestamp_end\\\":\\\"2026-04-01T12:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T12:00:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, len(rows), 2)
	assert.Equal(t, *rows[1].SrcHost, "mapped.example.net")
	assert.Equal(t, *rows[1].Src2LD, "example.net")
	assert.Equal(t, *rows[1].SrcTLD, "net")
}

func TestRunPrefersMACMappingOverNeighbourMapping(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()
	stubReverseLookup(t, nil)

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	macAddress := "aa:bb:cc:dd:ee:ff"
	writeSourceParquetRecords(t, sourcePath, []model.FlowRecord{
		{
			SrcIP:       "192.168.1.20",
			DstIP:       "198.51.100.20",
			SrcPort:     123,
			DstPort:     443,
			IPVersion:   model.IPVersion4,
			Protocol:    6,
			Packets:     1,
			Bytes:       2,
			InSrcMAC:    &macAddress,
			TimeStartNs: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC).UnixNano(),
			TimeEndNs:   time.Date(2026, 4, 1, 12, 0, 1, 0, time.UTC).UnixNano(),
			DurationNs:  int64(time.Second),
		},
		{
			SrcIP:       "2001:db8::10",
			DstIP:       "198.51.100.20",
			SrcPort:     123,
			DstPort:     443,
			IPVersion:   model.IPVersion6,
			Protocol:    6,
			Packets:     1,
			Bytes:       2,
			OutDstMAC:   &macAddress,
			TimeStartNs: time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC).UnixNano(),
			TimeEndNs:   time.Date(2026, 4, 1, 12, 30, 1, 0, time.UTC).UnixNano(),
			DurationNs:  int64(time.Second),
		},
	})

	logPath := filepath.Join(srcLogDir, "2026-04-01.jsonl")
	logContents := []byte(
		"{\"line\":\"{\\\"answers\\\":[\\\"192.168.1.20\\\"],\\\"query_name\\\":\\\"mac.example.net\\\",\\\"timestamp_end\\\":\\\"2026-04-01T12:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T12:00:00Z\"}\n" +
			"{\"line\":\"{\\\"answers\\\":[\\\"192.168.1.10\\\"],\\\"query_name\\\":\\\"neighbour.example.net\\\",\\\"timestamp_end\\\":\\\"2026-04-01T12:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T12:00:00Z\"}\n",
	)
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	neighbourLogPath := filepath.Join(srcLogDir, "2026-04-10.jsonl")
	neighbourLogContents := []byte("{\"line\":\"{\\\"dst\\\":\\\"192.168.1.10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:00Z\"}\n" +
		"{\"line\":\"{\\\"dst\\\":\\\"2001:db8::10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:01Z\"}\n")
	assert.NilError(t, os.WriteFile(neighbourLogPath, neighbourLogContents, 0o600))

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, *rows[1].SrcHost, "mac.example.net")
}

func TestRunResolvesIPv6ThroughNeighbourMappedIPv4ReverseCache(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, model.FlowRecord{
		SrcIP:       "2001:db8::10",
		DstIP:       "2001:db8::30",
		SrcPort:     123,
		DstPort:     443,
		IPVersion:   model.IPVersion6,
		Protocol:    6,
		Packets:     1,
		Bytes:       2,
		TimeStartNs: time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC).UnixNano(),
		TimeEndNs:   time.Date(2026, 4, 1, 12, 30, 1, 0, time.UTC).UnixNano(),
		DurationNs:  int64(time.Second),
	})

	cachePath := filepath.Join(dstDir, reverseDNSCacheFilename)
	cacheContents := []byte("{\"host\":\"cached.example.net\",\"ip\":\"192.168.1.10\",\"resolvedAtNs\":1775044800000000000}\n")
	assert.NilError(t, os.WriteFile(cachePath, cacheContents, 0o600))

	neighbourLogPath := filepath.Join(srcLogDir, "2026-04-10.jsonl")
	neighbourLogContents := []byte("{\"line\":\"{\\\"dst\\\":\\\"192.168.1.10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:00Z\"}\n" +
		"{\"line\":\"{\\\"dst\\\":\\\"2001:db8::10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:01Z\"}\n")
	assert.NilError(t, os.WriteFile(neighbourLogPath, neighbourLogContents, 0o600))

	var lookupCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCount.Add(1)
		return []string{"live.example.net."}, nil
	})

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, *rows[0].SrcHost, localIPv6Host)
	assert.Equal(t, *rows[0].Src2LD, localIPv6Host)
	assert.Equal(t, *rows[0].SrcTLD, localIPv6Host)
	assert.Assert(t, rows[0].SrcIsPrivate)
	assert.Equal(t, lookupCount.Load(), int32(0))
}

func TestRunFallsBackToLocalIPv6ForObservedPrefixWithoutMappedDNS(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()

	var lookupCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCount.Add(1)
		return []string{"live.example.net."}, nil
	})

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, model.FlowRecord{
		SrcIP:       "2001:db8:1:2::20",
		DstIP:       "2001:db8:1:2::30",
		SrcPort:     123,
		DstPort:     443,
		IPVersion:   model.IPVersion6,
		Protocol:    6,
		Packets:     1,
		Bytes:       2,
		TimeStartNs: time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC).UnixNano(),
		TimeEndNs:   time.Date(2026, 4, 1, 12, 30, 1, 0, time.UTC).UnixNano(),
		DurationNs:  int64(time.Second),
	})

	neighbourLogPath := filepath.Join(srcLogDir, "2026-04-10.jsonl")
	neighbourLogContents := []byte("{\"line\":\"{\\\"dst\\\":\\\"2001:db8:1:2::10\\\"}\",\"timestamp\":\"2026-04-10T12:00:01Z\"}\n")
	assert.NilError(t, os.WriteFile(neighbourLogPath, neighbourLogContents, 0o600))

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	rows := readRows(t, filepath.Join(dstDir, "nfcap_2026040112.parquet"))
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, *rows[0].SrcHost, localIPv6Host)
	assert.Equal(t, *rows[0].Src2LD, localIPv6Host)
	assert.Equal(t, *rows[0].SrcTLD, localIPv6Host)
	assert.Assert(t, rows[0].SrcIsPrivate)
	assert.Equal(t, *rows[0].DstHost, localIPv6Host)
	assert.Equal(t, *rows[0].Dst2LD, localIPv6Host)
	assert.Equal(t, *rows[0].DstTLD, localIPv6Host)
	assert.Equal(t, lookupCount.Load(), int32(0))
}

func TestRunMarksObservedLocalIPv6DNSLookupClientPrivate(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()
	stubReverseLookup(t, nil)

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())

	logPath := filepath.Join(srcLogDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"NXDOMAIN\\\"],\\\"client_ip\\\":\\\"2001:db8:1:2::20\\\",\\\"query_name\\\":\\\"Missing.Example.\\\",\\\"query_type\\\":\\\"AAAA\\\",\\\"timestamp_end\\\":\\\"2026-04-01T12:30:00Z\\\"}\",\"timestamp\":\"2026-04-01T12:30:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	neighbourLogPath := filepath.Join(srcLogDir, "2026-04-10.jsonl")
	neighbourLogContents := []byte("{\"line\":\"{\\\"dst\\\":\\\"2001:db8:1:2::10\\\"}\",\"timestamp\":\"2026-04-10T12:00:01Z\"}\n")
	assert.NilError(t, os.WriteFile(neighbourLogPath, neighbourLogContents, 0o600))

	assert.NilError(t, Run(Config{
		DstPath:        dstDir,
		SkipDNSLookups: true,
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	var records []model.DNSLookupRecord
	assert.NilError(t, parquetout.ReadDNSLookupFile(filepath.Join(dstDir, "dns_lookups_2026040112.parquet"), func(record model.DNSLookupRecord) error {
		records = append(records, record)
		return nil
	}))

	assert.Equal(t, len(records), 1)
	assert.Equal(t, records[0].ClientIP, "2001:db8:1:2::20")
	assert.Equal(t, *records[0].ClientHost, localIPv6Host)
	assert.Equal(t, *records[0].Client2LD, localIPv6Host)
	assert.Equal(t, *records[0].ClientTLD, localIPv6Host)
	assert.Assert(t, records[0].ClientIsPrivate)
}

func TestRunDoesNotRebuildOldOutputWhenFutureNeighbourLogChanges(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()
	stubReverseLookup(t, nil)

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())

	logPath := filepath.Join(srcLogDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"192.0.2.10\\\"],\\\"query_name\\\":\\\"www.fingon.iki.fi\\\",\\\"timestamp_end\\\":\\\"2026-04-01T12:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T12:00:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	neighbourLogPath := filepath.Join(srcLogDir, "2026-04-10.jsonl")
	firstNeighbourLogContents := []byte("{\"line\":\"{\\\"dst\\\":\\\"192.0.2.10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:00Z\"}\n")
	assert.NilError(t, os.WriteFile(neighbourLogPath, firstNeighbourLogContents, 0o600))

	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))
	outputPath := filepath.Join(dstDir, "nfcap_2026040112.parquet")
	firstInfo, err := os.Stat(outputPath)
	assert.NilError(t, err)

	secondNeighbourLogContents := []byte(string(firstNeighbourLogContents) + "{\"line\":\"{\\\"dst\\\":\\\"2001:db8::10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:01Z\"}\n")
	assert.NilError(t, os.WriteFile(neighbourLogPath, secondNeighbourLogContents, 0o600))
	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))

	secondInfo, err := os.Stat(outputPath)
	assert.NilError(t, err)
	assert.Equal(t, secondInfo.ModTime(), firstInfo.ModTime())
}

func TestRunReportsRowProgressAcrossStaleJobs(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()
	stubReverseLookup(t, nil)

	writeSourceParquet(t, filepath.Join(srcParquetDir, "nfcap_2026040111.parquet"), sampleEnrichRecord())
	writeSourceParquet(t, filepath.Join(srcParquetDir, "nfcap_2026040112.parquet"), sampleEnrichRecord())

	type progressUpdate struct {
		done  int64
		total int64
	}

	var (
		progressMu sync.Mutex
		updates    []progressUpdate
	)

	assert.NilError(t, Run(Config{
		DstPath: dstDir,
		Progress: func(doneRowCount, totalRowCount int64) {
			progressMu.Lock()
			defer progressMu.Unlock()
			updates = append(updates, progressUpdate{done: doneRowCount, total: totalRowCount})
		},
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	assert.Assert(t, len(updates) >= 2)
	assert.Equal(t, updates[0].done, int64(0))
	assert.Equal(t, updates[0].total, int64(2))
	for index := 1; index < len(updates); index++ {
		assert.Assert(t, updates[index].done >= updates[index-1].done)
		assert.Equal(t, updates[index].total, int64(2))
	}
	assert.Equal(t, updates[len(updates)-1].done, int64(2))
	assert.Equal(t, updates[len(updates)-1].total, int64(2))
}

func TestRunDoesNotReportProgressWhenNothingRebuilds(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()
	stubReverseLookup(t, nil)

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())
	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))

	var progressCalls atomic.Int32
	assert.NilError(t, Run(Config{
		DstPath: dstDir,
		Progress: func(_, _ int64) {
			progressCalls.Add(1)
		},
		SrcLogPath:     srcLogDir,
		SrcParquetPath: srcParquetDir,
	}))

	assert.Equal(t, progressCalls.Load(), int32(0))
}

func TestRunPrunesReverseDNSCacheWhenNothingRebuilds(t *testing.T) {
	srcParquetDir := t.TempDir()
	srcLogDir := t.TempDir()
	dstDir := t.TempDir()
	stubReverseLookup(t, nil)

	sourcePath := filepath.Join(srcParquetDir, "nfcap_2026040112.parquet")
	writeSourceParquet(t, sourcePath, sampleEnrichRecord())
	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))

	cachePath := filepath.Join(dstDir, reverseDNSCacheFilename)
	cacheContents := []byte("{\"host\":\"local.example\",\"ip\":\"192.168.1.10\",\"resolvedAtNs\":1775044800000000000}\n" +
		"{\"host\":\"public.example\",\"ip\":\"192.0.2.10\",\"resolvedAtNs\":1775044800000000000}\n")
	assert.NilError(t, os.WriteFile(cachePath, cacheContents, 0o600))

	assert.NilError(t, Run(Config{DstPath: dstDir, SrcLogPath: srcLogDir, SrcParquetPath: srcParquetDir}))

	prunedContents, err := os.ReadFile(cachePath)
	assert.NilError(t, err)
	assert.Equal(t, string(prunedContents), string(cacheContents))
}

func TestIsPrivateIPAddress(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		ipAddress string
		name      string
		private   bool
	}{
		{name: "ipv4 class a", ipAddress: "10.0.0.1", private: true},
		{name: "ipv4 172 lower bound", ipAddress: "172.16.0.1", private: true},
		{name: "ipv4 172 upper bound", ipAddress: "172.31.255.255", private: true},
		{name: "ipv4 172 public below", ipAddress: "172.15.255.255", private: false},
		{name: "ipv4 172 public above", ipAddress: "172.32.0.1", private: false},
		{name: "ipv4 class c", ipAddress: "192.168.1.10", private: true},
		{name: "ipv4 public", ipAddress: "192.0.2.10", private: false},
		{name: "ipv6 ula", ipAddress: "fd00::1", private: true},
		{name: "ipv6 site local", ipAddress: "fec0::1", private: true},
		{name: "ipv6 link local", ipAddress: "fe80::1", private: true},
		{name: "ipv6 gua", ipAddress: "2001:db8::1", private: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, isPrivateIPAddress(testCase.ipAddress), testCase.private)
		})
	}
}

func stubReverseLookup(t *testing.T, lookup func(string) ([]string, error)) {
	t.Helper()

	previousLookup := reverseLookupAddr
	if lookup == nil {
		reverseLookupAddr = func(string) ([]string, error) {
			return nil, nil
		}
	} else {
		reverseLookupAddr = lookup
	}

	t.Cleanup(func() {
		reverseLookupAddr = previousLookup
	})
}

func sampleEnrichRecord() model.FlowRecord {
	return model.FlowRecord{
		SrcIP:       "192.0.2.10",
		DstIP:       "198.51.100.20",
		SrcPort:     123,
		DstPort:     443,
		IPVersion:   model.IPVersion4,
		Protocol:    6,
		Packets:     1,
		Bytes:       2,
		TimeStartNs: time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC).UnixNano(),
		TimeEndNs:   time.Date(2026, 4, 1, 12, 30, 1, 0, time.UTC).UnixNano(),
		DurationNs:  int64(time.Second),
	}
}

func writeSourceParquet(t *testing.T, path string, record model.FlowRecord) {
	t.Helper()

	writer, finalize, err := parquetout.Create(path, model.RefreshManifest{Version: 1})
	assert.NilError(t, err)
	assert.NilError(t, writer.Write(record))
	assert.NilError(t, finalize())
}

func writeSourceParquetRecords(t *testing.T, path string, records []model.FlowRecord) {
	t.Helper()

	writer, finalize, err := parquetout.Create(path, model.RefreshManifest{Version: 1})
	assert.NilError(t, err)
	assert.NilError(t, writer.WriteBatch(records))
	assert.NilError(t, finalize())
}

func readRows(t *testing.T, path string) []parquetout.Row {
	t.Helper()

	records := make([]parquetout.Row, 0, 4)
	assert.NilError(t, parquetout.ReadFile(path, func(record model.FlowRecord) error {
		records = append(records, parquetout.Row{
			Dst2LD:       record.Dst2LD,
			DstHost:      record.DstHost,
			DstIP:        record.DstIP,
			DstIsPrivate: record.DstIsPrivate,
			InDstMAC:     record.InDstMAC,
			InSrcMAC:     record.InSrcMAC,
			DstPort:      record.DstPort,
			DstTLD:       record.DstTLD,
			IPVersion:    record.IPVersion,
			OutDstMAC:    record.OutDstMAC,
			OutSrcMAC:    record.OutSrcMAC,
			Src2LD:       record.Src2LD,
			SrcHost:      record.SrcHost,
			SrcIP:        record.SrcIP,
			SrcIsPrivate: record.SrcIsPrivate,
			SrcPort:      record.SrcPort,
			SrcTLD:       record.SrcTLD,
			TimeEndNs:    record.TimeEndNs,
			TimeStartNs:  record.TimeStartNs,
		})
		return nil
	}))
	return records
}
