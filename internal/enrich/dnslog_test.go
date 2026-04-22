package enrich

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fingon/homenetflow/internal/model"
	"gotest.tools/v3/assert"
)

func TestDNSLogLoaderParsesStructuredAndLegacyRecords(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"192.0.2.10\\\"],\\\"query_name\\\":\\\"www.fingon.iki.fi\\\",\\\"timestamp_end\\\":\\\"2026-04-01T12:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T12:00:00Z\"}\n" +
		"{\"line\":\"{\\\"message\\\":\\\"1 192.168.1.1/123 cached cer.lan is 192.0.2.11\\\"}\",\"timestamp\":\"2026-04-01T12:00:01Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	loader := newDNSLogLoader()
	index, err := loader.Load([]model.SourceFile{{
		AbsPath: logPath,
		RelPath: "2026-04-01.jsonl",
		Period:  model.Period{Kind: model.PeriodDay, Start: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
	}})
	assert.NilError(t, err)

	structuredNames := index.Lookup("192.0.2.10", time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC))
	assert.Equal(t, structuredNames.host, "www.fingon.iki.fi")

	legacyNames := index.Lookup("192.0.2.11", time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC))
	assert.Equal(t, legacyNames.host, "cer.lan")
}

func TestDNSLogLoaderParsesStructuredLookupEvents(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"NXDOMAIN\\\"],\\\"client_ip\\\":\\\"2001:db8::10\\\",\\\"query_name\\\":\\\"Example.COM.\\\",\\\"query_type\\\":\\\"AAAA\\\",\\\"timestamp_end\\\":\\\"2026-04-01T12:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T12:00:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	loader := newDNSLogLoader()
	index, err := loader.Load([]model.SourceFile{{
		AbsPath: logPath,
		Period:  model.Period{Kind: model.PeriodDay, Start: time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC)},
	}})
	assert.NilError(t, err)

	assert.Equal(t, len(index.lookupEvents), 1)
	assert.Equal(t, index.lookupEvents[0].answer, model.DNSAnswerNXDOMAIN)
	assert.Equal(t, index.lookupEvents[0].clientIP, "2001:db8::10")
	assert.Equal(t, index.lookupEvents[0].queryName, "example.com")
	assert.Equal(t, index.lookupEvents[0].queryType, "AAAA")
}

func TestDNSLogLoaderOnlyUsesStructuredAAndAAAAForReverseCache(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "2026-04-01.jsonl")
	logContents := []byte("{\"line\":\"{\\\"answers\\\":[\\\"192.0.2.10\\\"],\\\"query_name\\\":\\\"ipv4.example.net\\\",\\\"query_type\\\":\\\"A\\\",\\\"timestamp_end\\\":\\\"2026-04-01T08:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T08:00:00Z\"}\n" +
		"{\"line\":\"{\\\"answers\\\":[\\\"192.0.2.10\\\"],\\\"query_name\\\":\\\"txt.example.net\\\",\\\"query_type\\\":\\\"TXT\\\",\\\"timestamp_end\\\":\\\"2026-04-01T09:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T09:00:00Z\"}\n" +
		"{\"line\":\"{\\\"answers\\\":[\\\"2001:db8::10\\\"],\\\"query_name\\\":\\\"ipv6.example.net\\\",\\\"query_type\\\":\\\"AAAA\\\",\\\"timestamp_end\\\":\\\"2026-04-01T10:00:00Z\\\"}\",\"timestamp\":\"2026-04-01T10:00:00Z\"}\n")
	assert.NilError(t, os.WriteFile(logPath, logContents, 0o600))

	loader := newDNSLogLoader()
	index, err := loader.Load([]model.SourceFile{{
		AbsPath: logPath,
		Period:  model.Period{Kind: model.PeriodDay, Start: time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC)},
	}})
	assert.NilError(t, err)

	host, found := index.LookupForReverseCache("192.0.2.10", time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC))
	assert.Assert(t, found)
	assert.Equal(t, host, "ipv4.example.net")

	host, found = index.LookupForReverseCache("2001:db8::10", time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC))
	assert.Assert(t, found)
	assert.Equal(t, host, "ipv6.example.net")
}
