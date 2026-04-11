package enrich

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fingon/homenetflow/internal/model"
	"gotest.tools/v3/assert"
)

func TestNeighbourIndexMapsIPv6ToIPv4ByLLAddr(t *testing.T) {
	t.Parallel()

	index := loadNeighbourIndexFromContent(t,
		"{\"line\":\"{\\\"dst\\\":\\\"192.0.2.10\\\",\\\"lladdr\\\":\\\"AA:BB:CC:DD:EE:FF\\\"}\",\"timestamp\":\"2026-04-10T12:00:00Z\"}\n"+
			"{\"line\":\"{\\\"dst\\\":\\\"2001:db8::10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:01Z\"}\n",
	)

	ipv4Address, ok := index.LookupIPv4("2001:db8::10", time.Date(2026, 4, 10, 12, 30, 0, 0, time.UTC))
	assert.Assert(t, ok)
	assert.Equal(t, ipv4Address, "192.0.2.10")
}

func TestNeighbourIndexBackfillsOnlyUniqueIPv4ForOlderFlows(t *testing.T) {
	t.Parallel()

	uniqueIndex := loadNeighbourIndexFromContent(t,
		"{\"line\":\"{\\\"dst\\\":\\\"192.0.2.10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:00Z\"}\n"+
			"{\"line\":\"{\\\"dst\\\":\\\"2001:db8::10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:01Z\"}\n",
	)

	ipv4Address, ok := uniqueIndex.LookupIPv4("2001:db8::10", time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC))
	assert.Assert(t, ok)
	assert.Equal(t, ipv4Address, "192.0.2.10")

	ambiguousIndex := loadNeighbourIndexFromContent(t,
		"{\"line\":\"{\\\"dst\\\":\\\"192.0.2.10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:00Z\"}\n"+
			"{\"line\":\"{\\\"dst\\\":\\\"192.0.2.11\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:05:00Z\"}\n"+
			"{\"line\":\"{\\\"dst\\\":\\\"2001:db8::10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:10:00Z\"}\n",
	)

	_, ok = ambiguousIndex.LookupIPv4("2001:db8::10", time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC))
	assert.Assert(t, !ok)
}

func TestNeighbourIndexUsesTimeAppropriateIPv4(t *testing.T) {
	t.Parallel()

	index := loadNeighbourIndexFromContent(t,
		"{\"line\":\"{\\\"dst\\\":\\\"192.0.2.10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:00Z\"}\n"+
			"{\"line\":\"{\\\"dst\\\":\\\"192.0.2.11\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T13:00:00Z\"}\n"+
			"{\"line\":\"{\\\"dst\\\":\\\"2001:db8::10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T13:05:00Z\"}\n",
	)

	ipv4Address, ok := index.LookupIPv4("2001:db8::10", time.Date(2026, 4, 10, 12, 30, 0, 0, time.UTC))
	assert.Assert(t, ok)
	assert.Equal(t, ipv4Address, "192.0.2.10")

	ipv4Address, ok = index.LookupIPv4("2001:db8::10", time.Date(2026, 4, 10, 13, 30, 0, 0, time.UTC))
	assert.Assert(t, ok)
	assert.Equal(t, ipv4Address, "192.0.2.11")
}

func TestNeighbourIndexSkipsIPv6LLAddrConflicts(t *testing.T) {
	t.Parallel()

	index := loadNeighbourIndexFromContent(t,
		"{\"line\":\"{\\\"dst\\\":\\\"192.0.2.10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:00Z\"}\n"+
			"{\"line\":\"{\\\"dst\\\":\\\"192.0.2.11\\\",\\\"lladdr\\\":\\\"00:11:22:33:44:55\\\"}\",\"timestamp\":\"2026-04-10T12:00:00Z\"}\n"+
			"{\"line\":\"{\\\"dst\\\":\\\"2001:db8::10\\\",\\\"lladdr\\\":\\\"aa:bb:cc:dd:ee:ff\\\"}\",\"timestamp\":\"2026-04-10T12:00:01Z\"}\n"+
			"{\"line\":\"{\\\"dst\\\":\\\"2001:db8::10\\\",\\\"lladdr\\\":\\\"00:11:22:33:44:55\\\"}\",\"timestamp\":\"2026-04-10T12:00:02Z\"}\n",
	)

	_, ok := index.LookupIPv4("2001:db8::10", time.Date(2026, 4, 10, 12, 30, 0, 0, time.UTC))
	assert.Assert(t, !ok)
}

func loadNeighbourIndexFromContent(t *testing.T, logContents string) *neighbourIndex {
	t.Helper()

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "2026-04-10.jsonl")
	assert.NilError(t, os.WriteFile(logPath, []byte(logContents), 0o600))

	index, err := loadNeighbourIndex([]model.SourceFile{{
		AbsPath: logPath,
		RelPath: "2026-04-10.jsonl",
		Period:  model.Period{Kind: model.PeriodDay, Start: neighbourLogStart},
	}})
	assert.NilError(t, err)
	return index
}
