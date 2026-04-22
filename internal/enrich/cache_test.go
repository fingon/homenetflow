package enrich

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func testDNSIndex() *dnsIndex {
	return &dnsIndex{observationsByIP: make(map[string][]dnsObservation)}
}

func TestReverseDNSCacheTreatsMalformedPTRAsMiss(t *testing.T) {
	cacheFilePath := filepath.Join(t.TempDir(), reverseDNSCacheFilename)
	cache, err := loadReverseDNSCache(cacheFilePath)
	assert.NilError(t, err)

	var lookupCallCount atomic.Int32
	stubReverseLookup(t, func(ipAddress string) ([]string, error) {
		lookupCallCount.Add(1)
		return nil, &net.DNSError{
			Err:  invalidPTRNameErrorFragment,
			Name: ipAddress,
		}
	})

	lookupTime := time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC)
	firstResult, err := cache.Lookup("192.0.2.10", lookupTime, testDNSIndex(), false)
	assert.NilError(t, err)
	assert.Assert(t, firstResult.names == nil)
	assert.ErrorContains(t, firstResult.warning, invalidPTRNameErrorFragment)

	secondResult, err := cache.Lookup("192.0.2.10", lookupTime, testDNSIndex(), false)
	assert.NilError(t, err)
	assert.Assert(t, secondResult.names == nil)
	assert.Assert(t, secondResult.warning == nil)

	assert.Equal(t, lookupCallCount.Load(), int32(1))

	fileBytes, err := os.ReadFile(cacheFilePath)
	if os.IsNotExist(err) {
		return
	}

	assert.NilError(t, err)
	assert.Equal(t, string(fileBytes), "{\"ip\":\"192.0.2.10\",\"miss\":true,\"resolvedAtNs\":1775046600000000000}\n")
}

func TestReverseDNSCacheReturnsErrorForResolverFailure(t *testing.T) {
	cacheFilePath := filepath.Join(t.TempDir(), reverseDNSCacheFilename)
	cache, err := loadReverseDNSCache(cacheFilePath)
	assert.NilError(t, err)

	stubReverseLookup(t, func(string) ([]string, error) {
		return nil, errors.New("resolver unavailable")
	})

	result, err := cache.Lookup("192.0.2.10", time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC), testDNSIndex(), false)
	assert.Assert(t, result.names == nil)
	assert.Assert(t, result.warning == nil)
	assert.ErrorContains(t, err, "lookup PTR for")
	assert.ErrorContains(t, err, "resolver unavailable")
}

func TestReverseDNSCacheSkipsLiveLookupButReturnsCachedEntries(t *testing.T) {
	cacheFilePath := filepath.Join(t.TempDir(), reverseDNSCacheFilename)
	cacheFileBytes := []byte("{\"host\":\"cached.example.net\",\"ip\":\"192.0.2.10\",\"resolvedAtNs\":1775044800000000000}\n")
	assert.NilError(t, os.WriteFile(cacheFilePath, cacheFileBytes, 0o600))

	cache, err := loadReverseDNSCache(cacheFilePath)
	assert.NilError(t, err)

	var lookupCallCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCallCount.Add(1)
		return []string{"live.example.net."}, nil
	})

	lookupTime := time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC)
	cachedResult, err := cache.Lookup("192.0.2.10", lookupTime, testDNSIndex(), true)
	assert.NilError(t, err)
	assert.Equal(t, cachedResult.names.host, "cached.example.net")

	skippedResult, err := cache.Lookup("198.51.100.20", lookupTime, testDNSIndex(), true)
	assert.NilError(t, err)
	assert.Assert(t, skippedResult.names == nil)
	assert.Assert(t, skippedResult.warning == nil)
	assert.Equal(t, lookupCallCount.Load(), int32(0))
}

func TestReverseDNSCacheIgnoresLegacyEntries(t *testing.T) {
	cacheFilePath := filepath.Join(t.TempDir(), reverseDNSCacheFilename)
	cacheFileBytes := []byte("{\"ip\":\"192.0.2.10\",\"host\":\"legacy.example.net\"}\n")
	assert.NilError(t, os.WriteFile(cacheFilePath, cacheFileBytes, 0o600))

	cache, err := loadReverseDNSCache(cacheFilePath)
	assert.NilError(t, err)
	assert.Equal(t, len(cache.entryByIP), 0)
}

func TestReverseDNSCachePromotesNegativeEntryFromLogs(t *testing.T) {
	cacheFilePath := filepath.Join(t.TempDir(), reverseDNSCacheFilename)
	cacheFileBytes := []byte("{\"ip\":\"192.0.2.10\",\"miss\":true,\"resolvedAtNs\":1775044800000000000}\n")
	assert.NilError(t, os.WriteFile(cacheFilePath, cacheFileBytes, 0o600))

	cache, err := loadReverseDNSCache(cacheFilePath)
	assert.NilError(t, err)

	logIndex := testDNSIndex()
	logIndex.addObservation("192.0.2.10", "promoted.example.net", time.Date(2026, 4, 1, 12, 20, 0, 0, time.UTC))

	var lookupCallCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCallCount.Add(1)
		return []string{"live.example.net."}, nil
	})

	result, err := cache.Lookup("192.0.2.10", time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC), logIndex, true)
	assert.NilError(t, err)
	assert.Equal(t, result.names.host, "promoted.example.net")
	assert.Equal(t, lookupCallCount.Load(), int32(0))

	fileBytes, err := os.ReadFile(cacheFilePath)
	assert.NilError(t, err)
	assert.Equal(t, string(fileBytes), "{\"ip\":\"192.0.2.10\",\"miss\":true,\"resolvedAtNs\":1775044800000000000}\n{\"host\":\"promoted.example.net\",\"ip\":\"192.0.2.10\",\"resolvedAtNs\":1775046600000000000}\n")
}

func TestReverseDNSCacheDoesNotPromoteFromFutureLogs(t *testing.T) {
	cacheFilePath := filepath.Join(t.TempDir(), reverseDNSCacheFilename)
	cacheFileBytes := []byte("{\"ip\":\"192.0.2.10\",\"miss\":true,\"resolvedAtNs\":1775044800000000000}\n")
	assert.NilError(t, os.WriteFile(cacheFilePath, cacheFileBytes, 0o600))

	cache, err := loadReverseDNSCache(cacheFilePath)
	assert.NilError(t, err)

	logIndex := testDNSIndex()
	logIndex.addObservation("192.0.2.10", "future.example.net", time.Date(2026, 4, 1, 12, 40, 0, 0, time.UTC))

	var lookupCallCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCallCount.Add(1)
		return []string{"live.example.net."}, nil
	})

	result, err := cache.Lookup("192.0.2.10", time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC), logIndex, true)
	assert.NilError(t, err)
	assert.Assert(t, result.names == nil)
	assert.Equal(t, lookupCallCount.Load(), int32(0))
}

func TestPruneReverseDNSCacheRemovesLocalIPv6Entries(t *testing.T) {
	cacheFilePath := filepath.Join(t.TempDir(), reverseDNSCacheFilename)
	cacheFileBytes := []byte("{\"host\":\"local-v4.example\",\"ip\":\"192.168.1.10\",\"resolvedAtNs\":1775044800000000000}\n" +
		"{\"host\":\"local-v6.example\",\"ip\":\"2001:db8:1:2::20\",\"resolvedAtNs\":1775044800000000000}\n" +
		"{\"host\":\"public.example\",\"ip\":\"192.0.2.10\",\"resolvedAtNs\":1775044800000000000}\n")
	assert.NilError(t, os.WriteFile(cacheFilePath, cacheFileBytes, 0o600))

	neighbourIndex := loadNeighbourIndexFromContent(t, "{\"line\":\"{\\\"dst\\\":\\\"2001:db8:1:2::10\\\"}\",\"timestamp\":\"2026-04-10T12:00:01Z\"}\n")

	assert.NilError(t, pruneReverseDNSCache(cacheFilePath, neighbourIndex))

	prunedBytes, err := os.ReadFile(cacheFilePath)
	assert.NilError(t, err)
	assert.Equal(t, string(prunedBytes), "{\"host\":\"local-v4.example\",\"ip\":\"192.168.1.10\",\"resolvedAtNs\":1775044800000000000}\n{\"host\":\"public.example\",\"ip\":\"192.0.2.10\",\"resolvedAtNs\":1775044800000000000}\n")
}
