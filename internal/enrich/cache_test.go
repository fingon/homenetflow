package enrich

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"gotest.tools/v3/assert"
)

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

	firstResult, err := cache.Lookup("192.0.2.10", false)
	assert.NilError(t, err)
	assert.Assert(t, firstResult.names == nil)
	assert.ErrorContains(t, firstResult.warning, invalidPTRNameErrorFragment)

	secondResult, err := cache.Lookup("192.0.2.10", false)
	assert.NilError(t, err)
	assert.Assert(t, secondResult.names == nil)
	assert.Assert(t, secondResult.warning == nil)

	assert.Equal(t, lookupCallCount.Load(), int32(1))

	fileBytes, err := os.ReadFile(cacheFilePath)
	if os.IsNotExist(err) {
		return
	}

	assert.NilError(t, err)
	assert.Equal(t, string(fileBytes), "")
}

func TestReverseDNSCacheReturnsErrorForResolverFailure(t *testing.T) {
	cacheFilePath := filepath.Join(t.TempDir(), reverseDNSCacheFilename)
	cache, err := loadReverseDNSCache(cacheFilePath)
	assert.NilError(t, err)

	stubReverseLookup(t, func(string) ([]string, error) {
		return nil, errors.New("resolver unavailable")
	})

	result, err := cache.Lookup("192.0.2.10", false)
	assert.Assert(t, result.names == nil)
	assert.Assert(t, result.warning == nil)
	assert.ErrorContains(t, err, "lookup PTR for")
	assert.ErrorContains(t, err, "resolver unavailable")
}

func TestReverseDNSCacheSkipsLiveLookupButReturnsCachedEntries(t *testing.T) {
	cacheFilePath := filepath.Join(t.TempDir(), reverseDNSCacheFilename)
	cacheFileBytes := []byte("{\"ip\":\"192.0.2.10\",\"host\":\"cached.example.net\"}\n")
	assert.NilError(t, os.WriteFile(cacheFilePath, cacheFileBytes, 0o600))

	cache, err := loadReverseDNSCache(cacheFilePath)
	assert.NilError(t, err)

	var lookupCallCount atomic.Int32
	stubReverseLookup(t, func(string) ([]string, error) {
		lookupCallCount.Add(1)
		return []string{"live.example.net."}, nil
	})

	cachedResult, err := cache.Lookup("192.0.2.10", true)
	assert.NilError(t, err)
	assert.Equal(t, cachedResult.names.host, "cached.example.net")

	skippedResult, err := cache.Lookup("198.51.100.20", true)
	assert.NilError(t, err)
	assert.Assert(t, skippedResult.names == nil)
	assert.Assert(t, skippedResult.warning == nil)
	assert.Equal(t, lookupCallCount.Load(), int32(0))
}

func TestPruneReverseDNSCacheRemovesLocalEntries(t *testing.T) {
	cacheFilePath := filepath.Join(t.TempDir(), reverseDNSCacheFilename)
	cacheFileBytes := []byte("{\"ip\":\"192.168.1.10\",\"host\":\"local-v4.example\"}\n" +
		"{\"ip\":\"2001:db8:1:2::20\",\"host\":\"local-v6.example\"}\n" +
		"{\"ip\":\"192.0.2.10\",\"host\":\"public.example\"}\n")
	assert.NilError(t, os.WriteFile(cacheFilePath, cacheFileBytes, 0o600))

	neighbourIndex := loadNeighbourIndexFromContent(t, "{\"line\":\"{\\\"dst\\\":\\\"2001:db8:1:2::10\\\"}\",\"timestamp\":\"2026-04-10T12:00:01Z\"}\n")

	assert.NilError(t, pruneReverseDNSCache(cacheFilePath, neighbourIndex))

	prunedBytes, err := os.ReadFile(cacheFilePath)
	assert.NilError(t, err)
	assert.Equal(t, string(prunedBytes), "{\"host\":\"public.example\",\"ip\":\"192.0.2.10\"}\n")
}
