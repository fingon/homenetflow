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

	firstResult, err := cache.Lookup("192.0.2.10")
	assert.NilError(t, err)
	assert.Assert(t, firstResult.names == nil)
	assert.ErrorContains(t, firstResult.warning, invalidPTRNameErrorFragment)

	secondResult, err := cache.Lookup("192.0.2.10")
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

	result, err := cache.Lookup("192.0.2.10")
	assert.Assert(t, result.names == nil)
	assert.Assert(t, result.warning == nil)
	assert.ErrorContains(t, err, "lookup PTR for")
	assert.ErrorContains(t, err, "resolver unavailable")
}
