package enrich

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

var reverseLookupAddr = net.LookupAddr

const invalidPTRNameErrorFragment = "DNS response contained records which contain invalid names"

type reverseDNSCache struct {
	filePath    string
	hostByIP    map[string]string
	missByIP    map[string]struct{}
	mu          sync.Mutex
	pendingByIP map[string]*lookupState
}

type lookupState struct {
	done    chan struct{}
	err     error
	host    string
	warning error
}

type cacheEntry struct {
	Host string `json:"host"`
	IP   string `json:"ip"`
}

type cacheLookupResult struct {
	names   *derivedNames
	warning error
}

func loadReverseDNSCache(filePath string) (*reverseDNSCache, error) {
	cache := &reverseDNSCache{
		filePath:    filePath,
		hostByIP:    make(map[string]string),
		missByIP:    make(map[string]struct{}),
		pendingByIP: make(map[string]*lookupState),
	}

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return cache, nil
		}

		return nil, fmt.Errorf("open reverse DNS cache %q: %w", filePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry cacheEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, fmt.Errorf("unmarshal reverse DNS cache entry in %q: %w", filePath, err)
		}

		if entry.IP == "" || entry.Host == "" {
			continue
		}

		cache.hostByIP[entry.IP] = entry.Host
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan reverse DNS cache %q: %w", filePath, err)
	}

	return cache, nil
}

func pruneReverseDNSCache(filePath string, neighbourIndex *neighbourIndex) error {
	entries, found, err := readReverseDNSCacheEntries(filePath)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	retainedEntries := make([]cacheEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IP == "" || entry.Host == "" {
			continue
		}
		if isLocalIPv6IPAddress(entry.IP, neighbourIndex) {
			continue
		}
		retainedEntries = append(retainedEntries, entry)
	}
	if len(retainedEntries) == len(entries) {
		return nil
	}

	tempFile, err := os.CreateTemp(filepath.Dir(filePath), filepath.Base(filePath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary reverse DNS cache %q: %w", filePath, err)
	}
	tempPath := tempFile.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			if err := os.Remove(tempPath); err != nil && !os.IsNotExist(err) {
				slog.Warn("remove temporary reverse DNS cache", "path", tempPath, "error", err)
			}
		}
	}()

	writer := bufio.NewWriter(tempFile)
	for _, entry := range retainedEntries {
		entryBytes, err := json.Marshal(entry)
		if err != nil {
			_ = tempFile.Close()
			return fmt.Errorf("marshal reverse DNS cache entry: %w", err)
		}
		if _, err := writer.Write(append(entryBytes, '\n')); err != nil {
			_ = tempFile.Close()
			return fmt.Errorf("write temporary reverse DNS cache %q: %w", tempPath, err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("flush temporary reverse DNS cache %q: %w", tempPath, err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close temporary reverse DNS cache %q: %w", tempPath, err)
	}
	if err := os.Chmod(tempPath, 0o600); err != nil {
		return fmt.Errorf("chmod temporary reverse DNS cache %q: %w", tempPath, err)
	}
	if err := os.Rename(tempPath, filePath); err != nil {
		return fmt.Errorf("replace reverse DNS cache %q: %w", filePath, err)
	}
	removeTemp = false

	slog.Info("pruned local IPv6 reverse DNS cache entries", "path", filePath, "removed", len(entries)-len(retainedEntries), "retained", len(retainedEntries))
	return nil
}

func readReverseDNSCacheEntries(filePath string) ([]cacheEntry, bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("open reverse DNS cache %q: %w", filePath, err)
	}
	defer file.Close()

	entries := make([]cacheEntry, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry cacheEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, true, fmt.Errorf("unmarshal reverse DNS cache entry in %q: %w", filePath, err)
		}

		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		return nil, true, fmt.Errorf("scan reverse DNS cache %q: %w", filePath, err)
	}

	return entries, true, nil
}

func (c *reverseDNSCache) Lookup(ipAddress string, skipDNSLookups bool) (cacheLookupResult, error) {
	c.mu.Lock()
	if host, ok := c.hostByIP[ipAddress]; ok {
		c.mu.Unlock()
		names := deriveNames(host)
		return cacheLookupResult{names: &names}, nil
	}

	if _, ok := c.missByIP[ipAddress]; ok {
		c.mu.Unlock()
		return cacheLookupResult{}, nil
	}

	if skipDNSLookups {
		c.mu.Unlock()
		return cacheLookupResult{}, nil
	}

	if pendingState, ok := c.pendingByIP[ipAddress]; ok {
		c.mu.Unlock()
		<-pendingState.done
		if pendingState.err != nil {
			return cacheLookupResult{}, pendingState.err
		}

		if pendingState.host == "" {
			return cacheLookupResult{warning: pendingState.warning}, nil
		}

		names := deriveNames(pendingState.host)
		return cacheLookupResult{names: &names, warning: pendingState.warning}, nil
	}

	pendingState := &lookupState{done: make(chan struct{})}
	c.pendingByIP[ipAddress] = pendingState
	c.mu.Unlock()

	host, warning, err := lookupAddress(ipAddress)

	c.mu.Lock()
	delete(c.pendingByIP, ipAddress)
	pendingState.err = err
	pendingState.host = host
	pendingState.warning = warning
	if err == nil {
		if host == "" {
			c.missByIP[ipAddress] = struct{}{}
		} else {
			c.hostByIP[ipAddress] = host
		}
	}
	close(pendingState.done)
	c.mu.Unlock()

	if err != nil {
		return cacheLookupResult{}, err
	}

	if host == "" {
		return cacheLookupResult{warning: warning}, nil
	}

	if err := c.append(ipAddress, host); err != nil {
		return cacheLookupResult{}, err
	}

	names := deriveNames(host)
	return cacheLookupResult{names: &names, warning: warning}, nil
}

func (c *reverseDNSCache) append(ipAddress, host string) error {
	file, err := os.OpenFile(c.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open reverse DNS cache %q for append: %w", c.filePath, err)
	}
	defer file.Close()

	entryBytes, err := json.Marshal(cacheEntry{Host: host, IP: ipAddress})
	if err != nil {
		return fmt.Errorf("marshal reverse DNS cache entry: %w", err)
	}

	if _, err := file.Write(append(entryBytes, '\n')); err != nil {
		return fmt.Errorf("append reverse DNS cache entry to %q: %w", c.filePath, err)
	}

	return nil
}

func lookupAddress(ipAddress string) (host string, warning, err error) {
	names, err := reverseLookupAddr(ipAddress)
	if err != nil {
		var dnsError *net.DNSError
		if errors.As(err, &dnsError) && dnsError.IsNotFound {
			return "", nil, nil
		}

		if isInvalidPTRNameError(err) {
			return "", fmt.Errorf("lookup PTR for %q: %w", ipAddress, err), nil
		}

		return "", nil, fmt.Errorf("lookup PTR for %q: %w", ipAddress, err)
	}

	normalizedNames := make([]string, 0, len(names))
	seenNames := make(map[string]struct{}, len(names))
	for _, name := range names {
		normalizedName := normalizeHostname(name)
		if normalizedName == "" {
			continue
		}

		if _, ok := seenNames[normalizedName]; ok {
			continue
		}

		seenNames[normalizedName] = struct{}{}
		normalizedNames = append(normalizedNames, normalizedName)
	}

	if len(normalizedNames) == 0 {
		return "", nil, nil
	}

	slices.Sort(normalizedNames)
	return normalizedNames[0], nil, nil
}

func isInvalidPTRNameError(err error) bool {
	var dnsError *net.DNSError
	if !errors.As(err, &dnsError) {
		return false
	}

	return strings.Contains(dnsError.Err, invalidPTRNameErrorFragment)
}
