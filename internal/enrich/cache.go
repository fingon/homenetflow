package enrich

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"sync"
)

var reverseLookupAddr = net.LookupAddr

type reverseDNSCache struct {
	filePath    string
	hostByIP    map[string]string
	missByIP    map[string]struct{}
	mu          sync.Mutex
	pendingByIP map[string]*lookupState
}

type lookupState struct {
	done chan struct{}
	err  error
	host string
}

type cacheEntry struct {
	Host string `json:"host"`
	IP   string `json:"ip"`
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

func (c *reverseDNSCache) Lookup(ipAddress string) (*derivedNames, error) {
	c.mu.Lock()
	if host, ok := c.hostByIP[ipAddress]; ok {
		c.mu.Unlock()
		names := deriveNames(host)
		return &names, nil
	}

	if _, ok := c.missByIP[ipAddress]; ok {
		c.mu.Unlock()
		return nil, nil
	}

	if pendingState, ok := c.pendingByIP[ipAddress]; ok {
		c.mu.Unlock()
		<-pendingState.done
		if pendingState.err != nil {
			return nil, pendingState.err
		}

		if pendingState.host == "" {
			return nil, nil
		}

		names := deriveNames(pendingState.host)
		return &names, nil
	}

	pendingState := &lookupState{done: make(chan struct{})}
	c.pendingByIP[ipAddress] = pendingState
	c.mu.Unlock()

	host, err := lookupAddress(ipAddress)

	c.mu.Lock()
	delete(c.pendingByIP, ipAddress)
	pendingState.err = err
	pendingState.host = host
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
		return nil, err
	}

	if host == "" {
		return nil, nil
	}

	if err := c.append(ipAddress, host); err != nil {
		return nil, err
	}

	names := deriveNames(host)
	return &names, nil
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

func lookupAddress(ipAddress string) (string, error) {
	names, err := reverseLookupAddr(ipAddress)
	if err != nil {
		var dnsError *net.DNSError
		if errors.As(err, &dnsError) && dnsError.IsNotFound {
			return "", nil
		}

		return "", fmt.Errorf("lookup PTR for %q: %w", ipAddress, err)
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
		return "", nil
	}

	slices.Sort(normalizedNames)
	return normalizedNames[0], nil
}
