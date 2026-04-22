package enrich

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/fingon/homenetflow/internal/model"
)

const (
	dnsQueryTypeA       = "A"
	dnsQueryTypeAAAA    = "AAAA"
	dnsQueryTypePTR     = "PTR"
	ip6ARPASuffix       = "ip6.arpa"
	inAddrARPASuffix    = "in-addr.arpa"
	ip6NibbleCount      = 32
	ipv4OctetCount      = 4
	logWindowDuration   = time.Hour
	messageFieldCount   = 6
	reverseIPv6GroupLen = 4
)

type dnsObservation struct {
	host               string
	reverseCacheSource bool
	time               time.Time
}

type dnsLookupEvent struct {
	answer    string
	clientIP  string
	queryName string
	queryType string
	time      time.Time
}

type reverseCacheLogEntry struct {
	host string
	miss bool
	time time.Time
}

type dnsIndex struct {
	observationsByIP        map[string][]dnsObservation
	lookupEvents            []dnsLookupEvent
	reverseCacheEntriesByIP map[string][]reverseCacheLogEntry
}

type dnsLogLoader struct {
	mu     sync.Mutex
	parsed map[string]*dnsIndex
}

type rawLogEntry struct {
	Line      string `json:"line"`
	Timestamp string `json:"timestamp"`
}

//nolint:tagliatelle
type nestedLogLine struct {
	Answers      []string `json:"answers"`
	ClientIP     string   `json:"client_ip"`
	Message      string   `json:"message"`
	QueryName    string   `json:"query_name"`
	QueryType    string   `json:"query_type"`
	TimestampEnd string   `json:"timestamp_end"`
}

func newDNSLogLoader() *dnsLogLoader {
	return &dnsLogLoader{parsed: make(map[string]*dnsIndex)}
}

func (l *dnsLogLoader) Load(logFiles []model.SourceFile) (*dnsIndex, error) {
	index := &dnsIndex{
		observationsByIP:        make(map[string][]dnsObservation),
		reverseCacheEntriesByIP: make(map[string][]reverseCacheLogEntry),
	}
	for _, logFile := range logFiles {
		fileIndex, err := l.loadFile(logFile.AbsPath)
		if err != nil {
			return nil, err
		}

		for ipAddress, observations := range fileIndex.observationsByIP {
			index.observationsByIP[ipAddress] = append(index.observationsByIP[ipAddress], observations...)
		}
		for ipAddress, entries := range fileIndex.reverseCacheEntriesByIP {
			index.reverseCacheEntriesByIP[ipAddress] = append(index.reverseCacheEntriesByIP[ipAddress], entries...)
		}
		index.lookupEvents = append(index.lookupEvents, fileIndex.lookupEvents...)
	}

	for ipAddress := range index.observationsByIP {
		slices.SortFunc(index.observationsByIP[ipAddress], func(a, b dnsObservation) int {
			if a.time.Equal(b.time) {
				switch {
				case a.host < b.host:
					return -1
				case a.host > b.host:
					return 1
				default:
					return 0
				}
			}

			if a.time.Before(b.time) {
				return -1
			}

			return 1
		})
	}
	for ipAddress := range index.reverseCacheEntriesByIP {
		slices.SortFunc(index.reverseCacheEntriesByIP[ipAddress], func(a, b reverseCacheLogEntry) int {
			if a.time.Equal(b.time) {
				switch {
				case a.miss != b.miss:
					if a.miss {
						return -1
					}
					return 1
				case a.host < b.host:
					return -1
				case a.host > b.host:
					return 1
				default:
					return 0
				}
			}

			if a.time.Before(b.time) {
				return -1
			}

			return 1
		})
	}
	slices.SortFunc(index.lookupEvents, func(a, b dnsLookupEvent) int {
		if a.time.Equal(b.time) {
			if a.clientIP == b.clientIP {
				return strings.Compare(a.queryName, b.queryName)
			}
			return strings.Compare(a.clientIP, b.clientIP)
		}
		if a.time.Before(b.time) {
			return -1
		}
		return 1
	})

	return index, nil
}

func (l *dnsLogLoader) loadFile(path string) (*dnsIndex, error) {
	l.mu.Lock()
	if index, ok := l.parsed[path]; ok {
		l.mu.Unlock()
		return index, nil
	}
	l.mu.Unlock()

	index, err := parseDNSLogFile(path)
	if err != nil {
		return nil, err
	}

	l.mu.Lock()
	l.parsed[path] = index
	l.mu.Unlock()
	return index, nil
}

func parseDNSLogFile(path string) (*dnsIndex, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open dns log %q: %w", path, err)
	}
	defer file.Close()

	index := &dnsIndex{
		observationsByIP:        make(map[string][]dnsObservation),
		reverseCacheEntriesByIP: make(map[string][]reverseCacheLogEntry),
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if err := index.parseLine(scanner.Bytes()); err != nil {
			return nil, fmt.Errorf("parse dns log line in %q: %w", path, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan dns log %q: %w", path, err)
	}

	return index, nil
}

func (i *dnsIndex) parseLine(lineBytes []byte) error {
	var rawEntry rawLogEntry
	if err := json.Unmarshal(lineBytes, &rawEntry); err != nil {
		return fmt.Errorf("unmarshal outer log entry: %w", err)
	}

	var nestedEntry nestedLogLine
	if json.Unmarshal([]byte(rawEntry.Line), &nestedEntry) != nil {
		return nil //nolint:nilerr
	}

	if nestedEntry.QueryName != "" {
		entryTime, err := parseLogTime(rawEntry.Timestamp, nestedEntry.TimestampEnd)
		if err != nil {
			return err
		}

		i.addLookupEvent(nestedEntry.ClientIP, nestedEntry.QueryName, nestedEntry.QueryType, normalizedDNSAnswer(nestedEntry.Answers), entryTime)

		if isPTRQueryType(nestedEntry.QueryType) {
			i.addReverseCacheEntryFromPTR(nestedEntry.QueryName, nestedEntry.Answers, entryTime)
		}

		for _, answer := range nestedEntry.Answers {
			ipAddress := net.ParseIP(answer)
			if ipAddress == nil {
				continue
			}

			i.addObservation(ipAddress.String(), nestedEntry.QueryName, entryTime, isForwardReverseCacheQueryType(nestedEntry.QueryType))
		}

		return nil
	}

	if nestedEntry.Message != "" {
		entryTime, err := time.Parse(time.RFC3339, rawEntry.Timestamp)
		if err != nil {
			return fmt.Errorf("parse dns log timestamp %q: %w", rawEntry.Timestamp, err)
		}

		i.parseLegacyMessage(nestedEntry.Message, entryTime.UTC())
	}

	return nil
}

func (i *dnsIndex) parseLegacyMessage(message string, entryTime time.Time) {
	fields := strings.Fields(message)
	if len(fields) < messageFieldCount || fields[len(fields)-2] != "is" {
		return
	}

	answer := fields[len(fields)-1]
	if net.ParseIP(answer) == nil {
		return
	}

	kind := fields[2]
	if !isPositiveLegacyKind(kind) {
		return
	}

	i.addObservation(answer, fields[len(fields)-3], entryTime, false)
}

func (i *dnsIndex) addObservation(ipAddress, host string, entryTime time.Time, reverseCacheSource bool) {
	normalizedHost := normalizeHostname(host)
	if normalizedHost == "" {
		return
	}

	i.observationsByIP[ipAddress] = append(i.observationsByIP[ipAddress], dnsObservation{
		host:               normalizedHost,
		reverseCacheSource: reverseCacheSource,
		time:               entryTime.UTC(),
	})
}

func (i *dnsIndex) addReverseCacheEntry(ipAddress string, entry reverseCacheLogEntry) {
	i.reverseCacheEntriesByIP[ipAddress] = append(i.reverseCacheEntriesByIP[ipAddress], reverseCacheLogEntry{
		host: entry.host,
		miss: entry.miss,
		time: entry.time.UTC(),
	})
}

func (i *dnsIndex) addReverseCacheEntryFromPTR(queryName string, answers []string, entryTime time.Time) {
	ipAddress, found := parseReverseLookupIPAddress(queryName)
	if !found {
		return
	}

	entry, found := reverseCacheEntryFromPTRAnswers(answers, entryTime)
	if !found {
		return
	}

	i.addReverseCacheEntry(ipAddress, entry)
}

func (i *dnsIndex) addLookupEvent(clientIP, queryName, queryType, answer string, entryTime time.Time) {
	ipAddress := net.ParseIP(clientIP)
	if ipAddress == nil {
		return
	}

	normalizedQueryName := normalizeHostname(queryName)
	if normalizedQueryName == "" {
		return
	}

	i.lookupEvents = append(i.lookupEvents, dnsLookupEvent{
		answer:    answer,
		clientIP:  ipAddress.String(),
		queryName: normalizedQueryName,
		queryType: strings.ToUpper(strings.TrimSpace(queryType)),
		time:      entryTime.UTC(),
	})
}

func normalizedDNSAnswer(answers []string) string {
	normalizedAnswers := make([]string, 0, len(answers))
	for _, answer := range answers {
		normalizedAnswer := strings.TrimSpace(answer)
		if normalizedAnswer == "" {
			continue
		}
		if strings.EqualFold(normalizedAnswer, model.DNSAnswerNXDOMAIN) {
			normalizedAnswer = model.DNSAnswerNXDOMAIN
		}
		if strings.EqualFold(normalizedAnswer, model.DNSAnswerSERVFAIL) {
			normalizedAnswer = model.DNSAnswerSERVFAIL
		}
		normalizedAnswers = append(normalizedAnswers, normalizedAnswer)
	}
	return strings.Join(normalizedAnswers, ", ")
}

func (i *dnsIndex) Lookup(ipAddress string, flowStart time.Time) *derivedNames {
	observations := i.observationsByIP[ipAddress]
	for index := len(observations) - 1; index >= 0; index-- {
		observation := observations[index]
		if observation.time.After(flowStart) {
			continue
		}

		if observation.time.Before(flowStart.Add(-logWindowDuration)) {
			return nil
		}

		names := deriveNames(observation.host)
		return &names
	}

	return nil
}

func (i *dnsIndex) LookupForReverseCache(ipAddress string, lookupTime time.Time) (reverseCacheLogEntry, bool) {
	entry, found := i.lookupReverseCacheEntry(ipAddress, lookupTime)
	if !found {
		return reverseCacheLogEntry{}, false
	}

	return entry, true
}

func (i *dnsIndex) LookupNewerThan(ipAddress string, afterTime, flowStart time.Time) (string, bool) {
	observations := i.observationsByIP[ipAddress]
	for index := len(observations) - 1; index >= 0; index-- {
		observation := observations[index]
		if observation.time.After(flowStart) {
			continue
		}

		if !observation.time.After(afterTime) {
			return "", false
		}

		return observation.host, true
	}

	return "", false
}

func (i *dnsIndex) LookupNewerThanForReverseCache(ipAddress string, afterTime, lookupTime time.Time) (reverseCacheLogEntry, bool) {
	entry, found := i.lookupReverseCacheEntry(ipAddress, lookupTime)
	if !found || !entry.time.After(afterTime) {
		return reverseCacheLogEntry{}, false
	}

	return entry, true
}

func (i *dnsIndex) lookupReverseCacheEntry(ipAddress string, lookupTime time.Time) (reverseCacheLogEntry, bool) {
	entries := i.reverseCacheEntriesByIP[ipAddress]
	for index := len(entries) - 1; index >= 0; index-- {
		entry := entries[index]
		if entry.time.After(lookupTime) {
			continue
		}

		return entry, true
	}

	observations := i.observationsByIP[ipAddress]
	for index := len(observations) - 1; index >= 0; index-- {
		observation := observations[index]
		if observation.time.After(lookupTime) || !observation.reverseCacheSource {
			continue
		}

		return reverseCacheLogEntry{
			host: observation.host,
			time: observation.time,
		}, true
	}

	return reverseCacheLogEntry{}, false
}

func isPositiveLegacyKind(kind string) bool {
	switch kind {
	case "cached", "config", "reply":
		return true
	default:
		return strings.Contains(kind, "/hosts/")
	}
}

func isForwardReverseCacheQueryType(queryType string) bool {
	switch strings.ToUpper(strings.TrimSpace(queryType)) {
	case dnsQueryTypeA, dnsQueryTypeAAAA:
		return true
	default:
		return false
	}
}

func isPTRQueryType(queryType string) bool {
	return strings.EqualFold(strings.TrimSpace(queryType), dnsQueryTypePTR)
}

func reverseCacheEntryFromPTRAnswers(answers []string, entryTime time.Time) (reverseCacheLogEntry, bool) {
	normalizedHosts := normalizedHostnames(ptrAnswerHostnames(answers))
	if len(normalizedHosts) > 0 {
		return reverseCacheLogEntry{
			host: normalizedHosts[0],
			time: entryTime.UTC(),
		}, true
	}

	for _, answer := range answers {
		normalizedAnswer := strings.TrimSpace(answer)
		if strings.EqualFold(normalizedAnswer, model.DNSAnswerSERVFAIL) {
			return reverseCacheLogEntry{}, false
		}
	}
	for _, answer := range answers {
		normalizedAnswer := strings.TrimSpace(answer)
		if strings.EqualFold(normalizedAnswer, model.DNSAnswerNXDOMAIN) {
			return reverseCacheLogEntry{
				miss: true,
				time: entryTime.UTC(),
			}, true
		}
	}

	return reverseCacheLogEntry{}, false
}

func ptrAnswerHostnames(answers []string) []string {
	hostnames := make([]string, 0, len(answers))
	for _, answer := range answers {
		normalizedAnswer := strings.TrimSpace(answer)
		if normalizedAnswer == "" ||
			strings.EqualFold(normalizedAnswer, model.DNSAnswerNXDOMAIN) ||
			strings.EqualFold(normalizedAnswer, model.DNSAnswerSERVFAIL) {
			continue
		}
		hostnames = append(hostnames, normalizedAnswer)
	}
	return hostnames
}

func parseReverseLookupIPAddress(queryName string) (string, bool) {
	normalizedName := normalizeHostname(queryName)
	if normalizedName == "" {
		return "", false
	}

	if strings.HasSuffix(normalizedName, inAddrARPASuffix) {
		return parseReverseIPv4Lookup(normalizedName)
	}
	if strings.HasSuffix(normalizedName, ip6ARPASuffix) {
		return parseReverseIPv6Lookup(normalizedName)
	}

	return "", false
}

func parseReverseIPv4Lookup(queryName string) (string, bool) {
	labels := strings.Split(queryName, hostnameLabelSeparator)
	if len(labels) != ipv4OctetCount+2 {
		return "", false
	}
	if labels[len(labels)-2] != "in-addr" || labels[len(labels)-1] != "arpa" {
		return "", false
	}

	octets := make([]string, 0, ipv4OctetCount)
	for index := ipv4OctetCount - 1; index >= 0; index-- {
		label := labels[index]
		if label == "" {
			return "", false
		}
		octets = append(octets, label)
	}

	ipAddress := net.ParseIP(strings.Join(octets, "."))
	if ipAddress == nil {
		return "", false
	}

	return ipAddress.String(), true
}

func parseReverseIPv6Lookup(queryName string) (string, bool) {
	labels := strings.Split(queryName, hostnameLabelSeparator)
	if len(labels) != ip6NibbleCount+2 {
		return "", false
	}
	if labels[len(labels)-2] != "ip6" || labels[len(labels)-1] != "arpa" {
		return "", false
	}

	reversedNibbles := make([]string, 0, ip6NibbleCount)
	for index := ip6NibbleCount - 1; index >= 0; index-- {
		label := labels[index]
		if len(label) != 1 || !strings.ContainsRune("0123456789abcdef", rune(label[0])) {
			return "", false
		}
		reversedNibbles = append(reversedNibbles, label)
	}

	parts := make([]string, 0, ip6NibbleCount/reverseIPv6GroupLen)
	for index := 0; index < len(reversedNibbles); index += reverseIPv6GroupLen {
		parts = append(parts, strings.Join(reversedNibbles[index:index+reverseIPv6GroupLen], ""))
	}

	ipAddress := net.ParseIP(strings.Join(parts, ":"))
	if ipAddress == nil {
		return "", false
	}

	return ipAddress.String(), true
}

func parseLogTime(rawTimestamp, endTimestamp string) (time.Time, error) {
	if endTimestamp != "" {
		value, err := time.Parse(time.RFC3339, endTimestamp)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse dns log timestamp_end %q: %w", endTimestamp, err)
		}

		return value.UTC(), nil
	}

	value, err := time.Parse(time.RFC3339, rawTimestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse dns log timestamp %q: %w", rawTimestamp, err)
	}

	return value.UTC(), nil
}
