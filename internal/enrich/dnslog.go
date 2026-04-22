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
	logWindowDuration = time.Hour
	messageFieldCount = 6
)

type dnsObservation struct {
	host string
	time time.Time
}

type dnsLookupEvent struct {
	answer    string
	clientIP  string
	queryName string
	queryType string
	time      time.Time
}

type dnsIndex struct {
	observationsByIP map[string][]dnsObservation
	lookupEvents     []dnsLookupEvent
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
	index := &dnsIndex{observationsByIP: make(map[string][]dnsObservation)}
	for _, logFile := range logFiles {
		fileIndex, err := l.loadFile(logFile.AbsPath)
		if err != nil {
			return nil, err
		}

		for ipAddress, observations := range fileIndex.observationsByIP {
			index.observationsByIP[ipAddress] = append(index.observationsByIP[ipAddress], observations...)
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

	index := &dnsIndex{observationsByIP: make(map[string][]dnsObservation)}
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

		for _, answer := range nestedEntry.Answers {
			ipAddress := net.ParseIP(answer)
			if ipAddress == nil {
				continue
			}

			i.addObservation(ipAddress.String(), nestedEntry.QueryName, entryTime)
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

	i.addObservation(answer, fields[len(fields)-3], entryTime)
}

func (i *dnsIndex) addObservation(ipAddress, host string, entryTime time.Time) {
	normalizedHost := normalizeHostname(host)
	if normalizedHost == "" {
		return
	}

	i.observationsByIP[ipAddress] = append(i.observationsByIP[ipAddress], dnsObservation{
		host: normalizedHost,
		time: entryTime.UTC(),
	})
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

func isPositiveLegacyKind(kind string) bool {
	switch kind {
	case "cached", "config", "reply":
		return true
	default:
		return strings.Contains(kind, "/hosts/")
	}
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
