package enrich

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/fingon/homenetflow/internal/model"
)

var neighbourLogStart = time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC)

type neighbourObservation struct {
	ipAddress string
	time      time.Time
}

type neighbourIndex struct {
	conflictedIPv6        map[string]struct{}
	ipv4ObservationsByMAC map[string][]neighbourObservation
	lladdrByIPv6          map[string]string
	uniqueIPv4ByMAC       map[string]string
}

//nolint:tagliatelle
type neighbourLogLine struct {
	Dst    string `json:"dst"`
	LLAddr string `json:"lladdr"`
}

func loadNeighbourIndex(logFiles []model.SourceFile) (*neighbourIndex, error) {
	index := &neighbourIndex{
		conflictedIPv6:        make(map[string]struct{}),
		ipv4ObservationsByMAC: make(map[string][]neighbourObservation),
		lladdrByIPv6:          make(map[string]string),
		uniqueIPv4ByMAC:       make(map[string]string),
	}

	ipv4SeenByMAC := make(map[string]map[string]struct{})
	for _, logFile := range logFiles {
		if logFile.Period.Start.Before(neighbourLogStart) {
			continue
		}

		if err := index.parseFile(logFile.AbsPath, ipv4SeenByMAC); err != nil {
			return nil, err
		}
	}

	for macAddress, observations := range index.ipv4ObservationsByMAC {
		slices.SortFunc(observations, func(a, b neighbourObservation) int {
			if a.time.Equal(b.time) {
				return strings.Compare(a.ipAddress, b.ipAddress)
			}

			if a.time.Before(b.time) {
				return -1
			}

			return 1
		})
		index.ipv4ObservationsByMAC[macAddress] = observations
	}

	for macAddress, ipAddresses := range ipv4SeenByMAC {
		if len(ipAddresses) != 1 {
			continue
		}

		for ipAddress := range ipAddresses {
			index.uniqueIPv4ByMAC[macAddress] = ipAddress
		}
	}

	return index, nil
}

func (i *neighbourIndex) parseFile(path string, ipv4SeenByMAC map[string]map[string]struct{}) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open neighbour log %q: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if err := i.parseLine(scanner.Bytes(), ipv4SeenByMAC); err != nil {
			return fmt.Errorf("parse neighbour log line in %q: %w", path, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan neighbour log %q: %w", path, err)
	}

	return nil
}

func (i *neighbourIndex) parseLine(lineBytes []byte, ipv4SeenByMAC map[string]map[string]struct{}) error {
	var rawEntry rawLogEntry
	if err := json.Unmarshal(lineBytes, &rawEntry); err != nil {
		return fmt.Errorf("unmarshal outer log entry: %w", err)
	}

	var nestedEntry neighbourLogLine
	if json.Unmarshal([]byte(rawEntry.Line), &nestedEntry) != nil {
		return nil //nolint:nilerr
	}

	if nestedEntry.Dst == "" || nestedEntry.LLAddr == "" {
		return nil
	}

	entryTime, err := parseLogTime(rawEntry.Timestamp, "")
	if err != nil {
		return err
	}

	i.addObservation(nestedEntry.Dst, nestedEntry.LLAddr, entryTime, ipv4SeenByMAC)
	return nil
}

func (i *neighbourIndex) addObservation(
	ipAddress string,
	lladdr string,
	entryTime time.Time,
	ipv4SeenByMAC map[string]map[string]struct{},
) {
	address, err := netip.ParseAddr(ipAddress)
	if err != nil {
		return
	}

	normalizedIP := address.String()
	normalizedLLAddr := strings.ToLower(strings.TrimSpace(lladdr))
	if normalizedLLAddr == "" {
		return
	}

	if address.Is4() {
		if _, ok := ipv4SeenByMAC[normalizedLLAddr]; !ok {
			ipv4SeenByMAC[normalizedLLAddr] = make(map[string]struct{})
		}
		ipv4SeenByMAC[normalizedLLAddr][normalizedIP] = struct{}{}
		i.ipv4ObservationsByMAC[normalizedLLAddr] = append(i.ipv4ObservationsByMAC[normalizedLLAddr], neighbourObservation{
			ipAddress: normalizedIP,
			time:      entryTime.UTC(),
		})
		return
	}

	if !address.Is6() {
		return
	}

	if existingLLAddr, ok := i.lladdrByIPv6[normalizedIP]; ok && existingLLAddr != normalizedLLAddr {
		delete(i.lladdrByIPv6, normalizedIP)
		i.conflictedIPv6[normalizedIP] = struct{}{}
		return
	}

	if _, conflicted := i.conflictedIPv6[normalizedIP]; conflicted {
		return
	}

	i.lladdrByIPv6[normalizedIP] = normalizedLLAddr
}

func (i *neighbourIndex) LookupIPv4(ipAddress string, flowStart time.Time) (string, bool) {
	if i == nil {
		return "", false
	}

	address, err := netip.ParseAddr(ipAddress)
	if err != nil || !address.Is6() {
		return "", false
	}

	normalizedIP := address.String()
	if _, conflicted := i.conflictedIPv6[normalizedIP]; conflicted {
		return "", false
	}

	lladdr, ok := i.lladdrByIPv6[normalizedIP]
	if !ok {
		return "", false
	}

	observations := i.ipv4ObservationsByMAC[lladdr]
	for index := len(observations) - 1; index >= 0; index-- {
		observation := observations[index]
		if !observation.time.After(flowStart) {
			return observation.ipAddress, true
		}
	}

	ipv4Address, ok := i.uniqueIPv4ByMAC[lladdr]
	return ipv4Address, ok
}
