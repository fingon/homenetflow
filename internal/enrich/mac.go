package enrich

import (
	"slices"
	"strings"
	"time"

	"github.com/fingon/homenetflow/internal/model"
	"github.com/fingon/homenetflow/internal/parquetout"
)

type macObservation struct {
	ipAddress string
	time      time.Time
}

type macIndex struct {
	ipv4ObservationsByMAC map[string][]macObservation
	uniqueIPv4ByMAC       map[string]string
}

func loadMACIndex(path string) (*macIndex, error) {
	index := &macIndex{
		ipv4ObservationsByMAC: make(map[string][]macObservation),
		uniqueIPv4ByMAC:       make(map[string]string),
	}

	ipv4SeenByMAC := make(map[string]map[string]struct{})
	err := parquetout.ReadFileBatches(path, func(records []model.FlowRecord) error {
		for _, record := range records {
			index.addRecord(record, ipv4SeenByMAC)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	for macAddress, observations := range index.ipv4ObservationsByMAC {
		slices.SortFunc(observations, func(a, b macObservation) int {
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

func (i *macIndex) addRecord(record model.FlowRecord, ipv4SeenByMAC map[string]map[string]struct{}) {
	if i == nil || ipVersionForAddress(record.SrcIP) != model.IPVersion4 {
		return
	}

	observationTime := time.Unix(0, record.TimeStartNs).UTC()
	for _, macAddress := range flowMACCandidates(record) {
		if _, ok := ipv4SeenByMAC[macAddress]; !ok {
			ipv4SeenByMAC[macAddress] = make(map[string]struct{})
		}
		ipv4SeenByMAC[macAddress][record.SrcIP] = struct{}{}
		i.ipv4ObservationsByMAC[macAddress] = append(i.ipv4ObservationsByMAC[macAddress], macObservation{
			ipAddress: record.SrcIP,
			time:      observationTime,
		})
	}
}

func (i *macIndex) LookupIPv4(macAddresses []string, flowStart time.Time) (string, bool) {
	if i == nil {
		return "", false
	}

	for _, macAddress := range macAddresses {
		observations := i.ipv4ObservationsByMAC[macAddress]
		for index := len(observations) - 1; index >= 0; index-- {
			observation := observations[index]
			if !observation.time.After(flowStart) {
				return observation.ipAddress, true
			}
		}

		ipv4Address, ok := i.uniqueIPv4ByMAC[macAddress]
		if ok {
			return ipv4Address, true
		}
	}

	return "", false
}

func flowMACCandidates(record model.FlowRecord) []string {
	candidates := []*string{
		record.InSrcMAC,
		record.InDstMAC,
		record.OutSrcMAC,
		record.OutDstMAC,
	}

	seen := make(map[string]struct{}, len(candidates))
	result := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}

		normalizedMAC := normalizeMAC(*candidate)
		if normalizedMAC == "" {
			continue
		}
		if _, ok := seen[normalizedMAC]; ok {
			continue
		}

		seen[normalizedMAC] = struct{}{}
		result = append(result, normalizedMAC)
	}

	return result
}

func normalizeMAC(macAddress string) string {
	normalizedMAC := strings.ToLower(strings.TrimSpace(macAddress))
	return normalizedMAC
}
