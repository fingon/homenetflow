package parquetui

import (
	"strconv"
	"time"
)

var metricSuffixes = map[Metric][]string{
	MetricBytes:       {"", "kb", "mb", "gb", "tb", "pb", "eb"},
	MetricConnections: {"", "k", "m", "g", "t", "p", "e"},
}

func formatMetricValue(metric Metric, value int64) string {
	if value < 0 {
		return "-" + formatMetricValue(metric, -value)
	}

	suffixes, ok := metricSuffixes[metric]
	if !ok {
		return strconv.FormatInt(value, 10)
	}

	scaledValue := value
	suffixIndex := 0
	for scaledValue >= 10000 && suffixIndex < len(suffixes)-1 {
		scaledValue /= 1000
		suffixIndex++
	}

	return strconv.FormatInt(scaledValue, 10) + suffixes[suffixIndex]
}

func formatTimelineTickLabel(ns, spanWidthNs int64) string {
	if ns == 0 {
		return "-"
	}

	timestamp := time.Unix(0, ns).UTC()
	switch {
	case spanWidthNs <= int64(24*time.Hour):
		return timestamp.Format("15:04")
	case spanWidthNs <= int64(7*24*time.Hour):
		return timestamp.Format("02 Jan 15:04")
	case spanWidthNs <= int64(90*24*time.Hour):
		return timestamp.Format("02 Jan")
	default:
		return timestamp.Format("2006-01-02")
	}
}
