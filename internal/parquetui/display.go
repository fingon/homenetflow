package parquetui

import (
	"math"
	"strconv"
	"time"
)

const metricSuffixScale = 1000

var metricSuffixes = map[Metric][]string{
	MetricBytes:       {"", "kb", "mb", "gb", "tb", "pb", "eb"},
	MetricConnections: {"", "k", "m", "g", "t", "p", "e"},
	MetricDNSLookups:  {"", "k", "m", "g", "t", "p", "e"},
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
		scaledValue /= metricSuffixScale
		suffixIndex++
	}

	return strconv.FormatInt(scaledValue, 10) + suffixes[suffixIndex]
}

type timelineYAxisLabelScale struct {
	decimalPlaces int
	divisor       float64
	suffix        string
}

func newTimelineYAxisLabelScale(metric Metric, yAxisMaxValue, tickStep int64) timelineYAxisLabelScale {
	suffixes, ok := metricSuffixes[metric]
	if !ok {
		return timelineYAxisLabelScale{divisor: 1}
	}

	suffixIndex := 0
	divisor := float64(1)
	for yAxisMaxValue >= int64(divisor*metricSuffixScale) && suffixIndex < len(suffixes)-1 {
		divisor *= metricSuffixScale
		suffixIndex++
	}

	return timelineYAxisLabelScale{
		decimalPlaces: timelineYAxisDecimalPlaces(float64(tickStep) / divisor),
		divisor:       divisor,
		suffix:        suffixes[suffixIndex],
	}
}

func formatTimelineYAxisMetricValue(value int64, scale timelineYAxisLabelScale) string {
	if value == 0 {
		return "0"
	}
	if value < 0 {
		return "-" + formatTimelineYAxisMetricValue(-value, scale)
	}

	scaledValue := float64(value) / scale.divisor
	return strconv.FormatFloat(scaledValue, 'f', scale.decimalPlaces, 64) + scale.suffix
}

func timelineYAxisDecimalPlaces(scaledStep float64) int {
	const maxDecimalPlaces = 3

	for decimalPlaces := 0; decimalPlaces <= maxDecimalPlaces; decimalPlaces++ {
		multiplier := math.Pow10(decimalPlaces)
		if math.Abs(scaledStep*multiplier-math.Round(scaledStep*multiplier)) < 0.0000001 {
			return decimalPlaces
		}
	}
	return maxDecimalPlaces
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
