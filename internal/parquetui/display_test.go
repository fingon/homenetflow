package parquetui

import (
	"context"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fingon/homenetflow/internal/model"
	"gotest.tools/v3/assert"
)

func TestFormatMetricValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		metric   Metric
		name     string
		value    int64
		expected string
	}{
		{name: "bytes raw below thousand", metric: MetricBytes, value: 999, expected: "999"},
		{name: "bytes raw at thousand", metric: MetricBytes, value: 1000, expected: "1000"},
		{name: "bytes raw four digits", metric: MetricBytes, value: 9999, expected: "9999"},
		{name: "bytes suffix at five digits", metric: MetricBytes, value: 10000, expected: "10kb"},
		{name: "bytes millions", metric: MetricBytes, value: 9999500, expected: "9999kb"},
		{name: "bytes large stays four digits", metric: MetricBytes, value: 5124000000000, expected: "5124gb"},
		{name: "connections raw below thousand", metric: MetricConnections, value: 999, expected: "999"},
		{name: "connections raw at thousand", metric: MetricConnections, value: 1000, expected: "1000"},
		{name: "connections raw four digits", metric: MetricConnections, value: 9999, expected: "9999"},
		{name: "connections suffix at five digits", metric: MetricConnections, value: 10000, expected: "10k"},
		{name: "connections millions", metric: MetricConnections, value: 9999500, expected: "9999k"},
		{name: "connections large stays four digits", metric: MetricConnections, value: 5124000000000, expected: "5124g"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, formatMetricValue(testCase.metric, testCase.value), testCase.expected)
		})
	}
}

func TestFormatTimestampAtUsesCompactHumanLabels(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)
	helsinki := time.FixedZone("EEST", 3*60*60)
	testCases := []struct {
		now      time.Time
		value    time.Time
		name     string
		expected string
	}{
		{name: "zero", value: time.Time{}, expected: "-"},
		{name: "same day", value: time.Date(2026, time.April, 15, 1, 2, 3, 0, time.UTC), now: now, expected: "01:02:03"},
		{name: "same iso week", value: time.Date(2026, time.April, 13, 1, 2, 3, 0, time.UTC), now: now, expected: "Mon 01:02:03"},
		{name: "same year", value: time.Date(2026, time.January, 2, 1, 2, 3, 0, time.UTC), now: now, expected: "02.01 01:02:03"},
		{name: "different year", value: time.Date(2025, time.December, 31, 1, 2, 3, 0, time.UTC), now: now, expected: "31.12.2025 01:02:03"},
		{name: "non utc now", value: time.Date(2026, time.April, 14, 22, 30, 0, 0, time.UTC), now: time.Date(2026, time.April, 15, 1, 0, 0, 0, helsinki), expected: "22:30:00"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if testCase.value.IsZero() {
				assert.Equal(t, formatTimestampAt(0, testCase.now), testCase.expected)
				return
			}
			assert.Equal(t, formatTimestampAt(testCase.value.UnixNano(), testCase.now), testCase.expected)
		})
	}
}

func TestFormatTimelineYAxisMetricValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		axisMaxValue int64
		expected     string
		metric       Metric
		name         string
		tickStep     int64
		value        int64
	}{
		{name: "bytes zero", metric: MetricBytes, axisMaxValue: 4000, tickStep: 1000, value: 0, expected: "0"},
		{name: "bytes below thousand", metric: MetricBytes, axisMaxValue: 500, tickStep: 100, value: 500, expected: "500"},
		{name: "bytes compact thousand", metric: MetricBytes, axisMaxValue: 4000, tickStep: 1000, value: 1000, expected: "1kb"},
		{name: "bytes shared decimal granularity", metric: MetricBytes, axisMaxValue: 2000, tickStep: 500, value: 1500, expected: "1.5kb"},
		{name: "bytes shared decimal top", metric: MetricBytes, axisMaxValue: 2000, tickStep: 500, value: 2000, expected: "2.0kb"},
		{name: "connections compact", metric: MetricConnections, axisMaxValue: 4000, tickStep: 1000, value: 4000, expected: "4k"},
		{name: "dns compact", metric: MetricDNSLookups, axisMaxValue: 3000000, tickStep: 1000000, value: 2000000, expected: "2m"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			scale := newTimelineYAxisLabelScale(testCase.metric, testCase.axisMaxValue, testCase.tickStep)
			assert.Equal(t, formatTimelineYAxisMetricValue(testCase.value, scale), testCase.expected)
		})
	}
}

func TestHistogramYAxisScaleUsesOneSignificantDigitTicks(t *testing.T) {
	t.Parallel()

	yAxisMaxValue, tickStep, ticks := histogramYAxisScale(4000)

	assert.Equal(t, yAxisMaxValue, int64(4000))
	assert.Equal(t, tickStep, int64(1000))
	assert.DeepEqual(t, ticks, []int64{4000, 3000, 2000, 1000, 0})
	assert.Equal(t, histogramYAxisSignificantDigits(4000), histogramYAxisOneDigit)
}

func TestHistogramYAxisScaleUsesLadderForThreeGigabytes(t *testing.T) {
	t.Parallel()

	yAxisMaxValue, tickStep, ticks := histogramYAxisScale(2800000000)
	scale := newTimelineYAxisLabelScale(MetricBytes, yAxisMaxValue, tickStep)
	labels := make([]string, 0, len(ticks))
	for _, tick := range ticks {
		labels = append(labels, formatTimelineYAxisMetricValue(tick, scale))
	}

	assert.Equal(t, yAxisMaxValue, int64(3000000000))
	assert.Equal(t, tickStep, int64(1000000000))
	assert.DeepEqual(t, ticks, []int64{3000000000, 2000000000, 1000000000, 0})
	assert.DeepEqual(t, labels, []string{"3gb", "2gb", "1gb", "0"})
}

func TestHistogramYAxisScaleFallsBackToTwoSignificantDigits(t *testing.T) {
	t.Parallel()

	yAxisMaxValue, tickStep, ticks := histogramYAxisScale(2)

	assert.Equal(t, yAxisMaxValue, int64(2))
	assert.Equal(t, tickStep, int64(1))
	assert.DeepEqual(t, ticks, []int64{2, 1, 0})
	assert.Equal(t, histogramYAxisSignificantDigits(2), histogramYAxisTwoDigits)
}

func TestHistogramYAxisScaleRoundsDomainUp(t *testing.T) {
	t.Parallel()

	yAxisMaxValue, tickStep, ticks := histogramYAxisScale(1234)

	assert.Equal(t, yAxisMaxValue, int64(2000))
	assert.Equal(t, tickStep, int64(500))
	assert.DeepEqual(t, ticks, []int64{2000, 1500, 1000, 500, 0})
}

func TestHistogramSVGMarkupAddsAxisLabels(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC)
	bins := []HistogramBin{
		{FromNs: start.UnixNano(), ToNs: start.Add(6*time.Hour).UnixNano() - 1, Value: 1000},
		{FromNs: start.Add(6 * time.Hour).UnixNano(), ToNs: start.Add(12*time.Hour).UnixNano() - 1, Value: 2000},
		{FromNs: start.Add(12 * time.Hour).UnixNano(), ToNs: start.Add(18*time.Hour).UnixNano() - 1, Value: 3000},
		{FromNs: start.Add(18 * time.Hour).UnixNano(), ToNs: start.Add(24*time.Hour).UnixNano() - 1, Value: 4000},
	}

	markup := histogramSVGMarkupAt(MetricBytes, bins, time.Date(2026, time.January, 2, 12, 0, 0, 0, time.UTC))

	assert.Assert(t, strings.Contains(markup, "histogram-axis-label"))
	assert.Assert(t, strings.Contains(markup, "histogram-axis-label-y"))
	assert.Assert(t, strings.Contains(markup, fmt.Sprintf(`data-timestamp-ns="%d"`, start.UnixNano())))
	assert.Assert(t, strings.Contains(markup, `data-span-width-ns="86399999999999"`))
	assert.Assert(t, strings.Contains(markup, ">0<"))
	assert.Assert(t, strings.Contains(markup, ">4kb<"))
	assert.Assert(t, strings.Contains(markup, ">3kb<"))
	assert.Assert(t, strings.Contains(markup, ">2kb<"))
	assert.Assert(t, strings.Contains(markup, ">1kb<"))
	assert.Assert(t, strings.Contains(markup, ">00:00<"))
	assert.Assert(t, strings.Contains(markup, ">23:59<"))
	assert.Assert(t, strings.Contains(markup, "Value: 4000"))
	assert.Assert(t, strings.Contains(markup, `tabindex="0"`))
	assert.Assert(t, strings.Contains(markup, `data-from-label="00:00:00"`))
	assert.Assert(t, strings.Contains(markup, `data-to-label="23:59:59"`))
	assert.Assert(t, strings.Contains(markup, `data-value-label="4000"`))
}

func TestHistogramSVGMarkupFormatsYAxisLabelsForConnections(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC)
	bins := []HistogramBin{
		{FromNs: start.UnixNano(), ToNs: start.Add(time.Hour).UnixNano() - 1, Value: 10000},
	}

	markup := histogramSVGMarkup(MetricConnections, bins)

	assert.Assert(t, strings.Contains(markup, `class="histogram-axis-label histogram-axis-label-y"`))
	assert.Assert(t, strings.Contains(markup, ">10.0k<"))
	assert.Assert(t, strings.Contains(markup, ">7.5k<"))
	assert.Assert(t, strings.Contains(markup, ">5.0k<"))
	assert.Assert(t, strings.Contains(markup, ">2.5k<"))
}

func TestTopBarRendersTimePresetButtons(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, topBar(DashboardData{
		State: QueryState{
			FromNs:      10,
			ToNs:        20,
			Metric:      MetricBytes,
			Granularity: Granularity2LD,
			Sort:        SortBytes,
		},
	}))

	assert.Assert(t, !strings.Contains(markup, `id="preset-select"`))
	assert.Assert(t, strings.Contains(markup, `name="preset"`))
	assert.Assert(t, strings.Contains(markup, `value="1d"`))
	assert.Assert(t, strings.Contains(markup, `name="family"`))
	assert.Assert(t, strings.Contains(markup, `value="ipv4"`))
	assert.Assert(t, strings.Contains(markup, `value="ipv6"`))
	assert.Assert(t, strings.Contains(markup, `class="action-button danger"`))
	assert.Assert(t, !strings.Contains(markup, `name="view"`))
}

func TestGraphSVGMarkupUsesDenseHooksForCrowdedGraphs(t *testing.T) {
	t.Parallel()

	nodes := make([]Node, 0, graphDenseNodeCount)
	positions := make(map[string]LayoutPoint, graphDenseNodeCount)
	for index := range graphDenseNodeCount {
		nodeID := fmt.Sprintf("node-%d", index)
		nodes = append(nodes, Node{
			ID:    nodeID,
			Label: nodeID,
			Total: int64(graphDenseNodeCount - index),
		})
		positions[nodeID] = LayoutPoint{
			X: float64(10 + index),
			Y: float64(20 + index),
		}
	}

	markup := graphSVGMarkup(QueryState{Metric: MetricBytes}, GraphData{
		Nodes:         nodes,
		NodePositions: positions,
	})

	assert.Assert(t, strings.Contains(markup, `class="graph-svg is-dense"`))
	assert.Assert(t, strings.Contains(markup, `class="graph-scene"`))
	assert.Assert(t, strings.Contains(markup, `data-node-id="node-0"`))
	assert.Assert(t, strings.Contains(markup, `data-node-priority="36"`))
	assert.Assert(t, strings.Contains(markup, `data-label-persistent="false"`))
}

func TestGraphSVGMarkupMarksPersistentLabels(t *testing.T) {
	t.Parallel()

	markup := graphSVGMarkup(QueryState{Metric: MetricBytes}, GraphData{
		Nodes: []Node{
			{ID: "selected", Label: "selected", Total: 10, Selected: true},
			{ID: "synthetic", Label: "synthetic", Total: 5, Synthetic: true},
		},
		NodePositions: map[string]LayoutPoint{
			"selected":  {X: 100, Y: 100},
			"synthetic": {X: 200, Y: 200},
		},
	})

	assert.Assert(t, strings.Contains(markup, `data-node-id="selected"`))
	assert.Assert(t, strings.Contains(markup, `data-label-persistent="true"`))
}

func TestGraphSVGMarkupColorsPrivateAndMixedNodes(t *testing.T) {
	t.Parallel()

	markup := graphSVGMarkup(QueryState{Metric: MetricBytes}, GraphData{
		Nodes: []Node{
			{ID: "private", Label: "private", Total: 10, AddressClass: nodeAddressClassPrivate},
			{ID: "mixed", Label: "mixed", Total: 9, AddressClass: nodeAddressClassMixed},
			{ID: "public", Label: "public", Total: 8, AddressClass: nodeAddressClassPublic},
		},
		NodePositions: map[string]LayoutPoint{
			"private": {X: 100, Y: 100},
			"mixed":   {X: 200, Y: 100},
			"public":  {X: 300, Y: 100},
		},
	})

	assert.Assert(t, strings.Contains(markup, `data-node-id="private"`))
	assert.Assert(t, strings.Contains(markup, fmt.Sprintf(`fill="%s"`, privateEntityNodeFill)))
	assert.Assert(t, strings.Contains(markup, fmt.Sprintf(`fill="%s"`, mixedEntityNodeFill)))
	assert.Assert(t, strings.Contains(markup, fmt.Sprintf(`fill="%s"`, unselectedNodeFill)))
}

func TestPanelsDoNotRenderNestedPanelWrappers(t *testing.T) {
	t.Parallel()

	summaryMarkup := renderNodeString(t, SummaryPanel(QueryState{
		FromNs:      10,
		ToNs:        20,
		Metric:      MetricBytes,
		Granularity: Granularity2LD,
	}, GraphData{}))
	tableMarkup := renderNodeString(t, TablePanel(QueryState{}, TableData{}))

	assert.Assert(t, !strings.Contains(summaryMarkup, `class="panel summary-panel"`))
	assert.Assert(t, !strings.Contains(tableMarkup, `class="panel"`))
}

func TestDNSLookupResultClassesRender(t *testing.T) {
	t.Parallel()

	tableMarkup := renderNodeString(t, TablePanel(QueryState{Metric: MetricDNSLookups}, TableData{
		TotalCount: 2,
		TotalPages: 1,
		VisibleRows: []TableRow{
			{Connections: 1, Destination: "missing.example", DNSResultState: dnsResultStateNXDOMAIN, Source: "alpha.lan"},
			{Connections: 2, Destination: "www.example.com", DNSResultState: dnsResultStateMixed, Source: "alpha.lan"},
		},
	}))

	assert.Assert(t, strings.Contains(tableMarkup, `dns-result-nxdomain`))
	assert.Assert(t, strings.Contains(tableMarkup, `dns-result-mixed`))

	graphMarkup := graphSVGMarkup(QueryState{Metric: MetricDNSLookups}, GraphData{
		ActiveMetric: MetricDNSLookups,
		Edges: []Edge{
			{Connections: 1, Destination: "missing.example", DNSResultState: dnsResultStateNXDOMAIN, MetricValue: 1, Source: "alpha.lan"},
			{Connections: 2, Destination: "www.example.com", DNSResultState: dnsResultStateMixed, MetricValue: 2, Source: "alpha.lan"},
		},
		Nodes: []Node{
			{DNSResultState: dnsResultStateNXDOMAIN, ID: "missing.example", Label: "missing.example", Total: 1},
			{DNSResultState: dnsResultStateMixed, ID: "www.example.com", Label: "www.example.com", Total: 2},
			{ID: "alpha.lan", Label: "alpha.lan", Total: 3},
		},
		NodePositions: map[string]LayoutPoint{
			"alpha.lan":       {X: 100, Y: 100},
			"missing.example": {X: 200, Y: 100},
			"www.example.com": {X: 200, Y: 180},
		},
	})

	assert.Assert(t, strings.Contains(graphMarkup, `dns-result-nxdomain`))
	assert.Assert(t, strings.Contains(graphMarkup, `dns-result-mixed`))
	assert.Assert(t, strings.Contains(graphMarkup, fmt.Sprintf(`stroke="%s"`, nxdomainEdgeStroke)))
	assert.Assert(t, strings.Contains(graphMarkup, fmt.Sprintf(`stroke="%s"`, mixedDNSEdgeStroke)))
}

func TestDNSLookupPanelsHideBytes(t *testing.T) {
	t.Parallel()

	tableMarkup := renderNodeString(t, TablePanel(QueryState{Metric: MetricDNSLookups}, TableData{
		TotalCount: 1,
		TotalPages: 1,
		VisibleRows: []TableRow{
			{Bytes: 0, Connections: 7, Destination: "www.example.com", Source: "alpha.lan"},
		},
	}))

	assert.Assert(t, !strings.Contains(tableMarkup, ">Bytes<"))
	assert.Assert(t, !strings.Contains(tableMarkup, ">0</td>"))
	assert.Assert(t, strings.Contains(tableMarkup, ">DNS Lookups<"))
	assert.Assert(t, strings.Contains(tableMarkup, ">7</td>"))

	graphMarkup := graphSVGMarkup(QueryState{Metric: MetricDNSLookups}, GraphData{
		ActiveMetric: MetricDNSLookups,
		Edges: []Edge{
			{Bytes: 0, Connections: 7, Destination: "www.example.com", MetricValue: 7, Source: "alpha.lan"},
		},
		Nodes: []Node{
			{ID: "alpha.lan", Label: "alpha.lan", Total: 7},
			{ID: "www.example.com", Label: "www.example.com", Total: 7},
		},
		NodePositions: map[string]LayoutPoint{
			"alpha.lan":       {X: 100, Y: 100},
			"www.example.com": {X: 200, Y: 100},
		},
	})

	assert.Assert(t, !strings.Contains(graphMarkup, "Bytes:"))
	assert.Assert(t, strings.Contains(graphMarkup, "DNS Lookups: 7"))

	selectedEdgeMarkup := renderNodeString(t, selectedPanelAt(QueryState{Metric: MetricDNSLookups}, GraphData{
		ActiveMetric: MetricDNSLookups,
		SelectedEdge: &Edge{
			Bytes:       0,
			Connections: 7,
			Destination: "www.example.com",
			Source:      "alpha.lan",
		},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	assert.Assert(t, !strings.Contains(selectedEdgeMarkup, "Bytes:"))
	assert.Assert(t, strings.Contains(selectedEdgeMarkup, "DNS Lookups: 7"))
}

func TestTablePanelRendersCompactTimestampWithFullMetadata(t *testing.T) {
	t.Parallel()

	firstSeen := time.Date(2026, time.April, 15, 1, 2, 3, 0, time.UTC)
	lastSeen := time.Date(2026, time.January, 2, 4, 5, 6, 0, time.UTC)
	markup := renderNodeString(t, tablePanelAt(QueryState{}, TableData{
		Page:       1,
		TotalPages: 1,
		VisibleRows: []TableRow{
			{
				Source:      "alpha.lan",
				Destination: "dns.google",
				FirstSeenNs: firstSeen.UnixNano(),
				LastSeenNs:  lastSeen.UnixNano(),
			},
		},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	assert.Assert(t, strings.Contains(markup, `datetime="2026-04-15T01:02:03Z"`))
	assert.Assert(t, strings.Contains(markup, `title="2026-04-15T01:02:03Z"`))
	assert.Assert(t, strings.Contains(markup, fmt.Sprintf(`data-timestamp-ns="%d"`, firstSeen.UnixNano())))
	assert.Assert(t, strings.Contains(markup, `>01:02:03</time>`))
	assert.Assert(t, strings.Contains(markup, `datetime="2026-01-02T04:05:06Z"`))
	assert.Assert(t, strings.Contains(markup, fmt.Sprintf(`data-timestamp-ns="%d"`, lastSeen.UnixNano())))
	assert.Assert(t, strings.Contains(markup, `>02.01 04:05:06</time>`))
}

func TestSelectedPanelRendersCompactTimestampWithFullMetadata(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, selectedPanelAt(QueryState{}, GraphData{
		ActiveMetric: MetricBytes,
		SelectedEdge: &Edge{
			Source:      "alpha.lan",
			Destination: "dns.google",
			FirstSeenNs: time.Date(2026, time.April, 13, 1, 2, 3, 0, time.UTC).UnixNano(),
			LastSeenNs:  time.Date(2025, time.December, 31, 4, 5, 6, 0, time.UTC).UnixNano(),
		},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	assert.Assert(t, strings.Contains(markup, `datetime="2026-04-13T01:02:03Z"`))
	assert.Assert(t, strings.Contains(markup, fmt.Sprintf(`data-timestamp-ns="%d"`, time.Date(2026, time.April, 13, 1, 2, 3, 0, time.UTC).UnixNano())))
	assert.Assert(t, strings.Contains(markup, `>Mon 01:02:03</time>`))
	assert.Assert(t, strings.Contains(markup, `datetime="2025-12-31T04:05:06Z"`))
	assert.Assert(t, strings.Contains(markup, fmt.Sprintf(`data-timestamp-ns="%d"`, time.Date(2025, time.December, 31, 4, 5, 6, 0, time.UTC).UnixNano())))
	assert.Assert(t, strings.Contains(markup, `>31.12.2025 04:05:06</time>`))
}

func TestSelectedEntityHidesZeroDirectionValues(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		node       Node
		hiddenRow  string
		visibleRow string
	}{
		{
			name:       "zero ingress",
			node:       Node{ID: "alpha.lan", Label: "alpha.lan", Egress: 5},
			hiddenRow:  "Ingress:",
			visibleRow: "Egress: 5",
		},
		{
			name:       "zero egress",
			node:       Node{ID: "alpha.lan", Label: "alpha.lan", Ingress: 6},
			hiddenRow:  "Egress:",
			visibleRow: "Ingress: 6",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			markup := renderNodeString(t, selectedPanelAt(QueryState{}, GraphData{
				ActiveMetric: MetricConnections,
				SelectedNode: &testCase.node,
			}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

			assert.Assert(t, !strings.Contains(markup, testCase.hiddenRow))
			assert.Assert(t, strings.Contains(markup, testCase.visibleRow))
		})
	}
}

func TestSummaryPanelDNSLookupTotalsHideEntityAndEdgeCountsForTreeShape(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, SummaryPanel(QueryState{
		FromNs: 10,
		Metric: MetricDNSLookups,
		ToNs:   20,
	}, GraphData{
		Totals: Totals{
			Connections: 8,
			Edges:       5,
			Entities:    4,
		},
	}))

	assert.Assert(t, !strings.Contains(markup, ">Entities<"))
	assert.Assert(t, !strings.Contains(markup, ">Edges<"))
	assert.Assert(t, !strings.Contains(markup, ">Bytes<"))
	assert.Assert(t, strings.Contains(markup, ">DNS Lookups<"))
	assert.Assert(t, strings.Contains(markup, ">8<"))
}

func TestSummaryPanelDNSLookupTotalsKeepEntityAndEdgeCountsForNonTreeShape(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, SummaryPanel(QueryState{
		FromNs: 10,
		Metric: MetricDNSLookups,
		ToNs:   20,
	}, GraphData{
		Totals: Totals{
			Connections: 8,
			Edges:       5,
			Entities:    5,
		},
	}))

	assert.Assert(t, strings.Contains(markup, ">Entities<"))
	assert.Assert(t, strings.Contains(markup, ">Edges<"))
	assert.Assert(t, !strings.Contains(markup, ">Bytes<"))
	assert.Assert(t, strings.Contains(markup, ">DNS Lookups<"))
}

func TestSummaryPanelDoesNotRenderRankings(t *testing.T) {
	t.Parallel()

	summaryMarkup := renderNodeString(t, SummaryPanel(QueryState{
		FromNs:      10,
		ToNs:        20,
		Metric:      MetricBytes,
		Granularity: Granularity2LD,
	}, GraphData{
		ActiveMetric: MetricBytes,
		TopEntities: []Node{
			{ID: "alpha.lan", Label: "alpha.lan", Total: 100},
		},
		TopEdges: []Edge{
			{Source: "alpha.lan", Destination: "dns.google", MetricValue: 100},
		},
	}))

	assert.Assert(t, !strings.Contains(summaryMarkup, "Top Entities"))
	assert.Assert(t, !strings.Contains(summaryMarkup, "Top Flows"))
}

func TestSummaryPanelActiveFiltersOnlyRenderTimeAndEntityFilters(t *testing.T) {
	t.Parallel()

	summaryMarkup := renderNodeString(t, SummaryPanel(QueryState{
		FromNs:      10,
		ToNs:        20,
		Metric:      MetricConnections,
		Granularity: GranularityHostname,
		Include:     []string{"alpha.lan"},
		Exclude:     []string{"dns.google"},
	}, GraphData{}))

	assert.Assert(t, strings.Contains(summaryMarkup, "Time:"))
	assert.Assert(t, strings.Contains(summaryMarkup, `data-timestamp-ns="10"`))
	assert.Assert(t, strings.Contains(summaryMarkup, `data-timestamp-ns="20"`))
	assert.Assert(t, strings.Contains(summaryMarkup, "Entity: alpha.lan"))
	assert.Assert(t, strings.Contains(summaryMarkup, "Exclude: dns.google"))
	assert.Assert(t, !strings.Contains(summaryMarkup, "Metric:"))
	assert.Assert(t, !strings.Contains(summaryMarkup, "Granularity:"))
}

func TestSummaryPanelRendersAddressFamilyFilterChip(t *testing.T) {
	t.Parallel()

	summaryMarkup := renderNodeString(t, SummaryPanel(QueryState{
		AddressFamily: AddressFamilyIPv6,
		FromNs:        10,
		ToNs:          20,
		Metric:        MetricConnections,
		Granularity:   GranularityHostname,
	}, GraphData{}))

	assert.Assert(t, strings.Contains(summaryMarkup, "Address Family: IPv6"))
}

func TestAppShellRendersSeparateRankingsSection(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, AppShell(DashboardData{
		State: QueryState{
			FromNs:      10,
			ToNs:        20,
			Metric:      MetricBytes,
			Granularity: Granularity2LD,
		},
		Span: TimeSpan{StartNs: 10, EndNs: 20},
		Graph: GraphData{
			ActiveMetric: MetricBytes,
			TopEntities: []Node{
				{ID: "alpha.lan", Label: "alpha.lan", Total: 100},
			},
			TopEdges: []Edge{
				{Source: "alpha.lan", Destination: "dns.google", MetricValue: 100},
			},
		},
	}))

	assert.Assert(t, strings.Contains(markup, `id="rankings-section"`))
	assert.Assert(t, strings.Contains(markup, `class="section-panel section-block"`))
	assert.Assert(t, strings.Contains(markup, `class="rankings-section"`))
	assert.Assert(t, strings.Contains(markup, `class="rankings-panel"`))
	assert.Assert(t, strings.Contains(markup, `data-collapsible-toggle`))
	assert.Assert(t, strings.Contains(markup, `class="section-toggle-icon"`))
	assert.Assert(t, strings.Contains(markup, `aria-label="Collapse Graph"`))
	assert.Assert(t, strings.Contains(markup, `aria-label="Collapse Rankings"`))
	assert.Assert(t, strings.Contains(markup, `aria-label="Collapse Flows Table"`))
	assert.Assert(t, strings.Contains(markup, `aria-controls="graph-section-content"`))
	assert.Assert(t, strings.Contains(markup, `aria-controls="rankings-content"`))
	assert.Assert(t, strings.Contains(markup, `aria-controls="table-content"`))
	assert.Assert(t, !strings.Contains(markup, `class="section-toggle-label"`))
	assert.Assert(t, strings.Contains(markup, "Top Entities"))
	assert.Assert(t, strings.Contains(markup, "Top Flows"))
}

func TestAppShellDoesNotRenderBreadcrumbsOrIdleStatus(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, AppShell(DashboardData{
		State: QueryState{
			FromNs:      10,
			ToNs:        20,
			Metric:      MetricBytes,
			Granularity: Granularity2LD,
		},
		Span: TimeSpan{StartNs: 10, EndNs: 20},
	}))

	assert.Assert(t, !strings.Contains(markup, `class="breadcrumbs"`))
	assert.Assert(t, strings.Contains(markup, `id="loading-indicator"`))
	assert.Assert(t, !strings.Contains(markup, ">Idle<"))
}

func TestAppShellRendersRankingsVisibleByDefault(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, AppShell(DashboardData{
		State: QueryState{
			FromNs:      10,
			ToNs:        20,
			Metric:      MetricBytes,
			Granularity: Granularity2LD,
		},
		Span: TimeSpan{StartNs: 10, EndNs: 20},
	}))

	assert.Assert(t, strings.Contains(markup, `id="rankings-section"`))
	assert.Assert(t, strings.Contains(markup, `id="rankings-content"`))
	assert.Assert(t, !strings.Contains(markup, `class="rankings-section is-collapsed"`))
}

func TestServiceGraphKeepsNodePositionsStableAcrossMetrics(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 9000, 10, 20),
		sampleRecord("192.168.1.10", "9.9.9.9", "alpha.lan", "lan", "lan", "dns.quad9.net", "quad9.net", "net", 9000, 30, 40),
		sampleRecord("192.168.1.11", "8.8.4.4", "beta.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 50, 60),
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 100, 70, 80),
		sampleRecord("192.168.1.11", "1.0.0.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 100, 90, 100),
		sampleRecord("192.168.1.11", "149.112.112.112", "beta.lan", "lan", "lan", "dns.quad9.net", "quad9.net", "net", 100, 110, 120),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	baseState := QueryState{
		Granularity: GranularityHostname,
		NodeLimit:   2,
		EdgeLimit:   2,
	}
	bytesGraph, err := service.Graph(context.Background(), QueryState{
		Granularity: baseState.Granularity,
		Metric:      MetricBytes,
		NodeLimit:   baseState.NodeLimit,
		EdgeLimit:   baseState.EdgeLimit,
	})
	assert.NilError(t, err)
	connectionGraph, err := service.Graph(context.Background(), QueryState{
		Granularity: baseState.Granularity,
		Metric:      MetricConnections,
		NodeLimit:   baseState.NodeLimit,
		EdgeLimit:   baseState.EdgeLimit,
	})
	assert.NilError(t, err)

	for _, node := range bytesGraph.Nodes {
		position, ok := bytesGraph.NodePositions[node.ID]
		assert.Assert(t, ok, "missing position for %s", node.ID)
		assert.Assert(t, position.X >= 0)
		assert.Assert(t, position.X <= graphWidthPx)
		assert.Assert(t, position.Y >= 0)
		assert.Assert(t, position.Y <= graphHeightPx)
	}

	for _, node := range connectionGraph.Nodes {
		bytesPosition, bytesOK := bytesGraph.NodePositions[node.ID]
		connectionPosition, connectionOK := connectionGraph.NodePositions[node.ID]
		if !bytesOK || !connectionOK {
			continue
		}
		assert.Equal(t, bytesPosition, connectionPosition)
	}

	restSourcePosition, restSourceOK := bytesGraph.NodePositions[graphRestSourceID]
	restDestinationPosition, restDestinationOK := bytesGraph.NodePositions[graphRestDestination]
	if restSourceOK && restDestinationOK {
		assert.Assert(t, restSourcePosition.X < restDestinationPosition.X)
	}
}

func renderNodeString(t *testing.T, node interface{ Render(io.Writer) error }) string {
	t.Helper()

	var builder strings.Builder
	assert.NilError(t, node.Render(&builder))
	return builder.String()
}

func TestBuildLayoutRings(t *testing.T) {
	t.Parallel()

	nodes := make([]layoutNode, 0, 60)
	for index := range 60 {
		nodes = append(nodes, layoutNode{
			ID:    string(rune('a'+(index%26))) + strings.Repeat("x", index/26),
			Score: int64(100 - index),
		})
	}

	nodeRadiiByID := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		nodeRadiiByID[node.ID] = nodeRadius(node.Score, 100)
	}

	rings := buildLayoutRings(nodes, nodeRadiiByID, graphWidthPx/2-float64(layoutNodePaddingPx), graphHeightPx/2-float64(layoutNodePaddingPx))

	totalNodes := 0
	for _, ring := range rings {
		totalNodes += len(ring)
	}

	assert.Assert(t, len(rings) >= 3)
	assert.Assert(t, len(rings[0]) > 0)
	assert.Assert(t, len(rings[0]) <= layoutInnerRingCount)
	assert.Assert(t, len(rings[1]) <= layoutMiddleRingCount)
	assert.Assert(t, len(rings[2]) > 0)
	assert.Equal(t, totalNodes, len(nodes))
}

func TestOrderLayoutRingUsesPlacedNeighborAngles(t *testing.T) {
	t.Parallel()

	neighborsByNode := map[string][]layoutNeighbor{
		"left-child": {
			{otherID: "left-anchor", weight: 5},
		},
		"right-child": {
			{otherID: "right-anchor", weight: 5},
		},
	}
	placedAngles := map[string]float64{
		"left-anchor":  normalizeAngle(math.Pi),
		"right-anchor": 0,
	}
	ring := []layoutNode{
		{ID: "right-child", Score: 10},
		{ID: "left-child", Score: 10},
	}

	ordered := orderLayoutRing(ring, neighborsByNode, placedAngles)

	assert.Equal(t, ordered[0].ID, "right-child")
	assert.Equal(t, ordered[1].ID, "left-child")
}

func TestComputeStableNodePositionsAvoidsCircleOverlap(t *testing.T) {
	t.Parallel()

	nodes := make([]layoutNode, 0, 14)
	for index := range 14 {
		nodes = append(nodes, layoutNode{
			ID:    fmt.Sprintf("node-%d", index),
			Score: int64(200 - index*10),
		})
	}

	positions := computeStableNodePositions(nodes, nil, graphWidthPx, graphHeightPx)
	maxScore := nodes[0].Score
	for leftIndex := range nodes {
		leftNode := nodes[leftIndex]
		leftPosition := positions[leftNode.ID]
		leftRadius := nodeRadius(leftNode.Score, maxScore)
		for rightIndex := leftIndex + 1; rightIndex < len(nodes); rightIndex++ {
			rightNode := nodes[rightIndex]
			rightPosition := positions[rightNode.ID]
			requiredDistance := leftRadius + nodeRadius(rightNode.Score, maxScore) + layoutNodeGapPx - 0.1
			distance := math.Hypot(rightPosition.X-leftPosition.X, rightPosition.Y-leftPosition.Y)
			assert.Assert(t, distance >= requiredDistance, "%s and %s overlap", leftNode.ID, rightNode.ID)
		}
	}
}
