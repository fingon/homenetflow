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

func TestGraphSVGMarkupDisablesSelectionForLongRange(t *testing.T) {
	t.Parallel()

	markup := graphSVGMarkup(QueryState{
		FromNs: 1,
		Metric: MetricBytes,
		ToNs:   1 + int64(8*24*time.Hour),
	}, GraphData{
		Edges: []Edge{
			{Destination: "dns.google", MetricValue: 10, Source: "alpha.lan"},
		},
		Nodes: []Node{
			{ID: "alpha.lan", Label: "alpha.lan", Total: 10},
			{ID: "dns.google", Label: "dns.google", Total: 10},
		},
		NodePositions: map[string]LayoutPoint{
			"alpha.lan":  {X: 100, Y: 100},
			"dns.google": {X: 200, Y: 100},
		},
	})

	assert.Assert(t, strings.Contains(markup, `is-entity-actions-disabled`))
	assert.Assert(t, !strings.Contains(markup, `<a `))
	assert.Assert(t, !strings.Contains(markup, `selected_entity`))
	assert.Assert(t, !strings.Contains(markup, `selected_edge_src`))
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

	rankingsMarkup := renderNodeString(t, RankingsPanel(QueryState{
		FromNs:      10,
		Granularity: GranularityHostname,
		Metric:      MetricDNSLookups,
		Sort:        SortDNSLookups,
		ToNs:        20,
	}, GraphData{
		ActiveMetric: MetricDNSLookups,
		TopEntities: []Node{
			{DNSResultState: dnsResultStateNXDOMAIN, ID: "missing.example", Label: "missing.example", Total: 1},
			{DNSResultState: dnsResultStateMixed, ID: "www.example.com", Label: "www.example.com", Total: 2},
		},
		TopEdges: []Edge{
			{Destination: "missing.example", DNSResultState: dnsResultStateNXDOMAIN, MetricValue: 1, Source: "alpha.lan"},
			{Destination: "www.example.com", DNSResultState: dnsResultStateMixed, MetricValue: 2, Source: "alpha.lan"},
		},
	}))

	nxdomainEntityAnchor := anchorMarkupForLabel(t, rankingsMarkup, "missing.example")
	mixedEntityAnchor := anchorMarkupForLabel(t, rankingsMarkup, "www.example.com")
	nxdomainEdgeAnchor := anchorMarkupForLabel(t, rankingsMarkup, "alpha.lan -&gt; missing.example")
	mixedEdgeAnchor := anchorMarkupForLabel(t, rankingsMarkup, "alpha.lan -&gt; www.example.com")

	assert.Assert(t, strings.Contains(nxdomainEntityAnchor, `dns-result-nxdomain`))
	assert.Assert(t, strings.Contains(mixedEntityAnchor, `dns-result-mixed`))
	assert.Assert(t, strings.Contains(nxdomainEdgeAnchor, `dns-result-nxdomain`))
	assert.Assert(t, strings.Contains(mixedEdgeAnchor, `dns-result-mixed`))
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

func TestSelectedPanelRendersRawFlowLinkForEligibleSelections(t *testing.T) {
	t.Parallel()

	nodeMarkup := renderNodeString(t, selectedPanelAt(QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
	}, GraphData{
		ActiveMetric: MetricBytes,
		SelectedNode: &Node{ID: "alpha.lan", Label: "alpha.lan"},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	assert.Assert(t, strings.Contains(nodeMarkup, "Show matching flows"))
	assert.Assert(t, strings.Contains(nodeMarkup, "flow_scope=entity"))
	assert.Assert(t, strings.Contains(nodeMarkup, "flow_entity=alpha.lan"))

	edgeMarkup := renderNodeString(t, selectedPanelAt(QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
	}, GraphData{
		ActiveMetric: MetricBytes,
		SelectedEdge: &Edge{Source: "alpha.lan", Destination: "dns.google"},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	assert.Assert(t, strings.Contains(edgeMarkup, "flow_scope=edge"))
	assert.Assert(t, strings.Contains(edgeMarkup, "flow_source=alpha.lan"))
	assert.Assert(t, strings.Contains(edgeMarkup, "flow_destination=dns.google"))
}

func TestSelectedPanelHidesRawFlowLinkForLongRange(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, selectedPanelAt(QueryState{
		FromNs:      1,
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
		ToNs:        1 + int64(8*24*time.Hour),
	}, GraphData{
		ActiveMetric: MetricBytes,
		SelectedEdge: &Edge{Source: "alpha.lan", Destination: "dns.google"},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	assert.Assert(t, !strings.Contains(markup, "Show matching flows"))
	assert.Assert(t, !strings.Contains(markup, "flow_scope=edge"))
}

func TestTablePanelRendersRawFlowChevronForEligibleRows(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, TablePanel(QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
	}, TableData{
		Page:       1,
		TotalPages: 1,
		VisibleRows: []TableRow{
			{Destination: "dns.google", Source: "alpha.lan"},
			{Destination: graphRestID, Source: "beta.lan", Synthetic: true},
		},
	}))

	assert.Assert(t, strings.Contains(markup, `aria-label="Show flows from alpha.lan to dns.google"`))
	assert.Assert(t, strings.Contains(markup, "flow_scope=edge"))
	assert.Assert(t, !strings.Contains(markup, `aria-label="Show flows from beta.lan to Rest"`))
}

func TestFlowDetailTableRendersRawRows(t *testing.T) {
	t.Parallel()

	direction := directionEgressParquetValue
	markup := renderNodeString(t, flowDetailTableAt(FlowDetailData{
		Page:       1,
		TotalPages: 1,
		VisibleRows: []FlowDetailRow{
			{
				Bytes:       1200,
				Destination: "dns.google",
				DstIP:       "8.8.8.8",
				Direction:   &direction,
				EndNs:       time.Date(2026, time.April, 15, 1, 2, 4, 0, time.UTC).UnixNano(),
				Packets:     7,
				Protocol:    17,
				Source:      "alpha.lan",
				SrcIP:       "192.168.1.10",
				StartNs:     time.Date(2026, time.April, 15, 1, 2, 3, 0, time.UTC).UnixNano(),
			},
			{
				Bytes:       500,
				Destination: "other.lan",
				DstIP:       "192.168.1.20",
				EndNs:       time.Date(2026, time.April, 15, 1, 3, 4, 0, time.UTC).UnixNano(),
				Packets:     2,
				Protocol:    143,
				Source:      "alpha.lan",
				SrcIP:       "192.168.1.10",
				StartNs:     time.Date(2026, time.April, 15, 1, 3, 3, 0, time.UTC).UnixNano(),
			},
		},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	assert.Assert(t, strings.Contains(markup, "alpha.lan"))
	assert.Assert(t, strings.Contains(markup, "192.168.1.10:0"))
	assert.Assert(t, strings.Contains(markup, "dns.google"))
	assert.Assert(t, strings.Contains(markup, "8.8.8.8:0"))
	assert.Assert(t, strings.Contains(markup, ">17 (UDP)</td>"))
	assert.Assert(t, strings.Contains(markup, ">143</td>"))
	assert.Assert(t, strings.Contains(markup, ">7</td>"))
	assert.Assert(t, strings.Contains(markup, ">1200</td>"))
	assert.Assert(t, strings.Contains(markup, ">Egress</td>"))
}

func TestFlowDetailTableRendersSortLinks(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, flowDetailTableAt(FlowDetailData{
		Page:       1,
		TotalPages: 1,
		Query: FlowQuery{
			Entity:  "alpha.lan",
			Scope:   FlowScopeEntity,
			Sort:    FlowSortStart,
			SortDir: FlowSortDesc,
			State: QueryState{
				FromNs:      1,
				Granularity: GranularityHostname,
				Metric:      MetricBytes,
				ToNs:        20,
			},
		},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	assert.Assert(t, strings.Contains(markup, "Start ↓"))
	assert.Assert(t, strings.Contains(markup, "flow_sort_dir=asc"))
	assert.Assert(t, strings.Contains(markup, "flow_sort=bytes"))
	assert.Assert(t, !strings.Contains(markup, "flow_sort=direction"))
	assert.Assert(t, strings.Contains(markup, `<span class="list-button raw-flow-header-button disabled">Direction</span>`))
	assert.Assert(t, strings.Contains(markup, "flow_entity=alpha.lan"))
}

func TestFlowDetailTableRendersAscendingSortArrow(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, flowDetailTableAt(FlowDetailData{
		Page:       1,
		TotalPages: 1,
		Query: FlowQuery{
			Entity:  "alpha.lan",
			Scope:   FlowScopeEntity,
			Sort:    FlowSortEnd,
			SortDir: FlowSortAsc,
			State: QueryState{
				FromNs:      1,
				Granularity: GranularityHostname,
				Metric:      MetricBytes,
				ToNs:        20,
			},
		},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	assert.Assert(t, strings.Contains(markup, "End ↑"))
	assert.Assert(t, !strings.Contains(markup, "flow_sort_dir=desc"))
}

func TestFlowDetailTableLimitsLongRangeSortLinksToTime(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, flowDetailTableAt(FlowDetailData{
		Page:       1,
		TotalPages: 1,
		Query: FlowQuery{
			Entity: "alpha.lan",
			Scope:  FlowScopeEntity,
			State: QueryState{
				FromNs:      1,
				Granularity: GranularityHostname,
				Metric:      MetricBytes,
				ToNs:        1 + int64(8*24*time.Hour),
			},
		},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	assert.Assert(t, strings.Contains(markup, "Long ranges sort by time only."))
	assert.Assert(t, strings.Contains(markup, "flow_sort=end"))
	assert.Assert(t, !strings.Contains(markup, "flow_sort=bytes"))
}

func TestFlowDetailShellRendersPresetCountsAndReverseToggle(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, FlowDetailShell(FlowDetailData{
		Page: 1,
		PresetCounts: []FlowPresetCount{
			{Count: 12, Label: "All", Preset: presetAllValue},
			{Count: 3, Label: presetHourValue, Preset: presetHourValue},
		},
		Query: FlowQuery{
			Destination: "dns.google",
			Scope:       FlowScopeEdge,
			Source:      "alpha.lan",
			State: QueryState{
				FromNs:      1,
				Granularity: GranularityHostname,
				Metric:      MetricBytes,
				ToNs:        20,
			},
		},
		TotalCount: 12,
		TotalPages: 1,
	}))

	assert.Assert(t, strings.Contains(markup, "12 rows"))
	assert.Assert(t, strings.Contains(markup, "All (12)"))
	assert.Assert(t, strings.Contains(markup, "1h (3)"))
	assert.Assert(t, strings.Contains(markup, "Both directions"))
	assert.Assert(t, strings.Contains(markup, "Forward only"))
	assert.Assert(t, strings.Contains(markup, `name="flow_match"`))
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

func TestTablePanelEndpointLinksSelectGraphEntity(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, tablePanelAt(QueryState{
		Exclude:         []string{"drop.lan"},
		FromNs:          10,
		Granularity:     GranularityHostname,
		Include:         []string{"keep.lan"},
		Metric:          MetricConnections,
		Page:            3,
		Search:          "cloud",
		SelectedEdgeDst: "old-destination",
		SelectedEdgeSrc: "old-source",
		Sort:            SortLastSeen,
		ToNs:            20,
	}, TableData{
		Page:       3,
		TotalPages: 5,
		VisibleRows: []TableRow{
			{Connections: 7, Destination: "dns.google", Source: "alpha.lan"},
		},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	sourceAnchor := anchorMarkupForLabel(t, markup, "alpha.lan")
	destinationAnchor := anchorMarkupForLabel(t, markup, "dns.google")

	for _, anchor := range []string{sourceAnchor, destinationAnchor} {
		assert.Assert(t, strings.Contains(anchor, `class="table-link"`))
		assert.Assert(t, strings.Contains(anchor, `hx-target="#app-shell"`))
		assert.Assert(t, strings.Contains(anchor, `hx-select="#app-shell"`))
		assert.Assert(t, strings.Contains(anchor, `hx-push-url="true"`))
		assert.Assert(t, strings.Contains(anchor, `from=10`))
		assert.Assert(t, strings.Contains(anchor, `to=20`))
		assert.Assert(t, strings.Contains(anchor, `metric=connections`))
		assert.Assert(t, strings.Contains(anchor, `granularity=hostname`))
		assert.Assert(t, strings.Contains(anchor, `sort=last_seen`))
		assert.Assert(t, strings.Contains(anchor, `include=keep.lan`))
		assert.Assert(t, strings.Contains(anchor, `exclude=drop.lan`))
		assert.Assert(t, strings.Contains(anchor, `search=cloud`))
		assert.Assert(t, !strings.Contains(anchor, `page=`))
		assert.Assert(t, !strings.Contains(anchor, `selected_edge_src`))
		assert.Assert(t, !strings.Contains(anchor, `selected_edge_dst`))
	}

	assert.Assert(t, strings.Contains(sourceAnchor, `selected_entity=alpha.lan`))
	assert.Assert(t, strings.Contains(destinationAnchor, `selected_entity=dns.google`))
}

func TestTablePanelDisablesEntityActionsForLongRange(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, tablePanelAt(QueryState{
		FromNs:      1,
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
		Sort:        SortBytes,
		ToNs:        1 + int64(8*24*time.Hour),
	}, TableData{
		Page:       1,
		TotalPages: 1,
		VisibleRows: []TableRow{
			{Destination: "dns.google", Source: "alpha.lan"},
		},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	assert.Assert(t, strings.Contains(markup, `class="table-link disabled"`))
	assert.Assert(t, !strings.Contains(markup, `selected_entity=alpha.lan`))
	assert.Assert(t, !strings.Contains(markup, `selected_entity=dns.google`))
	assert.Assert(t, !strings.Contains(markup, `flow_scope=edge`))
	assert.Assert(t, !strings.Contains(markup, `aria-label="Show flows from alpha.lan to dns.google"`))
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

func TestSelectedPanelRendersDeselectForSelectedEdge(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, selectedPanelAt(QueryState{
		Exclude:         []string{"drop.lan"},
		FromNs:          10,
		Granularity:     GranularityHostname,
		Include:         []string{"keep.lan"},
		Metric:          MetricBytes,
		SelectedEdgeDst: "dns.google",
		SelectedEdgeSrc: "alpha.lan",
		Sort:            SortBytes,
		ToNs:            20,
	}, GraphData{
		ActiveMetric: MetricBytes,
		SelectedEdge: &Edge{
			Bytes:       100,
			Connections: 2,
			Destination: "dns.google",
			Source:      "alpha.lan",
		},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	deselectAnchor := anchorMarkupForLabel(t, markup, "Deselect")

	assert.Assert(t, strings.Contains(deselectAnchor, `class="action-button"`))
	assert.Assert(t, strings.Contains(deselectAnchor, `from=10`))
	assert.Assert(t, strings.Contains(deselectAnchor, `to=20`))
	assert.Assert(t, strings.Contains(deselectAnchor, `granularity=hostname`))
	assert.Assert(t, strings.Contains(deselectAnchor, `include=keep.lan`))
	assert.Assert(t, strings.Contains(deselectAnchor, `exclude=drop.lan`))
	assert.Assert(t, !strings.Contains(deselectAnchor, `selected_entity`))
	assert.Assert(t, !strings.Contains(deselectAnchor, `selected_edge_src`))
	assert.Assert(t, !strings.Contains(deselectAnchor, `selected_edge_dst`))
}

func TestSelectedPanelRendersDeselectForSelectedEntity(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, selectedPanelAt(QueryState{
		FromNs:         10,
		Granularity:    GranularityHostname,
		Metric:         MetricConnections,
		Search:         "alpha",
		SelectedEntity: "alpha.lan",
		Sort:           SortConnections,
		ToNs:           20,
	}, GraphData{
		ActiveMetric: MetricConnections,
		SelectedNode: &Node{
			Egress: 5,
			ID:     "alpha.lan",
			Label:  "alpha.lan",
		},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	deselectAnchor := anchorMarkupForLabel(t, markup, "Deselect")

	assert.Assert(t, strings.Contains(deselectAnchor, `class="action-button"`))
	assert.Assert(t, strings.Contains(deselectAnchor, `from=10`))
	assert.Assert(t, strings.Contains(deselectAnchor, `to=20`))
	assert.Assert(t, strings.Contains(deselectAnchor, `metric=connections`))
	assert.Assert(t, strings.Contains(deselectAnchor, `granularity=hostname`))
	assert.Assert(t, strings.Contains(deselectAnchor, `search=alpha`))
	assert.Assert(t, !strings.Contains(deselectAnchor, `selected_entity`))
	assert.Assert(t, !strings.Contains(deselectAnchor, `selected_edge_src`))
	assert.Assert(t, !strings.Contains(deselectAnchor, `selected_edge_dst`))
}

func TestSelectedPanelDisablesEntityActionsForLongRange(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, selectedPanelAt(QueryState{
		FromNs: 1,
		Metric: MetricBytes,
		ToNs:   1 + int64(8*24*time.Hour),
	}, GraphData{
		ActiveMetric: MetricBytes,
		SelectedNode: &Node{ID: "alpha.lan", Label: "alpha.lan"},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	assert.Assert(t, strings.Contains(markup, "Entity actions are available for ranges up to 7 days."))
	assert.Assert(t, !strings.Contains(markup, "Filter to this entity"))
	assert.Assert(t, !strings.Contains(markup, "Exclude"))
	assert.Assert(t, !strings.Contains(markup, "Show matching flows"))
}

func TestBreakdownPanelRendersTrafficBreakdown(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, BreakdownPanel(QueryState{
		Metric: MetricBytes,
	}, GraphData{
		ActiveMetric: MetricBytes,
		Breakdown: SelectionBreakdown{
			Protocols: &BreakdownChart{
				Label: "Protocols",
				Slices: []BreakdownSlice{
					{FilterParam: "protocol", FilterValue: "6", Label: "6 (TCP)", Value: 120},
					{FilterParam: "protocol", FilterValue: "17", Label: "17 (UDP)", Value: 80},
				},
			},
		},
	}))

	assert.Assert(t, strings.Contains(markup, "Breakdown"))
	assert.Assert(t, strings.Contains(markup, "Protocols"))
	assert.Assert(t, strings.Contains(markup, "6 (TCP)"))
	assert.Assert(t, strings.Contains(markup, "17 (UDP)"))
	assert.Assert(t, strings.Contains(markup, `class="breakdown-svg"`))
	assert.Assert(t, strings.Contains(markup, `protocol=6`))
	assert.Assert(t, strings.Contains(markup, `id="breakdown-section"`))
	assert.Assert(t, strings.Contains(markup, `class="section-panel section-block breakdown-sidebar"`))
	assert.Assert(t, !strings.Contains(markup, `data-collapses-with="graph-section-content"`))
	assert.Assert(t, !strings.Contains(markup, `aria-label="Collapse Breakdown"`))
	assert.Assert(t, !strings.Contains(markup, `id="breakdown-content"`))
	assert.Assert(t, !strings.Contains(markup, `aria-controls="breakdown-content"`))
}

func TestBreakdownPanelRendersFamilyLinks(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, BreakdownPanel(QueryState{
		Metric: MetricConnections,
	}, GraphData{
		ActiveMetric: MetricConnections,
		Breakdown: SelectionBreakdown{
			Family: &BreakdownChart{
				Label: "IP Family",
				Slices: []BreakdownSlice{
					{FilterParam: "family", FilterValue: "ipv4", Label: "IPv4", Value: 7},
					{FilterParam: "family", FilterValue: "ipv6", Label: "IPv6", Value: 3},
				},
			},
		},
	}))

	assert.Assert(t, strings.Contains(markup, "IP Family"))
	assert.Assert(t, strings.Contains(markup, "IPv4"))
	assert.Assert(t, strings.Contains(markup, "IPv6"))
	assert.Assert(t, strings.Contains(markup, `family=ipv4`))
}

func TestBreakdownPanelDoesNotRenderForDNSMetric(t *testing.T) {
	t.Parallel()

	node := BreakdownPanel(QueryState{
		Metric: MetricDNSLookups,
	}, GraphData{
		ActiveMetric: MetricDNSLookups,
	})

	assert.Assert(t, node == nil)
}

func TestSelectedPanelPeerLinksSelectPeerEntity(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, selectedPanelAt(QueryState{
		FromNs:         10,
		Granularity:    GranularityHostname,
		Metric:         MetricConnections,
		SelectedEntity: "alpha.lan",
		Sort:           SortConnections,
		ToNs:           20,
	}, GraphData{
		ActiveMetric: MetricConnections,
		SelectedNode: &Node{
			Egress: 5,
			ID:     "alpha.lan",
			Label:  "alpha.lan",
		},
		SelectedNodePeers: []DetailPeer{
			{Entity: "dns.google", MetricValue: 7},
		},
	}, time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)))

	peerAnchor := anchorMarkupForLabel(t, markup, "dns.google")

	assert.Assert(t, strings.Contains(peerAnchor, `class="table-link ranking-link"`))
	assert.Assert(t, strings.Contains(peerAnchor, `class="ranking-label"`))
	assert.Assert(t, strings.Contains(peerAnchor, `class="ranking-value"`))
	assert.Assert(t, strings.Contains(peerAnchor, `selected_entity=dns.google`))
	assert.Assert(t, strings.Contains(peerAnchor, `metric=connections`))
	assert.Assert(t, strings.Contains(peerAnchor, `granularity=hostname`))
	assert.Assert(t, strings.Contains(peerAnchor, `>7</span>`))
	assert.Assert(t, !strings.Contains(peerAnchor, `selected_entity=alpha.lan`))
	assert.Assert(t, !strings.Contains(peerAnchor, `selected_edge_src`))
	assert.Assert(t, !strings.Contains(peerAnchor, `selected_edge_dst`))
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

func TestRankingsPanelUsesSelectionLinkStyling(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, RankingsPanel(QueryState{
		FromNs:      10,
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
		Sort:        SortBytes,
		ToNs:        20,
	}, GraphData{
		ActiveMetric: MetricBytes,
		TopEntities: []Node{
			{ID: "alpha.lan", Label: "alpha.lan", Total: 100},
		},
		TopEdges: []Edge{
			{Destination: "dns.google", MetricValue: 200, Source: "alpha.lan"},
		},
	}))

	entityAnchor := anchorMarkupForLabel(t, markup, "alpha.lan")
	edgeAnchor := anchorMarkupForLabel(t, markup, "alpha.lan -&gt; dns.google")

	for _, anchor := range []string{entityAnchor, edgeAnchor} {
		assert.Assert(t, strings.Contains(anchor, `class="table-link ranking-link"`))
		assert.Assert(t, strings.Contains(anchor, `class="ranking-label"`))
		assert.Assert(t, strings.Contains(anchor, `class="ranking-value"`))
		assert.Assert(t, strings.Contains(anchor, `hx-target="#app-shell"`))
		assert.Assert(t, strings.Contains(anchor, `hx-select="#app-shell"`))
		assert.Assert(t, strings.Contains(anchor, `hx-push-url="true"`))
		assert.Assert(t, !strings.Contains(anchor, `list-button`))
	}
	assert.Assert(t, strings.Contains(entityAnchor, `selected_entity=alpha.lan`))
	assert.Assert(t, strings.Contains(entityAnchor, `>100</span>`))
	assert.Assert(t, strings.Contains(edgeAnchor, `selected_edge_src=alpha.lan`))
	assert.Assert(t, strings.Contains(edgeAnchor, `selected_edge_dst=dns.google`))
	assert.Assert(t, strings.Contains(edgeAnchor, `>200</span>`))
}

func TestRankingsPanelMarksIgnoredItems(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, RankingsPanel(QueryState{
		FromNs:      10,
		Granularity: GranularityHostname,
		Metric:      MetricDNSLookups,
		Sort:        SortDNSLookups,
		ToNs:        20,
	}, GraphData{
		ActiveMetric: MetricDNSLookups,
		TopEntities: []Node{
			{ID: "tapo1.lan", Ignored: true, Label: "tapo1.lan", Total: 100},
		},
		TopEdges: []Edge{
			{Destination: "tapo1.lan", Ignored: true, MetricValue: 200, Source: "fw.lan"},
		},
	}))

	assert.Assert(t, strings.Contains(markup, `ranking-link is-ignored`))
	assert.Assert(t, strings.Contains(markup, `>Ignored</span>`))
}

func TestRankingsPanelDisablesSelectionForLongRange(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, RankingsPanel(QueryState{
		FromNs:      1,
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
		Sort:        SortBytes,
		ToNs:        1 + int64(8*24*time.Hour),
	}, GraphData{
		ActiveMetric: MetricBytes,
		TopEntities: []Node{
			{ID: "alpha.lan", Label: "alpha.lan", Total: 100},
		},
		TopEdges: []Edge{
			{Destination: "dns.google", MetricValue: 200, Source: "alpha.lan"},
		},
	}))

	assert.Assert(t, strings.Contains(markup, `class="table-link ranking-link disabled"`))
	assert.Assert(t, !strings.Contains(markup, `selected_entity=alpha.lan`))
	assert.Assert(t, !strings.Contains(markup, `selected_edge_src=alpha.lan`))
	assert.Assert(t, !strings.Contains(markup, `hx-get=`))
}

func TestSummaryPanelRendersIgnoredDNSLookupTotals(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, SummaryPanel(QueryState{
		FromNs: 10,
		Metric: MetricDNSLookups,
		ToNs:   20,
	}, GraphData{
		Totals: Totals{
			Connections: 7,
			Ignored:     3,
		},
	}))

	assert.Assert(t, strings.Contains(markup, ">Ignored DNS Lookups<"))
	assert.Assert(t, strings.Contains(markup, ">3<"))
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

func TestAppShellRendersRankingsInSidebar(t *testing.T) {
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
	assert.Assert(t, strings.Contains(markup, `class="dashboard-sidebar"`))
	assert.Assert(t, strings.Contains(markup, `class="section-panel section-block rankings-panel"`))
	assert.Assert(t, strings.Contains(markup, `class="rankings-tabs"`))
	assert.Assert(t, strings.Contains(markup, `data-rankings-tab="entities"`))
	assert.Assert(t, strings.Contains(markup, `data-rankings-tab="flows"`))
	assert.Assert(t, strings.Contains(markup, `id="rankings-panel-entities"`))
	assert.Assert(t, strings.Contains(markup, `id="rankings-panel-flows"`))
	assert.Assert(t, !strings.Contains(markup, `aria-label="Collapse Graph"`))
	assert.Assert(t, !strings.Contains(markup, `aria-label="Collapse Rankings"`))
	assert.Assert(t, strings.Contains(markup, `aria-label="Expand Flows Table"`))
	assert.Assert(t, !strings.Contains(markup, `aria-controls="graph-section-content"`))
	assert.Assert(t, !strings.Contains(markup, `aria-controls="rankings-content"`))
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
	assert.Assert(t, strings.Contains(markup, `id="rankings-panel-entities"`))
	assert.Assert(t, strings.Contains(markup, `id="rankings-panel-flows"`))
	assert.Assert(t, strings.Contains(markup, `aria-selected="true"`))
	assert.Assert(t, strings.Contains(markup, `id="rankings-panel-flows" role="tabpanel" aria-labelledby="rankings-tab-flows" hidden`))
	assert.Assert(t, !strings.Contains(markup, `aria-label="Collapse Graph"`))
	assert.Assert(t, !strings.Contains(markup, `aria-label="Collapse Rankings"`))
	assert.Assert(t, !strings.Contains(markup, `class="content-grid is-collapsed"`))
	assert.Assert(t, !strings.Contains(markup, `class="rankings-panel is-collapsed"`))
}

func TestAppShellRendersBreakdownBesideTimelineAndGraph(t *testing.T) {
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
			Breakdown: SelectionBreakdown{
				Protocols: &BreakdownChart{
					Label: "Protocols",
					Slices: []BreakdownSlice{
						{FilterParam: "protocol", FilterValue: "6", Label: "6 (TCP)", Value: 120},
						{FilterParam: "protocol", FilterValue: "17", Label: "17 (UDP)", Value: 80},
					},
				},
			},
		},
	}))

	assert.Assert(t, strings.Contains(markup, `class="dashboard-main"`))
	assert.Assert(t, strings.Contains(markup, `class="dashboard-primary"`))
	assert.Assert(t, strings.Contains(markup, `class="dashboard-sidebar has-breakdown"`))
	assert.Assert(t, strings.Contains(markup, `id="histogram"`))
	assert.Assert(t, strings.Contains(markup, `id="graph-section"`))
	assert.Assert(t, strings.Contains(markup, `id="breakdown-section"`))
	assert.Assert(t, !strings.Contains(markup, `data-collapses-with="graph-section-content"`))
	assert.Assert(t, strings.Contains(markup, "Protocols"))
	assert.Assert(t, !strings.Contains(markup, `aria-label="Collapse Breakdown"`))
}

func TestAppShellOmitsBreakdownSidebarWhenBreakdownUnavailable(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, AppShell(DashboardData{
		State: QueryState{
			FromNs:      10,
			ToNs:        20,
			Metric:      MetricDNSLookups,
			Granularity: Granularity2LD,
		},
		Span: TimeSpan{StartNs: 10, EndNs: 20},
		Graph: GraphData{
			ActiveMetric: MetricDNSLookups,
		},
	}))

	assert.Assert(t, strings.Contains(markup, `class="dashboard-main"`))
	assert.Assert(t, strings.Contains(markup, `class="dashboard-sidebar"`))
	assert.Assert(t, !strings.Contains(markup, `class="dashboard-sidebar has-breakdown"`))
	assert.Assert(t, !strings.Contains(markup, `id="breakdown-section"`))
	assert.Assert(t, strings.Contains(markup, `id="rankings-section"`))
}

func TestAppShellRendersFlowsTableCollapsedByDefault(t *testing.T) {
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

	assert.Assert(t, strings.Contains(markup, `aria-label="Expand Flows Table"`))
	assert.Assert(t, strings.Contains(markup, `aria-controls="table-content"`))
	assert.Assert(t, strings.Contains(markup, `id="table-content" class="section-content is-collapsed"`))
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

	restPosition, restOK := bytesGraph.NodePositions[graphRestID]
	if restOK {
		assert.Assert(t, restPosition.Y > graphHeightPx/2)
	}
}

func renderNodeString(t *testing.T, node interface{ Render(io.Writer) error }) string {
	t.Helper()

	var builder strings.Builder
	assert.NilError(t, node.Render(&builder))
	return builder.String()
}

func anchorMarkupForLabel(t *testing.T, markup, label string) string {
	t.Helper()

	labelIndex := strings.Index(markup, ">"+label)
	assert.Assert(t, labelIndex >= 0, "anchor label %q not found", label)
	startIndex := strings.LastIndex(markup[:labelIndex], "<a ")
	assert.Assert(t, startIndex >= 0, "anchor start for %q not found", label)
	endOffset := strings.Index(markup[labelIndex:], "</a>")
	assert.Assert(t, endOffset >= 0, "anchor end for %q not found", label)
	return markup[startIndex : labelIndex+endOffset+len("</a>")]
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

func TestBuildLayoutRingsKeepsDefaultGraphOnOneRing(t *testing.T) {
	t.Parallel()

	nodes := make([]layoutNode, 0, 9)
	for index := range 9 {
		nodes = append(nodes, layoutNode{
			ID:    fmt.Sprintf("node-%d", index),
			Score: int64(100 - index),
		})
	}

	nodeRadiiByID := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		nodeRadiiByID[node.ID] = nodeRadius(node.Score, 100)
	}

	rings := buildLayoutRings(nodes, nodeRadiiByID, graphWidthPx/2-float64(layoutNodePaddingPx), graphHeightPx/2-float64(layoutNodePaddingPx))

	assert.Equal(t, len(rings), 1)
	assert.Equal(t, len(rings[0]), len(nodes))
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
