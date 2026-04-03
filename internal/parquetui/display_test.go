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

func TestHistogramSVGMarkupAddsAxisLabels(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC)
	bins := []HistogramBin{
		{FromNs: start.UnixNano(), ToNs: start.Add(6*time.Hour).UnixNano() - 1, Value: 1000},
		{FromNs: start.Add(6 * time.Hour).UnixNano(), ToNs: start.Add(12*time.Hour).UnixNano() - 1, Value: 2000},
		{FromNs: start.Add(12 * time.Hour).UnixNano(), ToNs: start.Add(18*time.Hour).UnixNano() - 1, Value: 3000},
		{FromNs: start.Add(18 * time.Hour).UnixNano(), ToNs: start.Add(24*time.Hour).UnixNano() - 1, Value: 4000},
	}

	markup := histogramSVGMarkup(MetricBytes, bins)

	assert.Assert(t, strings.Contains(markup, "histogram-axis-label"))
	assert.Assert(t, strings.Contains(markup, ">00:00<"))
	assert.Assert(t, strings.Contains(markup, ">23:59<"))
	assert.Assert(t, strings.Contains(markup, "Value: 4000"))
}

func TestTopBarRendersTimePresetButtons(t *testing.T) {
	t.Parallel()

	markup := renderNodeString(t, topBar(DashboardData{
		State: QueryState{
			FromNs:      10,
			ToNs:        20,
			Metric:      MetricBytes,
			Granularity: Granularity2LD,
			View:        ViewSplit,
			Sort:        SortBytes,
		},
	}))

	assert.Assert(t, !strings.Contains(markup, `id="preset-select"`))
	assert.Assert(t, strings.Contains(markup, `name="preset"`))
	assert.Assert(t, strings.Contains(markup, `value="1d"`))
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

	rings := buildLayoutRings(nodes)

	assert.Equal(t, len(rings), 3)
	assert.Equal(t, len(rings[0]), layoutInnerRingCount)
	assert.Equal(t, len(rings[1]), layoutMiddleRingCount)
	assert.Equal(t, len(rings[2]), 28)
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
