package parquetui

import (
	"net/http/httptest"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestParseQueryStateDefaults(t *testing.T) {
	request := httptest.NewRequest("GET", "/", nil)

	state := ParseQueryState(request)

	assert.Equal(t, state.Metric, MetricBytes)
	assert.Equal(t, state.Granularity, Granularity2LD)
	assert.Equal(t, state.EdgeLimit, defaultEdgeLimit)
	assert.Equal(t, state.Page, defaultPage)
	assert.Equal(t, state.PageSize, defaultPageSize)
	assert.Equal(t, state.View, defaultView)
}

func TestParseQueryStateDefaultsSortForConnectionsMetric(t *testing.T) {
	request := httptest.NewRequest("GET", "/?metric=connections", nil)

	state := ParseQueryState(request)

	assert.Equal(t, state.Metric, MetricConnections)
	assert.Equal(t, state.Sort, SortConnections)
}

func TestQueryStateNormalizedAppliesSpanAndNodeLimit(t *testing.T) {
	state := QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricConnections,
		FromNs:      0,
		ToNs:        0,
	}

	normalized := state.Normalized(TimeSpan{
		StartNs: 10,
		EndNs:   100,
	})

	assert.Equal(t, normalized.FromNs, int64(10))
	assert.Equal(t, normalized.ToNs, int64(100))
	assert.Equal(t, normalized.NodeLimit, 150)
	assert.Equal(t, normalized.Metric, MetricConnections)
}

func TestQueryStateNormalizedExpandsPresetIntoRange(t *testing.T) {
	state := QueryState{
		Preset: presetDayValue,
	}

	normalized := state.Normalized(TimeSpan{
		StartNs: 10,
		EndNs:   int64(48 * time.Hour),
	})

	assert.Equal(t, normalized.ToNs, int64(48*time.Hour))
	assert.Equal(t, normalized.FromNs, int64(24*time.Hour))
}

func TestQueryStateNormalizedSupportsLegacyDayPreset(t *testing.T) {
	state := QueryState{
		Preset: presetDayLegacy,
	}

	normalized := state.Normalized(TimeSpan{
		StartNs: 10,
		EndNs:   int64(48 * time.Hour),
	})

	assert.Equal(t, normalized.ToNs, int64(48*time.Hour))
	assert.Equal(t, normalized.FromNs, int64(24*time.Hour))
}

func TestQueryStateNormalizedDefaultsSortForMetric(t *testing.T) {
	state := QueryState{
		Metric: MetricConnections,
		Sort:   "",
	}

	normalized := state.Normalized(TimeSpan{
		StartNs: 10,
		EndNs:   100,
	})

	assert.Equal(t, normalized.Sort, SortConnections)
}

func TestQueryStateValuesSkipPreset(t *testing.T) {
	state := QueryState{
		FromNs:      10,
		ToNs:        20,
		Metric:      MetricBytes,
		Granularity: Granularity2LD,
		View:        ViewSplit,
		Sort:        SortBytes,
		Preset:      presetWeekValue,
	}

	values := state.Values()

	assert.Equal(t, values.Get("preset"), "")
}

func TestLayoutCacheStateClearsSelectedEdge(t *testing.T) {
	state := QueryState{
		Metric:          MetricConnections,
		SelectedEdgeDst: "dst.test",
		SelectedEdgeSrc: "src.test",
		View:            ViewTable,
	}

	cacheState := state.layoutCacheState()

	assert.Equal(t, cacheState.SelectedEdgeSrc, "")
	assert.Equal(t, cacheState.SelectedEdgeDst, "")
	assert.Equal(t, cacheState.Metric, MetricBytes)
	assert.Equal(t, cacheState.View, defaultView)
}
