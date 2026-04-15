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
	assert.Equal(t, state.AddressFamily, AddressFamilyAll)
	assert.Equal(t, state.Direction, DirectionBoth)
	assert.Equal(t, state.EdgeLimit, defaultEdgeLimit)
	assert.Equal(t, state.Page, defaultPage)
	assert.Equal(t, state.PageSize, defaultPageSize)
}

func TestParseFlowQueryEdge(t *testing.T) {
	request := httptest.NewRequest("GET", "/flows?metric=bytes&flow_scope=edge&flow_source=alpha.lan&flow_destination=dns.google", nil)

	query, err := ParseFlowQuery(request)

	assert.NilError(t, err)
	assert.Equal(t, query.Scope, FlowScopeEdge)
	assert.Equal(t, query.Source, "alpha.lan")
	assert.Equal(t, query.Destination, "dns.google")
	assert.Equal(t, query.State.Metric, MetricBytes)
}

func TestParseQueryStateAddressFamily(t *testing.T) {
	request := httptest.NewRequest("GET", "/?family=ipv6", nil)

	state := ParseQueryState(request)

	assert.Equal(t, state.AddressFamily, AddressFamilyIPv6)
}

func TestParseQueryStateInvalidAddressFamilyDefaultsToAll(t *testing.T) {
	request := httptest.NewRequest("GET", "/?family=bogus", nil)

	state := ParseQueryState(request)

	assert.Equal(t, state.AddressFamily, AddressFamilyAll)
}

func TestParseQueryStateDirection(t *testing.T) {
	request := httptest.NewRequest("GET", "/?direction=ingress", nil)

	state := ParseQueryState(request)

	assert.Equal(t, state.Direction, DirectionIngress)
}

func TestParseQueryStateInvalidDirectionDefaultsToBoth(t *testing.T) {
	for _, direction := range []string{"sideways", "inbound", "outbound"} {
		request := httptest.NewRequest("GET", "/?direction="+direction, nil)

		state := ParseQueryState(request)

		assert.Equal(t, state.Direction, DirectionBoth)
	}
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

func TestQueryStateNormalizedResetsDirectionForDNSLookups(t *testing.T) {
	state := QueryState{
		Direction: DirectionIngress,
		Metric:    MetricDNSLookups,
	}

	normalized := state.Normalized(TimeSpan{
		StartNs: 10,
		EndNs:   100,
	})

	assert.Equal(t, normalized.Direction, DirectionBoth)
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

func TestQueryStateNormalizedClearsEntityActionsForLongRange(t *testing.T) {
	state := QueryState{
		Exclude:         []string{"drop.lan"},
		FromNs:          1,
		Include:         []string{"keep.lan"},
		SelectedEdgeDst: "dns.google",
		SelectedEdgeSrc: "alpha.lan",
		SelectedEntity:  "alpha.lan",
		ToNs:            1 + int64(8*24*time.Hour),
	}

	normalized := state.Normalized(TimeSpan{
		StartNs: 1,
		EndNs:   1 + int64(8*24*time.Hour),
	})

	assert.Equal(t, normalized.SelectedEntity, "")
	assert.Equal(t, normalized.SelectedEdgeSrc, "")
	assert.Equal(t, normalized.SelectedEdgeDst, "")
	assert.Equal(t, len(normalized.Include), 0)
	assert.Equal(t, len(normalized.Exclude), 0)
}

func TestQueryStateNormalizedKeepsEntityActionsAtWeekRange(t *testing.T) {
	state := QueryState{
		Exclude:        []string{"drop.lan"},
		FromNs:         1,
		Include:        []string{"keep.lan"},
		SelectedEntity: "alpha.lan",
		ToNs:           1 + int64(7*24*time.Hour),
	}

	normalized := state.Normalized(TimeSpan{
		StartNs: 1,
		EndNs:   1 + int64(7*24*time.Hour),
	})

	assert.Equal(t, normalized.SelectedEntity, "alpha.lan")
	assert.DeepEqual(t, normalized.Include, []string{"keep.lan"})
	assert.DeepEqual(t, normalized.Exclude, []string{"drop.lan"})
}

func TestQueryStateValuesSkipPreset(t *testing.T) {
	state := QueryState{
		AddressFamily: AddressFamilyIPv4,
		Direction:     DirectionEgress,
		FromNs:        10,
		ToNs:          20,
		Metric:        MetricBytes,
		Granularity:   Granularity2LD,
		Sort:          SortBytes,
		Preset:        presetWeekValue,
	}

	values := state.Values()

	assert.Equal(t, values.Get("preset"), "")
	assert.Equal(t, values.Get("family"), "ipv4")
	assert.Equal(t, values.Get("direction"), "egress")
	assert.Equal(t, values.Get("view"), "")
}

func TestQueryStateValuesSkipDefaultDirection(t *testing.T) {
	state := QueryState{
		Direction:   DirectionBoth,
		Metric:      MetricBytes,
		Granularity: Granularity2LD,
		Sort:        SortBytes,
	}

	values := state.Values()

	assert.Equal(t, values.Get("direction"), "")
}

func TestParseQueryStateIgnoresLegacyView(t *testing.T) {
	request := httptest.NewRequest("GET", "/?view=table", nil)

	state := ParseQueryState(request)

	assert.Equal(t, state.Metric, MetricBytes)
	assert.Equal(t, state.Sort, SortBytes)
}

func TestLayoutCacheStateClearsSelectedEdge(t *testing.T) {
	state := QueryState{
		Metric:          MetricConnections,
		SelectedEdgeDst: "dst.test",
		SelectedEdgeSrc: "src.test",
	}

	cacheState := state.layoutCacheState()

	assert.Equal(t, cacheState.SelectedEdgeSrc, "")
	assert.Equal(t, cacheState.SelectedEdgeDst, "")
	assert.Equal(t, cacheState.Metric, MetricBytes)
}
