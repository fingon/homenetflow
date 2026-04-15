package parquetui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/fingon/homenetflow/internal/model"
	"github.com/fingon/homenetflow/internal/parquetout"
	"gotest.tools/v3/assert"
)

const (
	dnsLookupTestQuery2LD = "example.com"
	dnsLookupTestQueryTLD = "com"
)

func TestDirectionParquetValuesMatchIPFIX(t *testing.T) {
	assert.Equal(t, directionIngressParquetValue, int32(0))
	assert.Equal(t, directionEgressParquetValue, int32(1))
}

func TestTotalsFromEdgesAggregatesRawEdgeData(t *testing.T) {
	totals := totalsFromEdges([]Edge{
		{Bytes: 100, Connections: 2},
		{Bytes: 250, Connections: 3},
	}, 4, 1)

	assert.Equal(t, totals.Bytes, int64(350))
	assert.Equal(t, totals.Connections, int64(5))
	assert.Equal(t, totals.Entities, 4)
	assert.Equal(t, totals.Edges, 1)
}

func TestNewServiceRejectsBaseParquet(t *testing.T) {
	tempDir := t.TempDir()
	writeBaseParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"))

	_, err := NewService(context.Background(), tempDir, time.Hour)

	assert.ErrorContains(t, err, "is not enriched output")
}

func TestServiceGraphAddsRestNodesAtGranularLevels(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 200, 30, 40),
		sampleRecord("192.168.1.12", "9.9.9.9", "gamma.lan", "lan", "lan", "dns.quad9.net", "quad9.net", "net", 300, 50, 60),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
		NodeLimit:   1,
		EdgeLimit:   0,
	})
	assert.NilError(t, err)

	assert.Assert(t, len(graph.Nodes) >= 2)
	assert.Equal(t, graph.Totals.Connections, int64(3))
	assert.Equal(t, graph.Totals.Bytes, int64(600))
	assert.Assert(t, containsNode(graph.Nodes, graphRestSourceID))
	assert.Assert(t, containsNode(graph.Nodes, graphRestDestination))
	assert.Assert(t, containsNodeLabel(graph.Nodes, "Other Sources"))
	assert.Assert(t, containsNodeLabel(graph.Nodes, "Other Destinations"))
}

func TestAppIndexRendersMainRegions(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	app := &App{service: service}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	app.routes().ServeHTTP(recorder, request)

	assert.Equal(t, recorder.Code, http.StatusOK)
	assert.Assert(t, strings.Contains(recorder.Body.String(), "Graph"))
	assert.Assert(t, strings.Contains(recorder.Body.String(), "Timeline"))
	assert.Assert(t, strings.Contains(recorder.Body.String(), "Flows Table"))
	assert.Assert(t, strings.Contains(recorder.Body.String(), "/static/htmx.min.js"))
	assert.Assert(t, strings.Contains(recorder.Body.String(), `id="app-shell"`))
	assert.Assert(t, !strings.Contains(recorder.Body.String(), `data-dev-mode="true"`))
	assert.Assert(t, !strings.Contains(recorder.Body.String(), "initial-state-json"))
	assert.Assert(t, !strings.Contains(recorder.Body.String(), "span-json"))
}

func TestAppIndexRendersDevMetadataInDevMode(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	app := &App{
		devMode:         true,
		devSessionToken: "dev-token",
		service:         service,
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	app.routes().ServeHTTP(recorder, request)

	assert.Equal(t, recorder.Code, http.StatusOK)
	assert.Assert(t, strings.Contains(recorder.Body.String(), `data-dev-mode="true"`))
	assert.Assert(t, strings.Contains(recorder.Body.String(), `data-dev-session-token="dev-token"`))
}

func TestAppIndexDisablesDirectionForDNSLookups(t *testing.T) {
	const dnsLookupTestLAN = "lan"

	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, int64(time.Hour), int64(2*time.Hour)),
	})
	client2LD := dnsLookupTestLAN
	clientTLD := dnsLookupTestLAN
	query2LD := dnsLookupTestQuery2LD
	queryTLD := dnsLookupTestQueryTLD
	writeDNSLookupParquet(t, filepath.Join(tempDir, "dns_lookups_202604.parquet"), []model.DNSLookupRecord{
		{Client2LD: &client2LD, ClientIP: "192.168.1.10", ClientIPVersion: model.IPVersion4, ClientIsPrivate: true, ClientTLD: &clientTLD, Lookups: 1, Query2LD: &query2LD, QueryName: "www.example.com", QueryTLD: &queryTLD, QueryType: "A", TimeStartNs: int64(time.Hour)},
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	app := &App{service: service}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/?metric=dns_lookups&direction=ingress", nil)

	app.routes().ServeHTTP(recorder, request)

	assert.Equal(t, recorder.Code, http.StatusOK)
	body := recorder.Body.String()
	assert.Assert(t, strings.Contains(body, "Direction"))
	assert.Assert(t, strings.Contains(body, `name="direction"`))
	assert.Assert(t, strings.Contains(body, `value="both"`))
	assert.Assert(t, strings.Contains(body, `disabled`))
	assert.Assert(t, !strings.Contains(body, "Direction: Ingress"))
}

func TestAppIndexDisablesUnsupportedControlsForLongRange(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 1, 20),
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 200, 1+int64(8*24*time.Hour), 1+int64(8*24*time.Hour)+20),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	app := &App{service: service}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/?from=1&to="+strconv.FormatInt(1+int64(8*24*time.Hour), 10)+"&granularity=hostname&search=alpha", nil)

	app.routes().ServeHTTP(recorder, request)

	assert.Equal(t, recorder.Code, http.StatusOK)
	body := recorder.Body.String()
	assert.Assert(t, strings.Contains(body, `id="search-input"`))
	assert.Assert(t, strings.Contains(body, `disabled`))
	assert.Assert(t, !strings.Contains(body, `value="alpha"`))
	assert.Assert(t, !strings.Contains(body, `value="hostname" checked`))
	assert.Assert(t, strings.Contains(body, `value="2ld" checked`))
}

func TestAppIndexRendersAppShellForHTMXRequests(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	app := &App{service: service}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("HX-Request", "true")

	app.routes().ServeHTTP(recorder, request)

	assert.Equal(t, recorder.Code, http.StatusOK)
	assert.Assert(t, strings.Contains(recorder.Body.String(), `id="app-shell"`))
	assert.Assert(t, !strings.Contains(recorder.Body.String(), "<!DOCTYPE html>"))
}

func TestAppFlowsRendersSelectedFlowShellForHTMXRequests(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	app := &App{service: service}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/flows?metric=bytes&granularity=hostname&flow_scope=edge&flow_source=alpha.lan&flow_destination=dns.google", nil)
	request.Header.Set("HX-Request", "true")

	app.routes().ServeHTTP(recorder, request)

	assert.Equal(t, recorder.Code, http.StatusOK)
	body := recorder.Body.String()
	assert.Assert(t, strings.Contains(body, `id="app-shell"`))
	assert.Assert(t, strings.Contains(body, "Flows from alpha.lan to dns.google"))
	assert.Assert(t, strings.Contains(body, "192.168.1.10:0"))
	assert.Assert(t, !strings.Contains(body, "<!DOCTYPE html>"))
}

func TestAppVersionReturnsSessionToken(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	app := &App{
		devMode:         true,
		devSessionToken: "dev-token",
		service:         service,
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/version", nil)

	app.routes().ServeHTTP(recorder, request)

	assert.Equal(t, recorder.Code, http.StatusOK)
	assert.Equal(t, recorder.Body.String(), "dev-token")
	assert.Equal(t, recorder.Header().Get("Content-Type"), "text/plain; charset=utf-8")
}

func TestServiceHistogramAggregatesIntoOrderedBins(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 200, 110, 120),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	bins, err := service.Histogram(context.Background(), QueryState{
		Metric: MetricBytes,
		FromNs: 10,
		ToNs:   120,
	})
	assert.NilError(t, err)
	assert.Equal(t, len(bins), histogramBinCount)
	assert.Equal(t, bins[0].Value, int64(100))
	assert.Equal(t, bins[len(bins)-1].Value, int64(200))
}

func TestServiceTableDefaultsSortToMetric(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("host-a", "dst-one", "host-a", "lan", "lan", "dst-one", "one.test", "test", 500, 10, 20),
		sampleRecord("host-a", "dst-one", "host-a", "lan", "lan", "dst-one", "one.test", "test", 500, 30, 40),
		sampleRecord("host-a", "dst-two", "host-a", "lan", "lan", "dst-two", "two.test", "test", 1200, 50, 60),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	table, err := service.Table(context.Background(), QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricConnections,
	})
	assert.NilError(t, err)

	assert.Equal(t, table.Sort, SortConnections)
	assert.Equal(t, table.VisibleRows[0].Destination, "dst-one")
	assert.Equal(t, table.VisibleRows[0].Connections, int64(2))
}

func TestServiceFlowDetailsEntityIncludesSourceAndDestinationMatches(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("8.8.8.8", "192.168.1.10", "dns.google", "google.com", "com", "alpha.lan", "lan", "lan", 200, 30, 40),
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 300, 50, 60),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	flows, err := service.FlowDetails(context.Background(), FlowQuery{
		Entity: "alpha.lan",
		Scope:  FlowScopeEntity,
		State: QueryState{
			Granularity: GranularityHostname,
			Metric:      MetricBytes,
		},
	})
	assert.NilError(t, err)

	assert.Equal(t, flows.TotalCount, 2)
	assert.Equal(t, len(flows.VisibleRows), 2)
	assert.Equal(t, flows.VisibleRows[0].Bytes, int64(200))
	assert.Equal(t, flows.VisibleRows[0].Source, "dns.google")
	assert.Equal(t, flows.VisibleRows[1].Bytes, int64(100))
	assert.Equal(t, flows.VisibleRows[1].Destination, "dns.google")
}

func TestServiceFlowDetailsEdgeMatchesExactDirectionAndFilters(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 200, 30, 40),
		sampleRecord("8.8.8.8", "192.168.1.10", "dns.google", "google.com", "com", "alpha.lan", "lan", "lan", 300, 50, 60),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	flows, err := service.FlowDetails(context.Background(), FlowQuery{
		Destination: "dns.google",
		Scope:       FlowScopeEdge,
		Source:      "alpha.lan",
		State: QueryState{
			Granularity: GranularityHostname,
			Metric:      MetricBytes,
			FromNs:      25,
			ToNs:        45,
		},
	})
	assert.NilError(t, err)

	assert.Equal(t, flows.TotalCount, 1)
	assert.Equal(t, flows.VisibleRows[0].Bytes, int64(200))
	assert.Equal(t, flows.VisibleRows[0].Source, "alpha.lan")
	assert.Equal(t, flows.VisibleRows[0].Destination, "dns.google")
}

func TestServiceFlowDetailsRejectsLongRangeEntityActions(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 1, 20),
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 200, int64(8*24*time.Hour), int64(8*24*time.Hour)+20),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	_, err = service.FlowDetails(context.Background(), FlowQuery{
		Entity: "alpha.lan",
		Scope:  FlowScopeEntity,
		State: QueryState{
			Granularity: GranularityHostname,
			Metric:      MetricBytes,
			FromNs:      1,
			ToNs:        1 + int64(8*24*time.Hour),
		},
	})

	assert.ErrorContains(t, err, "entity actions are available for ranges up to 7 days")
}

func TestServiceGraphPresetRangeDiffersAcrossFiles(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_20260320.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 300, time.Date(2026, time.March, 20, 12, 0, 0, 0, time.UTC).UnixNano(), time.Date(2026, time.March, 20, 13, 0, 0, 0, time.UTC).UnixNano()),
	})
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_20260402.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 700, time.Date(2026, time.April, 2, 12, 0, 0, 0, time.UTC).UnixNano(), time.Date(2026, time.April, 2, 13, 0, 0, 0, time.UTC).UnixNano()),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	dayGraph, err := service.Graph(context.Background(), QueryState{
		Metric: MetricBytes,
		Preset: presetDayValue,
	})
	assert.NilError(t, err)

	monthGraph, err := service.Graph(context.Background(), QueryState{
		Metric: MetricBytes,
		Preset: presetMonthValue,
	})
	assert.NilError(t, err)

	assert.Equal(t, dayGraph.Totals.Bytes, int64(700))
	assert.Equal(t, monthGraph.Totals.Bytes, int64(1000))
}

func TestServiceGraphMonthPresetUsesBucketedSummaryFastPath(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_20260301.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "old.lan", "lan", "lan", "old.google", "google.com", "com", 300, time.Date(2026, time.March, 1, 1, 0, 0, 0, time.UTC).UnixNano(), time.Date(2026, time.March, 1, 2, 0, 0, 0, time.UTC).UnixNano()),
	})
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_20260415.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.11", "1.1.1.1", "new.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 700, time.Date(2026, time.April, 15, 1, 0, 0, 0, time.UTC).UnixNano(), time.Date(2026, time.April, 15, 2, 0, 0, 0, time.UTC).UnixNano()),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	span, err := service.Span(context.Background())
	assert.NilError(t, err)
	state := QueryState{
		Granularity: Granularity2LD,
		Metric:      MetricBytes,
		Preset:      presetMonthValue,
	}.Normalized(span)

	assert.Assert(t, service.canUseSummaryGraph(state, span))
	graph, err := service.Graph(context.Background(), state)
	assert.NilError(t, err)

	assert.Equal(t, graph.Totals.Bytes, int64(700))
	assert.Assert(t, containsNode(graph.Nodes, "one.one.one.one"))
	assert.Assert(t, !containsNode(graph.Nodes, "google.com"))

	cacheKey := summaryGraphSnapshotCacheKey(Granularity2LD, AddressFamilyAll, DirectionBoth, MetricBytes, service.currentRevision()) +
		":" + strconv.FormatInt(summaryBucketStartNs(state.FromNs), 10) +
		":" + strconv.FormatInt(summaryBucketStartNs(state.ToNs), 10)
	_, ok := service.summaryGraphCache.Get(cacheKey)
	assert.Assert(t, ok)
}

func TestServiceSearchFiltersCaseInsensitively(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "printer.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "search.cloudflare", "cloudflare.com", "com", 200, 30, 40),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
		Search:      "CLOUDFLARE",
	})
	assert.NilError(t, err)
	assert.Equal(t, graph.Totals.Connections, int64(1))
	assert.Equal(t, graph.Totals.Bytes, int64(200))
	assert.Assert(t, containsNode(graph.Nodes, "search.cloudflare"))
	assert.Assert(t, containsNode(graph.Nodes, "beta.lan"))
}

func TestServiceGraphFiltersByAddressFamilyIPv4(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("fd00::1", "2001:db8::1", "alpha.lan", "lan", "lan", "resolver.example", "example", "example", 200, 30, 40),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		AddressFamily: AddressFamilyIPv4,
		Granularity:   GranularityHostname,
		Metric:        MetricBytes,
	})
	assert.NilError(t, err)

	assert.Equal(t, graph.Totals.Connections, int64(1))
	assert.Equal(t, graph.Totals.Bytes, int64(100))
	assert.Assert(t, containsNode(graph.Nodes, "alpha.lan"))
	assert.Assert(t, containsNode(graph.Nodes, "dns.google"))
	assert.Assert(t, !containsNode(graph.Nodes, "resolver.example"))
}

func TestServiceGraphFiltersByAddressFamilyIPv6(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("fd00::1", "2001:db8::1", "alpha.lan", "lan", "lan", "resolver.example", "example", "example", 200, 30, 40),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		AddressFamily: AddressFamilyIPv6,
		Granularity:   GranularityHostname,
		Metric:        MetricBytes,
	})
	assert.NilError(t, err)

	assert.Equal(t, graph.Totals.Connections, int64(1))
	assert.Equal(t, graph.Totals.Bytes, int64(200))
	assert.Assert(t, containsNode(graph.Nodes, "alpha.lan"))
	assert.Assert(t, containsNode(graph.Nodes, "resolver.example"))
	assert.Assert(t, !containsNode(graph.Nodes, "dns.google"))
}

func TestServiceGraphFiltersByDirection(t *testing.T) {
	tempDir := t.TempDir()
	egressDirection := directionEgressParquetValue
	ingressDirection := directionIngressParquetValue
	egressRecord := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20)
	egressRecord.Direction = &egressDirection
	ingressRecord := sampleRecord("8.8.4.4", "192.168.1.11", "dns-alt.google", "google.com", "com", "beta.lan", "lan", "lan", 200, 30, 40)
	ingressRecord.Direction = &ingressDirection
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		egressRecord,
		ingressRecord,
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	egressGraph, err := service.Graph(context.Background(), QueryState{
		Direction:   DirectionEgress,
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
	})
	assert.NilError(t, err)

	ingressGraph, err := service.Graph(context.Background(), QueryState{
		Direction:   DirectionIngress,
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
	})
	assert.NilError(t, err)

	assert.Equal(t, egressGraph.Totals.Connections, int64(1))
	assert.Equal(t, egressGraph.Totals.Bytes, int64(100))
	assert.Assert(t, containsNode(egressGraph.Nodes, "alpha.lan"))
	assert.Assert(t, !containsNode(egressGraph.Nodes, "beta.lan"))
	assert.Equal(t, ingressGraph.Totals.Connections, int64(1))
	assert.Equal(t, ingressGraph.Totals.Bytes, int64(200))
	assert.Assert(t, containsNode(ingressGraph.Nodes, "beta.lan"))
	assert.Assert(t, !containsNode(ingressGraph.Nodes, "alpha.lan"))
}

func TestServiceHistogramFiltersByDirection(t *testing.T) {
	tempDir := t.TempDir()
	egressDirection := directionEgressParquetValue
	ingressDirection := directionIngressParquetValue
	egressRecord := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20)
	egressRecord.Direction = &egressDirection
	ingressRecord := sampleRecord("8.8.4.4", "192.168.1.11", "dns-alt.google", "google.com", "com", "beta.lan", "lan", "lan", 200, 110, 120)
	ingressRecord.Direction = &ingressDirection
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		egressRecord,
		ingressRecord,
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	bins, err := service.Histogram(context.Background(), QueryState{
		Direction: DirectionEgress,
		Metric:    MetricBytes,
		FromNs:    10,
		ToNs:      120,
	})
	assert.NilError(t, err)

	var total int64
	for _, bin := range bins {
		total += bin.Value
	}
	assert.Equal(t, total, int64(100))
}

func TestServiceGraphAddressFamilyUsesSummaryFastPath(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, int64(time.Hour), int64(2*time.Hour)),
		sampleRecord("fd00::1", "2001:db8::1", "beta.lan", "lan", "lan", "resolver.example", "example", "example", 200, int64(3*time.Hour), int64(4*time.Hour)),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	span, err := service.Span(context.Background())
	assert.NilError(t, err)
	state := QueryState{
		AddressFamily: AddressFamilyIPv4,
		Granularity:   Granularity2LD,
		Metric:        MetricBytes,
	}.Normalized(span)

	assert.Assert(t, service.canUseSummaryGraph(state, span))

	graph, err := service.Graph(context.Background(), state)
	assert.NilError(t, err)
	assert.Equal(t, graph.Totals.Bytes, int64(100))
	assert.Assert(t, containsNode(graph.Nodes, "google.com"))
	assert.Assert(t, !containsNode(graph.Nodes, "example"))

	cacheKey := summaryGraphSnapshotCacheKey(Granularity2LD, AddressFamilyIPv4, DirectionBoth, MetricBytes, service.currentRevision())
	_, ok := service.summaryGraphCache.Get(cacheKey)
	assert.Assert(t, ok)
}

func TestServiceGraphDirectionUsesSummaryFastPath(t *testing.T) {
	tempDir := t.TempDir()
	egressDirection := directionEgressParquetValue
	ingressDirection := directionIngressParquetValue
	egressRecord := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, int64(time.Hour), int64(2*time.Hour))
	egressRecord.Direction = &egressDirection
	ingressRecord := sampleRecord("8.8.4.4", "192.168.1.11", "dns-alt.google", "google.com", "com", "beta.lan", "lan", "lan", 200, int64(3*time.Hour), int64(4*time.Hour))
	ingressRecord.Direction = &ingressDirection
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		egressRecord,
		ingressRecord,
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	span, err := service.Span(context.Background())
	assert.NilError(t, err)
	state := QueryState{
		Direction:   DirectionEgress,
		Granularity: Granularity2LD,
		Metric:      MetricBytes,
	}.Normalized(span)

	assert.Assert(t, service.canUseSummaryGraph(state, span))

	graph, err := service.Graph(context.Background(), state)
	assert.NilError(t, err)
	assert.Equal(t, graph.Totals.Bytes, int64(100))
	assert.Assert(t, containsNode(graph.Nodes, "google.com"))
	assert.Assert(t, !containsNode(graph.Nodes, "beta.lan"))

	cacheKey := summaryGraphSnapshotCacheKey(Granularity2LD, AddressFamilyAll, DirectionEgress, MetricBytes, service.currentRevision())
	_, ok := service.summaryGraphCache.Get(cacheKey)
	assert.Assert(t, ok)
}

func TestServiceGraphShowsDNSLookups(t *testing.T) {
	const dnsLookupTestLAN = "lan"

	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, int64(time.Hour), int64(2*time.Hour)),
	})
	clientHost := "alpha.lan"
	client2LD := dnsLookupTestLAN
	clientTLD := dnsLookupTestLAN
	query2LD := dnsLookupTestQuery2LD
	queryTLD := dnsLookupTestQueryTLD
	writeDNSLookupParquet(t, filepath.Join(tempDir, "dns_lookups_202604.parquet"), []model.DNSLookupRecord{
		{Client2LD: &client2LD, ClientHost: &clientHost, ClientIP: "192.168.1.10", ClientIPVersion: model.IPVersion4, ClientIsPrivate: true, ClientTLD: &clientTLD, Lookups: 1, Query2LD: &query2LD, QueryName: "www.example.com", QueryTLD: &queryTLD, QueryType: "A", TimeStartNs: int64(time.Hour)},
		{Client2LD: &client2LD, ClientHost: &clientHost, ClientIP: "192.168.1.10", ClientIPVersion: model.IPVersion4, ClientIsPrivate: true, ClientTLD: &clientTLD, Lookups: 1, Query2LD: &query2LD, QueryName: "www.example.com", QueryTLD: &queryTLD, QueryType: "AAAA", TimeStartNs: int64(time.Hour) + 1},
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	span, err := service.Span(context.Background())
	assert.NilError(t, err)
	graph, err := service.Graph(context.Background(), QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricDNSLookups,
	}.Normalized(span))
	assert.NilError(t, err)

	assert.Equal(t, graph.Totals.Connections, int64(2))
	assert.Assert(t, containsNode(graph.Nodes, "alpha.lan"))
	assert.Assert(t, containsNode(graph.Nodes, "www.example.com"))
	assert.Equal(t, graph.Edges[0].Source, "alpha.lan")
	assert.Equal(t, graph.Edges[0].Destination, "www.example.com")
	assert.Equal(t, graph.Edges[0].MetricValue, int64(2))
}

func TestServiceGraphClassifiesDNSLookupResultStates(t *testing.T) {
	const dnsLookupTestLAN = "lan"

	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, int64(time.Hour), int64(2*time.Hour)),
	})
	clientHost := "alpha.lan"
	client2LD := dnsLookupTestLAN
	clientTLD := dnsLookupTestLAN
	query2LD := dnsLookupTestQuery2LD
	queryTLD := dnsLookupTestQueryTLD
	missing2LD := "missing.example"
	writeDNSLookupParquet(t, filepath.Join(tempDir, "dns_lookups_202604.parquet"), []model.DNSLookupRecord{
		{Answer: model.DNSAnswerNXDOMAIN, Client2LD: &client2LD, ClientHost: &clientHost, ClientIP: "192.168.1.10", ClientIPVersion: model.IPVersion4, ClientIsPrivate: true, ClientTLD: &clientTLD, Lookups: 1, Query2LD: &query2LD, QueryName: "www.example.com", QueryTLD: &queryTLD, QueryType: "A", TimeStartNs: int64(time.Hour)},
		{Answer: "192.0.2.10", Client2LD: &client2LD, ClientHost: &clientHost, ClientIP: "192.168.1.10", ClientIPVersion: model.IPVersion4, ClientIsPrivate: true, ClientTLD: &clientTLD, Lookups: 1, Query2LD: &query2LD, QueryName: "www.example.com", QueryTLD: &queryTLD, QueryType: "A", TimeStartNs: int64(time.Hour) + 1},
		{Answer: model.DNSAnswerNXDOMAIN, Client2LD: &client2LD, ClientHost: &clientHost, ClientIP: "192.168.1.10", ClientIPVersion: model.IPVersion4, ClientIsPrivate: true, ClientTLD: &clientTLD, Lookups: 1, Query2LD: &missing2LD, QueryName: "missing.example", QueryTLD: &queryTLD, QueryType: "A", TimeStartNs: int64(time.Hour) + 2},
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	span, err := service.Span(context.Background())
	assert.NilError(t, err)
	state := QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricDNSLookups,
	}.Normalized(span)
	graph, err := service.Graph(context.Background(), state)
	assert.NilError(t, err)

	mixedEdge := findEdge(t, graph.Edges, "alpha.lan", "www.example.com")
	assert.Equal(t, mixedEdge.DNSResultState, dnsResultStateMixed)
	assert.Equal(t, mixedEdge.NXDomainLookups, int64(1))
	assert.Equal(t, mixedEdge.SuccessfulLookups, int64(1))
	nxdomainEdge := findEdge(t, graph.Edges, "alpha.lan", "missing.example")
	assert.Equal(t, nxdomainEdge.DNSResultState, dnsResultStateNXDOMAIN)
	assert.Equal(t, nxdomainEdge.NXDomainLookups, int64(1))
	assert.Equal(t, nxdomainEdge.SuccessfulLookups, int64(0))

	table, err := service.Table(context.Background(), state)
	assert.NilError(t, err)
	mixedRow := findTableRow(t, table.Rows, "alpha.lan", "www.example.com")
	assert.Equal(t, mixedRow.DNSResultState, dnsResultStateMixed)
	nxdomainRow := findTableRow(t, table.Rows, "alpha.lan", "missing.example")
	assert.Equal(t, nxdomainRow.DNSResultState, dnsResultStateNXDOMAIN)
}

func TestServiceDNSLookupSummaryFastPath(t *testing.T) {
	const dnsLookupTestLAN = "lan"

	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, int64(time.Hour), int64(2*time.Hour)),
	})
	client2LD := dnsLookupTestLAN
	clientTLD := dnsLookupTestLAN
	query2LD := dnsLookupTestQuery2LD
	queryTLD := dnsLookupTestQueryTLD
	writeDNSLookupParquet(t, filepath.Join(tempDir, "dns_lookups_202604.parquet"), []model.DNSLookupRecord{
		{Client2LD: &client2LD, ClientIP: "192.168.1.10", ClientIPVersion: model.IPVersion4, ClientIsPrivate: true, ClientTLD: &clientTLD, Lookups: 1, Query2LD: &query2LD, QueryName: "www.example.com", QueryTLD: &queryTLD, QueryType: "A", TimeStartNs: int64(time.Hour)},
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	span, err := service.Span(context.Background())
	assert.NilError(t, err)
	state := QueryState{
		Granularity: Granularity2LD,
		Metric:      MetricDNSLookups,
	}.Normalized(span)

	assert.Assert(t, service.canUseSummaryGraph(state, span))
	graph, err := service.Graph(context.Background(), state)
	assert.NilError(t, err)
	assert.Equal(t, graph.Totals.Connections, int64(1))
	assert.Assert(t, containsNode(graph.Nodes, dnsLookupTestLAN))
	assert.Assert(t, containsNode(graph.Nodes, "example.com"))

	cacheKey := summaryGraphSnapshotCacheKey(Granularity2LD, AddressFamilyAll, DirectionBoth, MetricDNSLookups, service.currentRevision())
	_, ok := service.summaryGraphCache.Get(cacheKey)
	assert.Assert(t, ok)
}

func TestServiceRefreshMetadataInvalidatesCaches(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	state := QueryState{
		Granularity: Granularity2LD,
		Metric:      MetricBytes,
	}
	span, err := service.Span(context.Background())
	assert.NilError(t, err)
	state = state.Normalized(span)
	cacheKey := state.cacheKey(graphCacheKind, service.currentRevision())

	_, err = service.Graph(context.Background(), state)
	assert.NilError(t, err)
	_, ok := service.graphCache.Get(cacheKey)
	assert.Assert(t, ok)

	path := filepath.Join(tempDir, "nfcap_202605.parquet")
	writeEnrichedParquet(t, path, []model.FlowRecord{
		sampleRecord("192.168.1.12", "9.9.9.9", "gamma.lan", "lan", "lan", "dns.quad9.net", "quad9.net", "net", 300, 50, 60),
	})
	modTime := time.Now().Add(time.Second)
	assert.NilError(t, os.Chtimes(path, modTime, modTime))

	assert.NilError(t, service.refreshMetadata(context.Background()))
	_, ok = service.graphCache.Get(cacheKey)
	assert.Assert(t, !ok)
}

func TestServiceGraphBuildsSummarySnapshotCache(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 200, 30, 40),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	state := QueryState{
		Granularity: Granularity2LD,
		Metric:      MetricBytes,
	}
	span, err := service.Span(context.Background())
	assert.NilError(t, err)
	state = state.Normalized(span)

	_, err = service.Graph(context.Background(), state)
	assert.NilError(t, err)

	cacheKey := summaryGraphSnapshotCacheKey(Granularity2LD, AddressFamilyAll, DirectionBoth, MetricBytes, service.currentRevision())
	_, ok := service.summaryGraphCache.Get(cacheKey)
	assert.Assert(t, ok)
}

func TestNewServiceBuildsUISummaries(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, int64(time.Hour), int64(2*time.Hour)),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	assert.Equal(t, len(service.summaries.edgePathsByGranulariy[GranularityTLD]), 1)
	assert.Equal(t, len(service.summaries.edgePathsByGranulariy[Granularity2LD]), 1)
	assert.Equal(t, len(service.summaries.bucketedEdgePathsByGranulariy[GranularityTLD]), 1)
	assert.Equal(t, len(service.summaries.bucketedEdgePathsByGranulariy[Granularity2LD]), 1)
	assert.Equal(t, len(service.summaries.histogramPaths), 1)
	_, err = os.Stat(filepath.Join(tempDir, "ui_summary_edges_tld_202604.parquet"))
	assert.NilError(t, err)
	_, err = os.Stat(filepath.Join(tempDir, "ui_summary_edges_2ld_202604.parquet"))
	assert.NilError(t, err)
	_, err = os.Stat(filepath.Join(tempDir, "ui_summary_bucketed_edges_tld_202604.parquet"))
	assert.NilError(t, err)
	_, err = os.Stat(filepath.Join(tempDir, "ui_summary_bucketed_edges_2ld_202604.parquet"))
	assert.NilError(t, err)
	_, err = os.Stat(filepath.Join(tempDir, "ui_summary_histogram_202604.parquet"))
	assert.NilError(t, err)
}

func TestNewServiceBuildsUISummariesWithIPVersion(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, int64(time.Hour), int64(2*time.Hour)),
		sampleRecord("fd00::1", "2001:db8::1", "alpha.lan", "lan", "lan", "resolver.example", "example", "example", 200, int64(3*time.Hour), int64(4*time.Hour)),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	var edgeRows []parquetout.EdgeSummaryRow
	err = parquetout.ReadEdgeSummaryRows(filepath.Join(tempDir, "ui_summary_edges_2ld_202604.parquet"), func(rows []parquetout.EdgeSummaryRow) error {
		edgeRows = append(edgeRows, rows...)
		return nil
	})
	assert.NilError(t, err)

	var histogramRows []parquetout.HistogramSummaryRow
	err = parquetout.ReadHistogramSummaryRows(filepath.Join(tempDir, "ui_summary_histogram_202604.parquet"), func(rows []parquetout.HistogramSummaryRow) error {
		histogramRows = append(histogramRows, rows...)
		return nil
	})
	assert.NilError(t, err)

	assert.Assert(t, containsEdgeSummaryIPVersion(edgeRows, model.IPVersion4))
	assert.Assert(t, containsEdgeSummaryIPVersion(edgeRows, model.IPVersion6))
	assert.Assert(t, containsHistogramSummaryIPVersion(histogramRows, model.IPVersion4))
	assert.Assert(t, containsHistogramSummaryIPVersion(histogramRows, model.IPVersion6))
}

func TestNewServiceBuildsUISummariesWithDirection(t *testing.T) {
	tempDir := t.TempDir()
	egressDirection := directionEgressParquetValue
	ingressDirection := directionIngressParquetValue
	egressRecord := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, int64(time.Hour), int64(2*time.Hour))
	egressRecord.Direction = &egressDirection
	ingressRecord := sampleRecord("8.8.4.4", "192.168.1.11", "dns-alt.google", "google.com", "com", "beta.lan", "lan", "lan", 200, int64(3*time.Hour), int64(4*time.Hour))
	ingressRecord.Direction = &ingressDirection
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		egressRecord,
		ingressRecord,
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	var edgeRows []parquetout.EdgeSummaryRow
	err = parquetout.ReadEdgeSummaryRows(filepath.Join(tempDir, "ui_summary_edges_2ld_202604.parquet"), func(rows []parquetout.EdgeSummaryRow) error {
		edgeRows = append(edgeRows, rows...)
		return nil
	})
	assert.NilError(t, err)

	var histogramRows []parquetout.HistogramSummaryRow
	err = parquetout.ReadHistogramSummaryRows(filepath.Join(tempDir, "ui_summary_histogram_202604.parquet"), func(rows []parquetout.HistogramSummaryRow) error {
		histogramRows = append(histogramRows, rows...)
		return nil
	})
	assert.NilError(t, err)

	assert.Assert(t, containsEdgeSummaryDirection(edgeRows, directionEgressParquetValue))
	assert.Assert(t, containsEdgeSummaryDirection(edgeRows, directionIngressParquetValue))
	assert.Assert(t, containsHistogramSummaryDirection(histogramRows, directionEgressParquetValue))
	assert.Assert(t, containsHistogramSummaryDirection(histogramRows, directionIngressParquetValue))
}

func TestServiceRefreshMetadataRemovesStaleUISummaries(t *testing.T) {
	tempDir := t.TempDir()
	stalePath := filepath.Join(tempDir, "nfcap_202604.parquet")
	writeEnrichedParquet(t, stalePath, []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
	})
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202605.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 100, 30, 40),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	assert.NilError(t, os.Remove(stalePath))
	assert.NilError(t, service.refreshMetadata(context.Background()))
	_, err = os.Stat(filepath.Join(tempDir, "ui_summary_edges_tld_202604.parquet"))
	assert.Assert(t, os.IsNotExist(err))
}

func TestServiceRefreshMetadataRebuildsStaleUISummariesInBackground(t *testing.T) {
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "nfcap_202604.parquet")
	writeEnrichedParquet(t, sourcePath, []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	writeEnrichedParquet(t, sourcePath, []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("192.168.1.11", "9.9.9.9", "beta.lan", "lan", "lan", "dns.quad9.net", "quad9.net", "net", 200, 30, 40),
	})
	modTime := time.Now().Add(2 * time.Second)
	assert.NilError(t, os.Chtimes(sourcePath, modTime, modTime))

	assert.NilError(t, service.refreshMetadata(context.Background()))

	deadline := time.Now().Add(5 * time.Second)
	for {
		manifest, manifestErr := parquetout.ReadUISummaryManifest(filepath.Join(tempDir, "ui_summary_edges_tld_202604.parquet"))
		assert.NilError(t, manifestErr)
		if manifest.Source.ModTimeNs == modTime.UnixNano() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for stale UI summary rebuild")
		}
		time.Sleep(25 * time.Millisecond)
	}

	assert.Assert(t, !service.summaryRefreshPending)
}

func TestServiceTLDUsesUnknownForUnresolvedIPs(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.0.2.10", "2001:db8::1", "", "", "", "", "", "", 100, 10, 20),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity: GranularityTLD,
		Metric:      MetricBytes,
	})
	assert.NilError(t, err)
	assert.Assert(t, containsNode(graph.Nodes, unknownPublicEntityLabel))
	assert.Assert(t, !containsNode(graph.Nodes, "192.0.2.10"))
	assert.Assert(t, !containsNode(graph.Nodes, "2001:db8::1"))
}

func TestService2LDUsesUnknownForUnresolvedIPs(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.0.2.10", "2001:db8::1", "", "", "", "", "", "", 100, 10, 20),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity: Granularity2LD,
		Metric:      MetricBytes,
	})
	assert.NilError(t, err)
	assert.Assert(t, containsNode(graph.Nodes, unknownPublicEntityLabel))
	assert.Assert(t, !containsNode(graph.Nodes, "192.0.2.10"))
	assert.Assert(t, !containsNode(graph.Nodes, "2001:db8::1"))
}

func TestServiceTLDSplitsUnknownPrivateAndPublic(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "", "", "", "", "", "", 100, 10, 20),
		sampleRecord("2001:db8::1", "198.51.100.10", "", "", "", "", "", "", 50, 30, 40),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity: GranularityTLD,
		Metric:      MetricBytes,
	})
	assert.NilError(t, err)
	assert.Assert(t, containsNode(graph.Nodes, unknownPrivateEntityLabel))
	assert.Assert(t, containsNode(graph.Nodes, unknownPublicEntityLabel))
}

func TestServiceGraphClassifiesPrivateAndMixedNodes(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("2001:db8::1", "8.8.4.4", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 50, 30, 40),
		sampleRecord("fd00::1", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 75, 50, 60),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
	})
	assert.NilError(t, err)

	assert.Equal(t, nodeAddressClassForID(graph.Nodes, "alpha.lan"), nodeAddressClassMixed)
	assert.Equal(t, nodeAddressClassForID(graph.Nodes, "beta.lan"), nodeAddressClassPrivate)
}

func containsNode(nodes []Node, nodeID string) bool {
	for _, node := range nodes {
		if node.ID == nodeID {
			return true
		}
	}
	return false
}

func containsNodeLabel(nodes []Node, label string) bool {
	for _, node := range nodes {
		if node.Label == label {
			return true
		}
	}
	return false
}

func findEdge(t *testing.T, edges []Edge, source, destination string) Edge {
	t.Helper()

	for _, edge := range edges {
		if edge.Source == source && edge.Destination == destination {
			return edge
		}
	}
	t.Fatalf("edge %q -> %q not found", source, destination)
	return Edge{}
}

func findTableRow(t *testing.T, rows []TableRow, source, destination string) TableRow {
	t.Helper()

	for _, row := range rows {
		if row.Source == source && row.Destination == destination {
			return row
		}
	}
	t.Fatalf("table row %q -> %q not found", source, destination)
	return TableRow{}
}

func nodeAddressClassForID(nodes []Node, nodeID string) nodeAddressClass {
	for _, node := range nodes {
		if node.ID == nodeID {
			return node.AddressClass
		}
	}

	return ""
}

func containsEdgeSummaryIPVersion(rows []parquetout.EdgeSummaryRow, ipVersion int32) bool {
	for _, row := range rows {
		if row.IPVersion == ipVersion {
			return true
		}
	}
	return false
}

func containsHistogramSummaryIPVersion(rows []parquetout.HistogramSummaryRow, ipVersion int32) bool {
	for _, row := range rows {
		if row.IPVersion == ipVersion {
			return true
		}
	}
	return false
}

func containsEdgeSummaryDirection(rows []parquetout.EdgeSummaryRow, direction int32) bool {
	for _, row := range rows {
		if row.Direction != nil && *row.Direction == direction {
			return true
		}
	}
	return false
}

func containsHistogramSummaryDirection(rows []parquetout.HistogramSummaryRow, direction int32) bool {
	for _, row := range rows {
		if row.Direction != nil && *row.Direction == direction {
			return true
		}
	}
	return false
}

func writeBaseParquet(t *testing.T, path string) {
	t.Helper()

	writer, finalize, err := parquetout.Create(path, model.RefreshManifest{Version: 1})
	assert.NilError(t, err)
	assert.NilError(t, writer.Write(model.FlowRecord{
		SrcIP:       "192.168.1.10",
		DstIP:       "8.8.8.8",
		Bytes:       42,
		TimeStartNs: 10,
		TimeEndNs:   20,
	}))
	assert.NilError(t, finalize())
}

func writeEnrichedParquet(t *testing.T, path string, records []model.FlowRecord) {
	t.Helper()

	writer, finalize, err := parquetout.CreateEnriched(path, model.EnrichmentManifest{
		LogicVersion: model.EnrichmentLogicVersion,
		Version:      model.EnrichmentManifestVersion,
	})
	assert.NilError(t, err)
	assert.NilError(t, writer.WriteBatch(records))
	assert.NilError(t, finalize())
}

func writeDNSLookupParquet(t *testing.T, path string, records []model.DNSLookupRecord) {
	t.Helper()

	writer, finalize, err := parquetout.CreateDNSLookups(path, model.EnrichmentManifest{
		LogicVersion: model.EnrichmentLogicVersion,
		Version:      model.EnrichmentManifestVersion,
	})
	assert.NilError(t, err)
	assert.NilError(t, writer.WriteBatch(records))
	assert.NilError(t, finalize())
}

func sampleRecord(srcIP, dstIP, srcHost, src2LD, srcTLD, dstHost, dst2LD, dstTLD string, bytes, startNs, endNs int64) model.FlowRecord {
	return model.FlowRecord{
		Bytes:        bytes,
		Dst2LD:       strPtr(dst2LD),
		DstHost:      strPtr(dstHost),
		DstIP:        dstIP,
		DstIsPrivate: testIsPrivate(dstIP),
		DstTLD:       strPtr(dstTLD),
		IPVersion:    testIPVersion(srcIP),
		Src2LD:       strPtr(src2LD),
		SrcHost:      strPtr(srcHost),
		SrcIP:        srcIP,
		SrcIsPrivate: testIsPrivate(srcIP),
		SrcTLD:       strPtr(srcTLD),
		TimeEndNs:    endNs,
		TimeStartNs:  startNs,
	}
}

func testIsPrivate(ipAddress string) bool {
	address, err := netip.ParseAddr(ipAddress)
	if err != nil {
		return false
	}

	privatePrefixes := []netip.Prefix{
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("172.16.0.0/12"),
		netip.MustParsePrefix("192.168.0.0/16"),
		netip.MustParsePrefix("fc00::/7"),
		netip.MustParsePrefix("fec0::/10"),
		netip.MustParsePrefix("fe80::/10"),
	}
	for _, prefix := range privatePrefixes {
		if prefix.Contains(address) {
			return true
		}
	}

	return false
}

func testIPVersion(ipAddress string) int32 {
	address, err := netip.ParseAddr(ipAddress)
	if err != nil {
		return model.IPVersionUnknown
	}
	if address.Is4() {
		return model.IPVersion4
	}
	if address.Is6() {
		return model.IPVersion6
	}
	return model.IPVersionUnknown
}

func strPtr(value string) *string {
	return &value
}
