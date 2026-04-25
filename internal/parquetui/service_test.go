package parquetui

import (
	"context"
	"encoding/json"
	"errors"
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
	dnsLookupTestFW       = "fw.lan"
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
		{Bytes: 250, Connections: 3, IgnoredMetric: 2},
	}, 4, 1, MetricConnections)

	assert.Equal(t, totals.Bytes, int64(350))
	assert.Equal(t, totals.Connections, int64(5))
	assert.Equal(t, totals.Entities, 4)
	assert.Equal(t, totals.Edges, 1)
	assert.Equal(t, totals.Ignored, int64(2))
}

func TestChooseKeepEntitiesKeepsNodeLimitHard(t *testing.T) {
	nodes := []Node{
		{ID: "top"},
		{ID: "selected"},
		{ID: "included"},
		{ID: "overflow"},
	}

	keepEntities := chooseKeepEntities(nodes, QueryState{
		Include:        []string{"included", "overflow"},
		NodeLimit:      2,
		SelectedEntity: "selected",
	})

	assert.DeepEqual(t, keepEntities, []string{"selected", "included"})
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
	assert.Assert(t, containsNode(graph.Nodes, graphRestID))
	assert.Assert(t, containsNodeLabel(graph.Nodes, "Rest"))
}

func TestServiceGraphDeviceLocalIdentityUsesRemoteGranularity(t *testing.T) {
	tempDir := t.TempDir()
	first := sampleRecord("192.168.1.10", "140.82.114.3", "alpha-v4.lan", "alpha-v4", "Local", "lb-140-82-114-3-iad.github.com", "github.com", "com", 100, 10, 20)
	second := sampleRecord("fd00::10", "140.82.113.4", "alpha-v6.lan", "alpha-v6", "Local", "lb-140-82-113-4-iad.github.com", "github.com", "com", 200, 30, 40)
	applySourceDevice(&first, "mac:aa:bb:cc:dd:ee:ff", "alpha.lan")
	applySourceDevice(&second, "mac:aa:bb:cc:dd:ee:ff", "alpha.lan")
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{first, second})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity:   Granularity2LD,
		LocalIdentity: LocalIdentityDevice,
		Metric:        MetricBytes,
		EdgeLimit:     0,
	})
	assert.NilError(t, err)

	assert.Assert(t, containsNode(graph.Nodes, "alpha.lan"))
	assert.Assert(t, containsNode(graph.Nodes, "github.com"))
	assert.Assert(t, containsEdge(graph.Edges, "alpha.lan", "github.com"))
	assert.Equal(t, graph.Totals.Bytes, int64(300))
}

func TestServiceGraphHidesIgnoredTrafficByDefault(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 200, 30, 40),
	})
	writeIgnoreRules(t, tempDir, []IgnoreRule{{
		ID:      "ignore-google",
		Enabled: true,
		Name:    "Ignore google",
		Match: IgnoreRuleMatch{
			DestinationEntity: "google.com",
		},
	}})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity: Granularity2LD,
		Metric:      MetricBytes,
	})
	assert.NilError(t, err)
	assert.Equal(t, graph.Totals.Bytes, int64(200))
	assert.Equal(t, graph.Totals.Connections, int64(1))
	assert.Assert(t, !containsNode(graph.Nodes, "google.com"))
	assert.Assert(t, containsNode(graph.Nodes, "one.one.one.one"))
}

func TestServiceFlowDetailsShowsIgnoredRowsWhenRequested(t *testing.T) {
	tempDir := t.TempDir()
	record := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20)
	record.Protocol = 17
	record.SrcPort = 54000
	record.DstPort = 53
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		record,
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 200, 30, 40),
	})
	writeIgnoreRules(t, tempDir, []IgnoreRule{{
		ID:      "ignore-dns53",
		Enabled: true,
		Name:    "Ignore UDP 53",
		Match: IgnoreRuleMatch{
			Protocol:    17,
			ServicePort: 53,
		},
	}})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	flows, err := service.FlowDetails(context.Background(), FlowQuery{
		Entity: "alpha.lan",
		Scope:  FlowScopeEntity,
		State: QueryState{
			Granularity:    GranularityHostname,
			HideIgnored:    false,
			HideIgnoredSet: true,
			Metric:         MetricBytes,
		},
	})
	assert.NilError(t, err)
	assert.Equal(t, flows.TotalCount, 1)
	assert.Assert(t, flows.VisibleRows[0].Ignored)
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

func TestAppIgnoreRulesRendersManager(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
	})
	writeIgnoreRules(t, tempDir, []IgnoreRule{{
		ID:      "ignore-google",
		Enabled: true,
		Name:    "Ignore google",
		Match: IgnoreRuleMatch{
			DestinationEntity: "google.com",
		},
	}})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	app := &App{service: service}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/ignore-rules", nil)

	app.routes().ServeHTTP(recorder, request)

	assert.Equal(t, recorder.Code, http.StatusOK)
	assert.Assert(t, strings.Contains(recorder.Body.String(), "Ignored Traffic Rules"))
	assert.Assert(t, strings.Contains(recorder.Body.String(), "Ignore google"))
	assert.Assert(t, strings.Contains(recorder.Body.String(), `name="rule_destination_entity"`))
	assert.Assert(t, strings.Contains(recorder.Body.String(), `name="action" value="toggle_enabled"`))
	assert.Assert(t, strings.Contains(recorder.Body.String(), `class="chip rule-status-button"`))
}

func TestAppIgnoreRulesTogglesRuleEnabled(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
	})
	writeIgnoreRules(t, tempDir, []IgnoreRule{{
		ID:      "ignore-google",
		Enabled: false,
		Name:    "Ignore google",
		Match: IgnoreRuleMatch{
			DestinationEntity: "google.com",
		},
	}})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	app := &App{service: service}
	recorder := httptest.NewRecorder()
	form := strings.NewReader("action=toggle_enabled&rule_id=ignore-google")
	request := httptest.NewRequest(http.MethodPost, "/ignore-rules", form)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	app.routes().ServeHTTP(recorder, request)

	assert.Equal(t, recorder.Code, http.StatusOK)
	rules := service.ignoreRulesSnapshot()
	assert.Equal(t, len(rules), 1)
	assert.Assert(t, rules[0].Enabled)
	assert.Assert(t, strings.Contains(recorder.Body.String(), ">Enabled<"))
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

func TestServiceFlowDetailsEdgeIncludesReverseFlowsByDefault(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("8.8.8.8", "192.168.1.10", "dns.google", "google.com", "com", "alpha.lan", "lan", "lan", 200, 30, 40),
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
		},
	})
	assert.NilError(t, err)

	assert.Equal(t, flows.TotalCount, 2)
	assert.Equal(t, flows.VisibleRows[0].Source, "dns.google")
	assert.Equal(t, flows.VisibleRows[1].Source, "alpha.lan")
}

func TestServiceFlowDetailsEdgeForwardMatchExcludesReverseFlows(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("8.8.8.8", "192.168.1.10", "dns.google", "google.com", "com", "alpha.lan", "lan", "lan", 200, 30, 40),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	flows, err := service.FlowDetails(context.Background(), FlowQuery{
		Destination: "dns.google",
		Match:       FlowMatchForward,
		Scope:       FlowScopeEdge,
		Source:      "alpha.lan",
		State: QueryState{
			Granularity: GranularityHostname,
			Metric:      MetricBytes,
		},
	})
	assert.NilError(t, err)

	assert.Equal(t, flows.TotalCount, 1)
	assert.Equal(t, flows.VisibleRows[0].Source, "alpha.lan")
	assert.Equal(t, flows.VisibleRows[0].Bytes, int64(100))
}

func TestServiceFlowDetailsSortsRawRows(t *testing.T) {
	tempDir := t.TempDir()
	directionEgress := directionEgressParquetValue
	directionIngress := directionIngressParquetValue
	lowProtocol := sampleRecord("192.168.1.10", "192.168.1.20", "alpha.lan", "lan", "lan", "common.lan", "lan", "lan", 100, 10, 20)
	lowProtocol.Protocol = 6
	lowProtocol.Packets = 20
	lowProtocol.Direction = &directionIngress
	highProtocol := sampleRecord("192.168.1.20", "192.168.1.11", "common.lan", "lan", "lan", "zeta.lan", "lan", "lan", 300, 30, 40)
	highProtocol.Protocol = 17
	highProtocol.Packets = 10
	highProtocol.Direction = &directionEgress
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{lowProtocol, highProtocol})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	testCases := []struct {
		name          string
		sort          FlowSort
		sortDir       FlowSortDir
		expectedBytes int64
	}{
		{name: "start descending", sort: FlowSortStart, sortDir: FlowSortDesc, expectedBytes: 300},
		{name: "start ascending", sort: FlowSortStart, sortDir: FlowSortAsc, expectedBytes: 100},
		{name: "end descending", sort: FlowSortEnd, sortDir: FlowSortDesc, expectedBytes: 300},
		{name: "end ascending", sort: FlowSortEnd, sortDir: FlowSortAsc, expectedBytes: 100},
		{name: "source", sort: FlowSortSource, expectedBytes: 100},
		{name: "destination", sort: FlowSortDestination, expectedBytes: 100},
		{name: "protocol", sort: FlowSortProtocol, expectedBytes: 300},
		{name: "packets", sort: FlowSortPackets, expectedBytes: 100},
		{name: "bytes", sort: FlowSortBytes, expectedBytes: 300},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			flows, err := service.FlowDetails(context.Background(), FlowQuery{
				Entity:  "common.lan",
				Scope:   FlowScopeEntity,
				Sort:    testCase.sort,
				SortDir: testCase.sortDir,
				State: QueryState{
					Granularity: GranularityHostname,
					Metric:      MetricBytes,
				},
			})
			assert.NilError(t, err)
			assert.Equal(t, flows.VisibleRows[0].Bytes, testCase.expectedBytes)
		})
	}
}

func TestServiceFlowDetailsDisplaysHostnamesForCoarseGranularity(t *testing.T) {
	tempDir := t.TempDir()
	zeta := sampleRecord("192.168.1.20", "8.8.8.8", "zeta.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20)
	alpha := sampleRecord("192.168.1.10", "9.9.9.9", "alpha.lan", "lan", "lan", "quad9.net", "quad9.net", "net", 200, 30, 40)
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{zeta, alpha})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	flows, err := service.FlowDetails(context.Background(), FlowQuery{
		Entity: localEntityLabel,
		Scope:  FlowScopeEntity,
		Sort:   FlowSortSource,
		State: QueryState{
			Granularity: GranularityTLD,
			Metric:      MetricBytes,
		},
	})
	assert.NilError(t, err)
	assert.Equal(t, flows.TotalCount, 2)
	assert.Equal(t, flows.VisibleRows[0].Source, "alpha.lan")
	assert.Equal(t, flows.VisibleRows[0].Destination, "quad9.net")
	assert.Equal(t, flows.VisibleRows[1].Source, "zeta.lan")
	assert.Equal(t, flows.VisibleRows[1].Destination, "dns.google")
}

func TestServiceFlowDetailsLongRangesAreDisabled(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 900, 10, 20),
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 100, int64(8*24*time.Hour), int64(8*24*time.Hour)+20),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	flows, err := service.FlowDetails(context.Background(), FlowQuery{
		Entity: "lan",
		Scope:  FlowScopeEntity,
		Sort:   FlowSortBytes,
		State: QueryState{
			Granularity: Granularity2LD,
			Metric:      MetricBytes,
		},
	})

	assert.Assert(t, errors.Is(err, errEntityActionsDisabled))
	assert.Equal(t, len(flows.VisibleRows), 0)
}

func TestServiceFlowDetailsPresetCountsUseMatchingRows(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("8.8.8.8", "192.168.1.10", "dns.google", "google.com", "com", "alpha.lan", "lan", "lan", 200, int64(39*24*time.Hour), int64(39*24*time.Hour)+20),
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 300, int64(40*24*time.Hour), int64(40*24*time.Hour)),
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
			Preset:      presetWeekValue,
		},
	})
	assert.NilError(t, err)

	counts := make(map[string]int, len(flows.PresetCounts))
	for _, count := range flows.PresetCounts {
		counts[count.Preset] = count.Count
	}
	assert.Equal(t, counts[presetHourValue], 1)
	assert.Equal(t, counts[presetDayValue], 2)
	assert.Equal(t, counts[presetWeekValue], 2)
	_, allFound := counts[presetAllValue]
	_, monthFound := counts[presetMonthValue]
	assert.Assert(t, !allFound)
	assert.Assert(t, !monthFound)
	assert.Equal(t, flows.TotalCount, 2)
	assert.Equal(t, len(flows.VisibleRows), 2)
}

func TestServiceFlowDetailsRejectsLongRanges(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 1, 20),
		sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 200, int64(8*24*time.Hour), int64(8*24*time.Hour)+20),
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
			FromNs:      1,
			ToNs:        1 + int64(8*24*time.Hour),
		},
	})

	assert.Assert(t, errors.Is(err, errEntityActionsDisabled))
	assert.Equal(t, len(flows.VisibleRows), 0)
}

func TestServiceGraphSelectionBreakdownUsesSelectedEntity(t *testing.T) {
	tempDir := t.TempDir()
	tcpRecord := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 400, 10, 20)
	tcpRecord.Protocol = 6
	tcpRecord.DstPort = 443
	udpRecord := sampleRecord("fd00::1", "2001:db8::1", "alpha.lan", "lan", "lan", "resolver.example", "example", "example", 200, 30, 40)
	udpRecord.Protocol = 17
	udpRecord.SrcPort = 55000
	udpRecord.DstPort = 53
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{tcpRecord, udpRecord})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity:    GranularityHostname,
		Metric:         MetricBytes,
		SelectedEntity: "alpha.lan",
	})
	assert.NilError(t, err)

	assert.Assert(t, graph.Breakdown.Protocols != nil)
	assert.Assert(t, graph.Breakdown.Family != nil)
	assert.Assert(t, graph.Breakdown.Ports != nil)
	assert.DeepEqual(t, graph.Breakdown.Protocols.Slices, []BreakdownSlice{
		{FilterParam: "protocol", FilterValue: "6", Label: "6 (TCP)", Value: 400},
		{FilterParam: "protocol", FilterValue: "17", Label: "17 (UDP)", Value: 200},
	})
	assert.DeepEqual(t, graph.Breakdown.Family.Slices, []BreakdownSlice{
		{FilterParam: "family", FilterValue: "ipv4", Label: "IPv4", Value: 400},
		{FilterParam: "family", FilterValue: "ipv6", Label: "IPv6", Value: 200},
	})
	assert.DeepEqual(t, graph.Breakdown.Ports.Slices, []BreakdownSlice{
		{FilterParam: "port", FilterValue: "443", Label: "443", Value: 400},
		{FilterParam: "port", FilterValue: "53", Label: "53", Value: 200},
	})
}

func TestServiceGraphSelectionBreakdownUsesSelectedEdgeDirectionOnly(t *testing.T) {
	tempDir := t.TempDir()
	forwardRecord := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 300, 10, 20)
	forwardRecord.Protocol = 6
	forwardRecord.DstPort = 443
	secondForwardRecord := sampleRecord("fd00::1", "2001:db8::1", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 200, 21, 29)
	secondForwardRecord.Protocol = 17
	secondForwardRecord.SrcPort = 53000
	secondForwardRecord.DstPort = 53
	reverseRecord := sampleRecord("8.8.8.8", "192.168.1.10", "dns.google", "google.com", "com", "alpha.lan", "lan", "lan", 900, 30, 40)
	reverseRecord.Protocol = 17
	reverseRecord.SrcPort = 53
	reverseRecord.DstPort = 55000
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{forwardRecord, secondForwardRecord, reverseRecord})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity:     GranularityHostname,
		Metric:          MetricBytes,
		SelectedEdgeSrc: "alpha.lan",
		SelectedEdgeDst: "dns.google",
	})
	assert.NilError(t, err)

	assert.Assert(t, graph.Breakdown.Protocols != nil)
	assert.Assert(t, graph.Breakdown.Family != nil)
	assert.Assert(t, graph.Breakdown.Ports != nil)
	assert.DeepEqual(t, graph.Breakdown.Protocols.Slices, []BreakdownSlice{
		{FilterParam: "protocol", FilterValue: "6", Label: "6 (TCP)", Value: 300},
		{FilterParam: "protocol", FilterValue: "17", Label: "17 (UDP)", Value: 200},
	})
	assert.DeepEqual(t, graph.Breakdown.Family.Slices, []BreakdownSlice{
		{FilterParam: "family", FilterValue: "ipv4", Label: "IPv4", Value: 300},
		{FilterParam: "family", FilterValue: "ipv6", Label: "IPv6", Value: 200},
	})
	assert.DeepEqual(t, graph.Breakdown.Ports.Slices, []BreakdownSlice{
		{FilterParam: "port", FilterValue: "443", Label: "443", Value: 300},
		{FilterParam: "port", FilterValue: "53", Label: "53", Value: 200},
	})
}

func TestServiceGraphSelectionBreakdownFallsBackToWholeView(t *testing.T) {
	tempDir := t.TempDir()
	tcpRecord := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20)
	tcpRecord.Protocol = 6
	tcpRecord.DstPort = 443
	udpRecord := sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 250, 30, 40)
	udpRecord.Protocol = 17
	udpRecord.SrcPort = 53000
	udpRecord.DstPort = 53
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{tcpRecord, udpRecord})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
	})
	assert.NilError(t, err)

	assert.Assert(t, graph.Breakdown.Protocols != nil)
	assert.DeepEqual(t, graph.Breakdown.Protocols.Slices, []BreakdownSlice{
		{FilterParam: "protocol", FilterValue: "17", Label: "17 (UDP)", Value: 250},
		{FilterParam: "protocol", FilterValue: "6", Label: "6 (TCP)", Value: 100},
	})
}

func TestServiceGraphSelectionBreakdownUsesConnectionWeighting(t *testing.T) {
	tempDir := t.TempDir()
	firstRecord := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 500, 10, 20)
	firstRecord.Protocol = 6
	firstRecord.DstPort = 443
	secondRecord := sampleRecord("192.168.1.10", "8.8.4.4", "alpha.lan", "lan", "lan", "dns-alt.google", "google.com", "com", 100, 30, 40)
	secondRecord.Protocol = 17
	secondRecord.SrcPort = 51000
	secondRecord.DstPort = 53
	thirdRecord := sampleRecord("192.168.1.10", "1.1.1.1", "alpha.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 50, 50, 60)
	thirdRecord.Protocol = 17
	thirdRecord.SrcPort = 52000
	thirdRecord.DstPort = 53
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{firstRecord, secondRecord, thirdRecord})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity:    GranularityHostname,
		Metric:         MetricConnections,
		SelectedEntity: "alpha.lan",
	})
	assert.NilError(t, err)

	assert.DeepEqual(t, graph.Breakdown.Protocols.Slices, []BreakdownSlice{
		{FilterParam: "protocol", FilterValue: "17", Label: "17 (UDP)", Value: 2},
		{FilterParam: "protocol", FilterValue: "6", Label: "6 (TCP)", Value: 1},
	})
	assert.DeepEqual(t, graph.Breakdown.Ports.Slices, []BreakdownSlice{
		{FilterParam: "port", FilterValue: "53", Label: "53", Value: 2},
		{FilterParam: "port", FilterValue: "443", Label: "443", Value: 1},
	})
}

func TestServiceGraphFiltersByProtocolAndPort(t *testing.T) {
	tempDir := t.TempDir()
	tcpRecord := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20)
	tcpRecord.Protocol = 6
	tcpRecord.DstPort = 443
	udpRecord := sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 200, 30, 40)
	udpRecord.Protocol = 17
	udpRecord.DstPort = 53
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{tcpRecord, udpRecord})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	protocolGraph, err := service.Graph(context.Background(), QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
		Protocol:    17,
	})
	assert.NilError(t, err)
	assert.Equal(t, protocolGraph.Totals.Bytes, int64(200))
	assert.Assert(t, containsNode(protocolGraph.Nodes, "one.one.one.one"))
	assert.Assert(t, !containsNode(protocolGraph.Nodes, "dns.google"))

	portGraph, err := service.Graph(context.Background(), QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
		Port:        443,
	})
	assert.NilError(t, err)
	assert.Equal(t, portGraph.Totals.Bytes, int64(100))
	assert.Assert(t, containsNode(portGraph.Nodes, "dns.google"))
	assert.Assert(t, !containsNode(portGraph.Nodes, "one.one.one.one"))
}

func TestServiceFlowDetailsFiltersByPort(t *testing.T) {
	tempDir := t.TempDir()
	tcpRecord := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20)
	tcpRecord.Protocol = 6
	tcpRecord.DstPort = 443
	udpRecord := sampleRecord("192.168.1.10", "1.1.1.1", "alpha.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 200, 30, 40)
	udpRecord.Protocol = 17
	udpRecord.DstPort = 53
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{tcpRecord, udpRecord})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	flows, err := service.FlowDetails(context.Background(), FlowQuery{
		Entity: "alpha.lan",
		Scope:  FlowScopeEntity,
		State: QueryState{
			Granularity: GranularityHostname,
			Metric:      MetricBytes,
			Port:        53,
		},
	})
	assert.NilError(t, err)

	assert.Equal(t, flows.TotalCount, 1)
	assert.Equal(t, flows.VisibleRows[0].DstPort, int32(53))
}

func TestFoldBreakdownSlicesUsesTopFiveAndRest(t *testing.T) {
	slices := foldBreakdownSlices([]BreakdownSlice{
		{Label: "a", Value: 50},
		{Label: "b", Value: 20},
		{Label: "c", Value: 10},
		{Label: "d", Value: 8},
		{Label: "e", Value: 6},
		{Label: "f", Value: 4},
		{Label: "g", Value: 2},
	})

	assert.DeepEqual(t, slices, []BreakdownSlice{
		{Label: "a", Value: 50},
		{Label: "b", Value: 20},
		{Label: "c", Value: 10},
		{Label: "d", Value: 8},
		{Label: breakdownRestLabel, Value: 12},
	})
}

func TestServiceGraphSelectionBreakdownOmitsUniformChartsAndTransientPorts(t *testing.T) {
	tempDir := t.TempDir()
	firstRecord := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20)
	firstRecord.Protocol = 6
	firstRecord.SrcPort = 55001
	firstRecord.DstPort = 443
	secondRecord := sampleRecord("192.168.1.11", "8.8.4.4", "beta.lan", "lan", "lan", "dns-alt.google", "google.com", "com", 200, 30, 40)
	secondRecord.Protocol = 6
	secondRecord.SrcPort = 55002
	secondRecord.DstPort = 443
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{firstRecord, secondRecord})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricBytes,
	})
	assert.NilError(t, err)

	assert.Assert(t, graph.Breakdown.Protocols == nil)
	assert.Assert(t, graph.Breakdown.Family == nil)
	assert.Assert(t, graph.Breakdown.Ports == nil)
}

func TestServiceGraphSelectionBreakdownUnavailableForDNSMetric(t *testing.T) {
	tempDir := t.TempDir()
	record := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20)
	record.Protocol = 6
	record.DstPort = 443
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{record})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricDNSLookups,
	})
	assert.NilError(t, err)

	assert.Assert(t, graph.Breakdown.Protocols == nil)
	assert.Assert(t, graph.Breakdown.Family == nil)
	assert.Assert(t, graph.Breakdown.Ports == nil)
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
		summaryFilterCacheSuffix(state) +
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

	cacheKey := summaryGraphSnapshotCacheKey(Granularity2LD, AddressFamilyIPv4, DirectionBoth, MetricBytes, service.currentRevision()) + summaryFilterCacheSuffix(state)
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

	cacheKey := summaryGraphSnapshotCacheKey(Granularity2LD, AddressFamilyAll, DirectionEgress, MetricBytes, service.currentRevision()) + summaryFilterCacheSuffix(state)
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

func TestServiceGraphHidesIgnoredDNSLookupsByDefault(t *testing.T) {
	const dnsLookupTestLAN = "lan"

	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", dnsLookupTestFW, dnsLookupTestFW, "lan", "dns.google", "google.com", "com", 100, int64(time.Hour), int64(2*time.Hour)),
	})
	clientHost := dnsLookupTestFW
	client2LD := dnsLookupTestFW
	clientTLD := dnsLookupTestLAN
	tapo2LD := "tapo1.lan"
	tapoTLD := dnsLookupTestLAN
	example2LD := dnsLookupTestQuery2LD
	exampleTLD := dnsLookupTestQueryTLD
	writeDNSLookupParquet(t, filepath.Join(tempDir, "dns_lookups_202604.parquet"), []model.DNSLookupRecord{
		{Client2LD: &client2LD, ClientHost: &clientHost, ClientIP: "192.168.1.10", ClientIPVersion: model.IPVersion4, ClientIsPrivate: true, ClientTLD: &clientTLD, Lookups: 2, Query2LD: &tapo2LD, QueryName: "tapo1.lan", QueryTLD: &tapoTLD, QueryType: "A", TimeStartNs: int64(time.Hour)},
		{Client2LD: &client2LD, ClientHost: &clientHost, ClientIP: "192.168.1.10", ClientIPVersion: model.IPVersion4, ClientIsPrivate: true, ClientTLD: &clientTLD, Lookups: 1, Query2LD: &example2LD, QueryName: "www.example.com", QueryTLD: &exampleTLD, QueryType: "A", TimeStartNs: int64(time.Hour) + 1},
	})
	writeIgnoreRules(t, tempDir, []IgnoreRule{{
		ID:      "ignore-tapo",
		Enabled: true,
		Name:    "Ignore tapo",
		Match: IgnoreRuleMatch{
			DestinationEntity: "tapo1.lan",
			SourceEntity:      dnsLookupTestFW,
		},
	}})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	graph, err := service.Graph(context.Background(), QueryState{
		Granularity: GranularityHostname,
		Metric:      MetricDNSLookups,
	})
	assert.NilError(t, err)

	assert.Equal(t, graph.Totals.Connections, int64(1))
	assert.Equal(t, graph.Totals.Ignored, int64(0))
	assert.Assert(t, containsNode(graph.Nodes, dnsLookupTestFW))
	assert.Assert(t, containsNode(graph.Nodes, "www.example.com"))
	assert.Assert(t, !containsNode(graph.Nodes, "tapo1.lan"))
}

func TestServiceGraphMarksVisibleIgnoredDNSLookups(t *testing.T) {
	const dnsLookupTestLAN = "lan"

	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", dnsLookupTestFW, dnsLookupTestFW, "lan", "dns.google", "google.com", "com", 100, int64(time.Hour), int64(2*time.Hour)),
	})
	clientHost := dnsLookupTestFW
	client2LD := dnsLookupTestFW
	clientTLD := dnsLookupTestLAN
	tapo2LD := "tapo1.lan"
	tapoTLD := dnsLookupTestLAN
	example2LD := dnsLookupTestQuery2LD
	exampleTLD := dnsLookupTestQueryTLD
	writeDNSLookupParquet(t, filepath.Join(tempDir, "dns_lookups_202604.parquet"), []model.DNSLookupRecord{
		{Client2LD: &client2LD, ClientHost: &clientHost, ClientIP: "192.168.1.10", ClientIPVersion: model.IPVersion4, ClientIsPrivate: true, ClientTLD: &clientTLD, Lookups: 2, Query2LD: &tapo2LD, QueryName: "tapo1.lan", QueryTLD: &tapoTLD, QueryType: "A", TimeStartNs: int64(time.Hour)},
		{Client2LD: &client2LD, ClientHost: &clientHost, ClientIP: "192.168.1.10", ClientIPVersion: model.IPVersion4, ClientIsPrivate: true, ClientTLD: &clientTLD, Lookups: 1, Query2LD: &example2LD, QueryName: "www.example.com", QueryTLD: &exampleTLD, QueryType: "A", TimeStartNs: int64(time.Hour) + 1},
	})
	writeIgnoreRules(t, tempDir, []IgnoreRule{{
		ID:      "ignore-tapo",
		Enabled: true,
		Name:    "Ignore tapo",
		Match: IgnoreRuleMatch{
			DestinationEntity: "tapo1.lan",
			SourceEntity:      dnsLookupTestFW,
		},
	}})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	state := QueryState{
		Granularity:    GranularityHostname,
		HideIgnored:    false,
		HideIgnoredSet: true,
		Metric:         MetricDNSLookups,
	}
	graph, err := service.Graph(context.Background(), state)
	assert.NilError(t, err)

	assert.Equal(t, graph.Totals.Connections, int64(3))
	assert.Equal(t, graph.Totals.Ignored, int64(2))
	tapoEdge := findEdge(t, graph.Edges, dnsLookupTestFW, "tapo1.lan")
	assert.Assert(t, tapoEdge.Ignored)
	assert.Equal(t, tapoEdge.IgnoredMetric, int64(2))
	tapoNode := findNode(t, graph.Nodes, "tapo1.lan")
	assert.Assert(t, tapoNode.Ignored)

	table, err := service.Table(context.Background(), state)
	assert.NilError(t, err)
	tapoRow := findTableRow(t, table.Rows, dnsLookupTestFW, "tapo1.lan")
	assert.Assert(t, tapoRow.Ignored)
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
	assert.Assert(t, containsNode(graph.Nodes, localIPv4EntityLabel))
	assert.Assert(t, containsNode(graph.Nodes, "example.com"))

	cacheKey := summaryGraphSnapshotCacheKey(Granularity2LD, AddressFamilyAll, DirectionBoth, MetricDNSLookups, service.currentRevision()) + summaryFilterCacheSuffix(state)
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

	cacheKey := summaryGraphSnapshotCacheKey(Granularity2LD, AddressFamilyAll, DirectionBoth, MetricBytes, service.currentRevision()) + summaryFilterCacheSuffix(state)
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

func TestNewServiceBuildsUISummariesWithProtocolAndServicePort(t *testing.T) {
	tempDir := t.TempDir()
	record := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, int64(time.Hour), int64(2*time.Hour))
	record.Protocol = 6
	record.DstPort = 443
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{record})

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

	assert.Assert(t, containsEdgeSummaryProtocolAndPort(edgeRows, 6, 443))
	assert.Assert(t, containsHistogramSummaryProtocolAndPort(histogramRows, 6, 443))
}

func TestServiceGraphPortFilterUsesSummaryFastPath(t *testing.T) {
	tempDir := t.TempDir()
	tcpRecord := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, int64(time.Hour), int64(2*time.Hour))
	tcpRecord.Protocol = 6
	tcpRecord.DstPort = 443
	udpRecord := sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 200, int64(3*time.Hour), int64(4*time.Hour))
	udpRecord.Protocol = 17
	udpRecord.DstPort = 53
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{tcpRecord, udpRecord})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	span, err := service.Span(context.Background())
	assert.NilError(t, err)
	state := QueryState{
		Granularity: Granularity2LD,
		Metric:      MetricBytes,
		Port:        443,
	}.Normalized(span)

	assert.Assert(t, service.canUseSummaryGraph(state, span))
	graph, err := service.Graph(context.Background(), state)
	assert.NilError(t, err)

	assert.Equal(t, graph.Totals.Bytes, int64(100))
	assert.Assert(t, containsNode(graph.Nodes, "google.com"))
	assert.Assert(t, !containsNode(graph.Nodes, "one.one.one.one"))
	cacheKey := summaryGraphSnapshotCacheKey(Granularity2LD, AddressFamilyAll, DirectionBoth, MetricBytes, service.currentRevision()) + summaryFilterCacheSuffix(state)
	_, ok := service.summaryGraphCache.Get(cacheKey)
	assert.Assert(t, ok)
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
		service.mu.RLock()
		pending := service.summaryRefreshPending
		service.mu.RUnlock()
		if manifest.Source.ModTimeNs == modTime.UnixNano() && !pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for stale UI summary rebuild")
		}
		time.Sleep(25 * time.Millisecond)
	}

	service.mu.RLock()
	pending := service.summaryRefreshPending
	service.mu.RUnlock()
	assert.Assert(t, !pending)
}

func TestServiceLongRangeServicePortIgnoreUsesSummary(t *testing.T) {
	tempDir := t.TempDir()
	ignoredRecord := sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20)
	ignoredRecord.Protocol = 6
	ignoredRecord.SrcPort = 55000
	ignoredRecord.DstPort = 443
	visibleRecord := sampleRecord("192.168.1.11", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 200, 1+int64(8*24*time.Hour), 1+int64(8*24*time.Hour)+20)
	visibleRecord.Protocol = 17
	visibleRecord.SrcPort = 55001
	visibleRecord.DstPort = 53
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		ignoredRecord,
		visibleRecord,
	})
	writeIgnoreRules(t, tempDir, []IgnoreRule{{
		ID:      "ignore-https",
		Enabled: true,
		Name:    "Ignore HTTPS",
		Match: IgnoreRuleMatch{
			Protocol:    6,
			ServicePort: 443,
		},
	}})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	span, err := service.Span(context.Background())
	assert.NilError(t, err)
	state := QueryState{
		Granularity: Granularity2LD,
		Metric:      MetricBytes,
	}.Normalized(span)
	assert.Assert(t, !state.EntityActionsEnabled())
	assert.Assert(t, service.canUseSummaryGraph(state, span))

	graph, err := service.Graph(context.Background(), state)
	assert.NilError(t, err)
	assert.Equal(t, graph.Totals.Bytes, int64(200))
	assert.Assert(t, !containsNode(graph.Nodes, "google.com"))
	assert.Assert(t, containsNode(graph.Nodes, "one.one.one.one"))

	cacheKey := summaryGraphSnapshotCacheKey(Granularity2LD, AddressFamilyAll, DirectionBoth, MetricBytes, service.currentRevision()) + summaryFilterCacheSuffix(state)
	_, ok := service.summaryGraphCache.Get(cacheKey)
	assert.Assert(t, ok)
}

func TestServiceLongRangeSourceIPIgnoreUsesSummaryIdentity(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "alpha.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
		sampleRecord("10.0.0.5", "1.1.1.1", "beta.lan", "lan", "lan", "one.one.one.one", "one.one.one.one", "one.one.one.one", 200, 1+int64(8*24*time.Hour), 1+int64(8*24*time.Hour)+20),
	})
	writeIgnoreRules(t, tempDir, []IgnoreRule{{
		ID:      "ignore-192-168-1",
		Enabled: true,
		Name:    "Ignore 192.168.1.10",
		Match: IgnoreRuleMatch{
			SourceIP: "192.168.1.10",
		},
	}})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	span, err := service.Span(context.Background())
	assert.NilError(t, err)
	state := QueryState{
		Granularity: Granularity2LD,
		Metric:      MetricBytes,
	}.Normalized(span)
	assert.Assert(t, !state.EntityActionsEnabled())
	assert.Assert(t, service.canUseSummaryGraph(state, span))

	graph, err := service.Graph(context.Background(), state)
	assert.NilError(t, err)
	assert.Equal(t, graph.Totals.Bytes, int64(200))
	assert.Assert(t, !containsNode(graph.Nodes, "google.com"))
	assert.Assert(t, containsNode(graph.Nodes, "one.one.one.one"))

	cacheKey := summaryGraphSnapshotCacheKey(Granularity2LD, AddressFamilyAll, DirectionBoth, MetricBytes, service.currentRevision()) + summaryFilterCacheSuffix(state)
	_, ok := service.summaryGraphCache.Get(cacheKey)
	assert.Assert(t, ok)
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

func TestServiceLocalNamedHostsUseLocalTLDAndFirstLabel2LD(t *testing.T) {
	tempDir := t.TempDir()
	writeEnrichedParquet(t, filepath.Join(tempDir, "nfcap_202604.parquet"), []model.FlowRecord{
		sampleRecord("192.168.1.10", "8.8.8.8", "phone.lan", "lan", "lan", "dns.google", "google.com", "com", 100, 10, 20),
	})

	service, err := NewService(context.Background(), tempDir, time.Hour)
	assert.NilError(t, err)
	defer service.Close()

	tldGraph, err := service.Graph(context.Background(), QueryState{
		Granularity: GranularityTLD,
		Metric:      MetricBytes,
	})
	assert.NilError(t, err)
	assert.Assert(t, containsNode(tldGraph.Nodes, localEntityLabel))

	twoLDGraph, err := service.Graph(context.Background(), QueryState{
		Granularity: Granularity2LD,
		Metric:      MetricBytes,
	})
	assert.NilError(t, err)
	assert.Assert(t, containsNode(twoLDGraph.Nodes, "phone"))
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
	assert.Assert(t, containsNode(graph.Nodes, localIPv4EntityLabel))
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

func containsEdge(edges []Edge, source, destination string) bool {
	for _, edge := range edges {
		if edge.Source == source && edge.Destination == destination {
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

func applySourceDevice(record *model.FlowRecord, id, label string) {
	source := "mac"
	record.SrcDeviceID = &id
	record.SrcDeviceLabel = &label
	record.SrcDeviceSource = &source
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

func findNode(t *testing.T, nodes []Node, nodeID string) Node {
	t.Helper()

	for _, node := range nodes {
		if node.ID == nodeID {
			return node
		}
	}
	t.Fatalf("node %q not found", nodeID)
	return Node{}
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

func containsEdgeSummaryProtocolAndPort(rows []parquetout.EdgeSummaryRow, protocol, port int32) bool {
	for _, row := range rows {
		if row.Protocol == protocol && row.ServicePort != nil && *row.ServicePort == port {
			return true
		}
	}
	return false
}

func containsHistogramSummaryProtocolAndPort(rows []parquetout.HistogramSummaryRow, protocol, port int32) bool {
	for _, row := range rows {
		if row.Protocol == protocol && row.ServicePort != nil && *row.ServicePort == port {
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

func writeIgnoreRules(t *testing.T, dirPath string, rules []IgnoreRule) {
	t.Helper()

	bytes, err := json.Marshal(ignoreRuleFile{Rules: rules})
	assert.NilError(t, err)
	assert.NilError(t, os.WriteFile(filepath.Join(dirPath, ignoreRulesFilename), bytes, 0o644))
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
