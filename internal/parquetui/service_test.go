package parquetui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fingon/homenetflow/internal/model"
	"github.com/fingon/homenetflow/internal/parquetout"
	"gotest.tools/v3/assert"
)

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
	assert.Assert(t, !strings.Contains(recorder.Body.String(), "initial-state-json"))
	assert.Assert(t, !strings.Contains(recorder.Body.String(), "span-json"))
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
	assert.Equal(t, len(service.summaries.histogramPaths), 1)
	_, err = os.Stat(filepath.Join(tempDir, "ui_summary_edges_tld_202604.parquet"))
	assert.NilError(t, err)
	_, err = os.Stat(filepath.Join(tempDir, "ui_summary_edges_2ld_202604.parquet"))
	assert.NilError(t, err)
	_, err = os.Stat(filepath.Join(tempDir, "ui_summary_histogram_202604.parquet"))
	assert.NilError(t, err)
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
	assert.Assert(t, containsNode(graph.Nodes, unknownEntityLabel))
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
	assert.Assert(t, containsNode(graph.Nodes, unknownEntityLabel))
	assert.Assert(t, !containsNode(graph.Nodes, "192.0.2.10"))
	assert.Assert(t, !containsNode(graph.Nodes, "2001:db8::1"))
}

func containsNode(nodes []Node, nodeID string) bool {
	for _, node := range nodes {
		if node.ID == nodeID {
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

	writer, finalize, err := parquetout.CreateEnriched(path, model.EnrichmentManifest{Version: 1})
	assert.NilError(t, err)
	assert.NilError(t, writer.WriteBatch(records))
	assert.NilError(t, finalize())
}

func sampleRecord(srcIP, dstIP, srcHost, src2LD, srcTLD, dstHost, dst2LD, dstTLD string, bytes, startNs, endNs int64) model.FlowRecord {
	return model.FlowRecord{
		Bytes:       bytes,
		Dst2LD:      strPtr(dst2LD),
		DstHost:     strPtr(dstHost),
		DstIP:       dstIP,
		DstTLD:      strPtr(dstTLD),
		Src2LD:      strPtr(src2LD),
		SrcHost:     strPtr(srcHost),
		SrcIP:       srcIP,
		SrcTLD:      strPtr(srcTLD),
		TimeEndNs:   endNs,
		TimeStartNs: startNs,
	}
}

func strPtr(value string) *string {
	return &value
}
