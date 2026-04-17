package parquetui

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/fingon/homenetflow/internal/model"
	"github.com/fingon/homenetflow/internal/parquetout"
	"github.com/fingon/homenetflow/internal/scan"
	"golang.org/x/sync/errgroup"
)

const (
	summaryBucketWidthNs     = int64(time.Hour)
	summaryBuildJobLimit     = 2
	summaryBuildRowBatchSize = 1024
	summaryBucketedEdgeKind  = "bucketed_edges"
	summaryEdgeKind          = "edges"
	summaryFilenamePrefix    = "ui_summary_"
	summaryHistogramKind     = "histogram"
)

type summarySnapshot struct {
	bucketedEdgeGlobByGranularity    map[Granularity]string
	bucketedEdgePathsByGranulariy    map[Granularity][]string
	dnsBucketedEdgeGlobByGranularity map[Granularity]string
	dnsBucketedEdgePathsByGranulariy map[Granularity][]string
	dnsEdgeGlobByGranularity         map[Granularity]string
	dnsEdgePathsByGranulariy         map[Granularity][]string
	dnsHistogramGlob                 string
	dnsHistogramPaths                []string
	edgeGlobByGranularity            map[Granularity]string
	edgePathsByGranulariy            map[Granularity][]string
	histogramGlob                    string
	histogramPaths                   []string
	span                             TimeSpan
	spanValid                        bool
}

type summaryInspection struct {
	missingJobs []summaryJob
	snapshot    summarySnapshot
	staleJobs   []summaryJob
}

type summaryJob struct {
	paths      sourceSummaryPaths
	sourceFile model.SourceFile
}

type summarySourceState int

const (
	summarySourceStateMissing summarySourceState = iota
	summarySourceStateStale
	summarySourceStateFresh
)

type summarySourceStatus struct {
	bucketedTLdManifest      model.UISummaryManifest
	bucketedTwoLDManifest    model.UISummaryManifest
	dnsBucketedTLdManifest   model.UISummaryManifest
	dnsBucketedTwoLDManifest model.UISummaryManifest
	dnsHistogramManifest     model.UISummaryManifest
	dnsSourceFound           bool
	dnsTLdManifest           model.UISummaryManifest
	dnsTwoLDManifest         model.UISummaryManifest
	histogramManifest        model.UISummaryManifest
	state                    summarySourceState
	tldManifest              model.UISummaryManifest
}

type sourceSummaryPaths struct {
	bucketedTldEdges      string
	bucketedTwoLDEdges    string
	dnsBucketedTldEdges   string
	dnsBucketedTwoLDEdges string
	dnsHistogram          string
	dnsSource             string
	dnsTldEdges           string
	dnsTwoLDEdges         string
	histogram             string
	tldEdges              string
	twoLDEdges            string
}

func inspectSummaryState(srcRootPath string) (summaryInspection, error) {
	sourceFilesByPeriod, err := scan.FlatParquetTree(srcRootPath)
	if err != nil {
		return summaryInspection{}, fmt.Errorf("scan parquet source tree: %w", err)
	}
	if len(sourceFilesByPeriod) == 0 {
		return summaryInspection{}, fmt.Errorf("no parquet files found in %q", srcRootPath)
	}

	sourceFiles := sortedSourceFiles(sourceFilesByPeriod)
	expectedPaths := make(map[string]struct{}, len(sourceFiles)*3)
	inspection := summaryInspection{
		snapshot: summarySnapshot{
			bucketedEdgeGlobByGranularity: map[Granularity]string{
				GranularityTLD: filepath.ToSlash(filepath.Join(srcRootPath, summaryFilenamePrefix+"bucketed_edges_tld_*.parquet")),
				Granularity2LD: filepath.ToSlash(filepath.Join(srcRootPath, summaryFilenamePrefix+"bucketed_edges_2ld_*.parquet")),
			},
			bucketedEdgePathsByGranulariy: map[Granularity][]string{
				GranularityTLD: make([]string, 0, len(sourceFiles)),
				Granularity2LD: make([]string, 0, len(sourceFiles)),
			},
			dnsBucketedEdgeGlobByGranularity: map[Granularity]string{
				GranularityTLD: filepath.ToSlash(filepath.Join(srcRootPath, summaryFilenamePrefix+"dns_bucketed_edges_tld_*.parquet")),
				Granularity2LD: filepath.ToSlash(filepath.Join(srcRootPath, summaryFilenamePrefix+"dns_bucketed_edges_2ld_*.parquet")),
			},
			dnsBucketedEdgePathsByGranulariy: map[Granularity][]string{
				GranularityTLD: make([]string, 0, len(sourceFiles)),
				Granularity2LD: make([]string, 0, len(sourceFiles)),
			},
			dnsEdgeGlobByGranularity: map[Granularity]string{
				GranularityTLD: filepath.ToSlash(filepath.Join(srcRootPath, summaryFilenamePrefix+"dns_edges_tld_*.parquet")),
				Granularity2LD: filepath.ToSlash(filepath.Join(srcRootPath, summaryFilenamePrefix+"dns_edges_2ld_*.parquet")),
			},
			dnsEdgePathsByGranulariy: map[Granularity][]string{
				GranularityTLD: make([]string, 0, len(sourceFiles)),
				Granularity2LD: make([]string, 0, len(sourceFiles)),
			},
			dnsHistogramGlob:  filepath.ToSlash(filepath.Join(srcRootPath, summaryFilenamePrefix+"dns_histogram_*.parquet")),
			dnsHistogramPaths: make([]string, 0, len(sourceFiles)),
			edgeGlobByGranularity: map[Granularity]string{
				GranularityTLD: filepath.ToSlash(filepath.Join(srcRootPath, summaryFilenamePrefix+"edges_tld_*.parquet")),
				Granularity2LD: filepath.ToSlash(filepath.Join(srcRootPath, summaryFilenamePrefix+"edges_2ld_*.parquet")),
			},
			edgePathsByGranulariy: map[Granularity][]string{
				GranularityTLD: make([]string, 0, len(sourceFiles)),
				Granularity2LD: make([]string, 0, len(sourceFiles)),
			},
			histogramGlob:  filepath.ToSlash(filepath.Join(srcRootPath, summaryFilenamePrefix+"histogram_*.parquet")),
			histogramPaths: make([]string, 0, len(sourceFiles)),
		},
	}

	for _, sourceFile := range sourceFiles {
		job := summaryJob{
			paths:      summaryPathsForSource(srcRootPath, sourceFile),
			sourceFile: sourceFile,
		}
		for _, path := range []string{job.paths.tldEdges, job.paths.twoLDEdges, job.paths.bucketedTldEdges, job.paths.bucketedTwoLDEdges, job.paths.histogram} {
			expectedPaths[path] = struct{}{}
		}
		dnsSourceFound, err := fileExists(job.paths.dnsSource)
		if err != nil {
			return summaryInspection{}, err
		}
		if dnsSourceFound {
			for _, path := range []string{job.paths.dnsTldEdges, job.paths.dnsTwoLDEdges, job.paths.dnsBucketedTldEdges, job.paths.dnsBucketedTwoLDEdges, job.paths.dnsHistogram} {
				expectedPaths[path] = struct{}{}
			}
		}

		status, err := inspectSourceSummary(job)
		if err != nil {
			return summaryInspection{}, err
		}
		switch status.state {
		case summarySourceStateMissing:
			inspection.missingJobs = append(inspection.missingJobs, job)
		case summarySourceStateStale:
			inspection.staleJobs = append(inspection.staleJobs, job)
			addSummaryPaths(&inspection.snapshot, job, status)
		case summarySourceStateFresh:
			addSummaryPaths(&inspection.snapshot, job, status)
		}
	}

	if err := cleanupStaleSummaryFiles(srcRootPath, expectedPaths); err != nil {
		return summaryInspection{}, err
	}

	return inspection, nil
}

func inspectSourceSummary(job summaryJob) (summarySourceStatus, error) {
	tldManifest, tldFound, err := readUISummaryManifestIfPresent(job.paths.tldEdges)
	if err != nil {
		return summarySourceStatus{}, err
	}
	twoLDManifest, twoLDFound, err := readUISummaryManifestIfPresent(job.paths.twoLDEdges)
	if err != nil {
		return summarySourceStatus{}, err
	}
	bucketedTLDManifest, bucketedTLDFound, err := readUISummaryManifestIfPresent(job.paths.bucketedTldEdges)
	if err != nil {
		return summarySourceStatus{}, err
	}
	bucketedTwoLDManifest, bucketedTwoLDFound, err := readUISummaryManifestIfPresent(job.paths.bucketedTwoLDEdges)
	if err != nil {
		return summarySourceStatus{}, err
	}
	histogramManifest, histogramFound, err := readUISummaryManifestIfPresent(job.paths.histogram)
	if err != nil {
		return summarySourceStatus{}, err
	}
	if !tldFound || !twoLDFound || !bucketedTLDFound || !bucketedTwoLDFound || !histogramFound {
		return summarySourceStatus{state: summarySourceStateMissing}, nil
	}

	dnsSourceFound, err := fileExists(job.paths.dnsSource)
	if err != nil {
		return summarySourceStatus{}, err
	}
	var dnsTLDManifest model.UISummaryManifest
	var dnsTwoLDManifest model.UISummaryManifest
	var dnsBucketedTLDManifest model.UISummaryManifest
	var dnsBucketedTwoLDManifest model.UISummaryManifest
	var dnsHistogramManifest model.UISummaryManifest
	if dnsSourceFound {
		var dnsTLDFound bool
		var dnsTwoLDFound bool
		var dnsBucketedTLDFound bool
		var dnsBucketedTwoLDFound bool
		var dnsHistogramFound bool
		dnsTLDManifest, dnsTLDFound, err = readUISummaryManifestIfPresent(job.paths.dnsTldEdges)
		if err != nil {
			return summarySourceStatus{}, err
		}
		dnsTwoLDManifest, dnsTwoLDFound, err = readUISummaryManifestIfPresent(job.paths.dnsTwoLDEdges)
		if err != nil {
			return summarySourceStatus{}, err
		}
		dnsBucketedTLDManifest, dnsBucketedTLDFound, err = readUISummaryManifestIfPresent(job.paths.dnsBucketedTldEdges)
		if err != nil {
			return summarySourceStatus{}, err
		}
		dnsBucketedTwoLDManifest, dnsBucketedTwoLDFound, err = readUISummaryManifestIfPresent(job.paths.dnsBucketedTwoLDEdges)
		if err != nil {
			return summarySourceStatus{}, err
		}
		dnsHistogramManifest, dnsHistogramFound, err = readUISummaryManifestIfPresent(job.paths.dnsHistogram)
		if err != nil {
			return summarySourceStatus{}, err
		}
		if !dnsTLDFound || !dnsTwoLDFound || !dnsBucketedTLDFound || !dnsBucketedTwoLDFound || !dnsHistogramFound {
			return summarySourceStatus{dnsSourceFound: true, state: summarySourceStateMissing}, nil
		}
	}

	expectedSource := summarySourceManifest(job.sourceFile)
	manifests := []model.UISummaryManifest{tldManifest, twoLDManifest, bucketedTLDManifest, bucketedTwoLDManifest, histogramManifest}
	expectedKinds := []string{summaryEdgeKind, summaryEdgeKind, summaryBucketedEdgeKind, summaryBucketedEdgeKind, summaryHistogramKind}
	expectedGranularities := []string{string(GranularityTLD), string(Granularity2LD), string(GranularityTLD), string(Granularity2LD), ""}
	for index, manifest := range manifests {
		if manifest.Version != model.UISummaryManifestVersion ||
			manifest.LogicVersion != model.UISummaryLogicVersion ||
			manifest.Kind != expectedKinds[index] ||
			manifest.Granularity != expectedGranularities[index] ||
			manifest.Source != expectedSource {
			return summarySourceStatus{
				bucketedTLdManifest:      bucketedTLDManifest,
				bucketedTwoLDManifest:    bucketedTwoLDManifest,
				dnsBucketedTLdManifest:   dnsBucketedTLDManifest,
				dnsBucketedTwoLDManifest: dnsBucketedTwoLDManifest,
				dnsHistogramManifest:     dnsHistogramManifest,
				dnsSourceFound:           dnsSourceFound,
				dnsTLdManifest:           dnsTLDManifest,
				dnsTwoLDManifest:         dnsTwoLDManifest,
				histogramManifest:        histogramManifest,
				state:                    summarySourceStateStale,
				tldManifest:              tldManifest,
			}, nil
		}
	}

	if dnsSourceFound {
		expectedDNSSource, err := dnsSummarySourceManifest(job.paths.dnsSource)
		if err != nil {
			return summarySourceStatus{}, err
		}
		dnsManifests := []model.UISummaryManifest{dnsTLDManifest, dnsTwoLDManifest, dnsBucketedTLDManifest, dnsBucketedTwoLDManifest, dnsHistogramManifest}
		dnsExpectedKinds := []string{summaryEdgeKind, summaryEdgeKind, summaryBucketedEdgeKind, summaryBucketedEdgeKind, summaryHistogramKind}
		dnsExpectedGranularities := []string{string(GranularityTLD), string(Granularity2LD), string(GranularityTLD), string(Granularity2LD), ""}
		for index, manifest := range dnsManifests {
			if manifest.Version != model.UISummaryManifestVersion ||
				manifest.LogicVersion != model.UISummaryLogicVersion ||
				manifest.Kind != dnsExpectedKinds[index] ||
				manifest.Granularity != dnsExpectedGranularities[index] ||
				manifest.Source != expectedDNSSource {
				return summarySourceStatus{
					bucketedTLdManifest:      bucketedTLDManifest,
					bucketedTwoLDManifest:    bucketedTwoLDManifest,
					dnsBucketedTLdManifest:   dnsBucketedTLDManifest,
					dnsBucketedTwoLDManifest: dnsBucketedTwoLDManifest,
					dnsHistogramManifest:     dnsHistogramManifest,
					dnsSourceFound:           true,
					dnsTLdManifest:           dnsTLDManifest,
					dnsTwoLDManifest:         dnsTwoLDManifest,
					histogramManifest:        histogramManifest,
					state:                    summarySourceStateStale,
					tldManifest:              tldManifest,
				}, nil
			}
		}
	}

	return summarySourceStatus{
		bucketedTLdManifest:      bucketedTLDManifest,
		bucketedTwoLDManifest:    bucketedTwoLDManifest,
		dnsBucketedTLdManifest:   dnsBucketedTLDManifest,
		dnsBucketedTwoLDManifest: dnsBucketedTwoLDManifest,
		dnsHistogramManifest:     dnsHistogramManifest,
		dnsSourceFound:           dnsSourceFound,
		dnsTLdManifest:           dnsTLDManifest,
		dnsTwoLDManifest:         dnsTwoLDManifest,
		histogramManifest:        histogramManifest,
		state:                    summarySourceStateFresh,
		tldManifest:              tldManifest,
	}, nil
}

func addSummaryPaths(snapshot *summarySnapshot, job summaryJob, status summarySourceStatus) {
	snapshot.edgePathsByGranulariy[GranularityTLD] = append(snapshot.edgePathsByGranulariy[GranularityTLD], job.paths.tldEdges)
	snapshot.edgePathsByGranulariy[Granularity2LD] = append(snapshot.edgePathsByGranulariy[Granularity2LD], job.paths.twoLDEdges)
	snapshot.bucketedEdgePathsByGranulariy[GranularityTLD] = append(snapshot.bucketedEdgePathsByGranulariy[GranularityTLD], job.paths.bucketedTldEdges)
	snapshot.bucketedEdgePathsByGranulariy[Granularity2LD] = append(snapshot.bucketedEdgePathsByGranulariy[Granularity2LD], job.paths.bucketedTwoLDEdges)
	snapshot.histogramPaths = append(snapshot.histogramPaths, job.paths.histogram)
	if status.dnsSourceFound {
		snapshot.dnsEdgePathsByGranulariy[GranularityTLD] = append(snapshot.dnsEdgePathsByGranulariy[GranularityTLD], job.paths.dnsTldEdges)
		snapshot.dnsEdgePathsByGranulariy[Granularity2LD] = append(snapshot.dnsEdgePathsByGranulariy[Granularity2LD], job.paths.dnsTwoLDEdges)
		snapshot.dnsBucketedEdgePathsByGranulariy[GranularityTLD] = append(snapshot.dnsBucketedEdgePathsByGranulariy[GranularityTLD], job.paths.dnsBucketedTldEdges)
		snapshot.dnsBucketedEdgePathsByGranulariy[Granularity2LD] = append(snapshot.dnsBucketedEdgePathsByGranulariy[Granularity2LD], job.paths.dnsBucketedTwoLDEdges)
		snapshot.dnsHistogramPaths = append(snapshot.dnsHistogramPaths, job.paths.dnsHistogram)
	}

	if status.histogramManifest.SpanStartNs > 0 && status.histogramManifest.SpanEndNs > 0 {
		if !snapshot.spanValid || status.histogramManifest.SpanStartNs < snapshot.span.StartNs {
			snapshot.span.StartNs = status.histogramManifest.SpanStartNs
		}
		if !snapshot.spanValid || status.histogramManifest.SpanEndNs > snapshot.span.EndNs {
			snapshot.span.EndNs = status.histogramManifest.SpanEndNs
		}
		snapshot.spanValid = true
	}
	if status.tldManifest.SpanStartNs > 0 && status.tldManifest.SpanEndNs > 0 && !snapshot.spanValid {
		snapshot.span = TimeSpan{StartNs: status.tldManifest.SpanStartNs, EndNs: status.tldManifest.SpanEndNs}
		snapshot.spanValid = true
	}
}

func summaryPathsForSource(srcRootPath string, sourceFile model.SourceFile) sourceSummaryPaths {
	label := sourceFile.Period.Label()
	return sourceSummaryPaths{
		bucketedTldEdges:      filepath.Join(srcRootPath, summaryFilenamePrefix+"bucketed_edges_tld_"+label+".parquet"),
		bucketedTwoLDEdges:    filepath.Join(srcRootPath, summaryFilenamePrefix+"bucketed_edges_2ld_"+label+".parquet"),
		dnsBucketedTldEdges:   filepath.Join(srcRootPath, summaryFilenamePrefix+"dns_bucketed_edges_tld_"+label+".parquet"),
		dnsBucketedTwoLDEdges: filepath.Join(srcRootPath, summaryFilenamePrefix+"dns_bucketed_edges_2ld_"+label+".parquet"),
		dnsHistogram:          filepath.Join(srcRootPath, summaryFilenamePrefix+"dns_histogram_"+label+".parquet"),
		dnsSource:             sourceFile.Period.DNSLookupOutputPath(srcRootPath),
		dnsTldEdges:           filepath.Join(srcRootPath, summaryFilenamePrefix+"dns_edges_tld_"+label+".parquet"),
		dnsTwoLDEdges:         filepath.Join(srcRootPath, summaryFilenamePrefix+"dns_edges_2ld_"+label+".parquet"),
		histogram:             filepath.Join(srcRootPath, summaryFilenamePrefix+"histogram_"+label+".parquet"),
		tldEdges:              filepath.Join(srcRootPath, summaryFilenamePrefix+"edges_tld_"+label+".parquet"),
		twoLDEdges:            filepath.Join(srcRootPath, summaryFilenamePrefix+"edges_2ld_"+label+".parquet"),
	}
}

func rebuildSummaryJobs(ctx context.Context, jobs []summaryJob) error {
	if len(jobs) == 0 {
		return nil
	}

	group, groupContext := errgroup.WithContext(ctx)
	group.SetLimit(summaryBuildConcurrency())
	for _, job := range jobs {
		job := job
		group.Go(func() error {
			startTime := time.Now()
			if err := rebuildSummaryJob(groupContext, job); err != nil {
				return fmt.Errorf("rebuild summary for %q: %w", job.sourceFile.RelPath, err)
			}
			slog.Debug("rebuilt UI summaries", "source", job.sourceFile.RelPath, "duration_ms", time.Since(startTime).Milliseconds())
			return nil
		})
	}
	return group.Wait()
}

func rebuildSummaryJob(ctx context.Context, job summaryJob) error {
	db, err := openSummaryBuildDB(ctx)
	if err != nil {
		return err
	}
	defer db.Close()

	spanStartNs, spanEndNs, err := querySummarySpan(ctx, db, job.sourceFile.AbsPath)
	if err != nil {
		return err
	}

	if err := rebuildEdgeSummary(ctx, db, job.sourceFile, job.paths.tldEdges, GranularityTLD, spanStartNs, spanEndNs); err != nil {
		return err
	}
	if err := rebuildEdgeSummary(ctx, db, job.sourceFile, job.paths.twoLDEdges, Granularity2LD, spanStartNs, spanEndNs); err != nil {
		return err
	}
	if err := rebuildBucketedEdgeSummary(ctx, db, job.sourceFile, job.paths.bucketedTldEdges, GranularityTLD, spanStartNs, spanEndNs); err != nil {
		return err
	}
	if err := rebuildBucketedEdgeSummary(ctx, db, job.sourceFile, job.paths.bucketedTwoLDEdges, Granularity2LD, spanStartNs, spanEndNs); err != nil {
		return err
	}
	if err := rebuildHistogramSummary(ctx, db, job.sourceFile, job.paths.histogram, spanStartNs, spanEndNs); err != nil {
		return err
	}

	dnsSourceFound, err := fileExists(job.paths.dnsSource)
	if err != nil {
		return err
	}
	if dnsSourceFound {
		dnsSpanStartNs, dnsSpanEndNs, err := queryDNSLookupSummarySpan(ctx, db, job.paths.dnsSource)
		if err != nil {
			return err
		}
		if err := rebuildDNSEdgeSummary(ctx, db, job.paths, job.paths.dnsTldEdges, GranularityTLD, dnsSpanStartNs, dnsSpanEndNs); err != nil {
			return err
		}
		if err := rebuildDNSEdgeSummary(ctx, db, job.paths, job.paths.dnsTwoLDEdges, Granularity2LD, dnsSpanStartNs, dnsSpanEndNs); err != nil {
			return err
		}
		if err := rebuildDNSBucketedEdgeSummary(ctx, db, job.paths, job.paths.dnsBucketedTldEdges, GranularityTLD, dnsSpanStartNs, dnsSpanEndNs); err != nil {
			return err
		}
		if err := rebuildDNSBucketedEdgeSummary(ctx, db, job.paths, job.paths.dnsBucketedTwoLDEdges, Granularity2LD, dnsSpanStartNs, dnsSpanEndNs); err != nil {
			return err
		}
		if err := rebuildDNSHistogramSummary(ctx, db, job.paths, job.paths.dnsHistogram, dnsSpanStartNs, dnsSpanEndNs); err != nil {
			return err
		}
	}

	return nil
}

func openSummaryBuildDB(ctx context.Context) (*sql.DB, error) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, fmt.Errorf("create duckdb connector: %w", err)
	}
	db := sql.OpenDB(connector)
	threadCount := summaryBuildThreadsPerJob()
	if _, err := db.ExecContext(ctx, fmt.Sprintf("SET threads TO %d", threadCount)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set DuckDB threads to %d: %w", threadCount, err)
	}
	return db, nil
}

func querySummarySpan(ctx context.Context, db *sql.DB, sourcePath string) (int64, int64, error) {
	query := fmt.Sprintf("SELECT COALESCE(MIN(time_start_ns), 0), COALESCE(MAX(time_end_ns), 0) FROM read_parquet(%s)", quoteLiteral(sourcePath))
	row := db.QueryRowContext(ctx, query)
	var startNs int64
	var endNs int64
	if err := row.Scan(&startNs, &endNs); err != nil {
		return 0, 0, fmt.Errorf("query summary span for %q: %w", sourcePath, err)
	}
	return startNs, endNs, nil
}

func queryDNSLookupSummarySpan(ctx context.Context, db *sql.DB, sourcePath string) (int64, int64, error) {
	query := fmt.Sprintf("SELECT COALESCE(MIN(time_start_ns), 0), COALESCE(MAX(time_start_ns), 0) FROM read_parquet(%s)", quoteLiteral(sourcePath))
	row := db.QueryRowContext(ctx, query)
	var startNs int64
	var endNs int64
	if err := row.Scan(&startNs, &endNs); err != nil {
		return 0, 0, fmt.Errorf("query DNS lookup summary span for %q: %w", sourcePath, err)
	}
	return startNs, endNs, nil
}

func rebuildEdgeSummary(
	ctx context.Context,
	db *sql.DB,
	sourceFile model.SourceFile,
	outputPath string,
	granularity Granularity,
	spanStartNs int64,
	spanEndNs int64,
) error {
	srcExpr, dstExpr := entityExpressions(granularity)
	query := fmt.Sprintf(`
SELECT %s AS src_entity, %s AS dst_entity,
  COALESCE(SUM(bytes), 0) AS bytes_total,
  COUNT(*) AS connection_total,
  direction,
  COALESCE(SUM(CASE WHEN src_is_private THEN bytes ELSE 0 END), 0) AS src_private_bytes,
  COALESCE(SUM(CASE WHEN src_is_private THEN 1 ELSE 0 END), 0) AS src_private_connections,
  COALESCE(SUM(CASE WHEN src_is_private THEN 0 ELSE bytes END), 0) AS src_public_bytes,
  COALESCE(SUM(CASE WHEN src_is_private THEN 0 ELSE 1 END), 0) AS src_public_connections,
  COALESCE(SUM(CASE WHEN dst_is_private THEN bytes ELSE 0 END), 0) AS dst_private_bytes,
  COALESCE(SUM(CASE WHEN dst_is_private THEN 1 ELSE 0 END), 0) AS dst_private_connections,
  COALESCE(SUM(CASE WHEN dst_is_private THEN 0 ELSE bytes END), 0) AS dst_public_bytes,
  COALESCE(SUM(CASE WHEN dst_is_private THEN 0 ELSE 1 END), 0) AS dst_public_connections,
  COALESCE(MIN(time_start_ns), 0) AS first_seen_ns,
  ip_version,
  COALESCE(MAX(time_end_ns), 0) AS last_seen_ns,
  protocol,
  %s AS service_port
FROM read_parquet(%s)
GROUP BY src_entity, dst_entity, direction, ip_version, protocol, service_port
ORDER BY src_entity, dst_entity, direction, ip_version, protocol, service_port
`, srcExpr, dstExpr, rawServicePortExpression(), quoteLiteral(sourceFile.AbsPath))

	writer, finalize, err := parquetout.CreateUISummaryEdges(outputPath, model.NewUISummaryManifest(sourceFile, summaryEdgeKind, string(granularity), spanStartNs, spanEndNs))
	if err != nil {
		return err
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query %s summary edges for %q: %w", granularity, sourceFile.RelPath, err)
	}
	defer rows.Close()

	batch := make([]parquetout.EdgeSummaryRow, 0, summaryBuildRowBatchSize)
	for rows.Next() {
		var row parquetout.EdgeSummaryRow
		var direction sql.NullInt32
		var servicePort sql.NullInt32
		if err := rows.Scan(
			&row.Source,
			&row.Destination,
			&row.Bytes,
			&row.Connections,
			&direction,
			&row.SrcPrivateBytes,
			&row.SrcPrivateConnections,
			&row.SrcPublicBytes,
			&row.SrcPublicConnections,
			&row.DstPrivateBytes,
			&row.DstPrivateConnections,
			&row.DstPublicBytes,
			&row.DstPublicConnections,
			&row.FirstSeenNs,
			&row.IPVersion,
			&row.LastSeenNs,
			&row.Protocol,
			&servicePort,
		); err != nil {
			return fmt.Errorf("scan %s summary edge row for %q: %w", granularity, sourceFile.RelPath, err)
		}
		row.Direction = directionValue(direction)
		row.ServicePort = nullableInt32Value(servicePort)
		batch = append(batch, row)
		if len(batch) < summaryBuildRowBatchSize {
			continue
		}
		if err := writer.WriteBatch(batch); err != nil {
			return err
		}
		batch = batch[:0]
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s summary edge rows for %q: %w", granularity, sourceFile.RelPath, err)
	}
	if len(batch) > 0 {
		if err := writer.WriteBatch(batch); err != nil {
			return err
		}
	}
	if err := finalize(); err != nil {
		return err
	}
	return nil
}

func rebuildBucketedEdgeSummary(
	ctx context.Context,
	db *sql.DB,
	sourceFile model.SourceFile,
	outputPath string,
	granularity Granularity,
	spanStartNs int64,
	spanEndNs int64,
) error {
	srcExpr, dstExpr := entityExpressions(granularity)
	query := fmt.Sprintf(`
SELECT CAST(FLOOR(time_start_ns / %d) AS BIGINT) * %d AS bucket_start_ns,
  %s AS src_entity, %s AS dst_entity,
  COALESCE(SUM(bytes), 0) AS bytes_total,
  COUNT(*) AS connection_total,
  direction,
  COALESCE(SUM(CASE WHEN src_is_private THEN bytes ELSE 0 END), 0) AS src_private_bytes,
  COALESCE(SUM(CASE WHEN src_is_private THEN 1 ELSE 0 END), 0) AS src_private_connections,
  COALESCE(SUM(CASE WHEN src_is_private THEN 0 ELSE bytes END), 0) AS src_public_bytes,
  COALESCE(SUM(CASE WHEN src_is_private THEN 0 ELSE 1 END), 0) AS src_public_connections,
  COALESCE(SUM(CASE WHEN dst_is_private THEN bytes ELSE 0 END), 0) AS dst_private_bytes,
  COALESCE(SUM(CASE WHEN dst_is_private THEN 1 ELSE 0 END), 0) AS dst_private_connections,
  COALESCE(SUM(CASE WHEN dst_is_private THEN 0 ELSE bytes END), 0) AS dst_public_bytes,
  COALESCE(SUM(CASE WHEN dst_is_private THEN 0 ELSE 1 END), 0) AS dst_public_connections,
  COALESCE(MIN(time_start_ns), 0) AS first_seen_ns,
  ip_version,
  COALESCE(MAX(time_end_ns), 0) AS last_seen_ns,
  protocol,
  %s AS service_port
FROM read_parquet(%s)
GROUP BY bucket_start_ns, src_entity, dst_entity, direction, ip_version, protocol, service_port
ORDER BY bucket_start_ns, src_entity, dst_entity, direction, ip_version, protocol, service_port
`, summaryBucketWidthNs, summaryBucketWidthNs, srcExpr, dstExpr, rawServicePortExpression(), quoteLiteral(sourceFile.AbsPath))

	writer, finalize, err := parquetout.CreateUISummaryBucketedEdges(outputPath, model.NewUISummaryManifest(sourceFile, summaryBucketedEdgeKind, string(granularity), spanStartNs, spanEndNs))
	if err != nil {
		return err
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query %s bucketed summary edges for %q: %w", granularity, sourceFile.RelPath, err)
	}
	defer rows.Close()

	batch := make([]parquetout.BucketedEdgeSummaryRow, 0, summaryBuildRowBatchSize)
	for rows.Next() {
		var row parquetout.BucketedEdgeSummaryRow
		var direction sql.NullInt32
		var servicePort sql.NullInt32
		if err := rows.Scan(
			&row.BucketStartNs,
			&row.Source,
			&row.Destination,
			&row.Bytes,
			&row.Connections,
			&direction,
			&row.SrcPrivateBytes,
			&row.SrcPrivateConnections,
			&row.SrcPublicBytes,
			&row.SrcPublicConnections,
			&row.DstPrivateBytes,
			&row.DstPrivateConnections,
			&row.DstPublicBytes,
			&row.DstPublicConnections,
			&row.FirstSeenNs,
			&row.IPVersion,
			&row.LastSeenNs,
			&row.Protocol,
			&servicePort,
		); err != nil {
			return fmt.Errorf("scan %s bucketed summary edge row for %q: %w", granularity, sourceFile.RelPath, err)
		}
		row.Direction = directionValue(direction)
		row.ServicePort = nullableInt32Value(servicePort)
		batch = append(batch, row)
		if len(batch) < summaryBuildRowBatchSize {
			continue
		}
		if err := writer.WriteBatch(batch); err != nil {
			return err
		}
		batch = batch[:0]
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s bucketed summary edge rows for %q: %w", granularity, sourceFile.RelPath, err)
	}
	if len(batch) > 0 {
		if err := writer.WriteBatch(batch); err != nil {
			return err
		}
	}
	if err := finalize(); err != nil {
		return err
	}
	return nil
}

func rebuildHistogramSummary(
	ctx context.Context,
	db *sql.DB,
	sourceFile model.SourceFile,
	outputPath string,
	spanStartNs int64,
	spanEndNs int64,
) error {
	query := fmt.Sprintf(`
SELECT CAST(FLOOR(time_start_ns / %d) AS BIGINT) * %d AS bucket_start_ns,
  COALESCE(SUM(bytes), 0) AS bytes_total,
  COUNT(*) AS connection_total,
  direction,
  ip_version,
  protocol,
  %s AS service_port
FROM read_parquet(%s)
GROUP BY bucket_start_ns, direction, ip_version, protocol, service_port
ORDER BY bucket_start_ns, direction, ip_version, protocol, service_port
`, summaryBucketWidthNs, summaryBucketWidthNs, rawServicePortExpression(), quoteLiteral(sourceFile.AbsPath))

	writer, finalize, err := parquetout.CreateUISummaryHistogram(outputPath, model.NewUISummaryManifest(sourceFile, summaryHistogramKind, "", spanStartNs, spanEndNs))
	if err != nil {
		return err
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query histogram summary for %q: %w", sourceFile.RelPath, err)
	}
	defer rows.Close()

	batch := make([]parquetout.HistogramSummaryRow, 0, summaryBuildRowBatchSize)
	for rows.Next() {
		var row parquetout.HistogramSummaryRow
		var direction sql.NullInt32
		var servicePort sql.NullInt32
		if err := rows.Scan(&row.BucketStartNs, &row.Bytes, &row.Connections, &direction, &row.IPVersion, &row.Protocol, &servicePort); err != nil {
			return fmt.Errorf("scan histogram summary row for %q: %w", sourceFile.RelPath, err)
		}
		row.Direction = directionValue(direction)
		row.ServicePort = nullableInt32Value(servicePort)
		batch = append(batch, row)
		if len(batch) < summaryBuildRowBatchSize {
			continue
		}
		if err := writer.WriteBatch(batch); err != nil {
			return err
		}
		batch = batch[:0]
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate histogram summary rows for %q: %w", sourceFile.RelPath, err)
	}
	if len(batch) > 0 {
		if err := writer.WriteBatch(batch); err != nil {
			return err
		}
	}
	if err := finalize(); err != nil {
		return err
	}
	return nil
}

func rebuildDNSEdgeSummary(
	ctx context.Context,
	db *sql.DB,
	paths sourceSummaryPaths,
	outputPath string,
	granularity Granularity,
	spanStartNs int64,
	spanEndNs int64,
) error {
	srcExpr, dstExpr := dnsLookupEntityExpressions(granularity)
	dnsSourceHasAnswer, err := parquetPathHasColumn(ctx, db, paths.dnsSource, "answer")
	if err != nil {
		return err
	}
	answerExpr := quoteLiteral("")
	if dnsSourceHasAnswer {
		answerExpr = dnsLookupAnswerExpression
	}
	query := fmt.Sprintf(`
SELECT %s AS src_entity, %s AS dst_entity,
  0 AS bytes_total,
  COALESCE(SUM(lookups), 0) AS lookup_total,
  COALESCE(SUM(CASE WHEN %s = %s THEN lookups ELSE 0 END), 0) AS nxdomain_lookup_total,
  COALESCE(SUM(CASE WHEN %s != '' AND %s != %s THEN lookups ELSE 0 END), 0) AS successful_lookup_total,
  COALESCE(SUM(CASE WHEN client_is_private THEN lookups ELSE 0 END), 0) AS src_private_connections,
  COALESCE(SUM(CASE WHEN client_is_private THEN 0 ELSE lookups END), 0) AS src_public_connections,
  0 AS dst_private_connections,
  COALESCE(SUM(lookups), 0) AS dst_public_connections,
  COALESCE(MIN(time_start_ns), 0) AS first_seen_ns,
  client_ip_version,
  COALESCE(MAX(time_start_ns), 0) AS last_seen_ns
FROM read_parquet(%s)
GROUP BY src_entity, dst_entity, client_ip_version
ORDER BY src_entity, dst_entity, client_ip_version
`, srcExpr, dstExpr, answerExpr, quoteLiteral(model.DNSAnswerNXDOMAIN), answerExpr, answerExpr, quoteLiteral(model.DNSAnswerNXDOMAIN), quoteLiteral(paths.dnsSource))

	sourceManifest, err := dnsSummarySourceManifest(paths.dnsSource)
	if err != nil {
		return err
	}
	manifest := model.UISummaryManifest{
		Granularity:  string(granularity),
		Kind:         summaryEdgeKind,
		LogicVersion: model.UISummaryLogicVersion,
		Source:       sourceManifest,
		SpanEndNs:    spanEndNs,
		SpanStartNs:  spanStartNs,
		Version:      model.UISummaryManifestVersion,
	}
	writer, finalize, err := parquetout.CreateUISummaryEdges(outputPath, manifest)
	if err != nil {
		return err
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query %s DNS summary edges for %q: %w", granularity, paths.dnsSource, err)
	}
	defer rows.Close()

	batch := make([]parquetout.EdgeSummaryRow, 0, summaryBuildRowBatchSize)
	for rows.Next() {
		var row parquetout.EdgeSummaryRow
		if err := rows.Scan(
			&row.Source,
			&row.Destination,
			&row.Bytes,
			&row.Connections,
			&row.NXDomainLookups,
			&row.SuccessfulLookups,
			&row.SrcPrivateConnections,
			&row.SrcPublicConnections,
			&row.DstPrivateConnections,
			&row.DstPublicConnections,
			&row.FirstSeenNs,
			&row.IPVersion,
			&row.LastSeenNs,
		); err != nil {
			return fmt.Errorf("scan %s DNS summary edge row for %q: %w", granularity, paths.dnsSource, err)
		}
		batch = append(batch, row)
		if len(batch) < summaryBuildRowBatchSize {
			continue
		}
		if err := writer.WriteBatch(batch); err != nil {
			return err
		}
		batch = batch[:0]
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s DNS summary edge rows for %q: %w", granularity, paths.dnsSource, err)
	}
	if len(batch) > 0 {
		if err := writer.WriteBatch(batch); err != nil {
			return err
		}
	}
	if err := finalize(); err != nil {
		return err
	}
	return nil
}

func rebuildDNSBucketedEdgeSummary(
	ctx context.Context,
	db *sql.DB,
	paths sourceSummaryPaths,
	outputPath string,
	granularity Granularity,
	spanStartNs int64,
	spanEndNs int64,
) error {
	srcExpr, dstExpr := dnsLookupEntityExpressions(granularity)
	dnsSourceHasAnswer, err := parquetPathHasColumn(ctx, db, paths.dnsSource, "answer")
	if err != nil {
		return err
	}
	answerExpr := quoteLiteral("")
	if dnsSourceHasAnswer {
		answerExpr = dnsLookupAnswerExpression
	}
	query := fmt.Sprintf(`
SELECT CAST(FLOOR(time_start_ns / %d) AS BIGINT) * %d AS bucket_start_ns,
  %s AS src_entity, %s AS dst_entity,
  0 AS bytes_total,
  COALESCE(SUM(lookups), 0) AS lookup_total,
  COALESCE(SUM(CASE WHEN %s = %s THEN lookups ELSE 0 END), 0) AS nxdomain_lookup_total,
  COALESCE(SUM(CASE WHEN %s != '' AND %s != %s THEN lookups ELSE 0 END), 0) AS successful_lookup_total,
  COALESCE(SUM(CASE WHEN client_is_private THEN lookups ELSE 0 END), 0) AS src_private_connections,
  COALESCE(SUM(CASE WHEN client_is_private THEN 0 ELSE lookups END), 0) AS src_public_connections,
  0 AS dst_private_connections,
  COALESCE(SUM(lookups), 0) AS dst_public_connections,
  COALESCE(MIN(time_start_ns), 0) AS first_seen_ns,
  client_ip_version,
  COALESCE(MAX(time_start_ns), 0) AS last_seen_ns
FROM read_parquet(%s)
GROUP BY bucket_start_ns, src_entity, dst_entity, client_ip_version
ORDER BY bucket_start_ns, src_entity, dst_entity, client_ip_version
`, summaryBucketWidthNs, summaryBucketWidthNs, srcExpr, dstExpr, answerExpr, quoteLiteral(model.DNSAnswerNXDOMAIN), answerExpr, answerExpr, quoteLiteral(model.DNSAnswerNXDOMAIN), quoteLiteral(paths.dnsSource))

	sourceManifest, err := dnsSummarySourceManifest(paths.dnsSource)
	if err != nil {
		return err
	}
	manifest := model.UISummaryManifest{
		Granularity:  string(granularity),
		Kind:         summaryBucketedEdgeKind,
		LogicVersion: model.UISummaryLogicVersion,
		Source:       sourceManifest,
		SpanEndNs:    spanEndNs,
		SpanStartNs:  spanStartNs,
		Version:      model.UISummaryManifestVersion,
	}
	writer, finalize, err := parquetout.CreateUISummaryBucketedEdges(outputPath, manifest)
	if err != nil {
		return err
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query %s DNS bucketed summary edges for %q: %w", granularity, paths.dnsSource, err)
	}
	defer rows.Close()

	batch := make([]parquetout.BucketedEdgeSummaryRow, 0, summaryBuildRowBatchSize)
	for rows.Next() {
		var row parquetout.BucketedEdgeSummaryRow
		if err := rows.Scan(
			&row.BucketStartNs,
			&row.Source,
			&row.Destination,
			&row.Bytes,
			&row.Connections,
			&row.NXDomainLookups,
			&row.SuccessfulLookups,
			&row.SrcPrivateConnections,
			&row.SrcPublicConnections,
			&row.DstPrivateConnections,
			&row.DstPublicConnections,
			&row.FirstSeenNs,
			&row.IPVersion,
			&row.LastSeenNs,
		); err != nil {
			return fmt.Errorf("scan %s DNS bucketed summary edge row for %q: %w", granularity, paths.dnsSource, err)
		}
		batch = append(batch, row)
		if len(batch) < summaryBuildRowBatchSize {
			continue
		}
		if err := writer.WriteBatch(batch); err != nil {
			return err
		}
		batch = batch[:0]
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s DNS bucketed summary edges for %q: %w", granularity, paths.dnsSource, err)
	}
	if len(batch) > 0 {
		if err := writer.WriteBatch(batch); err != nil {
			return err
		}
	}
	if err := finalize(); err != nil {
		return err
	}
	return nil
}

func rebuildDNSHistogramSummary(
	ctx context.Context,
	db *sql.DB,
	paths sourceSummaryPaths,
	outputPath string,
	spanStartNs int64,
	spanEndNs int64,
) error {
	query := fmt.Sprintf(`
SELECT CAST(FLOOR(time_start_ns / %d) AS BIGINT) * %d AS bucket_start_ns,
  0 AS bytes_total,
  COALESCE(SUM(lookups), 0) AS lookup_total,
  client_ip_version
FROM read_parquet(%s)
GROUP BY bucket_start_ns, client_ip_version
ORDER BY bucket_start_ns, client_ip_version
`, summaryBucketWidthNs, summaryBucketWidthNs, quoteLiteral(paths.dnsSource))

	sourceManifest, err := dnsSummarySourceManifest(paths.dnsSource)
	if err != nil {
		return err
	}
	manifest := model.UISummaryManifest{
		Kind:         summaryHistogramKind,
		LogicVersion: model.UISummaryLogicVersion,
		Source:       sourceManifest,
		SpanEndNs:    spanEndNs,
		SpanStartNs:  spanStartNs,
		Version:      model.UISummaryManifestVersion,
	}
	writer, finalize, err := parquetout.CreateUISummaryHistogram(outputPath, manifest)
	if err != nil {
		return err
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query DNS histogram summary for %q: %w", paths.dnsSource, err)
	}
	defer rows.Close()

	batch := make([]parquetout.HistogramSummaryRow, 0, summaryBuildRowBatchSize)
	for rows.Next() {
		var row parquetout.HistogramSummaryRow
		if err := rows.Scan(&row.BucketStartNs, &row.Bytes, &row.Connections, &row.IPVersion); err != nil {
			return fmt.Errorf("scan DNS histogram summary row for %q: %w", paths.dnsSource, err)
		}
		batch = append(batch, row)
		if len(batch) < summaryBuildRowBatchSize {
			continue
		}
		if err := writer.WriteBatch(batch); err != nil {
			return err
		}
		batch = batch[:0]
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate DNS histogram summary rows for %q: %w", paths.dnsSource, err)
	}
	if len(batch) > 0 {
		if err := writer.WriteBatch(batch); err != nil {
			return err
		}
	}
	if err := finalize(); err != nil {
		return err
	}
	return nil
}

func cleanupStaleSummaryFiles(srcRootPath string, expectedPaths map[string]struct{}) error {
	entries, err := os.ReadDir(srcRootPath)
	if err != nil {
		return fmt.Errorf("read dir %q: %w", srcRootPath, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), summaryFilenamePrefix) || !strings.HasSuffix(entry.Name(), ".parquet") {
			continue
		}
		path := filepath.Join(srcRootPath, entry.Name())
		if _, ok := expectedPaths[path]; ok {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale summary %q: %w", path, err)
		}
	}
	return nil
}

func sortedSourceFiles(sourceFilesByPeriod map[model.Period]model.SourceFile) []model.SourceFile {
	files := make([]model.SourceFile, 0, len(sourceFilesByPeriod))
	for _, sourceFile := range sourceFilesByPeriod {
		files = append(files, sourceFile)
	}
	slices.SortFunc(files, func(a, b model.SourceFile) int {
		if a.Period.Start.Equal(b.Period.Start) {
			return strings.Compare(a.Period.Kind, b.Period.Kind)
		}
		if a.Period.Start.Before(b.Period.Start) {
			return -1
		}
		return 1
	})
	return files
}

func summaryBuildConcurrency() int {
	return min(summaryBuildJobLimit, max(1, runtime.GOMAXPROCS(0)))
}

func summaryBuildThreadsPerJob() int {
	return max(1, runtime.GOMAXPROCS(0)/summaryBuildConcurrency())
}

func readUISummaryManifestIfPresent(path string) (model.UISummaryManifest, bool, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return model.UISummaryManifest{}, false, nil
		}
		return model.UISummaryManifest{}, false, fmt.Errorf("stat %q: %w", path, err)
	}

	manifest, manifestFound := func() (model.UISummaryManifest, bool) {
		manifest, err := parquetout.ReadUISummaryManifest(path)
		if err != nil {
			return model.UISummaryManifest{}, false
		}
		return manifest, true
	}()
	if !manifestFound {
		return model.UISummaryManifest{}, false, nil
	}
	return manifest, true, nil
}

func directionValue(value sql.NullInt32) *int32 {
	if !value.Valid {
		return nil
	}
	direction := value.Int32
	return &direction
}

func nullableInt32Value(value sql.NullInt32) *int32 {
	if !value.Valid {
		return nil
	}
	ret := value.Int32
	return &ret
}

func parquetPathHasColumn(ctx context.Context, db *sql.DB, path, columnName string) (bool, error) {
	query := fmt.Sprintf("SELECT * FROM read_parquet(%s) LIMIT 0", quoteLiteral(path))
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return false, fmt.Errorf("query parquet schema for %q: %w", path, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return false, fmt.Errorf("read parquet schema columns for %q: %w", path, err)
	}
	for _, column := range columns {
		if column == columnName {
			return true, nil
		}
	}
	return false, nil
}

func fileExists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat %q: %w", path, err)
	}
	return true, nil
}

func summarySourceManifest(sourceFile model.SourceFile) model.SourceManifest {
	return model.SourceManifest{
		Path:      sourceFile.RelPath,
		SizeByte:  sourceFile.SizeByte,
		ModTimeNs: sourceFile.ModTime.UnixNano(),
	}
}

func dnsSummarySourceManifest(path string) (model.SourceManifest, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return model.SourceManifest{}, fmt.Errorf("stat %q: %w", path, err)
	}
	return model.SourceManifest{
		Path:      filepath.Base(path),
		SizeByte:  fileInfo.Size(),
		ModTimeNs: fileInfo.ModTime().UTC().UnixNano(),
	}, nil
}
