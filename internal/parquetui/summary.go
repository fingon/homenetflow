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
	summaryEdgeKind          = "edges"
	summaryFilenamePrefix    = "ui_summary_"
	summaryHistogramKind     = "histogram"
	summaryManifestVersion   = 1
	unknownEntityLabel       = "Unknown"
)

type summarySnapshot struct {
	edgeGlobByGranularity map[Granularity]string
	edgePathsByGranulariy map[Granularity][]string
	histogramGlob         string
	histogramPaths        []string
	span                  TimeSpan
	spanValid             bool
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
	histogramManifest model.UISummaryManifest
	state             summarySourceState
	tldManifest       model.UISummaryManifest
}

type sourceSummaryPaths struct {
	histogram  string
	tldEdges   string
	twoLDEdges string
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
		for _, path := range []string{job.paths.tldEdges, job.paths.twoLDEdges, job.paths.histogram} {
			expectedPaths[path] = struct{}{}
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
	histogramManifest, histogramFound, err := readUISummaryManifestIfPresent(job.paths.histogram)
	if err != nil {
		return summarySourceStatus{}, err
	}
	if !tldFound || !twoLDFound || !histogramFound {
		return summarySourceStatus{state: summarySourceStateMissing}, nil
	}

	expectedSource := summarySourceManifest(job.sourceFile)
	manifests := []model.UISummaryManifest{tldManifest, twoLDManifest, histogramManifest}
	expectedKinds := []string{summaryEdgeKind, summaryEdgeKind, summaryHistogramKind}
	expectedGranularities := []string{string(GranularityTLD), string(Granularity2LD), ""}
	for index, manifest := range manifests {
		if manifest.Version != summaryManifestVersion || manifest.Kind != expectedKinds[index] || manifest.Granularity != expectedGranularities[index] || manifest.Source != expectedSource {
			return summarySourceStatus{
				histogramManifest: histogramManifest,
				state:             summarySourceStateStale,
				tldManifest:       tldManifest,
			}, nil
		}
	}

	return summarySourceStatus{
		histogramManifest: histogramManifest,
		state:             summarySourceStateFresh,
		tldManifest:       tldManifest,
	}, nil
}

func addSummaryPaths(snapshot *summarySnapshot, job summaryJob, status summarySourceStatus) {
	snapshot.edgePathsByGranulariy[GranularityTLD] = append(snapshot.edgePathsByGranulariy[GranularityTLD], job.paths.tldEdges)
	snapshot.edgePathsByGranulariy[Granularity2LD] = append(snapshot.edgePathsByGranulariy[Granularity2LD], job.paths.twoLDEdges)
	snapshot.histogramPaths = append(snapshot.histogramPaths, job.paths.histogram)

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
		histogram:  filepath.Join(srcRootPath, summaryFilenamePrefix+"histogram_"+label+".parquet"),
		tldEdges:   filepath.Join(srcRootPath, summaryFilenamePrefix+"edges_tld_"+label+".parquet"),
		twoLDEdges: filepath.Join(srcRootPath, summaryFilenamePrefix+"edges_2ld_"+label+".parquet"),
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
	if err := rebuildHistogramSummary(ctx, db, job.sourceFile, job.paths.histogram, spanStartNs, spanEndNs); err != nil {
		return err
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
  COALESCE(MIN(time_start_ns), 0) AS first_seen_ns,
  COALESCE(MAX(time_end_ns), 0) AS last_seen_ns
FROM read_parquet(%s)
GROUP BY src_entity, dst_entity
ORDER BY src_entity, dst_entity
`, srcExpr, dstExpr, quoteLiteral(sourceFile.AbsPath))

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
		if err := rows.Scan(&row.Source, &row.Destination, &row.Bytes, &row.Connections, &row.FirstSeenNs, &row.LastSeenNs); err != nil {
			return fmt.Errorf("scan %s summary edge row for %q: %w", granularity, sourceFile.RelPath, err)
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
  COUNT(*) AS connection_total
FROM read_parquet(%s)
GROUP BY bucket_start_ns
ORDER BY bucket_start_ns
`, summaryBucketWidthNs, summaryBucketWidthNs, quoteLiteral(sourceFile.AbsPath))

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
		if err := rows.Scan(&row.BucketStartNs, &row.Bytes, &row.Connections); err != nil {
			return fmt.Errorf("scan histogram summary row for %q: %w", sourceFile.RelPath, err)
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

func summarySourceManifest(sourceFile model.SourceFile) model.SourceManifest {
	return model.SourceManifest{
		Path:      sourceFile.RelPath,
		SizeByte:  sourceFile.SizeByte,
		ModTimeNs: sourceFile.ModTime.UnixNano(),
	}
}
