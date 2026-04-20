package enrich

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fingon/homenetflow/internal/model"
	"github.com/fingon/homenetflow/internal/parquetout"
	"github.com/fingon/homenetflow/internal/refresh"
	"github.com/fingon/homenetflow/internal/scan"
	"golang.org/x/sync/errgroup"
)

const (
	localIPv6Host           = "Local IPv6"
	reverseDNSCacheFilename = "reverse_dns_cache.jsonl"
)

type Config struct {
	DstPath        string
	Progress       func(doneRowCount, totalRowCount int64)
	SkipDNSLookups bool
	SrcLogPath     string
	SrcParquetPath string
}

type periodJob struct {
	dstPath    string
	dnsPath    string
	logFiles   []model.SourceFile
	sourceFile model.SourceFile
}

func Run(config Config) error {
	if config.SrcParquetPath == "" {
		return errors.New("source parquet path is required")
	}

	if config.SrcLogPath == "" {
		return errors.New("source log path is required")
	}

	if config.DstPath == "" {
		return errors.New("destination path is required")
	}

	if err := os.MkdirAll(config.DstPath, 0o755); err != nil {
		return fmt.Errorf("mkdir %q: %w", config.DstPath, err)
	}

	sourceFilesByPeriod, err := scan.FlatParquetTree(config.SrcParquetPath)
	if err != nil {
		return err
	}

	logFiles, err := scan.LogTree(config.SrcLogPath)
	if err != nil {
		return err
	}

	cache, err := loadReverseDNSCache(filepath.Join(config.DstPath, reverseDNSCacheFilename))
	if err != nil {
		return err
	}

	logLoader := newDNSLogLoader()
	jobs, err := staleJobs(config.DstPath, sourceFilesByPeriod, logFiles, config.SkipDNSLookups)
	if err != nil {
		return err
	}

	if len(jobs) == 0 {
		if err := cleanupDeletedOutputs(config.DstPath, sourceFilesByPeriod); err != nil {
			return err
		}

		return nil
	}

	neighbourIndex, err := loadNeighbourIndex(logFiles)
	if err != nil {
		return err
	}

	if err := rebuildJobs(jobs, logLoader, neighbourIndex, cache, config.Progress, config.SkipDNSLookups); err != nil {
		return err
	}

	if err := cleanupDeletedOutputs(config.DstPath, sourceFilesByPeriod); err != nil {
		return err
	}

	return nil
}

func staleJobs(
	dstRootPath string,
	sourceFilesByPeriod map[model.Period]model.SourceFile,
	logFiles []model.SourceFile,
	skipDNSLookups bool,
) ([]periodJob, error) {
	periods := make([]model.Period, 0, len(sourceFilesByPeriod))
	for period := range sourceFilesByPeriod {
		periods = append(periods, period)
	}

	slices.SortFunc(periods, func(a, b model.Period) int {
		if a.Start.Equal(b.Start) {
			switch {
			case a.Kind < b.Kind:
				return -1
			case a.Kind > b.Kind:
				return 1
			default:
				return 0
			}
		}

		if a.Start.Before(b.Start) {
			return -1
		}

		return 1
	})

	jobs := make([]periodJob, 0, len(periods))
	for _, period := range periods {
		sourceFile := sourceFilesByPeriod[period]
		relevantLogFiles := relevantLogFiles(period, logFiles)
		dstPath := period.OutputPath(dstRootPath)

		rebuild, err := refresh.NeedsEnrichmentRebuild(dstPath, sourceFile, relevantLogFiles, skipDNSLookups, parquetout.ReadEnrichmentManifest)
		if err != nil {
			return nil, fmt.Errorf("check rebuild for %q: %w", dstPath, err)
		}
		dnsPath := period.DNSLookupOutputPath(dstRootPath)
		if !rebuild {
			rebuild, err = refresh.NeedsEnrichmentRebuild(dnsPath, sourceFile, relevantLogFiles, skipDNSLookups, parquetout.ReadDNSLookupManifest)
			if err != nil {
				return nil, fmt.Errorf("check DNS lookup rebuild for %q: %w", dnsPath, err)
			}
		}

		if !rebuild {
			slog.Debug("enriched parquet already up to date", "path", dstPath)
			continue
		}

		jobs = append(jobs, periodJob{
			dstPath:    dstPath,
			dnsPath:    dnsPath,
			logFiles:   relevantLogFiles,
			sourceFile: sourceFile,
		})
	}

	return jobs, nil
}

func rebuildJobs(
	jobs []periodJob,
	logLoader *dnsLogLoader,
	neighbourIndex *neighbourIndex,
	cache *reverseDNSCache,
	progress func(doneRowCount, totalRowCount int64),
	skipDNSLookups bool,
) error {
	reportProgress, err := newProgressReporter(jobs, progress)
	if err != nil {
		return err
	}

	group, ctx := errgroup.WithContext(context.Background())
	group.SetLimit(max(1, runtime.GOMAXPROCS(0)))

	for _, job := range jobs {
		job := job
		group.Go(func() error {
			if err := rebuildJob(ctx, job, logLoader, neighbourIndex, cache, reportProgress, skipDNSLookups); err != nil {
				return fmt.Errorf("rebuild %q: %w", job.dstPath, err)
			}

			slog.Info("rebuilt enriched parquet", "path", job.dstPath, "logs", len(job.logFiles), "source", job.sourceFile.RelPath)
			return nil
		})
	}

	return group.Wait()
}

func newProgressReporter(
	jobs []periodJob,
	progress func(doneRowCount, totalRowCount int64),
) (func(deltaRowCount int64), error) {
	if progress == nil || len(jobs) == 0 {
		return nil, nil
	}

	var totalRowCount int64
	for _, job := range jobs {
		rowCount, err := parquetout.RowCount(job.sourceFile.AbsPath)
		if err != nil {
			return nil, fmt.Errorf("read row count for %q: %w", job.sourceFile.AbsPath, err)
		}
		totalRowCount += rowCount
	}

	if totalRowCount == 0 {
		return nil, nil
	}

	progress(0, totalRowCount)
	var doneRowCount atomic.Int64
	var progressMu sync.Mutex

	return func(deltaRowCount int64) {
		if deltaRowCount <= 0 {
			return
		}

		progressMu.Lock()
		defer progressMu.Unlock()

		progress(doneRowCount.Add(deltaRowCount), totalRowCount)
	}, nil
}

func rebuildJob(
	ctx context.Context,
	job periodJob,
	logLoader *dnsLogLoader,
	neighbourIndex *neighbourIndex,
	cache *reverseDNSCache,
	reportProgress func(deltaRowCount int64),
	skipDNSLookups bool,
) error {
	logIndex, err := logLoader.Load(job.logFiles)
	if err != nil {
		return err
	}

	writer, finalize, err := parquetout.CreateEnriched(job.dstPath, model.NewEnrichmentManifest(job.sourceFile, job.logFiles, skipDNSLookups))
	if err != nil {
		return err
	}

	if err := parquetout.ReadFileBatches(job.sourceFile.AbsPath, func(records []model.FlowRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		enrichedRecords := make([]model.FlowRecord, 0, len(records))
		for _, record := range records {
			enrichedRecord, enrichErr := enrichRecord(record, logIndex, neighbourIndex, cache, skipDNSLookups)
			if enrichErr != nil {
				return enrichErr
			}

			enrichedRecords = append(enrichedRecords, enrichedRecord)
		}

		if err := writer.WriteBatch(enrichedRecords); err != nil {
			return fmt.Errorf("write enriched rows for %q: %w", job.sourceFile.AbsPath, err)
		}

		if reportProgress != nil {
			reportProgress(int64(len(enrichedRecords)))
		}

		return nil
	}); err != nil {
		return err
	}

	if err := writeDNSLookupParquet(ctx, job, logIndex, neighbourIndex, cache, skipDNSLookups); err != nil {
		return err
	}

	if err := finalize(); err != nil {
		return err
	}

	return nil
}

func writeDNSLookupParquet(
	ctx context.Context,
	job periodJob,
	logIndex *dnsIndex,
	neighbourIndex *neighbourIndex,
	cache *reverseDNSCache,
	skipDNSLookups bool,
) error {
	writer, finalize, err := parquetout.CreateDNSLookups(job.dnsPath, model.NewEnrichmentManifest(job.sourceFile, job.logFiles, skipDNSLookups))
	if err != nil {
		return err
	}

	periodStart := job.sourceFile.Period.Start
	periodEnd := job.sourceFile.Period.End()
	batch := make([]model.DNSLookupRecord, 0, 1024)
	for _, event := range logIndex.lookupEvents {
		if err := ctx.Err(); err != nil {
			return err
		}
		if event.time.Before(periodStart) || !event.time.Before(periodEnd) {
			continue
		}

		record, err := dnsLookupRecordForEvent(event, logIndex, neighbourIndex, cache, skipDNSLookups)
		if err != nil {
			return err
		}
		batch = append(batch, record)
		if len(batch) < 1024 {
			continue
		}
		if err := writer.WriteBatch(batch); err != nil {
			return err
		}
		batch = batch[:0]
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

func dnsLookupRecordForEvent(
	event dnsLookupEvent,
	logIndex *dnsIndex,
	neighbourIndex *neighbourIndex,
	cache *reverseDNSCache,
	skipDNSLookups bool,
) (model.DNSLookupRecord, error) {
	clientNames, err := resolveNames(event.clientIP, event.time, logIndex, neighbourIndex, cache, skipDNSLookups)
	if err != nil {
		return model.DNSLookupRecord{}, err
	}

	queryNames := deriveNames(event.queryName)
	record := model.DNSLookupRecord{
		Answer:          event.answer,
		ClientIP:        event.clientIP,
		ClientIPVersion: ipVersionForAddress(event.clientIP),
		ClientIsPrivate: isLocalIPAddress(event.clientIP, neighbourIndex),
		Lookups:         1,
		Query2LD:        queryNames.two,
		QueryName:       event.queryName,
		QueryTLD:        queryNames.tld,
		QueryType:       event.queryType,
		TimeStartNs:     event.time.UnixNano(),
	}
	if clientNames != nil {
		record.ClientHost = &clientNames.host
		record.Client2LD = clientNames.two
		record.ClientTLD = clientNames.tld
	}
	return record, nil
}

func enrichRecord(
	record model.FlowRecord,
	logIndex *dnsIndex,
	neighbourIndex *neighbourIndex,
	cache *reverseDNSCache,
	skipDNSLookups bool,
) (model.FlowRecord, error) {
	flowStart := time.Unix(0, record.TimeStartNs).UTC()
	record.SrcIsPrivate = isLocalIPAddress(record.SrcIP, neighbourIndex)
	record.DstIsPrivate = isLocalIPAddress(record.DstIP, neighbourIndex)

	srcNames, err := resolveNames(record.SrcIP, flowStart, logIndex, neighbourIndex, cache, skipDNSLookups)
	if err != nil {
		return model.FlowRecord{}, err
	}

	dstNames, err := resolveNames(record.DstIP, flowStart, logIndex, neighbourIndex, cache, skipDNSLookups)
	if err != nil {
		return model.FlowRecord{}, err
	}

	record.SrcHost = nil
	record.Src2LD = nil
	record.SrcTLD = nil
	if srcNames != nil {
		record.SrcHost = &srcNames.host
		record.Src2LD = srcNames.two
		record.SrcTLD = srcNames.tld
	}

	record.DstHost = nil
	record.Dst2LD = nil
	record.DstTLD = nil
	if dstNames != nil {
		record.DstHost = &dstNames.host
		record.Dst2LD = dstNames.two
		record.DstTLD = dstNames.tld
	}

	return record, nil
}

func resolveNames(
	ipAddress string,
	flowStart time.Time,
	logIndex *dnsIndex,
	neighbourIndex *neighbourIndex,
	cache *reverseDNSCache,
	skipDNSLookups bool,
) (*derivedNames, error) {
	if neighbourIndex.ContainsIPv6LocalPrefix(ipAddress) {
		if mappedIPv4, ok := neighbourIndex.LookupIPv4(ipAddress, flowStart); ok && isPrivateIPAddress(mappedIPv4) {
			if names := logIndex.Lookup(mappedIPv4, flowStart); names != nil {
				return names, nil
			}
		}

		return &derivedNames{host: localIPv6Host}, nil
	}

	if mappedIPv4, ok := neighbourIndex.LookupIPv4(ipAddress, flowStart); ok {
		names, err := resolveNamesForIP(mappedIPv4, flowStart, logIndex, cache, skipDNSLookups)
		if err != nil {
			return nil, err
		}

		if names != nil {
			return names, nil
		}
	}

	return resolveNamesForIP(ipAddress, flowStart, logIndex, cache, skipDNSLookups)
}

func resolveNamesForIP(
	ipAddress string,
	flowStart time.Time,
	logIndex *dnsIndex,
	cache *reverseDNSCache,
	skipDNSLookups bool,
) (*derivedNames, error) {
	if names := logIndex.Lookup(ipAddress, flowStart); names != nil {
		return names, nil
	}

	result, err := cache.Lookup(ipAddress, skipDNSLookups)
	if err != nil {
		return nil, err
	}

	if result.warning != nil {
		slog.Debug("skipping malformed PTR response", "ip", ipAddress, "error", result.warning)
	}

	return result.names, nil
}

func relevantLogFiles(period model.Period, logFiles []model.SourceFile) []model.SourceFile {
	windowStart := period.Start.Add(-logWindowDuration)
	windowEnd := period.End()

	relevantFiles := make([]model.SourceFile, 0, len(logFiles))
	for _, logFile := range logFiles {
		logStart := logFile.Period.Start
		logEnd := logFile.Period.End()
		if logEnd.After(windowStart) && logStart.Before(windowEnd) {
			relevantFiles = append(relevantFiles, logFile)
		}
	}

	return relevantFiles
}

func cleanupDeletedOutputs(dstRootPath string, sourceFilesByPeriod map[model.Period]model.SourceFile) error {
	sourceNames := make(map[string]struct{}, len(sourceFilesByPeriod))
	for _, sourceFile := range sourceFilesByPeriod {
		sourceNames[sourceFile.Period.Filename()] = struct{}{}
	}

	dstPaths, err := scan.SortedFlatParquetPaths(dstRootPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	for _, dstPath := range dstPaths {
		fileName := filepath.Base(dstPath)
		if _, ok := sourceNames[fileName]; ok {
			continue
		}

		if !strings.HasPrefix(fileName, "nfcap_") {
			continue
		}

		if err := os.Remove(dstPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale enriched parquet %q: %w", dstPath, err)
		}
	}

	for _, sourceFile := range sourceFilesByPeriod {
		sourceNames[sourceFile.Period.DNSLookupFilename()] = struct{}{}
	}
	entries, err := os.ReadDir(dstRootPath)
	if err != nil {
		return fmt.Errorf("read dir %q: %w", dstRootPath, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "dns_lookups_") || !strings.HasSuffix(entry.Name(), ".parquet") {
			continue
		}
		if _, ok := sourceNames[entry.Name()]; ok {
			continue
		}
		path := filepath.Join(dstRootPath, entry.Name())
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale DNS lookup parquet %q: %w", path, err)
		}
	}

	return nil
}
