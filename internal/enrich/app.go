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
	"time"

	"github.com/fingon/homenetflow/internal/model"
	"github.com/fingon/homenetflow/internal/parquetout"
	"github.com/fingon/homenetflow/internal/refresh"
	"github.com/fingon/homenetflow/internal/scan"
	"golang.org/x/sync/errgroup"
)

const reverseDNSCacheFilename = "reverse_dns_cache.jsonl"

type Config struct {
	DstPath        string
	SrcLogPath     string
	SrcParquetPath string
}

type periodJob struct {
	dstPath    string
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
	jobs, err := staleJobs(config.DstPath, sourceFilesByPeriod, logFiles)
	if err != nil {
		return err
	}

	if err := rebuildJobs(jobs, logLoader, cache); err != nil {
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

		rebuild, err := refresh.NeedsEnrichmentRebuild(dstPath, sourceFile, relevantLogFiles, parquetout.ReadEnrichmentManifest)
		if err != nil {
			return nil, fmt.Errorf("check rebuild for %q: %w", dstPath, err)
		}

		if !rebuild {
			slog.Debug("enriched parquet already up to date", "path", dstPath)
			continue
		}

		jobs = append(jobs, periodJob{
			dstPath:    dstPath,
			logFiles:   relevantLogFiles,
			sourceFile: sourceFile,
		})
	}

	return jobs, nil
}

func rebuildJobs(jobs []periodJob, logLoader *dnsLogLoader, cache *reverseDNSCache) error {
	group, ctx := errgroup.WithContext(context.Background())
	group.SetLimit(max(1, runtime.GOMAXPROCS(0)))

	for _, job := range jobs {
		job := job
		group.Go(func() error {
			if err := rebuildJob(ctx, job, logLoader, cache); err != nil {
				return fmt.Errorf("rebuild %q: %w", job.dstPath, err)
			}

			slog.Info("rebuilt enriched parquet", "path", job.dstPath, "logs", len(job.logFiles), "source", job.sourceFile.RelPath)
			return nil
		})
	}

	return group.Wait()
}

func rebuildJob(ctx context.Context, job periodJob, logLoader *dnsLogLoader, cache *reverseDNSCache) error {
	logIndex, err := logLoader.Load(job.logFiles)
	if err != nil {
		return err
	}

	writer, finalize, err := parquetout.CreateEnriched(job.dstPath, model.NewEnrichmentManifest(job.sourceFile, job.logFiles))
	if err != nil {
		return err
	}

	if err := parquetout.ReadFile(job.sourceFile.AbsPath, func(record model.FlowRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		enrichedRecord, err := enrichRecord(record, logIndex, cache)
		if err != nil {
			return err
		}

		if err := writer.Write(enrichedRecord); err != nil {
			return fmt.Errorf("write enriched row for %q: %w", job.sourceFile.AbsPath, err)
		}

		return nil
	}); err != nil {
		return err
	}

	if err := finalize(); err != nil {
		return err
	}

	return nil
}

func enrichRecord(record model.FlowRecord, logIndex *dnsIndex, cache *reverseDNSCache) (model.FlowRecord, error) {
	flowStart := time.Unix(0, record.TimeStartNs).UTC()

	srcNames, err := resolveNames(record.SrcIP, flowStart, logIndex, cache)
	if err != nil {
		return model.FlowRecord{}, err
	}

	dstNames, err := resolveNames(record.DstIP, flowStart, logIndex, cache)
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

func resolveNames(ipAddress string, flowStart time.Time, logIndex *dnsIndex, cache *reverseDNSCache) (*derivedNames, error) {
	if names := logIndex.Lookup(ipAddress, flowStart); names != nil {
		return names, nil
	}

	return cache.Lookup(ipAddress)
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

	return nil
}
