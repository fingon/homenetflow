package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"github.com/fingon/homenetflow/internal/model"
	"github.com/fingon/homenetflow/internal/parquetout"
	"github.com/fingon/homenetflow/internal/refresh"
	"github.com/fingon/homenetflow/internal/scan"
	"golang.org/x/sync/errgroup"
)

const (
	autoParallelismMax         = 4
	parseBatchBufferMultiplier = 2
	parseChunkRecordCount      = 1024
)

type Config struct {
	DstPath     string
	Now         time.Time
	Parallelism int
	SrcPath     string
}

func Run(config Config, newParser func() model.FlowParser) error {
	if config.SrcPath == "" {
		return errors.New("source path is required")
	}

	if config.DstPath == "" {
		return errors.New("destination path is required")
	}

	if newParser == nil {
		return errors.New("parser factory is required")
	}

	if err := os.MkdirAll(config.DstPath, 0o755); err != nil {
		return fmt.Errorf("mkdir %q: %w", config.DstPath, err)
	}

	periodSourceFiles, err := scan.SourceTree(config.SrcPath, config.Now.UTC())
	if err != nil {
		return err
	}

	staleJobs := make([]periodJob, 0, len(periodSourceFiles))
	periods := sortedPeriods(periodSourceFiles)
	for _, period := range periods {
		sourceFiles := periodSourceFiles[period]
		dstPath := period.OutputPath(config.DstPath)

		rebuild, err := refresh.NeedsRebuild(dstPath, sourceFiles, parquetout.ReadManifest)
		if err != nil {
			return fmt.Errorf("check rebuild for %q: %w", dstPath, err)
		}

		if rebuild {
			staleJobs = append(staleJobs, periodJob{
				dstPath:     dstPath,
				period:      period,
				sourceFiles: sourceFiles,
			})
		} else {
			slog.Debug("parquet already up to date", "path", dstPath, "sources", len(sourceFiles))
		}
	}

	if err := rebuildPeriods(
		context.Background(),
		staleJobs,
		newParser,
		config.parserWorkersPerOutput(),
		config.outputJobs(),
	); err != nil {
		return err
	}

	for _, period := range periods {
		if err := refresh.CleanupSuperseded(config.DstPath, period); err != nil {
			return fmt.Errorf("cleanup superseded outputs for %q: %w", period.OutputPath(config.DstPath), err)
		}
	}

	return nil
}

type periodJob struct {
	dstPath     string
	period      model.Period
	sourceFiles []model.SourceFile
}

func (c Config) parserWorkersPerOutput() int {
	if c.Parallelism > 0 {
		return c.Parallelism
	}

	return min(autoParallelismMax, max(1, runtime.GOMAXPROCS(0)/2))
}

func (c Config) outputJobs() int {
	return max(1, runtime.GOMAXPROCS(0)/c.parserWorkersPerOutput())
}

func rebuildPeriods(
	ctx context.Context,
	jobs []periodJob,
	newParser func() model.FlowParser,
	parserWorkersPerOutput int,
	outputJobs int,
) error {
	group, groupContext := errgroup.WithContext(ctx)
	group.SetLimit(outputJobs)

	for _, job := range jobs {
		job := job
		group.Go(func() error {
			if err := rebuildPeriod(groupContext, job.dstPath, job.sourceFiles, newParser, parserWorkersPerOutput); err != nil {
				return fmt.Errorf("rebuild %q: %w", job.dstPath, err)
			}

			slog.Info("rebuilt parquet", "path", job.dstPath, "sources", len(job.sourceFiles), "period", job.period.Label())
			return nil
		})
	}

	return group.Wait()
}

func rebuildPeriod(
	ctx context.Context,
	dstPath string,
	sourceFiles []model.SourceFile,
	newParser func() model.FlowParser,
	parserWorkers int,
) error {
	manifest := model.NewRefreshManifest(sourceFiles)
	writer, finalize, err := parquetout.Create(dstPath, manifest)
	if err != nil {
		return err
	}

	if err := rebuildBatches(ctx, writer, sourceFiles, newParser, min(parserWorkers, len(sourceFiles))); err != nil {
		return err
	}

	if err := finalize(); err != nil {
		return err
	}

	return nil
}

func rebuildBatches(
	parentCtx context.Context,
	writer *parquetout.FileWriter,
	sourceFiles []model.SourceFile,
	newParser func() model.FlowParser,
	workerCount int,
) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	parseGroup, parseContext := errgroup.WithContext(ctx)
	sourceFileChannel := make(chan model.SourceFile)
	recordBatchChannel := make(chan []model.FlowRecord, workerCount*parseBatchBufferMultiplier)
	writerErrorChannel := make(chan error, 1)

	go func() {
		writerError := writeRecordBatches(ctx, writer, recordBatchChannel)
		if writerError != nil {
			cancel()
		}
		writerErrorChannel <- writerError
	}()

	parseGroup.Go(func() error {
		defer close(sourceFileChannel)
		for _, sourceFile := range sourceFiles {
			select {
			case sourceFileChannel <- sourceFile:
			case <-parseContext.Done():
				return parseContext.Err()
			}
		}

		return nil
	})

	for range workerCount {
		parseGroup.Go(func() error {
			for {
				select {
				case <-parseContext.Done():
					return parseContext.Err()
				case sourceFile, ok := <-sourceFileChannel:
					if !ok {
						return nil
					}

					parser := newParser()
					if parser == nil {
						return errors.New("parser factory returned nil")
					}

					slog.Debug("reading source file", "path", sourceFile.AbsPath)
					if err := parseSourceFile(parseContext, parser, sourceFile, recordBatchChannel); err != nil {
						return fmt.Errorf("parse %q: %w", sourceFile.AbsPath, err)
					}
				}
			}
		})
	}

	parseError := parseGroup.Wait()
	close(recordBatchChannel)
	writerError := <-writerErrorChannel

	if writerError != nil && !errors.Is(writerError, context.Canceled) {
		return writerError
	}

	if parseError != nil && !errors.Is(parseError, context.Canceled) {
		return parseError
	}

	if parseError != nil {
		return parseError
	}

	return writerError
}

func writeRecordBatches(
	ctx context.Context,
	writer *parquetout.FileWriter,
	recordBatchChannel <-chan []model.FlowRecord,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case records, ok := <-recordBatchChannel:
			if !ok {
				return nil
			}

			if err := writer.WriteBatch(records); err != nil {
				return fmt.Errorf("write parquet batch: %w", err)
			}
		}
	}
}

func parseSourceFile(
	ctx context.Context,
	parser model.FlowParser,
	sourceFile model.SourceFile,
	recordBatchChannel chan<- []model.FlowRecord,
) error {
	batch := make([]model.FlowRecord, 0, parseChunkRecordCount)
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		recordBatch := slices.Clone(batch)
		select {
		case recordBatchChannel <- recordBatch:
		case <-ctx.Done():
			return ctx.Err()
		}

		batch = batch[:0]
		return nil
	}

	err := parser.ParseFile(sourceFile.AbsPath, func(record model.FlowRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch = append(batch, record)
		if len(batch) < parseChunkRecordCount {
			return nil
		}

		return flushBatch()
	})
	if err != nil {
		return err
	}

	if err := flushBatch(); err != nil {
		return fmt.Errorf("flush %q: %w", sourceFile.AbsPath, err)
	}

	return nil
}

func sortedPeriods(periodSourceFiles map[model.Period][]model.SourceFile) []model.Period {
	periods := make([]model.Period, 0, len(periodSourceFiles))
	for period := range periodSourceFiles {
		periods = append(periods, period)
	}

	slices.SortFunc(periods, func(a, b model.Period) int {
		if a.Start.Equal(b.Start) {
			return compareStrings(a.Kind, b.Kind)
		}

		if a.Start.Before(b.Start) {
			return -1
		}

		return 1
	})

	return periods
}

func compareStrings(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func ExistingOutputs(dstPath string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dstPath, "nfcap_*.parquet"))
	if err != nil {
		return nil, fmt.Errorf("glob parquet outputs in %q: %w", dstPath, err)
	}

	return matches, nil
}
