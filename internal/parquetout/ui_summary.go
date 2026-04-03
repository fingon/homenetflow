package parquetout

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/fingon/homenetflow/internal/model"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
)

const uiSummaryManifestMetadataKey = "homenetflow.parquetflowui.summary.manifest"

type EdgeSummaryRow struct {
	Bytes       int64  `parquet:"bytes"`
	Connections int64  `parquet:"connections"`
	Destination string `parquet:"dst_entity"`
	FirstSeenNs int64  `parquet:"first_seen_ns"`
	LastSeenNs  int64  `parquet:"last_seen_ns"`
	Source      string `parquet:"src_entity"`
}

type HistogramSummaryRow struct {
	BucketStartNs int64 `parquet:"bucket_start_ns"`
	Bytes         int64 `parquet:"bytes"`
	Connections   int64 `parquet:"connections"`
}

type EdgeSummaryWriter struct {
	rows   []EdgeSummaryRow
	writer *parquet.GenericWriter[EdgeSummaryRow]
}

type HistogramSummaryWriter struct {
	rows   []HistogramSummaryRow
	writer *parquet.GenericWriter[HistogramSummaryRow]
}

func CreateUISummaryEdges(path string, manifest model.UISummaryManifest) (*EdgeSummaryWriter, func() error, error) {
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal UI summary manifest: %w", err)
	}

	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return nil, nil, fmt.Errorf("create %q: %w", tempPath, err)
	}

	writer := parquet.NewGenericWriter[EdgeSummaryRow](file, parquet.Compression(&snappy.Codec{}))
	writer.SetKeyValueMetadata(uiSummaryManifestMetadataKey, string(manifestBytes))

	outputWriter := &EdgeSummaryWriter{
		rows:   make([]EdgeSummaryRow, 0, writerBufferRowCount),
		writer: writer,
	}

	finalize := func() error {
		return finalizeSummaryWriter(path, tempPath, file, outputWriter.flush, outputWriter.writer.Close)
	}

	return outputWriter, finalize, nil
}

func CreateUISummaryHistogram(path string, manifest model.UISummaryManifest) (*HistogramSummaryWriter, func() error, error) {
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal UI summary manifest: %w", err)
	}

	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return nil, nil, fmt.Errorf("create %q: %w", tempPath, err)
	}

	writer := parquet.NewGenericWriter[HistogramSummaryRow](file, parquet.Compression(&snappy.Codec{}))
	writer.SetKeyValueMetadata(uiSummaryManifestMetadataKey, string(manifestBytes))

	outputWriter := &HistogramSummaryWriter{
		rows:   make([]HistogramSummaryRow, 0, writerBufferRowCount),
		writer: writer,
	}

	finalize := func() error {
		return finalizeSummaryWriter(path, tempPath, file, outputWriter.flush, outputWriter.writer.Close)
	}

	return outputWriter, finalize, nil
}

func (w *EdgeSummaryWriter) WriteBatch(rows []EdgeSummaryRow) error {
	for _, row := range rows {
		w.rows = append(w.rows, row)
		if len(w.rows) < writerBufferRowCount {
			continue
		}
		if err := w.flush(); err != nil {
			return err
		}
	}
	return nil
}

func (w *HistogramSummaryWriter) WriteBatch(rows []HistogramSummaryRow) error {
	for _, row := range rows {
		w.rows = append(w.rows, row)
		if len(w.rows) < writerBufferRowCount {
			continue
		}
		if err := w.flush(); err != nil {
			return err
		}
	}
	return nil
}

func (w *EdgeSummaryWriter) flush() error {
	if len(w.rows) == 0 {
		return nil
	}
	if _, err := w.writer.Write(w.rows); err != nil {
		return fmt.Errorf("write UI edge summary rows: %w", err)
	}
	w.rows = w.rows[:0]
	return nil
}

func (w *HistogramSummaryWriter) flush() error {
	if len(w.rows) == 0 {
		return nil
	}
	if _, err := w.writer.Write(w.rows); err != nil {
		return fmt.Errorf("write UI histogram summary rows: %w", err)
	}
	w.rows = w.rows[:0]
	return nil
}

func ReadUISummaryManifest(path string) (model.UISummaryManifest, error) {
	return readMetadata(path, uiSummaryManifestMetadataKey, "UI summary manifest", &model.UISummaryManifest{})
}

func ReadEdgeSummaryRows(path string, emit func([]EdgeSummaryRow) error) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %q: %w", path, err)
	}
	defer file.Close()

	reader := parquet.NewGenericReader[EdgeSummaryRow](file)
	defer reader.Close()

	rows := make([]EdgeSummaryRow, readerBufferRowCount)
	for {
		rowCount, err := reader.Read(rows)
		if rowCount > 0 {
			if emitErr := emit(rows[:rowCount]); emitErr != nil {
				return emitErr
			}
		}

		if err == nil {
			continue
		}
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("read UI edge summary rows from %q: %w", path, err)
	}
}

func ReadHistogramSummaryRows(path string, emit func([]HistogramSummaryRow) error) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %q: %w", path, err)
	}
	defer file.Close()

	reader := parquet.NewGenericReader[HistogramSummaryRow](file)
	defer reader.Close()

	rows := make([]HistogramSummaryRow, readerBufferRowCount)
	for {
		rowCount, err := reader.Read(rows)
		if rowCount > 0 {
			if emitErr := emit(rows[:rowCount]); emitErr != nil {
				return emitErr
			}
		}

		if err == nil {
			continue
		}
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("read UI histogram summary rows from %q: %w", path, err)
	}
}

func finalizeSummaryWriter(path, tempPath string, file *os.File, flush, closeWriter func() error) error {
	if err := flush(); err != nil {
		_ = file.Close()
		return err
	}
	if err := closeWriter(); err != nil {
		_ = file.Close()
		return fmt.Errorf("close parquet writer for %q: %w", tempPath, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %q: %w", tempPath, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("rename %q to %q: %w", tempPath, path, err)
	}
	return nil
}
