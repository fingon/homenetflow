package parquetout

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/fingon/homenetflow/internal/model"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
)

const (
	enrichmentManifestMetadataKey = "homenetflow.parquethosts.manifest"
	manifestMetadataKey           = "go-nfdump2parquet.manifest"
	readerBufferRowCount          = 1024
	writerBufferRowCount          = 1024
)

type Row struct {
	Bytes       int64   `parquet:"bytes"`
	DurationNs  int64   `parquet:"duration_ns"`
	Dst2LD      *string `parquet:"dst_2ld,optional"`
	DstAS       *int32  `parquet:"dst_as,optional"`
	DstHost     *string `parquet:"dst_host,optional"`
	DstIP       string  `parquet:"dst_ip"`
	DstMask     *int32  `parquet:"dst_mask,optional"`
	DstPort     int32   `parquet:"dst_port"`
	DstTLD      *string `parquet:"dst_tld,optional"`
	NextHopIP   *string `parquet:"next_hop_ip,optional"`
	Packets     int64   `parquet:"packets"`
	Protocol    int32   `parquet:"protocol"`
	RouterIP    *string `parquet:"router_ip,optional"`
	Src2LD      *string `parquet:"src_2ld,optional"`
	SrcAS       *int32  `parquet:"src_as,optional"`
	SrcHost     *string `parquet:"src_host,optional"`
	SrcIP       string  `parquet:"src_ip"`
	SrcMask     *int32  `parquet:"src_mask,optional"`
	SrcPort     int32   `parquet:"src_port"`
	SrcTLD      *string `parquet:"src_tld,optional"`
	TCPFlags    *int32  `parquet:"tcp_flags,optional"`
	TimeEndNs   int64   `parquet:"time_end_ns"`
	TimeStartNs int64   `parquet:"time_start_ns"`
}

type FileWriter struct {
	rows   []Row
	writer *parquet.GenericWriter[Row]
}

func Create(path string, manifest model.RefreshManifest) (*FileWriter, func() error, error) {
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal manifest: %w", err)
	}

	return createWithMetadata(path, map[string]string{
		manifestMetadataKey: string(manifestBytes),
	})
}

func CreateEnriched(path string, manifest model.EnrichmentManifest) (*FileWriter, func() error, error) {
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal enrichment manifest: %w", err)
	}

	return createWithMetadata(path, map[string]string{
		enrichmentManifestMetadataKey: string(manifestBytes),
	})
}

func createWithMetadata(path string, metadata map[string]string) (*FileWriter, func() error, error) {
	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return nil, nil, fmt.Errorf("create %q: %w", tempPath, err)
	}

	writer := parquet.NewGenericWriter[Row](file, parquet.Compression(&snappy.Codec{}))
	for key, value := range metadata {
		writer.SetKeyValueMetadata(key, value)
	}

	outputWriter := &FileWriter{
		rows:   make([]Row, 0, writerBufferRowCount),
		writer: writer,
	}

	finalize := func() error {
		if err := outputWriter.flush(); err != nil {
			_ = file.Close()
			return err
		}

		if err := outputWriter.writer.Close(); err != nil {
			_ = file.Close()
			return fmt.Errorf("close parquet writer for %q: %w", tempPath, err)
		}

		if err := file.Close(); err != nil {
			return fmt.Errorf("close %q: %w", tempPath, err)
		}

		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("mkdir %q: %w", filepath.Dir(path), err)
		}

		if err := os.Rename(tempPath, path); err != nil {
			return fmt.Errorf("rename %q to %q: %w", tempPath, path, err)
		}

		return nil
	}

	return outputWriter, finalize, nil
}

func (w *FileWriter) Write(record model.FlowRecord) error {
	return w.WriteBatch([]model.FlowRecord{record})
}

func (w *FileWriter) WriteBatch(records []model.FlowRecord) error {
	for _, record := range records {
		w.rows = append(w.rows, Row{
			Bytes:       record.Bytes,
			DurationNs:  record.DurationNs,
			DstAS:       record.DstAS,
			Dst2LD:      record.Dst2LD,
			DstHost:     record.DstHost,
			DstIP:       record.DstIP,
			DstMask:     record.DstMask,
			DstPort:     record.DstPort,
			DstTLD:      record.DstTLD,
			NextHopIP:   record.NextHopIP,
			Packets:     record.Packets,
			Protocol:    record.Protocol,
			RouterIP:    record.RouterIP,
			Src2LD:      record.Src2LD,
			SrcAS:       record.SrcAS,
			SrcHost:     record.SrcHost,
			SrcIP:       record.SrcIP,
			SrcMask:     record.SrcMask,
			SrcPort:     record.SrcPort,
			SrcTLD:      record.SrcTLD,
			TCPFlags:    record.TCPFlags,
			TimeEndNs:   record.TimeEndNs,
			TimeStartNs: record.TimeStartNs,
		})

		if len(w.rows) < writerBufferRowCount {
			continue
		}

		if err := w.flush(); err != nil {
			return err
		}
	}

	return nil
}

func (w *FileWriter) flush() error {
	if len(w.rows) == 0 {
		return nil
	}

	if _, err := w.writer.Write(w.rows); err != nil {
		return fmt.Errorf("write parquet rows: %w", err)
	}

	w.rows = w.rows[:0]
	return nil
}

func ReadManifest(path string) (model.RefreshManifest, error) {
	return readMetadata(path, manifestMetadataKey, "manifest", &model.RefreshManifest{})
}

func ReadEnrichmentManifest(path string) (model.EnrichmentManifest, error) {
	return readMetadata(path, enrichmentManifestMetadataKey, "enrichment manifest", &model.EnrichmentManifest{})
}

func RowCount(path string) (int64, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat %q: %w", path, err)
	}

	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open %q: %w", path, err)
	}
	defer file.Close()

	parquetFile, err := parquet.OpenFile(file, fileInfo.Size())
	if err != nil {
		return 0, fmt.Errorf("open parquet %q: %w", path, err)
	}

	return parquetFile.NumRows(), nil
}

func ReadFile(path string, emit func(model.FlowRecord) error) error {
	return ReadFileBatches(path, func(records []model.FlowRecord) error {
		for _, record := range records {
			if err := emit(record); err != nil {
				return err
			}
		}

		return nil
	})
}

func ReadFileBatches(path string, emit func([]model.FlowRecord) error) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %q: %w", path, err)
	}
	defer file.Close()

	reader := parquet.NewGenericReader[Row](file)
	defer reader.Close()

	rows := make([]Row, readerBufferRowCount)
	records := make([]model.FlowRecord, 0, readerBufferRowCount)
	for {
		rowCount, err := reader.Read(rows)
		records = records[:0]
		for _, row := range rows[:rowCount] {
			records = append(records, row.toFlowRecord())
		}
		if len(records) > 0 {
			if emitErr := emit(records); emitErr != nil {
				return emitErr
			}
		}

		if err == nil {
			continue
		}

		if errors.Is(err, io.EOF) {
			return nil
		}

		return fmt.Errorf("read parquet rows from %q: %w", path, err)
	}
}

func (r Row) toFlowRecord() model.FlowRecord {
	return model.FlowRecord{
		Bytes:       r.Bytes,
		DurationNs:  r.DurationNs,
		Dst2LD:      r.Dst2LD,
		DstAS:       r.DstAS,
		DstHost:     r.DstHost,
		DstIP:       r.DstIP,
		DstMask:     r.DstMask,
		DstPort:     r.DstPort,
		DstTLD:      r.DstTLD,
		NextHopIP:   r.NextHopIP,
		Packets:     r.Packets,
		Protocol:    r.Protocol,
		RouterIP:    r.RouterIP,
		Src2LD:      r.Src2LD,
		SrcAS:       r.SrcAS,
		SrcHost:     r.SrcHost,
		SrcIP:       r.SrcIP,
		SrcMask:     r.SrcMask,
		SrcPort:     r.SrcPort,
		SrcTLD:      r.SrcTLD,
		TCPFlags:    r.TCPFlags,
		TimeEndNs:   r.TimeEndNs,
		TimeStartNs: r.TimeStartNs,
	}
}

func readMetadata[T any](path, metadataKey, description string, target *T) (T, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return *target, fmt.Errorf("stat %q: %w", path, err)
	}

	file, err := os.Open(path)
	if err != nil {
		return *target, fmt.Errorf("open %q: %w", path, err)
	}
	defer file.Close()

	parquetFile, err := parquet.OpenFile(file, fileInfo.Size())
	if err != nil {
		return *target, fmt.Errorf("open parquet %q: %w", path, err)
	}

	manifestValue, ok := parquetFile.Lookup(metadataKey)
	if !ok {
		return *target, fmt.Errorf("%s metadata %q missing", description, metadataKey)
	}

	if err := json.Unmarshal([]byte(manifestValue), target); err != nil {
		return *target, fmt.Errorf("unmarshal %s for %q: %w", description, path, err)
	}

	return *target, nil
}
