package parquetout

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fingon/go-nfdump2parquet/internal/model"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
)

const (
	manifestMetadataKey  = "go-nfdump2parquet.manifest"
	writerBufferRowCount = 1024
)

type Row struct {
	Bytes       int64   `parquet:"bytes"`
	DurationNs  int64   `parquet:"duration_ns"`
	DstAS       *int32  `parquet:"dst_as,optional"`
	DstIP       string  `parquet:"dst_ip"`
	DstMask     *int32  `parquet:"dst_mask,optional"`
	DstPort     int32   `parquet:"dst_port"`
	NextHopIP   *string `parquet:"next_hop_ip,optional"`
	Packets     int64   `parquet:"packets"`
	Protocol    int32   `parquet:"protocol"`
	RouterIP    *string `parquet:"router_ip,optional"`
	SrcAS       *int32  `parquet:"src_as,optional"`
	SrcIP       string  `parquet:"src_ip"`
	SrcMask     *int32  `parquet:"src_mask,optional"`
	SrcPort     int32   `parquet:"src_port"`
	TCPFlags    *int32  `parquet:"tcp_flags,optional"`
	TimeEndNs   int64   `parquet:"time_end_ns"`
	TimeStartNs int64   `parquet:"time_start_ns"`
}

type FileWriter struct {
	rows   []Row
	writer *parquet.GenericWriter[Row]
}

func Create(path string, manifest model.RefreshManifest) (*FileWriter, func() error, error) {
	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return nil, nil, fmt.Errorf("create %q: %w", tempPath, err)
	}

	writer := parquet.NewGenericWriter[Row](file, parquet.Compression(&snappy.Codec{}))
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		closeErr := file.Close()
		if closeErr != nil {
			return nil, nil, fmt.Errorf("marshal manifest: %w; close temp file: %w", err, closeErr)
		}

		return nil, nil, fmt.Errorf("marshal manifest: %w", err)
	}

	writer.SetKeyValueMetadata(manifestMetadataKey, string(manifestBytes))

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
			DstIP:       record.DstIP,
			DstMask:     record.DstMask,
			DstPort:     record.DstPort,
			NextHopIP:   record.NextHopIP,
			Packets:     record.Packets,
			Protocol:    record.Protocol,
			RouterIP:    record.RouterIP,
			SrcAS:       record.SrcAS,
			SrcIP:       record.SrcIP,
			SrcMask:     record.SrcMask,
			SrcPort:     record.SrcPort,
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
	fileInfo, err := os.Stat(path)
	if err != nil {
		return model.RefreshManifest{}, fmt.Errorf("stat %q: %w", path, err)
	}

	file, err := os.Open(path)
	if err != nil {
		return model.RefreshManifest{}, fmt.Errorf("open %q: %w", path, err)
	}
	defer file.Close()

	parquetFile, err := parquet.OpenFile(file, fileInfo.Size())
	if err != nil {
		return model.RefreshManifest{}, fmt.Errorf("open parquet %q: %w", path, err)
	}

	manifestValue, ok := parquetFile.Lookup(manifestMetadataKey)
	if !ok {
		return model.RefreshManifest{}, fmt.Errorf("manifest metadata %q missing", manifestMetadataKey)
	}

	var manifest model.RefreshManifest
	if err := json.Unmarshal([]byte(manifestValue), &manifest); err != nil {
		return model.RefreshManifest{}, fmt.Errorf("unmarshal manifest for %q: %w", path, err)
	}

	return manifest, nil
}
