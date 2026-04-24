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

const dnsLookupManifestMetadataKey = "homenetflow.dnslookups.manifest"

type DNSLookupRow struct {
	Answer             string  `parquet:"answer"`
	Client2LD          *string `parquet:"client_2ld,optional"`
	ClientDeviceID     *string `parquet:"client_device_id,optional"`
	ClientDeviceLabel  *string `parquet:"client_device_label,optional"`
	ClientDeviceMAC    *string `parquet:"client_device_mac,optional"`
	ClientDeviceSource *string `parquet:"client_device_source,optional"`
	ClientHost         *string `parquet:"client_host,optional"`
	ClientIP           string  `parquet:"client_ip"`
	ClientIPVersion    int32   `parquet:"client_ip_version"`
	ClientIsPrivate    bool    `parquet:"client_is_private"`
	ClientTLD          *string `parquet:"client_tld,optional"`
	Lookups            int64   `parquet:"lookups"`
	Query2LD           *string `parquet:"query_2ld,optional"`
	QueryName          string  `parquet:"query_name"`
	QueryTLD           *string `parquet:"query_tld,optional"`
	QueryType          string  `parquet:"query_type"`
	TimeStartNs        int64   `parquet:"time_start_ns"`
}

type DNSLookupWriter struct {
	rows   []DNSLookupRow
	writer *parquet.GenericWriter[DNSLookupRow]
}

func CreateDNSLookups(path string, manifest model.EnrichmentManifest) (*DNSLookupWriter, func() error, error) {
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal DNS lookup manifest: %w", err)
	}

	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return nil, nil, fmt.Errorf("create %q: %w", tempPath, err)
	}

	writer := parquet.NewGenericWriter[DNSLookupRow](file, parquet.Compression(&snappy.Codec{}))
	writer.SetKeyValueMetadata(dnsLookupManifestMetadataKey, string(manifestBytes))

	outputWriter := &DNSLookupWriter{
		rows:   make([]DNSLookupRow, 0, writerBufferRowCount),
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

func (w *DNSLookupWriter) Write(record model.DNSLookupRecord) error {
	return w.WriteBatch([]model.DNSLookupRecord{record})
}

func (w *DNSLookupWriter) WriteBatch(records []model.DNSLookupRecord) error {
	for _, record := range records {
		w.rows = append(w.rows, DNSLookupRow{
			Answer:             record.Answer,
			Client2LD:          record.Client2LD,
			ClientDeviceID:     record.ClientDeviceID,
			ClientDeviceLabel:  record.ClientDeviceLabel,
			ClientDeviceMAC:    record.ClientDeviceMAC,
			ClientDeviceSource: record.ClientDeviceSource,
			ClientHost:         record.ClientHost,
			ClientIP:           record.ClientIP,
			ClientIPVersion:    record.ClientIPVersion,
			ClientIsPrivate:    record.ClientIsPrivate,
			ClientTLD:          record.ClientTLD,
			Lookups:            record.Lookups,
			Query2LD:           record.Query2LD,
			QueryName:          record.QueryName,
			QueryTLD:           record.QueryTLD,
			QueryType:          record.QueryType,
			TimeStartNs:        record.TimeStartNs,
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

func (w *DNSLookupWriter) flush() error {
	if len(w.rows) == 0 {
		return nil
	}
	if _, err := w.writer.Write(w.rows); err != nil {
		return fmt.Errorf("write DNS lookup rows: %w", err)
	}
	w.rows = w.rows[:0]
	return nil
}

func ReadDNSLookupManifest(path string) (model.EnrichmentManifest, error) {
	return readMetadata(path, dnsLookupManifestMetadataKey, "DNS lookup manifest", &model.EnrichmentManifest{})
}

func ReadDNSLookupFile(path string, emit func(model.DNSLookupRecord) error) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %q: %w", path, err)
	}
	defer file.Close()

	reader := parquet.NewGenericReader[DNSLookupRow](file)
	defer reader.Close()

	rows := make([]DNSLookupRow, readerBufferRowCount)
	for {
		rowCount, err := reader.Read(rows)
		for _, row := range rows[:rowCount] {
			if emitErr := emit(row.toDNSLookupRecord()); emitErr != nil {
				return emitErr
			}
		}

		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("read DNS lookup rows from %q: %w", path, err)
	}
}

func (r DNSLookupRow) toDNSLookupRecord() model.DNSLookupRecord {
	return model.DNSLookupRecord{
		Answer:             r.Answer,
		Client2LD:          r.Client2LD,
		ClientDeviceID:     r.ClientDeviceID,
		ClientDeviceLabel:  r.ClientDeviceLabel,
		ClientDeviceMAC:    r.ClientDeviceMAC,
		ClientDeviceSource: r.ClientDeviceSource,
		ClientHost:         r.ClientHost,
		ClientIP:           r.ClientIP,
		ClientIPVersion:    r.ClientIPVersion,
		ClientIsPrivate:    r.ClientIsPrivate,
		ClientTLD:          r.ClientTLD,
		Lookups:            r.Lookups,
		Query2LD:           r.Query2LD,
		QueryName:          r.QueryName,
		QueryTLD:           r.QueryTLD,
		QueryType:          r.QueryType,
		TimeStartNs:        r.TimeStartNs,
	}
}
