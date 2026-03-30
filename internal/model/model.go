package model

import (
	"fmt"
	"path/filepath"
	"sort"
	"time"
)

const (
	PeriodMonth = "month"
	PeriodDay   = "day"
	PeriodHour  = "hour"
)

type Period struct {
	Kind  string
	Start time.Time
}

func (p Period) Filename() string {
	return fmt.Sprintf("nfcap_%s.parquet", p.Label())
}

func (p Period) Label() string {
	switch p.Kind {
	case PeriodMonth:
		return p.Start.Format("200601")
	case PeriodDay:
		return p.Start.Format("20060102")
	case PeriodHour:
		return p.Start.Format("2006010215")
	default:
		return p.Start.Format(time.RFC3339)
	}
}

func (p Period) OutputPath(dstPath string) string {
	return filepath.Join(dstPath, p.Filename())
}

type SourceFile struct {
	AbsPath  string
	RelPath  string
	Period   Period
	SizeByte int64
	ModTime  time.Time
}

type SourceManifest struct {
	Path      string `json:"path"`
	SizeByte  int64  `json:"sizeBytes"`
	ModTimeNs int64  `json:"mtimeNs"`
}

type RefreshManifest struct {
	Version int              `json:"version"`
	Sources []SourceManifest `json:"sources"`
}

func NewRefreshManifest(sourceFiles []SourceFile) RefreshManifest {
	manifest := RefreshManifest{
		Version: 1,
		Sources: make([]SourceManifest, 0, len(sourceFiles)),
	}

	for _, sourceFile := range sourceFiles {
		manifest.Sources = append(manifest.Sources, SourceManifest{
			Path:      sourceFile.RelPath,
			SizeByte:  sourceFile.SizeByte,
			ModTimeNs: sourceFile.ModTime.UnixNano(),
		})
	}

	sort.Slice(manifest.Sources, func(i, j int) bool {
		return manifest.Sources[i].Path < manifest.Sources[j].Path
	})

	return manifest
}

type FlowRecord struct {
	TimeStartNs int64
	TimeEndNs   int64
	DurationNs  int64
	Protocol    int32
	SrcIP       string
	DstIP       string
	SrcPort     int32
	DstPort     int32
	Packets     int64
	Bytes       int64
	RouterIP    *string
	NextHopIP   *string
	SrcAS       *int32
	DstAS       *int32
	SrcMask     *int32
	DstMask     *int32
	TCPFlags    *int32
}

type FlowParser interface {
	ParseFile(path string, emit func(FlowRecord) error) error
}
