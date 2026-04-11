package model

import (
	"fmt"
	"path/filepath"
	"sort"
	"time"
)

const (
	EnrichmentLogicVersion          = 3
	EnrichmentManifestVersion       = 1
	IPVersionUnknown          int32 = 0
	IPVersion4                int32 = 4
	IPVersion6                int32 = 6
	PeriodDay                       = "day"
	PeriodHour                      = "hour"
	PeriodMonth                     = "month"
	RefreshManifestVersion          = 1
	UISummaryLogicVersion           = 4
	UISummaryManifestVersion        = 1
)

type Period struct {
	Kind  string
	Start time.Time
}

func (p Period) End() time.Time {
	switch p.Kind {
	case PeriodMonth:
		return p.Start.AddDate(0, 1, 0)
	case PeriodDay:
		return p.Start.AddDate(0, 0, 1)
	case PeriodHour:
		return p.Start.Add(time.Hour)
	default:
		return p.Start
	}
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

type EnrichmentManifest struct {
	LogicVersion int              `json:"logicVersion"`
	Logs         []SourceManifest `json:"logs"`
	Source       SourceManifest   `json:"source"`
	Version      int              `json:"version"`
}

type UISummaryManifest struct {
	Granularity  string         `json:"granularity"`
	Kind         string         `json:"kind"`
	LogicVersion int            `json:"logicVersion"`
	Source       SourceManifest `json:"source"`
	SpanEndNs    int64          `json:"spanEndNs"`
	SpanStartNs  int64          `json:"spanStartNs"`
	Version      int            `json:"version"`
}

func NewRefreshManifest(sourceFiles []SourceFile) RefreshManifest {
	manifest := RefreshManifest{
		Version: RefreshManifestVersion,
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

func NewEnrichmentManifest(sourceFile SourceFile, logFiles []SourceFile) EnrichmentManifest {
	manifest := EnrichmentManifest{
		LogicVersion: EnrichmentLogicVersion,
		Logs:         make([]SourceManifest, 0, len(logFiles)),
		Source:       sourceManifestForFile(sourceFile),
		Version:      EnrichmentManifestVersion,
	}

	for _, logFile := range logFiles {
		manifest.Logs = append(manifest.Logs, sourceManifestForFile(logFile))
	}

	sort.Slice(manifest.Logs, func(i, j int) bool {
		return manifest.Logs[i].Path < manifest.Logs[j].Path
	})

	return manifest
}

func sourceManifestForFile(sourceFile SourceFile) SourceManifest {
	return SourceManifest{
		Path:      sourceFile.RelPath,
		SizeByte:  sourceFile.SizeByte,
		ModTimeNs: sourceFile.ModTime.UnixNano(),
	}
}

func NewUISummaryManifest(sourceFile SourceFile, kind, granularity string, spanStartNs, spanEndNs int64) UISummaryManifest {
	return UISummaryManifest{
		Granularity:  granularity,
		Kind:         kind,
		LogicVersion: UISummaryLogicVersion,
		Source:       sourceManifestForFile(sourceFile),
		SpanEndNs:    spanEndNs,
		SpanStartNs:  spanStartNs,
		Version:      UISummaryManifestVersion,
	}
}

type FlowRecord struct {
	TimeStartNs  int64
	TimeEndNs    int64
	DurationNs   int64
	IPVersion    int32
	Protocol     int32
	SrcIP        string
	DstIP        string
	SrcPort      int32
	DstPort      int32
	Packets      int64
	Bytes        int64
	RouterIP     *string
	NextHopIP    *string
	SrcAS        *int32
	DstAS        *int32
	SrcMask      *int32
	DstMask      *int32
	TCPFlags     *int32
	SrcHost      *string
	DstHost      *string
	Src2LD       *string
	Dst2LD       *string
	SrcTLD       *string
	DstTLD       *string
	SrcIsPrivate bool
	DstIsPrivate bool
}

type FlowParser interface {
	ParseFile(path string, emit func(FlowRecord) error) error
}
