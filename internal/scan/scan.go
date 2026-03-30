package scan

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/fingon/go-nfdump2parquet/internal/model"
)

const hierarchyDepth = 5

func SourceTree(srcRootPath string, now time.Time) (map[model.Period][]model.SourceFile, error) {
	periodSourceFiles := make(map[model.Period][]model.SourceFile)

	err := filepath.WalkDir(srcRootPath, func(path string, dirEntry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walk %q: %w", path, walkErr)
		}

		if dirEntry.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(srcRootPath, path)
		if err != nil {
			return fmt.Errorf("relative path for %q: %w", path, err)
		}

		sourceFile, ok, err := sourceFileForPath(path, relPath, now)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}

		periodSourceFiles[sourceFile.Period] = append(periodSourceFiles[sourceFile.Period], sourceFile)
		return nil
	})
	if err != nil {
		return nil, err
	}

	for period := range periodSourceFiles {
		slices.SortFunc(periodSourceFiles[period], func(a, b model.SourceFile) int {
			return strings.Compare(a.RelPath, b.RelPath)
		})
	}

	return periodSourceFiles, nil
}

func sourceFileForPath(absPath, relPath string, now time.Time) (model.SourceFile, bool, error) {
	baseName := filepath.Base(absPath)
	if !strings.HasPrefix(baseName, "nfcapd.") {
		return model.SourceFile{}, false, nil
	}

	parts := strings.Split(filepath.ToSlash(relPath), "/")
	if len(parts) != hierarchyDepth {
		return model.SourceFile{}, false, nil
	}

	timestamp, err := parsePathTimestamp(parts[:4], now.Location())
	if err == nil {
		nowUTC := now.UTC()
		timestampUTC := timestamp.UTC()
		if timestampUTC.After(nowUTC) {
			return model.SourceFile{}, false, fmt.Errorf("future-dated input %q for --now %s", relPath, nowUTC.Format(time.RFC3339))
		}

		fileInfo, err := os.Stat(absPath)
		if err != nil {
			return model.SourceFile{}, false, fmt.Errorf("stat %q: %w", absPath, err)
		}

		return model.SourceFile{
			AbsPath:  absPath,
			RelPath:  filepath.ToSlash(relPath),
			Period:   bucketFor(timestampUTC, nowUTC),
			SizeByte: fileInfo.Size(),
			ModTime:  fileInfo.ModTime().UTC(),
		}, true, nil
	}

	return model.SourceFile{}, false, nil
}

func parsePathTimestamp(parts []string, location *time.Location) (time.Time, error) {
	if len(parts) != 4 {
		return time.Time{}, errors.New("expected four timestamp directories")
	}

	values := make([]int, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil {
			return time.Time{}, fmt.Errorf("atoi %q: %w", part, err)
		}

		values = append(values, value)
	}

	return time.Date(values[0], time.Month(values[1]), values[2], values[3], 0, 0, 0, location), nil
}

func bucketFor(timestamp, now time.Time) model.Period {
	monthStart := time.Date(timestamp.Year(), timestamp.Month(), 1, 0, 0, 0, 0, time.UTC)
	dayStart := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, time.UTC)
	hourStart := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), 0, 0, 0, time.UTC)

	switch {
	case timestamp.Year() != now.Year() || timestamp.Month() != now.Month():
		return model.Period{Kind: model.PeriodMonth, Start: monthStart}
	case timestamp.Day() != now.Day():
		return model.Period{Kind: model.PeriodDay, Start: dayStart}
	default:
		return model.Period{Kind: model.PeriodHour, Start: hourStart}
	}
}
