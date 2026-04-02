package scan

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/fingon/homenetflow/internal/model"
)

const logFilenameSuffix = ".jsonl"

func LogTree(srcRootPath string) ([]model.SourceFile, error) {
	entries, err := os.ReadDir(srcRootPath)
	if err != nil {
		return nil, fmt.Errorf("read dir %q: %w", srcRootPath, err)
	}

	sourceFiles := make([]model.SourceFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), logFilenameSuffix) {
			continue
		}

		label := strings.TrimSuffix(entry.Name(), logFilenameSuffix)
		start, err := time.Parse("2006-01-02", label)
		if err != nil {
			continue
		}

		absPath := filepath.Join(srcRootPath, entry.Name())
		fileInfo, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("stat %q: %w", absPath, err)
		}

		sourceFiles = append(sourceFiles, model.SourceFile{
			AbsPath:  absPath,
			RelPath:  entry.Name(),
			Period:   model.Period{Kind: model.PeriodDay, Start: start.UTC()},
			SizeByte: fileInfo.Size(),
			ModTime:  fileInfo.ModTime().UTC(),
		})
	}

	slices.SortFunc(sourceFiles, func(a, b model.SourceFile) int {
		return strings.Compare(a.RelPath, b.RelPath)
	})

	return sourceFiles, nil
}
