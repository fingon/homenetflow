package scan

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/fingon/homenetflow/internal/model"
)

const parquetFilenamePrefix = "nfcap_"

func FlatParquetTree(srcRootPath string) (map[model.Period]model.SourceFile, error) {
	periodSourceFiles := make(map[model.Period]model.SourceFile)

	err := filepath.WalkDir(srcRootPath, func(path string, dirEntry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walk %q: %w", path, walkErr)
		}

		if dirEntry.IsDir() || filepath.Dir(path) != srcRootPath {
			return nil
		}

		relPath, err := filepath.Rel(srcRootPath, path)
		if err != nil {
			return fmt.Errorf("relative path for %q: %w", path, err)
		}

		sourceFile, ok, err := parquetSourceFileForPath(path, relPath)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}

		periodSourceFiles[sourceFile.Period] = sourceFile
		return nil
	})
	if err != nil {
		return nil, err
	}

	return periodSourceFiles, nil
}

func SortedFlatParquetPaths(srcRootPath string) ([]string, error) {
	entries, err := os.ReadDir(srcRootPath)
	if err != nil {
		return nil, fmt.Errorf("read dir %q: %w", srcRootPath, err)
	}

	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), parquetFilenamePrefix) || !strings.HasSuffix(entry.Name(), ".parquet") {
			continue
		}

		paths = append(paths, filepath.Join(srcRootPath, entry.Name()))
	}

	slices.Sort(paths)
	return paths, nil
}

func parquetSourceFileForPath(absPath, relPath string) (model.SourceFile, bool, error) {
	period, ok, err := periodForParquetName(filepath.Base(relPath))
	if err != nil || !ok {
		return model.SourceFile{}, ok, err
	}

	fileInfo, err := os.Stat(absPath)
	if err != nil {
		return model.SourceFile{}, false, fmt.Errorf("stat %q: %w", absPath, err)
	}

	return model.SourceFile{
		AbsPath:  absPath,
		RelPath:  filepath.ToSlash(relPath),
		Period:   period,
		SizeByte: fileInfo.Size(),
		ModTime:  fileInfo.ModTime().UTC(),
	}, true, nil
}

func periodForParquetName(name string) (model.Period, bool, error) {
	if !strings.HasPrefix(name, parquetFilenamePrefix) || !strings.HasSuffix(name, ".parquet") {
		return model.Period{}, false, nil
	}

	label := strings.TrimSuffix(strings.TrimPrefix(name, parquetFilenamePrefix), ".parquet")
	switch len(label) {
	case 6:
		start, err := time.Parse("200601", label)
		if err != nil {
			return model.Period{}, false, fmt.Errorf("parse month parquet name %q: %w", name, err)
		}

		return model.Period{Kind: model.PeriodMonth, Start: start.UTC()}, true, nil
	case 8:
		start, err := time.Parse("20060102", label)
		if err != nil {
			return model.Period{}, false, fmt.Errorf("parse day parquet name %q: %w", name, err)
		}

		return model.Period{Kind: model.PeriodDay, Start: start.UTC()}, true, nil
	case 10:
		start, err := time.Parse("2006010215", label)
		if err != nil {
			return model.Period{}, false, fmt.Errorf("parse hour parquet name %q: %w", name, err)
		}

		return model.Period{Kind: model.PeriodHour, Start: start.UTC()}, true, nil
	default:
		return model.Period{}, false, nil
	}
}
