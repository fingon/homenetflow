package refresh

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/fingon/go-nfdump2parquet/internal/model"
)

func NeedsRebuild(dstPath string, sourceFiles []model.SourceFile, readManifest func(string) (model.RefreshManifest, error)) (bool, error) {
	if len(sourceFiles) == 0 {
		return false, nil
	}

	if _, err := os.Stat(dstPath); err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}

		return false, fmt.Errorf("stat %q: %w", dstPath, err)
	}

	manifest, err := readManifest(dstPath)
	if err == nil {
		expectedManifest := model.NewRefreshManifest(sourceFiles)
		if manifest.Version != expectedManifest.Version {
			return true, nil
		}

		return !slices.Equal(manifest.Sources, expectedManifest.Sources), nil
	}

	return true, nil
}

func CleanupSuperseded(dstRootPath string, period model.Period) error {
	switch period.Kind {
	case model.PeriodDay:
		return cleanupDay(dstRootPath, period.Start)
	case model.PeriodMonth:
		return cleanupMonth(dstRootPath, period.Start)
	default:
		return nil
	}
}

func cleanupDay(dstRootPath string, dayStart time.Time) error {
	for hour := range 24 {
		hourPath := filepath.Join(dstRootPath, fmt.Sprintf("nfcap_%s.parquet", dayStart.Add(time.Duration(hour)*time.Hour).Format("2006010215")))
		if err := removeIfExists(hourPath); err != nil {
			return err
		}
	}

	return nil
}

func cleanupMonth(dstRootPath string, monthStart time.Time) error {
	nextMonth := monthStart.AddDate(0, 1, 0)
	for day := monthStart; day.Before(nextMonth); day = day.AddDate(0, 0, 1) {
		dayPath := filepath.Join(dstRootPath, fmt.Sprintf("nfcap_%s.parquet", day.Format("20060102")))
		if err := removeIfExists(dayPath); err != nil {
			return err
		}
	}

	return nil
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove %q: %w", path, err)
	}

	return nil
}
