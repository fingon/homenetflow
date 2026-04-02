package main

import (
	"io"
	"log/slog"
	"sync"

	"github.com/schollz/progressbar/v3"
)

const progressDescription = "enrich"

type enrichProgress struct {
	mu       sync.Mutex
	bar      *progressbar.ProgressBar
	done     int64
	finished bool
	total    int64
	writer   io.Writer
}

func newEnrichProgress(writer io.Writer) *enrichProgress {
	return &enrichProgress{writer: writer}
}

func (p *enrichProgress) callback(doneRowCount, totalRowCount int64) {
	if totalRowCount <= 0 || doneRowCount < 0 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.finished {
		return
	}

	if p.bar == nil {
		p.total = totalRowCount
		p.bar = progressbar.NewOptions64(
			totalRowCount,
			progressbar.OptionSetDescription(progressDescription),
			progressbar.OptionSetWriter(p.writer),
		)
	}

	if doneRowCount < p.done {
		return
	}

	deltaRowCount := doneRowCount - p.done
	p.done = doneRowCount
	if deltaRowCount > 0 {
		if err := p.bar.Add64(deltaRowCount); err != nil {
			slog.Debug("progress add failed", "error", err)
		}
	}

	if p.done >= p.total {
		if err := p.bar.Finish(); err != nil {
			slog.Debug("progress finish failed", "error", err)
		}
		p.finished = true
	}
}
