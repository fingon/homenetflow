package main

import (
	"io"
	"testing"

	"gotest.tools/v3/assert"
)

func TestEnrichProgressCallback(t *testing.T) {
	progress := newEnrichProgress(io.Discard)
	progress.callback(0, 3)
	progress.callback(1, 3)
	progress.callback(2, 3)
	progress.callback(3, 3)
	progress.callback(3, 3)

	assert.Equal(t, progress.done, int64(3))
	assert.Equal(t, progress.total, int64(3))
	assert.Assert(t, progress.finished)
}
