package app

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fingon/go-nfdump2parquet/internal/model"
	"github.com/fingon/go-nfdump2parquet/internal/parquetout"
	"github.com/parquet-go/parquet-go"
	"gotest.tools/v3/assert"
)

const (
	releaseDelay = 50 * time.Millisecond
	runTimeout   = 2 * time.Second
)

func TestRunBuildsExpectedOutputs(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	mustSourceFile(t, srcDir, "2026/01/08/03/nfcapd.202601080320")
	mustSourceFile(t, srcDir, "2026/03/29/10/nfcapd.202603291000")
	mustSourceFile(t, srcDir, "2026/03/30/03/nfcapd.202603300320")

	parser := fakeParser{
		recordsByPath: map[string][]model.FlowRecord{
			filepath.Join(srcDir, "2026/01/08/03/nfcapd.202601080320"): {sampleRecord()},
			filepath.Join(srcDir, "2026/03/29/10/nfcapd.202603291000"): {sampleRecord()},
			filepath.Join(srcDir, "2026/03/30/03/nfcapd.202603300320"): {sampleRecord()},
		},
	}

	err := Run(Config{
		DstPath: dstDir,
		Now:     testNow(),
		SrcPath: srcDir,
	}, fakeParserFactory(parser))
	assert.NilError(t, err)

	assert.NilError(t, fileExists(filepath.Join(dstDir, "nfcap_202601.parquet")))
	assert.NilError(t, fileExists(filepath.Join(dstDir, "nfcap_20260329.parquet")))
	assert.NilError(t, fileExists(filepath.Join(dstDir, "nfcap_2026033003.parquet")))
}

func TestRunLeavesOrphanedOutputUntouched(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	targetPath := filepath.Join(dstDir, "nfcap_20260329.parquet")

	writer, finalize, err := parquetout.Create(targetPath, model.RefreshManifest{
		Version: 1,
		Sources: []model.SourceManifest{{
			Path:      "2026/03/29/10/nfcapd.202603291000",
			SizeByte:  123,
			ModTimeNs: time.Now().UTC().UnixNano(),
		}},
	})
	assert.NilError(t, err)
	assert.NilError(t, writer.Write(sampleRecord()))
	assert.NilError(t, finalize())

	originalInfo, err := os.Stat(targetPath)
	assert.NilError(t, err)

	err = Run(Config{
		DstPath: dstDir,
		Now:     testNow(),
		SrcPath: srcDir,
	}, fakeParserFactory(fakeParser{}))
	assert.NilError(t, err)

	updatedInfo, err := os.Stat(targetPath)
	assert.NilError(t, err)
	assert.Equal(t, updatedInfo.ModTime(), originalInfo.ModTime())
}

func TestRunDeletesHourlyWhenDailyReplacementExists(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	mustSourceFile(t, srcDir, "2026/03/29/10/nfcapd.202603291000")
	hourlyPath := filepath.Join(dstDir, "nfcap_2026032910.parquet")
	writer, finalize, err := parquetout.Create(hourlyPath, model.RefreshManifest{
		Version: 1,
		Sources: []model.SourceManifest{{
			Path:      "2026/03/29/10/nfcapd.202603291000",
			SizeByte:  123,
			ModTimeNs: time.Now().UTC().UnixNano(),
		}},
	})
	assert.NilError(t, err)
	assert.NilError(t, writer.Write(sampleRecord()))
	assert.NilError(t, finalize())

	err = Run(Config{
		DstPath: dstDir,
		Now:     testNow(),
		SrcPath: srcDir,
	}, fakeParserFactory(fakeParser{
		recordsByPath: map[string][]model.FlowRecord{
			filepath.Join(srcDir, "2026/03/29/10/nfcapd.202603291000"): {sampleRecord()},
		},
	}))
	assert.NilError(t, err)

	assert.Assert(t, !fileExistsBool(hourlyPath))
	assert.NilError(t, fileExists(filepath.Join(dstDir, "nfcap_20260329.parquet")))
}

func TestRunRebuildsWhenSourceRemovedButBucketStillNonEmpty(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	pathOne := mustSourceFile(t, srcDir, "2026/03/29/10/nfcapd.202603291000")
	pathTwo := mustSourceFile(t, srcDir, "2026/03/29/11/nfcapd.202603291100")
	targetPath := filepath.Join(dstDir, "nfcap_20260329.parquet")

	parser := fakeParser{
		recordsByPath: map[string][]model.FlowRecord{
			pathOne: {sampleRecord()},
			pathTwo: {sampleRecord()},
		},
	}

	err := Run(Config{
		DstPath: dstDir,
		Now:     testNow(),
		SrcPath: srcDir,
	}, fakeParserFactory(parser))
	assert.NilError(t, err)

	assert.NilError(t, os.Remove(pathTwo))

	err = Run(Config{
		DstPath: dstDir,
		Now:     testNow(),
		SrcPath: srcDir,
	}, fakeParserFactory(fakeParser{
		recordsByPath: map[string][]model.FlowRecord{
			pathOne: {sampleRecord()},
		},
	}))
	assert.NilError(t, err)

	manifest, err := parquetout.ReadManifest(targetPath)
	assert.NilError(t, err)
	assert.Equal(t, len(manifest.Sources), 1)
	assert.Equal(t, manifest.Sources[0].Path, "2026/03/29/10/nfcapd.202603291000")
}

func TestRunParsesFilesConcurrently(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	pathOne := mustSourceFile(t, srcDir, "2026/03/29/10/nfcapd.202603291000")
	pathTwo := mustSourceFile(t, srcDir, "2026/03/29/11/nfcapd.202603291100")

	parserState := &sharedParserState{
		recordsByPath: map[string][]model.FlowRecord{
			pathOne: {sampleRecord()},
			pathTwo: {sampleRecord()},
		},
		releaseByPath: map[string]chan struct{}{
			pathOne: make(chan struct{}),
			pathTwo: make(chan struct{}),
		},
		startedPaths: make(chan string, 2),
	}

	resultChannel := make(chan error, 1)
	go func() {
		resultChannel <- Run(Config{
			DstPath:     dstDir,
			Now:         testNow(),
			Parallelism: 2,
			SrcPath:     srcDir,
		}, parserState.newParser)
	}()

	waitForStartedPaths(t, parserState.startedPaths, 2)
	assert.Equal(t, parserState.maxActive.Load(), int32(2))

	close(parserState.releaseByPath[pathOne])
	close(parserState.releaseByPath[pathTwo])

	assert.NilError(t, waitForRunResult(t, resultChannel))
}

func TestRunWritesAllRowsWhenLaterFileFinishesFirst(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	pathOne := mustSourceFile(t, srcDir, "2026/03/29/10/nfcapd.202603291000")
	pathTwo := mustSourceFile(t, srcDir, "2026/03/29/11/nfcapd.202603291100")
	outputPath := filepath.Join(dstDir, "nfcap_20260329.parquet")

	parserState := &sharedParserState{
		recordsByPath: map[string][]model.FlowRecord{
			pathOne: {sampleRecordForIP("192.0.2.10", 10), sampleRecordForIP("192.0.2.11", 20)},
			pathTwo: {sampleRecordForIP("192.0.2.20", 30)},
		},
		releaseByPath: map[string]chan struct{}{
			pathOne: make(chan struct{}),
			pathTwo: make(chan struct{}),
		},
		startedPaths: make(chan string, 2),
	}

	resultChannel := make(chan error, 1)
	go func() {
		resultChannel <- Run(Config{
			DstPath:     dstDir,
			Now:         testNow(),
			Parallelism: 2,
			SrcPath:     srcDir,
		}, parserState.newParser)
	}()

	waitForStartedPaths(t, parserState.startedPaths, 2)
	close(parserState.releaseByPath[pathTwo])
	time.Sleep(releaseDelay)
	close(parserState.releaseByPath[pathOne])

	assert.NilError(t, waitForRunResult(t, resultChannel))

	rows := readRows(t, outputPath)
	assert.Equal(t, len(rows), 3)
	assert.Assert(t, rowsContainSrcIP(rows, "192.0.2.10"))
	assert.Assert(t, rowsContainSrcIP(rows, "192.0.2.11"))
	assert.Assert(t, rowsContainSrcIP(rows, "192.0.2.20"))
}

func TestRunRebuildsPeriodsConcurrently(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	pathMonth := mustSourceFile(t, srcDir, "2026/01/08/03/nfcapd.202601080320")
	pathDay := mustSourceFile(t, srcDir, "2026/03/29/10/nfcapd.202603291000")

	parserState := &sharedParserState{
		recordsByPath: map[string][]model.FlowRecord{
			pathMonth: {sampleRecord()},
			pathDay:   {sampleRecord()},
		},
		releaseByPath: map[string]chan struct{}{
			pathMonth: make(chan struct{}),
			pathDay:   make(chan struct{}),
		},
		startedPaths: make(chan string, 2),
	}

	resultChannel := make(chan error, 1)
	go func() {
		resultChannel <- Run(Config{
			DstPath:     dstDir,
			Now:         testNow(),
			Parallelism: 1,
			SrcPath:     srcDir,
		}, parserState.newParser)
	}()

	waitForStartedPaths(t, parserState.startedPaths, 2)
	assert.Equal(t, parserState.maxActive.Load(), int32(2))

	close(parserState.releaseByPath[pathMonth])
	close(parserState.releaseByPath[pathDay])

	assert.NilError(t, waitForRunResult(t, resultChannel))
}

func TestRunReturnsWorkerParseErrors(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	pathOne := mustSourceFile(t, srcDir, "2026/03/29/10/nfcapd.202603291000")
	pathTwo := mustSourceFile(t, srcDir, "2026/03/29/11/nfcapd.202603291100")

	parserState := &sharedParserState{
		recordsByPath: map[string][]model.FlowRecord{
			pathTwo: {sampleRecord()},
		},
		errByPath: map[string]error{
			pathOne: errors.New("boom"),
		},
	}

	resultChannel := make(chan error, 1)
	go func() {
		resultChannel <- Run(Config{
			DstPath:     dstDir,
			Now:         testNow(),
			Parallelism: 2,
			SrcPath:     srcDir,
		}, parserState.newParser)
	}()

	err := waitForRunResult(t, resultChannel)
	assert.ErrorContains(t, err, "parse")
	assert.ErrorContains(t, err, "boom")
}

func TestConfigAutoParallelismUsesMixedWorkloadDefaults(t *testing.T) {
	previousMaxProcs := runtime.GOMAXPROCS(8)
	t.Cleanup(func() {
		runtime.GOMAXPROCS(previousMaxProcs)
	})

	config := Config{}
	assert.Equal(t, config.parserWorkersPerOutput(), 4)
	assert.Equal(t, config.outputJobs(), 2)

	explicitConfig := Config{Parallelism: 3}
	assert.Equal(t, explicitConfig.parserWorkersPerOutput(), 3)
	assert.Equal(t, explicitConfig.outputJobs(), 2)
}

type fakeParser struct {
	recordsByPath map[string][]model.FlowRecord
}

func fakeParserFactory(parser model.FlowParser) func() model.FlowParser {
	return func() model.FlowParser {
		return parser
	}
}

func (f fakeParser) ParseFile(path string, emit func(model.FlowRecord) error) error {
	for _, record := range f.recordsByPath[path] {
		if err := emit(record); err != nil {
			return err
		}
	}

	return nil
}

type sharedParserState struct {
	active        atomic.Int32
	errByPath     map[string]error
	maxActive     atomic.Int32
	recordsByPath map[string][]model.FlowRecord
	releaseByPath map[string]chan struct{}
	startedPaths  chan string
}

func (s *sharedParserState) newParser() model.FlowParser {
	return concurrentFakeParser{state: s}
}

type concurrentFakeParser struct {
	state *sharedParserState
}

func (p concurrentFakeParser) ParseFile(path string, emit func(model.FlowRecord) error) error {
	activeCount := p.state.active.Add(1)
	updateMaxActive(&p.state.maxActive, activeCount)
	defer p.state.active.Add(-1)

	if p.state.startedPaths != nil {
		p.state.startedPaths <- path
	}

	if releaseChannel, ok := p.state.releaseByPath[path]; ok {
		<-releaseChannel
	}

	if err, ok := p.state.errByPath[path]; ok {
		return err
	}

	for _, record := range p.state.recordsByPath[path] {
		if err := emit(record); err != nil {
			return err
		}
	}

	return nil
}

func updateMaxActive(maxActive *atomic.Int32, activeCount int32) {
	for {
		currentMaxActive := maxActive.Load()
		if activeCount <= currentMaxActive {
			return
		}

		if maxActive.CompareAndSwap(currentMaxActive, activeCount) {
			return
		}
	}
}

func sampleRecord() model.FlowRecord {
	return sampleRecordForIP("192.0.2.1", 10)
}

func sampleRecordForIP(srcIP string, timeStartNs int64) model.FlowRecord {
	return model.FlowRecord{
		Bytes:       1,
		DurationNs:  2,
		DstIP:       "198.51.100.1",
		DstPort:     443,
		Packets:     3,
		Protocol:    6,
		SrcIP:       srcIP,
		SrcPort:     12345,
		TimeEndNs:   timeStartNs + 10,
		TimeStartNs: timeStartNs,
	}
}

func testNow() time.Time {
	return time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
}

func mustSourceFile(t *testing.T, srcDir, relPath string) string {
	t.Helper()

	absPath := filepath.Join(srcDir, relPath)
	assert.NilError(t, os.MkdirAll(filepath.Dir(absPath), 0o755))
	assert.NilError(t, os.WriteFile(absPath, []byte("fixture"), 0o600))
	return absPath
}

func fileExists(path string) error {
	_, err := os.Stat(path)
	return err
}

func fileExistsBool(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func waitForStartedPaths(t *testing.T, startedPaths <-chan string, count int) {
	t.Helper()

	for range count {
		select {
		case <-startedPaths:
		case <-time.After(runTimeout):
			t.Fatal("timed out waiting for parser start")
		}
	}
}

func waitForRunResult(t *testing.T, resultChannel <-chan error) error {
	t.Helper()

	select {
	case err := <-resultChannel:
		return err
	case <-time.After(runTimeout):
		t.Fatal("timed out waiting for run result")
		return nil
	}
}

func readRows(t *testing.T, path string) []parquetout.Row {
	t.Helper()

	file, err := os.Open(path)
	assert.NilError(t, err)
	defer file.Close()

	reader := parquet.NewGenericReader[parquetout.Row](file)
	defer reader.Close()

	var rows []parquetout.Row
	buffer := make([]parquetout.Row, 2)
	for {
		rowCount, err := reader.Read(buffer)
		rows = append(rows, buffer[:rowCount]...)
		if err == nil {
			continue
		}

		if errors.Is(err, io.EOF) {
			return rows
		}

		assert.NilError(t, err)
	}
}

func rowsContainSrcIP(rows []parquetout.Row, srcIP string) bool {
	for _, row := range rows {
		if row.SrcIP == srcIP {
			return true
		}
	}

	return false
}
