package lokileech

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

const testQuery = `{source="dnsmasq"}`

func TestDayJobs(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 11, 12, 34, 56, 0, time.FixedZone("EEST", 3*60*60))
	defaultCfg := validTestConfig(t)

	tests := []struct {
		name      string
		cfg       Config
		wantLabel []string
	}{
		{
			name: "without also today starts yesterday",
			cfg: Config{
				Addr:             defaultCfg.Addr,
				Batch:            defaultCfg.Batch,
				Days:             3,
				DstPath:          defaultCfg.DstPath,
				Now:              now,
				ParallelDuration: defaultCfg.ParallelDuration,
				ParallelWorkers:  defaultCfg.ParallelWorkers,
				Query:            defaultCfg.Query,
			},
			wantLabel: []string{"2026-04-10", "2026-04-09", "2026-04-08"},
		},
		{
			name: "also today starts today",
			cfg: Config{
				Addr:             defaultCfg.Addr,
				Batch:            defaultCfg.Batch,
				Days:             3,
				DstPath:          defaultCfg.DstPath,
				Now:              now,
				ParallelDuration: defaultCfg.ParallelDuration,
				ParallelWorkers:  defaultCfg.ParallelWorkers,
				Query:            defaultCfg.Query,
				AlsoToday:        true,
			},
			wantLabel: []string{"2026-04-11", "2026-04-10", "2026-04-09"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client, err := NewClient(test.cfg)
			assert.NilError(t, err)

			jobs := client.dayJobs()
			gotLabels := make([]string, 0, len(jobs))
			for _, job := range jobs {
				gotLabels = append(gotLabels, job.label)
			}

			assert.DeepEqual(t, gotLabels, test.wantLabel)
		})
	}
}

func TestAlsoTodayDeletesNewestDailyOutput(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	mustWriteFile(t, filepath.Join(tempDir, "2026-04-09.jsonl"), "old\n")
	mustWriteFile(t, filepath.Join(tempDir, "2026-04-10.jsonl"), "newest\n")
	mustWriteFile(t, filepath.Join(tempDir, "2026-04-10.jsonl.new"), "partial\n")
	mustWriteFile(t, filepath.Join(tempDir, "2026-04-10.jsonl.stderr"), "stderr\n")
	mustWriteFile(t, filepath.Join(tempDir, "not-a-day.jsonl"), "kept\n")

	cfg := validTestConfig(t)
	cfg.DstPath = tempDir
	client, err := NewClient(cfg)
	assert.NilError(t, err)

	assert.NilError(t, client.deleteNewestDailyOutput())

	assertFileContent(t, filepath.Join(tempDir, "2026-04-09.jsonl"), "old\n")
	assertFileContent(t, filepath.Join(tempDir, "not-a-day.jsonl"), "kept\n")
	assertFileMissing(t, filepath.Join(tempDir, "2026-04-10.jsonl"))
	assertFileMissing(t, filepath.Join(tempDir, "2026-04-10.jsonl.new"))
	assertFileMissing(t, filepath.Join(tempDir, "2026-04-10.jsonl.stderr"))
}

func TestRunSkipsNonEmptyFilesAndFetchesEmptyFiles(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	mustWriteFile(t, filepath.Join(tempDir, "2026-04-10.jsonl"), "already here\n")
	mustWriteFile(t, filepath.Join(tempDir, "2026-04-09.jsonl"), "")

	requests := make([]string, 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.Query().Get("start"))
		writeLokiResponse(t, w, []testLogEntry{
			{Labels: map[string]string{"source": "dnsmasq"}, Line: `{"message":"fetched"}`, Time: time.Date(2026, 4, 9, 1, 2, 3, 0, time.UTC)},
		})
	}))
	defer server.Close()

	err := Run(context.Background(), Config{
		Addr:             server.URL,
		Batch:            5000,
		Days:             2,
		DstPath:          tempDir,
		Now:              time.Date(2026, 4, 11, 9, 0, 0, 0, time.UTC),
		ParallelDuration: 24 * time.Hour,
		ParallelWorkers:  1,
		Query:            testQuery,
	})
	assert.NilError(t, err)

	assert.Equal(t, len(requests), 1)
	assertFileContent(t, filepath.Join(tempDir, "2026-04-10.jsonl"), "already here\n")

	var entry LogEntry
	readJSONLine(t, filepath.Join(tempDir, "2026-04-09.jsonl"), &entry)
	assert.Equal(t, entry.Timestamp, "2026-04-09T01:02:03Z")
	assert.DeepEqual(t, entry.Labels, map[string]string{"source": "dnsmasq"})
	assert.Equal(t, entry.Line, `{"message":"fetched"}`)
}

func TestQueryRangeParametersAndJSONLShape(t *testing.T) {
	t.Parallel()

	var gotQuery map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, r.URL.Path, "/loki/api/v1/query_range")
		gotQuery = map[string]string{
			"direction": r.URL.Query().Get("direction"),
			"end":       r.URL.Query().Get("end"),
			"limit":     r.URL.Query().Get("limit"),
			"query":     r.URL.Query().Get("query"),
			"start":     r.URL.Query().Get("start"),
		}
		writeLokiResponse(t, w, []testLogEntry{
			{Labels: map[string]string{"source": "dnsmasq"}, Line: `{"message":"newer"}`, Time: time.Date(2026, 4, 10, 0, 2, 0, 0, time.UTC)},
			{Labels: map[string]string{"source": "ip_neighbour"}, Line: `{"message":"older"}`, Time: time.Date(2026, 4, 10, 0, 1, 0, 0, time.UTC)},
		})
	}))
	defer server.Close()

	cfg := validTestConfig(t)
	cfg.Addr = server.URL
	cfg.Batch = 123
	cfg.Query = `{source=~"dnsmasq|ip_neighbour"}`
	client, err := NewClient(cfg)
	assert.NilError(t, err)

	from := time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 4, 10, 0, 15, 0, 0, time.UTC)
	entries, err := client.queryRange(context.Background(), from, to)
	assert.NilError(t, err)

	assert.DeepEqual(t, gotQuery, map[string]string{
		"direction": "BACKWARD",
		"end":       strconv.FormatInt(to.UnixNano(), 10),
		"limit":     "123",
		"query":     `{source=~"dnsmasq|ip_neighbour"}`,
		"start":     strconv.FormatInt(from.UnixNano(), 10),
	})
	assert.Equal(t, len(entries), 2)
	assert.Equal(t, entries[0].Timestamp, "2026-04-10T00:02:00Z")
	assert.DeepEqual(t, entries[0].Labels, map[string]string{"source": "dnsmasq"})
	assert.Equal(t, entries[0].Line, `{"message":"newer"}`)
}

func TestQueryCompleteRangePaginatesFullBatches(t *testing.T) {
	t.Parallel()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		switch requests {
		case 1:
			writeLokiResponse(t, w, []testLogEntry{
				{Labels: map[string]string{}, Line: "three", Time: time.Date(2026, 4, 10, 0, 3, 0, 0, time.UTC)},
				{Labels: map[string]string{}, Line: "two", Time: time.Date(2026, 4, 10, 0, 2, 0, 0, time.UTC)},
			})
		case 2:
			writeLokiResponse(t, w, []testLogEntry{
				{Labels: map[string]string{}, Line: "two", Time: time.Date(2026, 4, 10, 0, 2, 0, 0, time.UTC)},
				{Labels: map[string]string{}, Line: "one", Time: time.Date(2026, 4, 10, 0, 1, 0, 0, time.UTC)},
			})
		default:
			writeLokiResponse(t, w, nil)
		}
	}))
	defer server.Close()

	cfg := validTestConfig(t)
	cfg.Addr = server.URL
	cfg.Batch = 2
	client, err := NewClient(cfg)
	assert.NilError(t, err)

	entries, err := client.queryCompleteRange(
		context.Background(),
		time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 4, 10, 0, 4, 0, 0, time.UTC),
	)
	assert.NilError(t, err)

	lines := make([]string, 0, len(entries))
	for _, entry := range entries {
		lines = append(lines, entry.Line)
	}
	assert.DeepEqual(t, lines, []string{"three", "two", "one"})
	assert.Equal(t, requests, 3)
}

func TestNewClientRequiresUserFacingConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(Config) Config
		wantErr string
	}{
		{
			name: "addr",
			mutate: func(cfg Config) Config {
				cfg.Addr = ""
				return cfg
			},
			wantErr: "addr must not be empty",
		},
		{
			name: "batch",
			mutate: func(cfg Config) Config {
				cfg.Batch = 0
				return cfg
			},
			wantErr: "batch must be positive: 0",
		},
		{
			name: "days",
			mutate: func(cfg Config) Config {
				cfg.Days = 0
				return cfg
			},
			wantErr: "days must be positive: 0",
		},
		{
			name: "dst path",
			mutate: func(cfg Config) Config {
				cfg.DstPath = ""
				return cfg
			},
			wantErr: "dst path must not be empty",
		},
		{
			name: "parallel duration",
			mutate: func(cfg Config) Config {
				cfg.ParallelDuration = 0
				return cfg
			},
			wantErr: "parallel duration must be positive: 0s",
		},
		{
			name: "parallel workers",
			mutate: func(cfg Config) Config {
				cfg.ParallelWorkers = 0
				return cfg
			},
			wantErr: "parallel workers must be positive: 0",
		},
		{
			name: "query",
			mutate: func(cfg Config) Config {
				cfg.Query = " \t"
				return cfg
			},
			wantErr: "query must not be empty",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewClient(test.mutate(validTestConfig(t)))
			assert.Error(t, err, test.wantErr)
		})
	}
}

type testLogEntry struct {
	Labels map[string]string
	Line   string
	Time   time.Time
}

func validTestConfig(t *testing.T) Config {
	t.Helper()

	return Config{
		Addr:             "http://127.0.0.1:3100",
		Batch:            5000,
		Days:             1,
		DstPath:          t.TempDir(),
		ParallelDuration: 15 * time.Minute,
		ParallelWorkers:  1,
		Query:            testQuery,
	}
}

func writeLokiResponse(t *testing.T, w http.ResponseWriter, entries []testLogEntry) {
	t.Helper()

	streamsByLabel := make(map[string]streamResult)
	for _, entry := range entries {
		key := fmt.Sprintf("%v", entry.Labels)
		stream := streamsByLabel[key]
		if stream.Stream == nil {
			stream.Stream = entry.Labels
		}
		stream.Values = append(stream.Values, []string{strconv.FormatInt(entry.Time.UnixNano(), 10), entry.Line})
		streamsByLabel[key] = stream
	}

	streams := make([]streamResult, 0, len(streamsByLabel))
	for _, stream := range streamsByLabel {
		streams = append(streams, stream)
	}

	w.Header().Set("Content-Type", "application/json")
	assert.NilError(t, json.NewEncoder(w).Encode(queryRangeResponse{
		Data:   queryRangeData{Result: streams},
		Status: "success",
	}))
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()

	assert.NilError(t, os.WriteFile(path, []byte(content), 0o644))
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()

	gotBytes, err := os.ReadFile(path)
	assert.NilError(t, err)
	assert.Equal(t, string(gotBytes), want)
}

func assertFileMissing(t *testing.T, path string) {
	t.Helper()

	_, err := os.Stat(path)
	assert.Assert(t, os.IsNotExist(err))
}

func readJSONLine(t *testing.T, path string, out any) {
	t.Helper()

	file, err := os.Open(path)
	assert.NilError(t, err)
	defer file.Close()

	assert.NilError(t, json.NewDecoder(file).Decode(out))
}
