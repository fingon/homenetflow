package lokileech

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	filenameDateLayout = "2006-01-02"
	jsonlSuffix        = ".jsonl"
	newSuffix          = ".new"
	stderrSuffix       = ".stderr"
)

type Config struct {
	Addr             string
	Batch            int
	Days             int
	DstPath          string
	HTTPClient       *http.Client
	Now              time.Time
	ParallelDuration time.Duration
	ParallelWorkers  int
	Query            string
	AlsoToday        bool
}

type Client struct {
	cfg Config
}

type dayJob struct {
	day   time.Time
	label string
}

type rangeJob struct {
	index int
	from  time.Time
	to    time.Time
}

type rangeResult struct {
	err     error
	entries []LogEntry
	index   int
}

type queryRangeResponse struct {
	Data   queryRangeData `json:"data"`
	Status string         `json:"status"`
	Error  string         `json:"error"`
}

type queryRangeData struct {
	Result []streamResult `json:"result"`
}

type streamResult struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

type LogEntry struct {
	Labels    map[string]string `json:"labels"`
	Line      string            `json:"line"`
	Timestamp string            `json:"timestamp"`
	time      time.Time
}

func NewClient(cfg Config) (*Client, error) {
	normalizedCfg := cfg
	if normalizedCfg.HTTPClient == nil {
		normalizedCfg.HTTPClient = http.DefaultClient
	}
	if normalizedCfg.Now.IsZero() {
		normalizedCfg.Now = time.Now().UTC()
	} else {
		normalizedCfg.Now = normalizedCfg.Now.UTC()
	}

	if strings.TrimSpace(normalizedCfg.Addr) == "" {
		return nil, errors.New("addr must not be empty")
	}
	if normalizedCfg.Batch < 1 {
		return nil, fmt.Errorf("batch must be positive: %d", normalizedCfg.Batch)
	}
	if normalizedCfg.Days < 1 {
		return nil, fmt.Errorf("days must be positive: %d", normalizedCfg.Days)
	}
	if normalizedCfg.DstPath == "" {
		return nil, errors.New("dst path must not be empty")
	}
	if normalizedCfg.ParallelDuration <= 0 {
		return nil, fmt.Errorf("parallel duration must be positive: %s", normalizedCfg.ParallelDuration)
	}
	if normalizedCfg.ParallelWorkers < 1 {
		return nil, fmt.Errorf("parallel workers must be positive: %d", normalizedCfg.ParallelWorkers)
	}
	if strings.TrimSpace(normalizedCfg.Query) == "" {
		return nil, errors.New("query must not be empty")
	}

	return &Client{cfg: normalizedCfg}, nil
}

func Run(ctx context.Context, cfg Config) error {
	client, err := NewClient(cfg)
	if err != nil {
		return err
	}

	return client.Run(ctx)
}

func (c *Client) Run(ctx context.Context) error {
	if err := os.MkdirAll(c.cfg.DstPath, 0o755); err != nil {
		return fmt.Errorf("create destination directory %q: %w", c.cfg.DstPath, err)
	}

	if c.cfg.AlsoToday {
		if err := c.deleteNewestDailyOutput(); err != nil {
			return err
		}
	}

	for _, job := range c.dayJobs() {
		if err := c.fetchDayIfNeeded(ctx, job); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) dayJobs() []dayJob {
	today := dayStartUTC(c.cfg.Now)
	firstOffset := 1
	if c.cfg.AlsoToday {
		firstOffset = 0
	}

	jobs := make([]dayJob, 0, c.cfg.Days)
	for offset := firstOffset; offset < firstOffset+c.cfg.Days; offset++ {
		day := today.AddDate(0, 0, -offset)
		jobs = append(jobs, dayJob{day: day, label: day.Format(filenameDateLayout)})
	}

	return jobs
}

func dayStartUTC(t time.Time) time.Time {
	utc := t.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}

func (c *Client) deleteNewestDailyOutput() error {
	entries, err := os.ReadDir(c.cfg.DstPath)
	if err != nil {
		return fmt.Errorf("read destination directory %q: %w", c.cfg.DstPath, err)
	}

	var newest string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), jsonlSuffix) {
			continue
		}

		label := strings.TrimSuffix(entry.Name(), jsonlSuffix)
		if _, err := time.Parse(filenameDateLayout, label); err != nil {
			continue
		}
		if newest == "" || entry.Name() > newest {
			newest = entry.Name()
		}
	}
	if newest == "" {
		return nil
	}

	paths := []string{
		filepath.Join(c.cfg.DstPath, newest),
		filepath.Join(c.cfg.DstPath, newest+newSuffix),
		filepath.Join(c.cfg.DstPath, newest+stderrSuffix),
	}
	for _, path := range paths {
		if err := removeIfExists(path); err != nil {
			return err
		}
	}

	slog.Info("deleted newest daily output", "file", newest)

	return nil
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %q: %w", path, err)
	}

	return nil
}

func (c *Client) fetchDayIfNeeded(ctx context.Context, job dayJob) error {
	outPath := filepath.Join(c.cfg.DstPath, job.label+jsonlSuffix)
	fileInfo, err := os.Stat(outPath)
	if err == nil && fileInfo.Size() > 0 {
		slog.Info("skipping existing log file", "day", job.label, "path", outPath)
		return nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat %q: %w", outPath, err)
	}

	if err := c.fetchDay(ctx, job.day, outPath); err != nil {
		return err
	}

	return nil
}

func (c *Client) fetchDay(ctx context.Context, day time.Time, outPath string) error {
	tmpPath := outPath + newSuffix
	if err := removeIfExists(tmpPath); err != nil {
		return err
	}

	slog.Info("fetching daily logs", "day", day.Format(filenameDateLayout), "path", outPath)

	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create temp output %q: %w", tmpPath, err)
	}

	writeErr := c.writeDay(ctx, file, day)
	closeErr := file.Close()
	if writeErr != nil {
		if err := removeIfExists(tmpPath); err != nil {
			return errors.Join(writeErr, err)
		}

		return writeErr
	}
	if closeErr != nil {
		if err := removeIfExists(tmpPath); err != nil {
			return errors.Join(fmt.Errorf("close temp output %q: %w", tmpPath, closeErr), err)
		}

		return fmt.Errorf("close temp output %q: %w", tmpPath, closeErr)
	}

	if err := os.Rename(tmpPath, outPath); err != nil {
		if removeErr := removeIfExists(tmpPath); removeErr != nil {
			return errors.Join(fmt.Errorf("rename %q to %q: %w", tmpPath, outPath, err), removeErr)
		}

		return fmt.Errorf("rename %q to %q: %w", tmpPath, outPath, err)
	}

	fileInfo, err := os.Stat(outPath)
	if err != nil {
		return fmt.Errorf("stat %q: %w", outPath, err)
	}
	if fileInfo.Size() == 0 {
		if err := removeIfExists(outPath); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) writeDay(ctx context.Context, writer io.Writer, day time.Time) error {
	entries, err := c.fetchDayEntries(ctx, day)
	if err != nil {
		return err
	}

	bufferedWriter := bufio.NewWriter(writer)
	encoder := json.NewEncoder(bufferedWriter)
	for _, entry := range entries {
		if err := encoder.Encode(entry); err != nil {
			return fmt.Errorf("write log entry: %w", err)
		}
	}
	if err := bufferedWriter.Flush(); err != nil {
		return fmt.Errorf("flush log output: %w", err)
	}

	return nil
}

func (c *Client) fetchDayEntries(ctx context.Context, day time.Time) ([]LogEntry, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ranges := c.rangeJobs(day, day.AddDate(0, 0, 1))
	rangeJobsCh := make(chan rangeJob)
	resultsCh := make(chan rangeResult, len(ranges))

	workerCount := min(c.cfg.ParallelWorkers, len(ranges))
	var wg sync.WaitGroup
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range rangeJobsCh {
				entries, err := c.queryCompleteRange(ctx, job.from, job.to)
				resultsCh <- rangeResult{err: err, entries: entries, index: job.index}
			}
		}()
	}

	go func() {
		defer close(rangeJobsCh)
		for _, job := range ranges {
			select {
			case <-ctx.Done():
				return
			case rangeJobsCh <- job:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	results := make([][]LogEntry, len(ranges))
	for result := range resultsCh {
		if result.err != nil {
			cancel()
			return nil, result.err
		}
		results[result.index] = result.entries
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("fetch daily logs canceled: %w", err)
	}

	entries := make([]LogEntry, 0)
	for index := len(results) - 1; index >= 0; index-- {
		entries = append(entries, results[index]...)
	}

	return entries, nil
}

func (c *Client) rangeJobs(from, to time.Time) []rangeJob {
	jobs := make([]rangeJob, 0, int(to.Sub(from)/c.cfg.ParallelDuration)+1)
	for index, start := 0, from; start.Before(to); index, start = index+1, start.Add(c.cfg.ParallelDuration) {
		end := start.Add(c.cfg.ParallelDuration)
		if end.After(to) {
			end = to
		}
		jobs = append(jobs, rangeJob{index: index, from: start, to: end})
	}

	return jobs
}

func (c *Client) queryCompleteRange(ctx context.Context, from, to time.Time) ([]LogEntry, error) {
	rangeEnd := to
	entries := make([]LogEntry, 0)
	seen := make(map[string]struct{})
	for rangeEnd.After(from) {
		batchEntries, err := c.queryRange(ctx, from, rangeEnd)
		if err != nil {
			return nil, err
		}
		if len(batchEntries) == 0 {
			return entries, nil
		}

		for _, entry := range batchEntries {
			key, err := entry.dedupeKey()
			if err != nil {
				return nil, err
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			entries = append(entries, entry)
		}

		if len(batchEntries) < c.cfg.Batch {
			return entries, nil
		}

		oldest := batchEntries[len(batchEntries)-1].time
		if !oldest.Before(rangeEnd) {
			return nil, fmt.Errorf("query pagination did not advance from %s", rangeEnd.Format(time.RFC3339Nano))
		}
		rangeEnd = oldest
	}

	return entries, nil
}

func (e LogEntry) dedupeKey() (string, error) {
	labelsBytes, err := json.Marshal(e.Labels)
	if err != nil {
		return "", fmt.Errorf("marshal log entry labels: %w", err)
	}

	return e.Timestamp + "\x00" + e.Line + "\x00" + string(labelsBytes), nil
}

func (c *Client) queryRange(ctx context.Context, from, to time.Time) ([]LogEntry, error) {
	endpoint, err := url.Parse(strings.TrimRight(c.cfg.Addr, "/") + "/loki/api/v1/query_range")
	if err != nil {
		return nil, fmt.Errorf("parse Loki address %q: %w", c.cfg.Addr, err)
	}

	values := endpoint.Query()
	values.Set("direction", "BACKWARD")
	values.Set("end", formatUnixNano(to))
	values.Set("limit", strconv.Itoa(c.cfg.Batch))
	values.Set("query", c.cfg.Query)
	values.Set("start", formatUnixNano(from))
	endpoint.RawQuery = values.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create Loki request: %w", err)
	}

	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query Loki range %s to %s: %w", from.Format(time.RFC3339), to.Format(time.RFC3339), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if err != nil {
			return nil, fmt.Errorf("read Loki error response: %w", err)
		}

		return nil, fmt.Errorf("loki query failed status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	var response queryRangeResponse
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&response); err != nil {
		return nil, fmt.Errorf("decode Loki query response: %w", err)
	}
	if response.Status != "success" {
		return nil, fmt.Errorf("loki query failed status=%q error=%q", response.Status, response.Error)
	}

	entries, err := response.logEntries()
	if err != nil {
		return nil, err
	}

	return entries, nil
}

func formatUnixNano(t time.Time) string {
	return strconv.FormatInt(t.UTC().UnixNano(), 10)
}

func (r queryRangeResponse) logEntries() ([]LogEntry, error) {
	entries := make([]LogEntry, 0)
	for _, stream := range r.Data.Result {
		for _, value := range stream.Values {
			if len(value) != 2 {
				return nil, fmt.Errorf("loki value has %d fields", len(value))
			}

			entryTime, timestamp, err := parseLokiTimestamp(value[0])
			if err != nil {
				return nil, err
			}

			entries = append(entries, LogEntry{
				Labels:    stream.Stream,
				Line:      value[1],
				Timestamp: timestamp,
				time:      entryTime,
			})
		}
	}

	slices.SortStableFunc(entries, func(a, b LogEntry) int {
		if a.time.Equal(b.time) {
			return strings.Compare(a.Line, b.Line)
		}
		if a.time.After(b.time) {
			return -1
		}
		return 1
	})

	return entries, nil
}

func parseLokiTimestamp(value string) (time.Time, string, error) {
	timestampNS, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("parse Loki timestamp %q: %w", value, err)
	}

	entryTime := time.Unix(0, timestampNS).UTC()
	return entryTime, entryTime.Format(time.RFC3339Nano), nil
}
