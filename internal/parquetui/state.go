package parquetui

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	defaultEdgeLimit = 100
	defaultPage      = 1
	defaultPageSize  = 100
	defaultPort      = 8080
	defaultView      = ViewSplit
	presetAllValue   = "all"
	presetHourValue  = "1h"
	presetDayValue   = "1d"
	presetDayLegacy  = "24h"
	presetWeekValue  = "7d"
	presetMonthValue = "30d"
)

type Metric string

const (
	MetricBytes       Metric = "bytes"
	MetricConnections Metric = "connections"
)

type Granularity string

const (
	GranularityTLD      Granularity = "tld"
	Granularity2LD      Granularity = "2ld"
	GranularityHostname Granularity = "hostname"
	GranularityIP       Granularity = "ip"
)

type View string

const (
	ViewGraph View = "graph"
	ViewTable View = "table"
	ViewSplit View = "split"
)

type TableSort string

const (
	SortBytes       TableSort = "bytes"
	SortConnections TableSort = "connections"
	SortFirstSeen   TableSort = "first_seen"
	SortLastSeen    TableSort = "last_seen"
	SortSource      TableSort = "source"
	SortDestination TableSort = "destination"
)

type QueryState struct {
	EdgeLimit       int
	Exclude         []string
	FromNs          int64
	Granularity     Granularity
	Include         []string
	NodeLimit       int
	Page            int
	PageSize        int
	Search          string
	SelectedEdgeDst string
	SelectedEdgeSrc string
	SelectedEntity  string
	Sort            TableSort
	ToNs            int64
	View            View
	Metric          Metric
	Preset          string
}

//nolint:tagliatelle
type ClientState struct {
	EdgeLimit       int      `json:"edge_limit"`
	Exclude         []string `json:"exclude"`
	From            int64    `json:"from"`
	Granularity     string   `json:"granularity"`
	Include         []string `json:"include"`
	Metric          string   `json:"metric"`
	NodeLimit       int      `json:"node_limit"`
	Page            int      `json:"page"`
	PageSize        int      `json:"page_size"`
	Search          string   `json:"search"`
	SelectedEdgeDst string   `json:"selected_edge_dst"`
	SelectedEdgeSrc string   `json:"selected_edge_src"`
	SelectedEntity  string   `json:"selected_entity"`
	Sort            string   `json:"sort"`
	To              int64    `json:"to"`
	View            string   `json:"view"`
}

type ClientSpan struct {
	End   int64 `json:"end"`
	Start int64 `json:"start"`
}

func ParseQueryState(r *http.Request) QueryState {
	query := r.URL.Query()
	state := QueryState{
		EdgeLimit:       defaultEdgeLimit,
		Exclude:         compactValues(query["exclude"]),
		Granularity:     Granularity2LD,
		Include:         compactValues(query["include"]),
		Page:            defaultPage,
		PageSize:        defaultPageSize,
		Search:          strings.TrimSpace(query.Get("search")),
		SelectedEdgeDst: strings.TrimSpace(query.Get("selected_edge_dst")),
		SelectedEdgeSrc: strings.TrimSpace(query.Get("selected_edge_src")),
		SelectedEntity:  strings.TrimSpace(query.Get("selected_entity")),
		Sort:            defaultSortForMetric(MetricBytes),
		View:            defaultView,
		Metric:          MetricBytes,
		Preset:          strings.TrimSpace(query.Get("preset")),
	}

	if metric := Metric(query.Get("metric")); metric.valid() {
		state.Metric = metric
		state.Sort = defaultSortForMetric(metric)
	}

	if granularity := Granularity(query.Get("granularity")); granularity.valid() {
		state.Granularity = granularity
	}

	if view := View(query.Get("view")); view.valid() {
		state.View = view
	}

	if sort := TableSort(query.Get("sort")); sort.valid() {
		state.Sort = sort
	}

	state.FromNs = parseInt64(query.Get("from"))
	state.ToNs = parseInt64(query.Get("to"))

	if page := parsePositiveInt(query.Get("page")); page > 0 {
		state.Page = page
	}

	if pageSize := parsePositiveInt(query.Get("page_size")); pageSize > 0 {
		state.PageSize = pageSize
	}

	if edgeLimit := parseNonNegativeInt(query.Get("edge_limit")); edgeLimit >= 0 {
		state.EdgeLimit = edgeLimit
	}

	if nodeLimit := parseNonNegativeInt(query.Get("node_limit")); nodeLimit >= 0 {
		state.NodeLimit = nodeLimit
	}

	return state
}

func (s QueryState) Normalized(span TimeSpan) QueryState {
	state := s
	if !state.Granularity.valid() {
		state.Granularity = Granularity2LD
	}
	if !state.Metric.valid() {
		state.Metric = MetricBytes
	}
	if !state.View.valid() {
		state.View = defaultView
	}
	if !state.Sort.valid() {
		state.Sort = defaultSortForMetric(state.Metric)
	}
	if state.EdgeLimit < 0 {
		state.EdgeLimit = defaultEdgeLimit
	}
	if state.Page <= 0 {
		state.Page = defaultPage
	}
	if state.PageSize <= 0 {
		state.PageSize = defaultPageSize
	}
	if state.FromNs == 0 && state.ToNs == 0 {
		if fromNs, toNs, ok := presetRange(state.Preset, span); ok {
			state.FromNs = fromNs
			state.ToNs = toNs
		}
	}
	if state.FromNs == 0 || state.FromNs < span.StartNs {
		state.FromNs = span.StartNs
	}
	if state.ToNs == 0 || state.ToNs > span.EndNs {
		state.ToNs = span.EndNs
	}
	if state.FromNs > state.ToNs {
		state.FromNs = span.StartNs
		state.ToNs = span.EndNs
	}
	if state.NodeLimit == 0 {
		state.NodeLimit = defaultNodeLimit(state.Granularity)
	}
	return state
}

func (s QueryState) Values() url.Values {
	values := make(url.Values)
	if s.FromNs > 0 {
		values.Set("from", strconv.FormatInt(s.FromNs, 10))
	}
	if s.ToNs > 0 {
		values.Set("to", strconv.FormatInt(s.ToNs, 10))
	}
	values.Set("metric", string(s.Metric))
	values.Set("granularity", string(s.Granularity))
	values.Set("view", string(s.View))
	values.Set("sort", string(s.Sort))
	if s.EdgeLimit != defaultEdgeLimit {
		values.Set("edge_limit", strconv.Itoa(s.EdgeLimit))
	}
	if s.NodeLimit > 0 && s.NodeLimit != defaultNodeLimit(s.Granularity) {
		values.Set("node_limit", strconv.Itoa(s.NodeLimit))
	}
	if s.Page != defaultPage {
		values.Set("page", strconv.Itoa(s.Page))
	}
	if s.PageSize != defaultPageSize {
		values.Set("page_size", strconv.Itoa(s.PageSize))
	}
	if s.Search != "" {
		values.Set("search", s.Search)
	}
	if s.SelectedEntity != "" {
		values.Set("selected_entity", s.SelectedEntity)
	}
	if s.SelectedEdgeSrc != "" {
		values.Set("selected_edge_src", s.SelectedEdgeSrc)
	}
	if s.SelectedEdgeDst != "" {
		values.Set("selected_edge_dst", s.SelectedEdgeDst)
	}
	for _, value := range s.Include {
		values.Add("include", value)
	}
	for _, value := range s.Exclude {
		values.Add("exclude", value)
	}
	return values
}

func (s QueryState) Clone() QueryState {
	clone := s
	clone.Include = append([]string(nil), s.Include...)
	clone.Exclude = append([]string(nil), s.Exclude...)
	return clone
}

func (s QueryState) ClientState() ClientState {
	return ClientState{
		EdgeLimit:       s.EdgeLimit,
		Exclude:         append([]string(nil), s.Exclude...),
		From:            s.FromNs,
		Granularity:     string(s.Granularity),
		Include:         append([]string(nil), s.Include...),
		Metric:          string(s.Metric),
		NodeLimit:       s.NodeLimit,
		Page:            s.Page,
		PageSize:        s.PageSize,
		Search:          s.Search,
		SelectedEdgeDst: s.SelectedEdgeDst,
		SelectedEdgeSrc: s.SelectedEdgeSrc,
		SelectedEntity:  s.SelectedEntity,
		Sort:            string(s.Sort),
		To:              s.ToNs,
		View:            string(s.View),
	}
}

func (s TimeSpan) ClientSpan() ClientSpan {
	return ClientSpan{
		End:   s.EndNs,
		Start: s.StartNs,
	}
}

func (s QueryState) WithIncluded(entity string) QueryState {
	state := s.Clone()
	if entity == "" || slices.Contains(state.Include, entity) {
		return state
	}
	state.Include = append(state.Include, entity)
	return state
}

func (s QueryState) WithExcluded(entity string) QueryState {
	state := s.Clone()
	if entity == "" || slices.Contains(state.Exclude, entity) {
		return state
	}
	state.Exclude = append(state.Exclude, entity)
	return state
}

func (s QueryState) ResetSelection() QueryState {
	state := s.Clone()
	state.SelectedEntity = ""
	state.SelectedEdgeSrc = ""
	state.SelectedEdgeDst = ""
	return state
}

func (s QueryState) layoutCacheState() QueryState {
	state := s.Clone()
	state.Metric = MetricBytes
	state.View = defaultView
	state.Sort = defaultSortForMetric(state.Metric)
	state.Page = defaultPage
	state.PageSize = defaultPageSize
	state.Preset = ""
	return state
}

func defaultNodeLimit(granularity Granularity) int {
	switch granularity {
	case GranularityTLD:
		return 0
	case Granularity2LD:
		return 100
	case GranularityHostname:
		return 150
	case GranularityIP:
		return 200
	default:
		return 100
	}
}

func defaultSortForMetric(metric Metric) TableSort {
	if metric == MetricConnections {
		return SortConnections
	}
	return SortBytes
}

func presetRange(preset string, span TimeSpan) (int64, int64, bool) {
	if span.StartNs == 0 || span.EndNs == 0 || span.StartNs > span.EndNs {
		return 0, 0, false
	}
	if preset == "" || preset == presetAllValue {
		return span.StartNs, span.EndNs, true
	}

	duration := presetDuration(preset)
	if duration <= 0 {
		return 0, 0, false
	}

	fromNs := max(span.StartNs, span.EndNs-int64(duration))
	return fromNs, span.EndNs, true
}

func presetDuration(preset string) time.Duration {
	switch preset {
	case presetHourValue:
		return time.Hour
	case presetDayValue, presetDayLegacy:
		return 24 * time.Hour
	case presetWeekValue:
		return 7 * 24 * time.Hour
	case presetMonthValue:
		return 30 * 24 * time.Hour
	default:
		return 0
	}
}

func compactValues(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	compacted := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		compacted = append(compacted, trimmed)
	}
	return compacted
}

func parseInt64(value string) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func parsePositiveInt(value string) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return 0
	}
	return parsed
}

func parseNonNegativeInt(value string) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed < 0 {
		return -1
	}
	return parsed
}

func (m Metric) valid() bool {
	return m == MetricBytes || m == MetricConnections
}

func (g Granularity) valid() bool {
	return g == GranularityTLD || g == Granularity2LD || g == GranularityHostname || g == GranularityIP
}

func (v View) valid() bool {
	return v == ViewGraph || v == ViewTable || v == ViewSplit
}

func (s TableSort) valid() bool {
	return s == SortBytes || s == SortConnections || s == SortFirstSeen || s == SortLastSeen || s == SortSource || s == SortDestination
}

func (s QueryState) EntityBreadcrumb() string {
	if len(s.Include) == 0 {
		return ""
	}
	return strings.Join(s.Include, ", ")
}

func (s QueryState) DescribeTime() string {
	if s.FromNs == 0 || s.ToNs == 0 {
		return "All"
	}
	return fmt.Sprintf("%d-%d", s.FromNs, s.ToNs)
}
