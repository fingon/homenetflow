package parquetui

import (
	"errors"
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
	falseValue       = "false"
	trueValue        = "true"
	entityActionDays = 7
	presetAllValue   = "all"
	presetHourValue  = "1h"
	presetDayValue   = "1d"
	presetDayLegacy  = "24h"
	presetWeekValue  = "7d"
	presetMonthValue = "30d"
	flowSortDirParam = "flow_sort_dir"
)

var (
	errEntityActionsDisabled  = errors.New("entity actions are available for ranges up to 7 days")
	errLongRangeSummaryNeeded = errors.New("views over 7 days require UI summaries")
)

type Metric string

const (
	MetricBytes       Metric = "bytes"
	MetricConnections Metric = "connections"
	MetricDNSLookups  Metric = "dns_lookups"
)

type Granularity string

const (
	GranularityTLD      Granularity = "tld"
	Granularity2LD      Granularity = "2ld"
	GranularityHostname Granularity = "hostname"
	GranularityIP       Granularity = "ip"
)

type LocalIdentity string

const (
	LocalIdentityAddress LocalIdentity = "address"
	LocalIdentityDevice  LocalIdentity = "device"
)

type TableSort string

const (
	SortBytes       TableSort = "bytes"
	SortConnections TableSort = "connections"
	SortDNSLookups  TableSort = "dns_lookups"
	SortFirstSeen   TableSort = "first_seen"
	SortLastSeen    TableSort = "last_seen"
	SortSource      TableSort = "source"
	SortDestination TableSort = "destination"
)

type FlowSort string

const (
	FlowSortStart       FlowSort = "start"
	FlowSortEnd         FlowSort = "end"
	FlowSortSource      FlowSort = "source"
	FlowSortDestination FlowSort = "destination"
	FlowSortProtocol    FlowSort = "protocol"
	FlowSortPackets     FlowSort = "packets"
	FlowSortBytes       FlowSort = "bytes"
)

type FlowSortDir string

const (
	FlowSortAsc  FlowSortDir = "asc"
	FlowSortDesc FlowSortDir = "desc"
)

type AddressFamily string

const (
	AddressFamilyAll  AddressFamily = "all"
	AddressFamilyIPv4 AddressFamily = "ipv4"
	AddressFamilyIPv6 AddressFamily = "ipv6"
)

type DirectionFilter string

const (
	DirectionBoth    DirectionFilter = "both"
	DirectionEgress  DirectionFilter = "egress"
	DirectionIngress DirectionFilter = "ingress"
)

type FlowScope string

const (
	FlowScopeEdge   FlowScope = "edge"
	FlowScopeEntity FlowScope = "entity"
)

type FlowMatch string

const (
	FlowMatchBoth    FlowMatch = "both"
	FlowMatchForward FlowMatch = "forward"
)

type QueryState struct {
	AddressFamily   AddressFamily
	Direction       DirectionFilter
	EdgeLimit       int
	Exclude         []string
	FromNs          int64
	Granularity     Granularity
	HideIgnored     bool
	HideIgnoredSet  bool
	Include         []string
	NodeLimit       int
	Page            int
	PageSize        int
	Port            int32
	Protocol        int32
	Search          string
	SelectedEdgeDst string
	SelectedEdgeSrc string
	SelectedEntity  string
	Sort            TableSort
	ToNs            int64
	Metric          Metric
	LocalIdentity   LocalIdentity
	Preset          string
}

type FlowQuery struct {
	Destination string
	Entity      string
	Match       FlowMatch
	Scope       FlowScope
	Source      string
	State       QueryState
	Sort        FlowSort
	SortDir     FlowSortDir
}

//nolint:tagliatelle
type ClientState struct {
	AddressFamily   string   `json:"family"`
	Direction       string   `json:"direction"`
	EdgeLimit       int      `json:"edge_limit"`
	Exclude         []string `json:"exclude"`
	From            int64    `json:"from"`
	Granularity     string   `json:"granularity"`
	HideIgnored     bool     `json:"hide_ignored"`
	Include         []string `json:"include"`
	LocalIdentity   string   `json:"local_identity"`
	Metric          string   `json:"metric"`
	NodeLimit       int      `json:"node_limit"`
	Page            int      `json:"page"`
	PageSize        int      `json:"page_size"`
	Port            int32    `json:"port"`
	Protocol        int32    `json:"protocol"`
	Search          string   `json:"search"`
	SelectedEdgeDst string   `json:"selected_edge_dst"`
	SelectedEdgeSrc string   `json:"selected_edge_src"`
	SelectedEntity  string   `json:"selected_entity"`
	Sort            string   `json:"sort"`
	To              int64    `json:"to"`
}

type ClientSpan struct {
	End   int64 `json:"end"`
	Start int64 `json:"start"`
}

func ParseQueryState(r *http.Request) QueryState {
	query := r.URL.Query()
	state := QueryState{
		AddressFamily:   AddressFamilyAll,
		Direction:       DirectionBoth,
		EdgeLimit:       defaultEdgeLimit,
		Exclude:         compactValues(query["exclude"]),
		Granularity:     Granularity2LD,
		HideIgnored:     parseBoolDefaultTrue(query.Get("hide_ignored")),
		HideIgnoredSet:  query.Has("hide_ignored"),
		Include:         compactValues(query["include"]),
		LocalIdentity:   LocalIdentityAddress,
		Page:            defaultPage,
		PageSize:        defaultPageSize,
		Search:          strings.TrimSpace(query.Get("search")),
		SelectedEdgeDst: strings.TrimSpace(query.Get("selected_edge_dst")),
		SelectedEdgeSrc: strings.TrimSpace(query.Get("selected_edge_src")),
		SelectedEntity:  strings.TrimSpace(query.Get("selected_entity")),
		Sort:            defaultSortForMetric(MetricBytes),
		Metric:          MetricBytes,
		Preset:          strings.TrimSpace(query.Get("preset")),
	}

	if addressFamily := AddressFamily(query.Get("family")); addressFamily.valid() {
		state.AddressFamily = addressFamily
	}

	if direction := DirectionFilter(query.Get("direction")); direction.valid() {
		state.Direction = direction
	}

	if metric := Metric(query.Get("metric")); metric.valid() {
		state.Metric = metric
		state.Sort = defaultSortForMetric(metric)
	}

	if granularity := Granularity(query.Get("granularity")); granularity.valid() {
		state.Granularity = granularity
	}

	if localIdentity := LocalIdentity(query.Get("local_identity")); localIdentity.valid() {
		state.LocalIdentity = localIdentity
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

	if protocol := parsePositiveInt32(query.Get("protocol")); protocol > 0 {
		state.Protocol = protocol
	}

	if port := parsePositiveInt32(query.Get("port")); port > 0 {
		state.Port = port
	}

	if edgeLimit := parseNonNegativeInt(query.Get("edge_limit")); edgeLimit >= 0 {
		state.EdgeLimit = edgeLimit
	}

	if nodeLimit := parseNonNegativeInt(query.Get("node_limit")); nodeLimit >= 0 {
		state.NodeLimit = nodeLimit
	}

	return state
}

func ParseFlowQuery(r *http.Request) (FlowQuery, error) {
	query := r.URL.Query()
	flowQuery := FlowQuery{
		Destination: strings.TrimSpace(query.Get("flow_destination")),
		Entity:      strings.TrimSpace(query.Get("flow_entity")),
		Match:       FlowMatch(strings.TrimSpace(query.Get("flow_match"))),
		Scope:       FlowScope(strings.TrimSpace(query.Get("flow_scope"))),
		Source:      strings.TrimSpace(query.Get("flow_source")),
		State:       ParseQueryState(r),
		Sort:        FlowSort(strings.TrimSpace(query.Get("flow_sort"))),
		SortDir:     FlowSortDir(strings.TrimSpace(query.Get(flowSortDirParam))),
	}
	if !flowQuery.Match.valid() {
		flowQuery.Match = FlowMatchBoth
	}
	if !flowQuery.Sort.valid() {
		flowQuery.Sort = FlowSortStart
	}
	if !flowQuery.SortDir.valid() || !flowQuery.Sort.timeSort() {
		flowQuery.SortDir = FlowSortDesc
	}
	if !flowQuery.Scope.valid() {
		return FlowQuery{}, fmt.Errorf("invalid flow scope %q", flowQuery.Scope)
	}
	switch flowQuery.Scope {
	case FlowScopeEntity:
		if flowQuery.Entity == "" {
			return FlowQuery{}, errors.New("flow entity is required")
		}
	case FlowScopeEdge:
		if flowQuery.Source == "" || flowQuery.Destination == "" {
			return FlowQuery{}, errors.New("flow source and destination are required")
		}
	}
	return flowQuery, nil
}

func (s QueryState) Normalized(span TimeSpan) QueryState {
	return s.normalized(span, true)
}

func (s QueryState) NormalizedForFlowDetails(span TimeSpan) QueryState {
	return s.normalized(span, false)
}

func (s QueryState) normalized(span TimeSpan, pruneEntityActions bool) QueryState {
	state := s
	if !state.Granularity.valid() {
		state.Granularity = Granularity2LD
	}
	if !state.LocalIdentity.valid() {
		state.LocalIdentity = LocalIdentityAddress
	}
	if !state.AddressFamily.valid() {
		state.AddressFamily = AddressFamilyAll
	}
	if !state.Direction.valid() {
		state.Direction = DirectionBoth
	}
	if !state.Metric.valid() {
		state.Metric = MetricBytes
	}
	if !state.HideIgnoredSet && !state.HideIgnored {
		state.HideIgnored = true
	}
	if state.Metric == MetricDNSLookups {
		state.Direction = DirectionBoth
		state.Port = 0
		state.Protocol = 0
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
	if pruneEntityActions && !state.EntityActionsEnabled() {
		if state.Granularity != GranularityTLD && state.Granularity != Granularity2LD {
			state.Granularity = Granularity2LD
			if state.NodeLimit == defaultNodeLimit(GranularityHostname) || state.NodeLimit == defaultNodeLimit(GranularityIP) {
				state.NodeLimit = defaultNodeLimit(state.Granularity)
			}
		}
		state = state.ResetSelection()
		state.Include = nil
		state.Exclude = nil
		state.Search = ""
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
	if s.AddressFamily != "" && s.AddressFamily != AddressFamilyAll {
		values.Set("family", string(s.AddressFamily))
	}
	if s.Direction != "" && s.Direction != DirectionBoth {
		values.Set("direction", string(s.Direction))
	}
	values.Set("metric", string(s.Metric))
	values.Set("granularity", string(s.Granularity))
	if s.LocalIdentity != "" && s.LocalIdentity != LocalIdentityAddress {
		values.Set("local_identity", string(s.LocalIdentity))
	}
	values.Set("sort", string(s.Sort))
	if !s.HideIgnored {
		values.Set("hide_ignored", "false")
	}
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
	if s.Protocol > 0 {
		values.Set("protocol", strconv.FormatInt(int64(s.Protocol), 10))
	}
	if s.Port > 0 {
		values.Set("port", strconv.FormatInt(int64(s.Port), 10))
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

func (q FlowQuery) Values() url.Values {
	values := q.State.Values()
	values.Set("flow_scope", string(q.Scope))
	if q.Sort != "" && q.Sort != FlowSortStart {
		values.Set("flow_sort", string(q.Sort))
	}
	if q.Sort.timeSort() && q.SortDir == FlowSortAsc {
		values.Set(flowSortDirParam, string(q.SortDir))
	}
	switch q.Scope {
	case FlowScopeEntity:
		values.Set("flow_entity", q.Entity)
	case FlowScopeEdge:
		values.Set("flow_source", q.Source)
		values.Set("flow_destination", q.Destination)
		if q.Match != "" && q.Match != FlowMatchBoth {
			values.Set("flow_match", string(q.Match))
		}
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
		AddressFamily:   string(s.AddressFamily),
		Direction:       string(s.Direction),
		EdgeLimit:       s.EdgeLimit,
		Exclude:         append([]string(nil), s.Exclude...),
		From:            s.FromNs,
		Granularity:     string(s.Granularity),
		HideIgnored:     s.HideIgnored,
		Include:         append([]string(nil), s.Include...),
		LocalIdentity:   string(s.LocalIdentity),
		Metric:          string(s.Metric),
		NodeLimit:       s.NodeLimit,
		Page:            s.Page,
		PageSize:        s.PageSize,
		Port:            s.Port,
		Protocol:        s.Protocol,
		Search:          s.Search,
		SelectedEdgeDst: s.SelectedEdgeDst,
		SelectedEdgeSrc: s.SelectedEdgeSrc,
		SelectedEntity:  s.SelectedEntity,
		Sort:            string(s.Sort),
		To:              s.ToNs,
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

func (s QueryState) EntityActionsEnabled() bool {
	if s.FromNs == 0 || s.ToNs == 0 || s.ToNs < s.FromNs {
		return true
	}
	return s.ToNs-s.FromNs <= int64(entityActionDays*24*time.Hour)
}

func (s FlowScope) valid() bool {
	return s == FlowScopeEntity || s == FlowScopeEdge
}

func (m FlowMatch) valid() bool {
	return m == FlowMatchBoth || m == FlowMatchForward
}

func (s FlowSort) valid() bool {
	return s == FlowSortStart || s == FlowSortEnd || s == FlowSortSource || s == FlowSortDestination || s == FlowSortProtocol || s == FlowSortPackets || s == FlowSortBytes
}

func (s FlowSort) timeSort() bool {
	return s == FlowSortStart || s == FlowSortEnd
}

func (d FlowSortDir) valid() bool {
	return d == FlowSortAsc || d == FlowSortDesc
}

func (s QueryState) layoutCacheState() QueryState {
	state := s.Clone()
	if state.Metric != MetricDNSLookups {
		state.Metric = MetricBytes
	}
	state.Sort = defaultSortForMetric(state.Metric)
	state.Page = defaultPage
	state.PageSize = defaultPageSize
	state.Preset = ""
	state.SelectedEdgeSrc = ""
	state.SelectedEdgeDst = ""
	return state
}

func defaultNodeLimit(_ Granularity) int {
	return 10
}

func defaultSortForMetric(metric Metric) TableSort {
	if metric == MetricDNSLookups {
		return SortDNSLookups
	}
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

func parsePositiveInt32(value string) int32 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
	if err != nil || parsed <= 0 {
		return 0
	}
	return int32(parsed)
}

func parseNonNegativeInt(value string) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed < 0 {
		return -1
	}
	return parsed
}

func parseBoolDefaultTrue(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "1", trueValue, "yes", "on":
		return true
	case "0", falseValue, "no", "off":
		return false
	default:
		return true
	}
}

func (m Metric) valid() bool {
	return m == MetricBytes || m == MetricConnections || m == MetricDNSLookups
}

func (g Granularity) valid() bool {
	return g == GranularityTLD || g == Granularity2LD || g == GranularityHostname || g == GranularityIP
}

func (i LocalIdentity) valid() bool {
	return i == LocalIdentityAddress || i == LocalIdentityDevice
}

func (a AddressFamily) valid() bool {
	return a == AddressFamilyAll || a == AddressFamilyIPv4 || a == AddressFamilyIPv6
}

func (d DirectionFilter) valid() bool {
	return d == DirectionBoth || d == DirectionEgress || d == DirectionIngress
}

func (s TableSort) valid() bool {
	return s == SortBytes || s == SortConnections || s == SortDNSLookups || s == SortFirstSeen || s == SortLastSeen || s == SortSource || s == SortDestination
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
