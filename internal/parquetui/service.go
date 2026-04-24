package parquetui

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/fingon/homenetflow/internal/model"
	"github.com/fingon/homenetflow/internal/parquetout"
	"github.com/fingon/homenetflow/internal/scan"
	"golang.org/x/sync/errgroup"
)

const (
	directionIngressParquetValue int32 = 0
	directionEgressParquetValue  int32 = 1
	dnsLookupAnswerExpression          = "COALESCE(answer, '')"
	breakdownSliceLimit                = 5
	breakdownRestLabel                 = "Rest"
	filteredCTEName                    = "filtered_flows"
	graphRestID                        = breakdownRestLabel
	histogramCacheKind                 = "histogram"
	histogramBinCount                  = 48
	layoutCacheKind                    = "layout"
	nodeDetailPeerLimit                = 12
	graphCacheKind                     = "graph"
	dnsLookupFilenamePrefix            = "dns_lookups_"
	resultCacheLimit                   = 96
	restTopEntityLimit                 = 10
	srcScopeEntityColumn               = "src_scope_entity"
	dstScopeEntityColumn               = "dst_scope_entity"
	rawQueryConcurrencyMax             = 4
	sqlIgnoredFalseExpression          = "false AS is_ignored"
	srcEntityColumn                    = "src_entity"
	summaryTopItemLimit                = 10
	tableCacheKind                     = "table"
	dstEntityColumn                    = "dst_entity"
)

type dnsResultState string

const (
	dnsResultStateMixed    dnsResultState = "mixed"
	dnsResultStateNXDOMAIN dnsResultState = "nxdomain"
	dnsResultStateSuccess  dnsResultState = ""
)

var requiredColumns = []string{
	"bytes",
	"direction",
	"dst_2ld",
	"dst_device_id",
	"dst_device_label",
	"dst_device_mac",
	"dst_device_source",
	"dst_host",
	"dst_ip",
	"dst_is_private",
	"dst_port",
	"dst_tld",
	"ip_version",
	"protocol",
	"src_port",
	"src_2ld",
	"src_device_id",
	"src_device_label",
	"src_device_mac",
	"src_device_source",
	"src_host",
	"src_ip",
	"src_is_private",
	"src_tld",
	"time_end_ns",
	"time_start_ns",
}

type Config struct {
	Dev            bool          `env:"HOMENETFLOW_UI_DEV" help:"Enable development mode with hot reload support."`
	PIDFile        string        `env:"HOMENETFLOW_UI_PID_FILE" help:"Write the running process ID to this file." name:"pid-file"`
	Port           int           `default:"8080" env:"HOMENETFLOW_UI_PORT" help:"HTTP port."`
	ReplaceRunning bool          `env:"HOMENETFLOW_UI_REPLACE_RUNNING" help:"Replace the running process recorded in the pid file before starting." name:"replace-running"`
	ReloadInterval time.Duration `default:"1m" env:"HOMENETFLOW_UI_RELOAD_INTERVAL" help:"Polling interval for parquet refresh." name:"reload-interval"`
	SrcParquetPath string        `arg:"" help:"Directory containing enriched parquet files." name:"src-parquet" required:""`
	Verbose        bool          `env:"HOMENETFLOW_UI_VERBOSE" help:"Enable verbose logging." short:"v"`
}

func (c Config) Validate() error {
	if c.SrcParquetPath == "" {
		return errors.New("source parquet path is required")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535, got %d", c.Port)
	}
	if c.ReloadInterval <= 0 {
		return fmt.Errorf("reload interval must be positive, got %s", c.ReloadInterval)
	}
	if c.ReplaceRunning && c.PIDFile == "" {
		return errors.New("pid file is required when replace-running is enabled")
	}
	return nil
}

type TimeSpan struct {
	EndNs   int64
	StartNs int64
}

type Totals struct {
	Bytes       int64
	Connections int64
	Edges       int
	Entities    int
	Ignored     int64
}

type Node struct {
	CollapsedEntityCount int              `json:"collapsedEntityCount"`
	AddressClass         nodeAddressClass `json:"addressClass"`
	DNSResultState       dnsResultState
	ID                   string `json:"id"`
	Ingress              int64  `json:"ingress"`
	Ignored              bool   `json:"ignored"`
	Label                string `json:"label"`
	Egress               int64  `json:"egress"`
	NXDomainLookups      int64
	PrivateMetric        int64
	PublicMetric         int64
	Selected             bool `json:"selected"`
	SuccessfulLookups    int64
	Synthetic            bool  `json:"synthetic"`
	Total                int64 `json:"total"`
}

type Edge struct {
	Bytes             int64  `json:"bytes"`
	Connections       int64  `json:"connections"`
	Destination       string `json:"destination"`
	DNSResultState    dnsResultState
	FirstSeenNs       int64 `json:"firstSeenNs"`
	Ignored           bool  `json:"ignored"`
	IgnoredMetric     int64 `json:"ignoredMetric"`
	LastSeenNs        int64 `json:"lastSeenNs"`
	MetricValue       int64 `json:"metricValue"`
	NXDomainLookups   int64
	Selected          bool   `json:"selected"`
	Source            string `json:"source"`
	SuccessfulLookups int64
	Synthetic         bool `json:"synthetic"`
}

type HistogramBin struct {
	FromNs int64 `json:"fromNs"`
	ToNs   int64 `json:"toNs"`
	Value  int64 `json:"value"`
}

type DetailPeer struct {
	Entity      string
	MetricValue int64
}

type BreakdownChart struct {
	Label  string
	Slices []BreakdownSlice
}

type BreakdownSlice struct {
	FilterParam string
	FilterValue string
	Label       string
	Value       int64
}

type SelectionBreakdown struct {
	Family    *BreakdownChart
	Ports     *BreakdownChart
	Protocols *BreakdownChart
}

type GraphData struct {
	ActiveGranularity Granularity `json:"activeGranularity"`
	ActiveMetric      Metric      `json:"activeMetric"`
	Breadcrumbs       []string    `json:"breadcrumbs"`
	Breakdown         SelectionBreakdown
	Edges             []Edge `json:"edges"`
	HiddenEdgeCount   int    `json:"hiddenEdgeCount"`
	HiddenNodeCount   int    `json:"hiddenNodeCount"`
	Nodes             []Node `json:"nodes"`
	NodePositions     map[string]LayoutPoint
	SelectedEdge      *Edge          `json:"selectedEdge"`
	SelectedNode      *Node          `json:"selectedNode"`
	SelectedNodePeers []DetailPeer   `json:"selectedNodePeers"`
	Sparkline         []HistogramBin `json:"sparkline"`
	Span              TimeSpan       `json:"span"`
	Totals            Totals         `json:"totals"`
	TopEdges          []Edge         `json:"topEdges"`
	TopEntities       []Node         `json:"topEntities"`
}

type TableRow struct {
	Bytes             int64
	Connections       int64
	Destination       string
	DNSResultState    dnsResultState
	FirstSeenNs       int64
	Ignored           bool
	LastSeenNs        int64
	NXDomainLookups   int64
	Source            string
	SuccessfulLookups int64
	Synthetic         bool
}

type FlowDetailRow struct {
	Bytes       int64
	Destination string
	DstIP       string
	DstPort     int32
	Direction   *int32
	EndNs       int64
	Ignored     bool
	IPVersion   int32
	Packets     int64
	Protocol    int32
	ServicePort int32
	Source      string
	SrcIP       string
	SrcPort     int32
	StartNs     int64
}

type FlowPresetCount struct {
	Count  int
	Label  string
	Preset string
}

type TableData struct {
	Page        int
	PageSize    int
	Rows        []TableRow
	Sort        TableSort
	TotalCount  int
	TotalPages  int
	VisibleRows []TableRow
}

type FlowDetailData struct {
	IgnoreRuleCount int
	Page            int
	PageSize        int
	PresetCounts    []FlowPresetCount
	Query           FlowQuery
	Rows            []FlowDetailRow
	Span            TimeSpan
	TotalCount      int
	TotalPages      int
	VisibleRows     []FlowDetailRow
}

type DashboardData struct {
	Graph           GraphData
	Histogram       []HistogramBin
	IgnoreRuleCount int
	Span            TimeSpan
	State           QueryState
	Table           TableData
}

type Service struct {
	bgCtx                 context.Context
	db                    *sql.DB
	graphCache            *resultCache[GraphData]
	dnsLookupGlobPath     string
	dnsLookupHasAnswer    bool
	ignoreRulePath        string
	ignoreRules           []IgnoreRule
	globPath              string
	histogramCache        *resultCache[[]HistogramBin]
	inetAvailable         bool
	layoutCache           *resultCache[map[string]LayoutPoint]
	reloadInterval        time.Duration
	srcParquetPath        string
	summaryGraphCache     *resultCache[*summaryGraphSnapshotData]
	summaryRefreshPending bool
	summaryRefreshRunning bool
	tableCache            *resultCache[TableData]
	mu                    sync.RWMutex
	summaries             summarySnapshot
	span                  TimeSpan
	spanValid             bool
	dnsLookupValid        bool
	fileModTimes          map[string]time.Time
	revision              uint64
}

func NewService(ctx context.Context, srcParquetPath string, reloadInterval time.Duration) (*Service, error) {
	if srcParquetPath == "" {
		return nil, errors.New("source parquet path is required")
	}
	absPath, err := filepath.Abs(srcParquetPath)
	if err != nil {
		return nil, fmt.Errorf("resolve source parquet path: %w", err)
	}
	globPath := filepath.ToSlash(filepath.Join(absPath, "nfcap_*.parquet"))
	dnsLookupGlobPath := filepath.ToSlash(filepath.Join(absPath, dnsLookupFilenamePrefix+"*.parquet"))

	threadCount := rawQueryThreadsPerQuery()
	connector, err := duckdb.NewConnector(fmt.Sprintf("?threads=%d", threadCount), nil)
	if err != nil {
		return nil, fmt.Errorf("create duckdb connector: %w", err)
	}
	db := sql.OpenDB(connector)
	queryConcurrency := rawQueryConcurrency()
	db.SetMaxOpenConns(queryConcurrency)
	db.SetMaxIdleConns(queryConcurrency)

	service := &Service{
		bgCtx:             ctx,
		db:                db,
		dnsLookupGlobPath: dnsLookupGlobPath,
		graphCache:        newResultCache[GraphData](resultCacheLimit),
		globPath:          globPath,
		histogramCache:    newResultCache[[]HistogramBin](resultCacheLimit),
		ignoreRulePath:    ignoreRulesPath(absPath),
		layoutCache:       newResultCache[map[string]LayoutPoint](resultCacheLimit),
		reloadInterval:    reloadInterval,
		srcParquetPath:    absPath,
		summaryGraphCache: newResultCache[*summaryGraphSnapshotData](resultCacheLimit),
		tableCache:        newResultCache[TableData](resultCacheLimit),
	}
	service.inetAvailable = service.detectINETSupport(ctx)

	if err := service.refreshMetadata(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return service, nil
}

func rawQueryConcurrency() int {
	return min(rawQueryConcurrencyMax, max(1, runtime.GOMAXPROCS(0)))
}

func rawQueryThreadsPerQuery() int {
	return max(1, runtime.GOMAXPROCS(0)/rawQueryConcurrency())
}

func (s *Service) Close() error {
	return s.db.Close()
}

func (s *Service) StartMonitor(ctx context.Context) {
	ticker := time.NewTicker(s.reloadInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.refreshMetadata(ctx); err != nil {
					slog.Warn("failed refreshing parquet metadata", "err", err)
				}
			}
		}
	}()
}

func (s *Service) Span(ctx context.Context) (TimeSpan, error) {
	if err := s.refreshMetadata(ctx); err != nil {
		return TimeSpan{}, err
	}

	s.mu.RLock()
	span := s.span
	valid := s.spanValid
	s.mu.RUnlock()
	if !valid {
		return TimeSpan{}, errors.New("time span not available")
	}
	return span, nil
}

func (s *Service) Graph(ctx context.Context, state QueryState) (GraphData, error) {
	span, err := s.Span(ctx)
	if err != nil {
		return GraphData{}, err
	}
	state = state.Normalized(span)
	return s.graphWithSpan(ctx, state, span)
}

func (s *Service) graphWithSpan(ctx context.Context, state QueryState, span TimeSpan) (GraphData, error) {
	if state.Metric == MetricDNSLookups && !s.hasDNSLookupData() {
		return GraphData{
			ActiveGranularity: state.Granularity,
			ActiveMetric:      state.Metric,
			Breadcrumbs:       buildBreadcrumbs(state),
			NodePositions:     map[string]LayoutPoint{},
			Span:              span,
		}, nil
	}
	if s.canUseSummaryGraph(state, span) {
		return s.summaryGraph(ctx, state, span)
	}
	if !state.EntityActionsEnabled() {
		return GraphData{}, errLongRangeSummaryNeeded
	}
	cacheKey := state.cacheKey(graphCacheKind, s.currentRevision())
	if graph, ok := s.graphCache.Get(cacheKey); ok {
		return graph, nil
	}

	queryStart := time.Now()
	nodeTotals, err := s.queryNodeTotals(ctx, state)
	if err != nil {
		return GraphData{}, err
	}
	nodeTotalsLoadedAt := time.Now()

	keepEntities := chooseKeepEntities(nodeTotals, state)
	keepLookup := make(map[string]struct{}, len(keepEntities))
	for _, entity := range keepEntities {
		keepLookup[entity] = struct{}{}
	}

	var edges []Edge
	var restNode *Node
	group, groupContext := errgroup.WithContext(ctx)
	group.Go(func() error {
		var queryErr error
		edges, queryErr = s.queryEdges(groupContext, state, keepEntities)
		return queryErr
	})
	group.Go(func() error {
		var queryErr error
		restNode, queryErr = s.queryRestNode(groupContext, state, keepEntities)
		return queryErr
	})
	if err := group.Wait(); err != nil {
		return GraphData{}, err
	}
	edgeDataLoadedAt := time.Now()

	visibleEdges, hiddenEdgeCount := limitEdges(edges, state.EdgeLimit, state.SelectedEntity)
	visibleNodeMap := make(map[string]Node, len(keepEntities)+2)
	for _, row := range nodeTotals {
		if _, ok := keepLookup[row.ID]; !ok {
			continue
		}
		row.Selected = row.ID == state.SelectedEntity
		visibleNodeMap[row.ID] = row
	}

	if restNode != nil {
		visibleNodeMap[restNode.ID] = *restNode
	}

	for _, edge := range visibleEdges {
		if node, ok := visibleNodeMap[edge.Source]; ok {
			node.Selected = node.Selected || edge.Source == state.SelectedEntity
			visibleNodeMap[edge.Source] = node
		}
		if node, ok := visibleNodeMap[edge.Destination]; ok {
			node.Selected = node.Selected || edge.Destination == state.SelectedEntity
			visibleNodeMap[edge.Destination] = node
		}
	}

	nodes := make([]Node, 0, len(visibleNodeMap))
	for _, node := range visibleNodeMap {
		nodes = append(nodes, node)
	}
	slices.SortFunc(nodes, func(a, b Node) int {
		if a.Total == b.Total {
			return strings.Compare(a.ID, b.ID)
		}
		if a.Total > b.Total {
			return -1
		}
		return 1
	})

	totals := totalsFromEdges(edges, len(nodeTotals), len(visibleEdges), state.Metric)
	topEntities := limitNodes(nodes, summaryTopItemLimit)
	topEdges := limitTopEdges(visibleEdges, summaryTopItemLimit)

	var selectedNode *Node
	var selectedEdge *Edge
	var breakdown SelectionBreakdown
	var peers []DetailPeer
	var sparkline []HistogramBin
	var nodePositions map[string]LayoutPoint
	var selectionDuration time.Duration
	var layoutDuration time.Duration
	group, groupContext = errgroup.WithContext(ctx)
	group.Go(func() error {
		startTime := time.Now()
		var queryErr error
		selectedNode, selectedEdge, peers, sparkline, breakdown, queryErr = s.selectionDetails(groupContext, state, span, keepEntities, visibleNodeMap, visibleEdges)
		selectionDuration = time.Since(startTime)
		return queryErr
	})
	group.Go(func() error {
		startTime := time.Now()
		var queryErr error
		nodePositions, queryErr = s.layoutPositions(groupContext, state)
		layoutDuration = time.Since(startTime)
		return queryErr
	})
	if err := group.Wait(); err != nil {
		return GraphData{}, err
	}
	layoutAndSelectionLoadedAt := time.Now()

	graph := GraphData{
		ActiveGranularity: state.Granularity,
		ActiveMetric:      state.Metric,
		Breadcrumbs:       buildBreadcrumbs(state),
		Breakdown:         breakdown,
		Edges:             visibleEdges,
		HiddenEdgeCount:   hiddenEdgeCount,
		HiddenNodeCount:   max(0, len(nodeTotals)-countNonSynthetic(nodeTotals, keepLookup)),
		Nodes:             nodes,
		NodePositions:     nodePositions,
		SelectedEdge:      selectedEdge,
		SelectedNode:      selectedNode,
		SelectedNodePeers: peers,
		Sparkline:         sparkline,
		Span:              span,
		Totals:            totals,
		TopEdges:          topEdges,
		TopEntities:       topEntities,
	}
	s.graphCache.Set(cacheKey, graph)
	slog.Debug(
		"UI raw graph query complete",
		"granularity", state.Granularity,
		"metric", state.Metric,
		"node_totals_ms", nodeTotalsLoadedAt.Sub(queryStart).Milliseconds(),
		"edge_data_ms", edgeDataLoadedAt.Sub(nodeTotalsLoadedAt).Milliseconds(),
		"selection_ms", selectionDuration.Milliseconds(),
		"layout_ms", layoutDuration.Milliseconds(),
		"layout_selection_ms", layoutAndSelectionLoadedAt.Sub(edgeDataLoadedAt).Milliseconds(),
		"duration_ms", time.Since(queryStart).Milliseconds(),
	)
	return graph, nil
}

func (s *Service) Dashboard(ctx context.Context, state QueryState) (DashboardData, error) {
	span, err := s.Span(ctx)
	if err != nil {
		return DashboardData{}, err
	}

	state = state.Normalized(span)
	var graph GraphData
	var histogram []HistogramBin
	group, groupContext := errgroup.WithContext(ctx)
	group.Go(func() error {
		var queryErr error
		graph, queryErr = s.graphWithSpan(groupContext, state, span)
		return queryErr
	})
	group.Go(func() error {
		var queryErr error
		histogram, queryErr = s.histogramWithSpan(groupContext, state, span)
		return queryErr
	})
	if err := group.Wait(); err != nil {
		return DashboardData{}, err
	}
	table := tableFromGraph(graph, state)

	return DashboardData{
		Graph:           graph,
		Histogram:       histogram,
		IgnoreRuleCount: s.ignoreRuleCount(),
		Span:            span,
		State:           state,
		Table:           table,
	}, nil
}

func (s *Service) Histogram(ctx context.Context, state QueryState) ([]HistogramBin, error) {
	span, err := s.Span(ctx)
	if err != nil {
		return nil, err
	}
	state = state.Normalized(span)
	return s.histogramWithSpan(ctx, state, span)
}

func (s *Service) histogramWithSpan(ctx context.Context, state QueryState, span TimeSpan) ([]HistogramBin, error) {
	if state.Metric == MetricDNSLookups && !s.hasDNSLookupData() {
		return nil, nil
	}
	if s.canUseSummaryHistogram(state, span) {
		return s.summaryHistogram(ctx, state)
	}
	if !state.EntityActionsEnabled() {
		return nil, errLongRangeSummaryNeeded
	}
	if state.ToNs <= state.FromNs {
		return nil, nil
	}
	cacheKey := state.cacheKey(histogramCacheKind, s.currentRevision())
	if bins, ok := s.histogramCache.Get(cacheKey); ok {
		return bins, nil
	}

	widthNs := max(int64(1), (state.ToNs-state.FromNs+1)/histogramBinCount)
	cte, args, err := s.filteredCTE(state)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`%s
SELECT bucket, SUM(metric_value) AS value
FROM %s
GROUP BY bucket
ORDER BY bucket
`, cteWithHistogramBucket(cte, state.Metric), "histogram")

	rows, err := s.db.QueryContext(ctx, query, append(args, state.FromNs, widthNs)...)
	if err != nil {
		return nil, fmt.Errorf("query histogram: %w", err)
	}
	defer rows.Close()

	values := make(map[int]int64, histogramBinCount)
	for rows.Next() {
		var bucket int
		var value int64
		if err := rows.Scan(&bucket, &value); err != nil {
			return nil, fmt.Errorf("scan histogram row: %w", err)
		}
		if bucket < 0 {
			bucket = 0
		}
		if bucket >= histogramBinCount {
			bucket = histogramBinCount - 1
		}
		values[bucket] += value
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate histogram rows: %w", err)
	}

	bins := make([]HistogramBin, 0, histogramBinCount)
	for bucket := range histogramBinCount {
		startNs := state.FromNs + int64(bucket)*widthNs
		endNs := startNs + widthNs - 1
		if bucket == histogramBinCount-1 {
			endNs = state.ToNs
		}
		bins = append(bins, HistogramBin{
			FromNs: startNs,
			ToNs:   endNs,
			Value:  values[bucket],
		})
	}
	s.histogramCache.Set(cacheKey, bins)
	return bins, nil
}

func (s *Service) Table(ctx context.Context, state QueryState) (TableData, error) {
	span, err := s.Span(ctx)
	if err != nil {
		return TableData{}, err
	}
	state = state.Normalized(span)
	cacheKey := state.cacheKey(tableCacheKind, s.currentRevision())
	if table, ok := s.tableCache.Get(cacheKey); ok {
		return table, nil
	}

	graph, err := s.Graph(ctx, state)
	if err != nil {
		return TableData{}, err
	}
	table := tableFromGraph(graph, state)
	s.tableCache.Set(cacheKey, table)
	return table, nil
}

func (s *Service) FlowDetails(ctx context.Context, query FlowQuery) (FlowDetailData, error) {
	span, err := s.Span(ctx)
	if err != nil {
		return FlowDetailData{}, err
	}
	state := query.State.NormalizedForFlowDetails(span)
	query.State = state
	if !query.Match.valid() {
		query.Match = FlowMatchBoth
	}
	if !query.Sort.valid() || (!state.EntityActionsEnabled() && !query.Sort.timeSort()) {
		query.Sort = FlowSortStart
	}
	if !query.SortDir.valid() || !query.Sort.timeSort() {
		query.SortDir = FlowSortDesc
	}
	if state.Metric == MetricDNSLookups {
		return FlowDetailData{}, errors.New("raw flow details are not available for DNS lookup metrics")
	}
	if !state.EntityActionsEnabled() {
		return FlowDetailData{}, errEntityActionsDisabled
	}
	if !query.Scope.valid() {
		return FlowDetailData{}, fmt.Errorf("invalid flow scope %q", query.Scope)
	}
	switch query.Scope {
	case FlowScopeEntity:
		if query.Entity == "" {
			return FlowDetailData{}, errors.New("flow entity is required")
		}
	case FlowScopeEdge:
		if query.Source == "" || query.Destination == "" {
			return FlowDetailData{}, errors.New("flow source and destination are required")
		}
	}

	page := state.Page
	if page <= 0 {
		page = defaultPage
	}
	pageSize := state.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	presetCounts, err := s.flowPresetCounts(ctx, query, span)
	if err != nil {
		return FlowDetailData{}, err
	}

	cte, args, err := s.filteredRawFlowsCTE(state)
	if err != nil {
		return FlowDetailData{}, err
	}
	scopeClause, scopeArgs := flowScopeClause(query)
	countArgs := append(append([]any(nil), args...), scopeArgs...)

	totalCount, err := s.countRawFlows(ctx, cte, scopeClause, countArgs)
	if err != nil {
		return FlowDetailData{}, err
	}
	totalPages := max(1, (totalCount+pageSize-1)/pageSize)
	if page > totalPages {
		page = totalPages
		query.State.Page = page
	}
	offset := (page - 1) * pageSize

	rowsQuery := fmt.Sprintf(`%s
SELECT time_start_ns, time_end_ns, src_entity, dst_entity, src_ip, dst_ip, src_port, dst_port, protocol, packets, bytes, direction, ip_version, service_port, is_ignored
FROM %s
WHERE %s
ORDER BY %s
LIMIT ? OFFSET ?
`, cte, filteredCTEName, scopeClause, flowSortOrderBy(query.Sort, query.SortDir))
	rowArgs := append(append([]any(nil), countArgs...), pageSize, offset)
	rows, err := s.db.QueryContext(ctx, rowsQuery, rowArgs...)
	if err != nil {
		return FlowDetailData{}, fmt.Errorf("query selected flows: %w", err)
	}
	defer rows.Close()

	visibleRows := make([]FlowDetailRow, 0, min(pageSize, totalCount))
	for rows.Next() {
		var row FlowDetailRow
		var direction sql.NullInt32
		var ignored bool
		var servicePort sql.NullInt32
		if err := rows.Scan(
			&row.StartNs,
			&row.EndNs,
			&row.Source,
			&row.Destination,
			&row.SrcIP,
			&row.DstIP,
			&row.SrcPort,
			&row.DstPort,
			&row.Protocol,
			&row.Packets,
			&row.Bytes,
			&direction,
			&row.IPVersion,
			&servicePort,
			&ignored,
		); err != nil {
			return FlowDetailData{}, fmt.Errorf("scan selected flow row: %w", err)
		}
		row.Ignored = ignored
		if direction.Valid {
			value := direction.Int32
			row.Direction = &value
		}
		if servicePort.Valid {
			row.ServicePort = servicePort.Int32
		}
		visibleRows = append(visibleRows, row)
	}
	if err := rows.Err(); err != nil {
		return FlowDetailData{}, fmt.Errorf("iterate selected flow rows: %w", err)
	}

	return FlowDetailData{
		IgnoreRuleCount: s.ignoreRuleCount(),
		Page:            page,
		PageSize:        pageSize,
		PresetCounts:    presetCounts,
		Query:           query,
		Rows:            visibleRows,
		Span:            span,
		TotalCount:      totalCount,
		TotalPages:      totalPages,
		VisibleRows:     visibleRows,
	}, nil
}

func (s *Service) flowPresetCounts(ctx context.Context, query FlowQuery, span TimeSpan) ([]FlowPresetCount, error) {
	presets := []struct {
		label  string
		preset string
	}{
		{label: "All", preset: presetAllValue},
		{label: presetHourValue, preset: presetHourValue},
		{label: presetDayValue, preset: presetDayValue},
		{label: presetWeekValue, preset: presetWeekValue},
		{label: presetMonthValue, preset: presetMonthValue},
	}
	counts := make([]FlowPresetCount, 0, len(presets))
	for _, preset := range presets {
		fromNs, toNs, ok := presetRange(preset.preset, span)
		if !ok {
			continue
		}
		state := query.State.Clone()
		state.Preset = preset.preset
		state.FromNs = fromNs
		state.ToNs = toNs
		state.Page = defaultPage
		state = state.NormalizedForFlowDetails(span)
		presetQuery := query
		presetQuery.State = state
		if !state.EntityActionsEnabled() {
			continue
		}
		cte, args, err := s.filteredRawFlowsCTE(state)
		if err != nil {
			return nil, err
		}
		scopeClause, scopeArgs := flowScopeClause(presetQuery)
		count, err := s.countRawFlows(ctx, cte, scopeClause, append(append([]any(nil), args...), scopeArgs...))
		if err != nil {
			return nil, err
		}
		counts = append(counts, FlowPresetCount{
			Count:  count,
			Label:  preset.label,
			Preset: preset.preset,
		})
	}
	return counts, nil
}

func (s *Service) countRawFlows(ctx context.Context, cte, scopeClause string, args []any) (int, error) {
	countQuery := fmt.Sprintf(`%s
SELECT COUNT(*)
FROM %s
WHERE %s
`, cte, filteredCTEName, scopeClause)

	var totalCount int
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return 0, fmt.Errorf("count selected flows: %w", err)
	}
	return totalCount, nil
}

func tableFromGraph(graph GraphData, state QueryState) TableData {
	rows := make([]TableRow, 0, len(graph.Edges))
	for _, edge := range graph.Edges {
		rows = append(rows, TableRow{
			Bytes:             edge.Bytes,
			Connections:       edge.Connections,
			Destination:       edge.Destination,
			DNSResultState:    edge.DNSResultState,
			FirstSeenNs:       edge.FirstSeenNs,
			Ignored:           edge.Ignored,
			LastSeenNs:        edge.LastSeenNs,
			NXDomainLookups:   edge.NXDomainLookups,
			Source:            edge.Source,
			SuccessfulLookups: edge.SuccessfulLookups,
			Synthetic:         edge.Synthetic,
		})
	}

	sortTableRows(rows, state.Sort)
	page := state.Page
	if page <= 0 {
		page = defaultPage
	}
	pageSize := state.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	totalCount := len(rows)
	totalPages := max(1, (totalCount+pageSize-1)/pageSize)
	if page > totalPages {
		page = totalPages
	}
	start := (page - 1) * pageSize
	end := min(totalCount, start+pageSize)

	visibleRows := []TableRow(nil)
	if start < end {
		visibleRows = rows[start:end]
	}

	return TableData{
		Page:        page,
		PageSize:    pageSize,
		Rows:        rows,
		Sort:        state.Sort,
		TotalCount:  totalCount,
		TotalPages:  totalPages,
		VisibleRows: visibleRows,
	}
}

func (s *Service) refreshMetadata(ctx context.Context) error {
	return s.refreshMetadataWithOptions(ctx, refreshMetadataOptions{})
}

type refreshMetadataOptions struct {
	forceSummary bool
}

func (s *Service) refreshMetadataWithOptions(ctx context.Context, options refreshMetadataOptions) error {
	refreshStart := time.Now()
	modTimes, err := collectModTimes(s.srcParquetPath)
	if err != nil {
		return err
	}
	nfcapPaths := sortedMapKeys(modTimes)
	dnsModTimes, err := collectDNSLookupModTimes(s.srcParquetPath)
	if err != nil {
		return err
	}
	for path, modTime := range dnsModTimes {
		modTimes[path] = modTime
	}
	ignoreRules, err := loadIgnoreRules(s.ignoreRulePath)
	if err != nil {
		return err
	}
	if info, statErr := os.Stat(s.ignoreRulePath); statErr == nil {
		modTimes[s.ignoreRulePath] = info.ModTime()
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("stat ignore rules %q: %w", s.ignoreRulePath, statErr)
	}

	s.mu.RLock()
	unchanged := mapsEqual(s.fileModTimes, modTimes) && s.spanValid && !s.summaryRefreshPending && !options.forceSummary
	s.mu.RUnlock()
	if unchanged {
		slog.Debug("UI metadata refresh unchanged", "duration_ms", time.Since(refreshStart).Milliseconds())
		return nil
	}

	if err := validateEnrichmentManifests(nfcapPaths); err != nil {
		return err
	}

	summaryStart := time.Now()
	inspection, err := inspectSummaryState(s.srcParquetPath)
	if err != nil {
		return err
	}
	if err := rebuildSummaryJobs(ctx, inspection.missingJobs); err != nil {
		return err
	}
	if len(inspection.missingJobs) > 0 {
		inspection, err = inspectSummaryState(s.srcParquetPath)
		if err != nil {
			return err
		}
	}
	summaries := inspection.snapshot
	slog.Debug("UI summary refresh complete", "duration_ms", time.Since(summaryStart).Milliseconds())

	columns, err := s.queryColumns(ctx)
	if err != nil {
		return err
	}
	if err := validateColumns(columns); err != nil {
		return err
	}
	dnsLookupHasAnswer := false
	if len(dnsModTimes) > 0 {
		dnsColumns, err := s.queryParquetColumns(ctx, s.dnsLookupGlobPath, true)
		if err != nil {
			return err
		}
		dnsLookupHasAnswer = columnsContain(dnsColumns, "answer")
	}

	span := summaries.span
	if !summaries.spanValid {
		span, err = s.querySpan(ctx)
		if err != nil {
			return err
		}
	}

	s.mu.Lock()
	s.fileModTimes = modTimes
	s.ignoreRules = ignoreRules
	s.summaries = summaries
	s.span = span
	s.spanValid = true
	s.dnsLookupValid = len(dnsModTimes) > 0
	s.dnsLookupHasAnswer = dnsLookupHasAnswer
	s.summaryRefreshPending = len(inspection.staleJobs) > 0
	s.revision++
	s.mu.Unlock()
	s.graphCache.Reset()
	s.histogramCache.Reset()
	s.layoutCache.Reset()
	s.summaryGraphCache.Reset()
	s.tableCache.Reset()
	s.scheduleSummaryRefresh(inspection.staleJobs)

	slog.Debug("UI metadata refresh complete", "duration_ms", time.Since(refreshStart).Milliseconds())
	return nil
}

func (s *Service) scheduleSummaryRefresh(jobs []summaryJob) {
	if len(jobs) == 0 {
		return
	}

	s.mu.Lock()
	if s.summaryRefreshRunning {
		s.mu.Unlock()
		return
	}
	s.summaryRefreshRunning = true
	bgCtx := s.bgCtx
	s.mu.Unlock()

	go func() {
		startTime := time.Now()
		defer func() {
			s.mu.Lock()
			s.summaryRefreshRunning = false
			s.mu.Unlock()
		}()

		if err := rebuildSummaryJobs(bgCtx, jobs); err != nil {
			slog.Warn("background UI summary refresh failed", "err", err)
			return
		}
		s.mu.Lock()
		s.summaryRefreshPending = false
		s.mu.Unlock()
		slog.Debug("background UI summary refresh complete", "duration_ms", time.Since(startTime).Milliseconds())
		if err := s.refreshMetadataWithOptions(bgCtx, refreshMetadataOptions{forceSummary: true}); err != nil {
			slog.Warn("publish refreshed UI summaries failed", "err", err)
		}
	}()
}

func (s *Service) queryColumns(ctx context.Context) ([]string, error) {
	return s.queryParquetColumns(ctx, s.globPath, false)
}

func (s *Service) queryParquetColumns(ctx context.Context, globPath string, unionByName bool) ([]string, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", readParquetExpression(globPath, unionByName))
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query parquet schema: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read parquet schema columns: %w", err)
	}
	return columns, nil
}

func (s *Service) querySpan(ctx context.Context) (TimeSpan, error) {
	query := fmt.Sprintf("SELECT COALESCE(MIN(time_start_ns), 0), COALESCE(MAX(time_end_ns), 0) FROM read_parquet(%s)", quoteLiteral(s.globPath))
	row := s.db.QueryRowContext(ctx, query)
	var span TimeSpan
	if err := row.Scan(&span.StartNs, &span.EndNs); err != nil {
		return TimeSpan{}, fmt.Errorf("query time span: %w", err)
	}
	if span.StartNs == 0 || span.EndNs == 0 {
		return TimeSpan{}, fmt.Errorf("no parquet rows found in %q", s.srcParquetPath)
	}
	return span, nil
}

func (s *Service) filteredCTE(state QueryState) (string, []any, error) {
	if state.Metric == MetricDNSLookups {
		return s.filteredDNSLookupCTE(state)
	}

	srcExpr, dstExpr := entityExpressionsForState(state)
	whereClause, args := filterClause(state, srcExpr, dstExpr)
	ignoreCondition, ignoreArgs, err := buildFlowIgnoreConditionSQL(s.enabledIgnoreRules(), s.inetSupportEnabled())
	if err != nil {
		return "", nil, err
	}
	ignoredSelect := sqlIgnoredFalseExpression
	if ignoreCondition != "" {
		if state.HideIgnored {
			whereClause = whereClause + " AND NOT (" + ignoreCondition + ")"
			args = append(args, ignoreArgs...)
		} else {
			ignoredSelect = "CASE WHEN " + ignoreCondition + " THEN true ELSE false END AS is_ignored"
			args = append(ignoreArgs, args...)
		}
	}
	return fmt.Sprintf("WITH %s AS (SELECT %s AS src_entity, %s AS dst_entity, bytes, 0 AS nxdomain_lookups, 0 AS successful_lookups, dst_is_private, ip_version, protocol, %s AS service_port, src_is_private, time_start_ns, time_end_ns, %s FROM read_parquet(%s) WHERE %s)",
		filteredCTEName,
		srcExpr,
		dstExpr,
		rawServicePortExpression(),
		ignoredSelect,
		quoteLiteral(s.globPath),
		whereClause,
	), args, nil
}

func (s *Service) filteredRawFlowsCTE(state QueryState) (string, []any, error) {
	srcScopeExpr, dstScopeExpr := entityExpressionsForState(state)
	srcDisplayExpr, dstDisplayExpr := entityExpressions(GranularityHostname)
	whereClause, args := filterClause(state, srcScopeExpr, dstScopeExpr)
	ignoreCondition, ignoreArgs, err := buildFlowIgnoreConditionSQL(s.enabledIgnoreRules(), s.inetSupportEnabled())
	if err != nil {
		return "", nil, err
	}
	ignoredSelect := sqlIgnoredFalseExpression
	if ignoreCondition != "" {
		if state.HideIgnored {
			whereClause = whereClause + " AND NOT (" + ignoreCondition + ")"
			args = append(args, ignoreArgs...)
		} else {
			ignoredSelect = "CASE WHEN " + ignoreCondition + " THEN true ELSE false END AS is_ignored"
			args = append(ignoreArgs, args...)
		}
	}
	return fmt.Sprintf("WITH %s AS (SELECT %s AS %s, %s AS %s, %s AS src_entity, %s AS dst_entity, bytes, direction, dst_ip, dst_port, ip_version, packets, protocol, %s AS service_port, src_ip, src_port, time_start_ns, time_end_ns, %s FROM read_parquet(%s) WHERE %s)",
		filteredCTEName,
		srcScopeExpr,
		srcScopeEntityColumn,
		dstScopeExpr,
		dstScopeEntityColumn,
		srcDisplayExpr,
		dstDisplayExpr,
		rawServicePortExpression(),
		ignoredSelect,
		quoteLiteral(s.globPath),
		whereClause,
	), args, nil
}

func flowScopeClause(query FlowQuery) (string, []any) {
	switch query.Scope {
	case FlowScopeEntity:
		return fmt.Sprintf("(%s = ? OR %s = ?)", srcScopeEntityColumn, dstScopeEntityColumn), []any{query.Entity, query.Entity}
	case FlowScopeEdge:
		if query.Match != FlowMatchForward {
			return fmt.Sprintf("((%s = ? AND %s = ?) OR (%s = ? AND %s = ?))", srcScopeEntityColumn, dstScopeEntityColumn, srcScopeEntityColumn, dstScopeEntityColumn), []any{query.Source, query.Destination, query.Destination, query.Source}
		}
		return fmt.Sprintf("(%s = ? AND %s = ?)", srcScopeEntityColumn, dstScopeEntityColumn), []any{query.Source, query.Destination}
	default:
		return "false", nil
	}
}

func flowSortOrderBy(sortKey FlowSort, sortDir FlowSortDir) string {
	tieBreakers := "time_start_ns DESC, time_end_ns DESC, src_entity, dst_entity, src_ip, dst_ip, src_port, dst_port, protocol"
	timeSortDirection := "DESC"
	if sortDir == FlowSortAsc {
		timeSortDirection = "ASC"
	}
	switch sortKey {
	case FlowSortStart:
		return "time_start_ns " + timeSortDirection + ", time_end_ns DESC, src_entity, dst_entity, src_ip, dst_ip, src_port, dst_port, protocol"
	case FlowSortEnd:
		return "time_end_ns " + timeSortDirection + ", time_start_ns DESC, src_entity, dst_entity, src_ip, dst_ip, src_port, dst_port, protocol"
	case FlowSortSource:
		return "src_entity, " + tieBreakers
	case FlowSortDestination:
		return "dst_entity, " + tieBreakers
	case FlowSortProtocol:
		return "protocol DESC, " + tieBreakers
	case FlowSortPackets:
		return "packets DESC, " + tieBreakers
	case FlowSortBytes:
		return "bytes DESC, " + tieBreakers
	default:
		return tieBreakers
	}
}

func (s *Service) filteredDNSLookupCTE(state QueryState) (string, []any, error) {
	srcExpr, dstExpr := dnsLookupEntityExpressionsForState(state)
	whereClause, args := filterClause(state, srcExpr, dstExpr)
	answerExpr := quoteLiteral("")
	if s.hasDNSLookupAnswer() {
		answerExpr = dnsLookupAnswerExpression
	}
	ignoreCondition, ignoreArgs, err := buildDNSIgnoreConditionSQL(s.enabledIgnoreRules(), s.inetSupportEnabled())
	if err != nil {
		return "", nil, err
	}
	ignoredSelect := sqlIgnoredFalseExpression
	if ignoreCondition != "" {
		if state.HideIgnored {
			whereClause = whereClause + " AND NOT (" + ignoreCondition + ")"
			args = append(args, ignoreArgs...)
		} else {
			ignoredSelect = "CASE WHEN " + ignoreCondition + " THEN true ELSE false END AS is_ignored"
			args = append(ignoreArgs, args...)
		}
	}
	return fmt.Sprintf("WITH %s AS (SELECT %s AS src_entity, %s AS dst_entity, 0 AS bytes, lookups, CASE WHEN %s = %s THEN lookups ELSE 0 END AS nxdomain_lookups, CASE WHEN %s != '' AND %s != %s THEN lookups ELSE 0 END AS successful_lookups, false AS dst_is_private, client_ip_version AS ip_version, client_is_private AS src_is_private, time_start_ns, time_start_ns AS time_end_ns, %s FROM %s WHERE %s)",
		filteredCTEName,
		srcExpr,
		dstExpr,
		answerExpr,
		quoteLiteral(model.DNSAnswerNXDOMAIN),
		answerExpr,
		answerExpr,
		quoteLiteral(model.DNSAnswerNXDOMAIN),
		ignoredSelect,
		readParquetExpression(s.dnsLookupGlobPath, true),
		whereClause,
	), args, nil
}

func cteWithHistogramBucket(cte string, metric Metric) string {
	return fmt.Sprintf(`%s,
histogram AS (
  SELECT CAST(FLOOR((time_start_ns - ?) / ?) AS BIGINT) AS bucket, %s AS metric_value
  FROM %s
)`, cte, metricValueExpression(metric), filteredCTEName)
}

func (s *Service) queryNodeTotals(ctx context.Context, state QueryState) ([]Node, error) {
	cte, args, err := s.filteredCTE(state)
	if err != nil {
		return nil, err
	}
	valueExpr := metricValueExpression(state.Metric)
	query := fmt.Sprintf(`%s
SELECT entity,
  SUM(total_metric) AS total_metric,
  SUM(ingress_metric) AS ingress_metric,
  SUM(egress_metric) AS egress_metric,
  SUM(ignored_metric) AS ignored_metric,
  SUM(nxdomain_metric) AS nxdomain_metric,
  SUM(successful_metric) AS successful_metric,
  SUM(private_metric) AS private_metric,
  SUM(public_metric) AS public_metric
FROM (
  SELECT src_entity AS entity, %s AS total_metric, 0 AS ingress_metric, %s AS egress_metric,
    CASE WHEN is_ignored THEN %s ELSE 0 END AS ignored_metric,
    nxdomain_lookups AS nxdomain_metric, successful_lookups AS successful_metric,
    %s AS private_metric, %s AS public_metric
  FROM %s
  UNION ALL
  SELECT dst_entity AS entity, %s AS total_metric, %s AS ingress_metric, 0 AS egress_metric,
    CASE WHEN is_ignored THEN %s ELSE 0 END AS ignored_metric,
    nxdomain_lookups AS nxdomain_metric, successful_lookups AS successful_metric,
    %s AS private_metric, %s AS public_metric
  FROM %s
) aggregate_nodes
GROUP BY entity
ORDER BY total_metric DESC, entity
`, cte,
		valueExpr,
		valueExpr,
		valueExpr,
		privateMetricExpression("src_is_private", state.Metric),
		publicMetricExpression("src_is_private", state.Metric),
		filteredCTEName,
		valueExpr,
		valueExpr,
		valueExpr,
		privateMetricExpression("dst_is_private", state.Metric),
		publicMetricExpression("dst_is_private", state.Metric),
		filteredCTEName,
	)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query node totals: %w", err)
	}
	defer rows.Close()

	nodes := make([]Node, 0, 128)
	for rows.Next() {
		var node Node
		var ignoredMetric int64
		if err := rows.Scan(&node.ID, &node.Total, &node.Ingress, &node.Egress, &ignoredMetric, &node.NXDomainLookups, &node.SuccessfulLookups, &node.PrivateMetric, &node.PublicMetric); err != nil {
			return nil, fmt.Errorf("scan node total row: %w", err)
		}
		node.Ignored = ignoredMetric > 0
		node.Label = node.ID
		node.AddressClass = classifyNodeAddress(node.PrivateMetric, node.PublicMetric)
		node.DNSResultState = dnsResultStateForCounts(node.NXDomainLookups, node.SuccessfulLookups)
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate node total rows: %w", err)
	}

	return nodes, nil
}

func (s *Service) queryRestNode(ctx context.Context, state QueryState, keepEntities []string) (*Node, error) {
	if state.NodeLimit == 0 || len(keepEntities) == 0 {
		return nil, nil
	}

	cte, args, err := s.filteredCTE(state)
	if err != nil {
		return nil, err
	}
	metricExpr := metricValueExpression(state.Metric)
	inPlaceholders := placeholders(len(keepEntities))
	query := fmt.Sprintf(`%s
SELECT
  COALESCE(SUM(entity_count), 0),
  COALESCE(SUM(egress_metric), 0),
  COALESCE(SUM(ingress_metric), 0),
  COALESCE(SUM(ignored_metric), 0),
  COALESCE(SUM(nxdomain_metric), 0),
  COALESCE(SUM(successful_metric), 0)
FROM (
  SELECT COUNT(*) AS entity_count, COALESCE(SUM(total_metric), 0) AS egress_metric, 0 AS ingress_metric, COALESCE(SUM(ignored_metric), 0) AS ignored_metric, COALESCE(SUM(nxdomain_metric), 0) AS nxdomain_metric, COALESCE(SUM(successful_metric), 0) AS successful_metric
  FROM (
    SELECT %s AS entity, SUM(%s) AS total_metric, SUM(CASE WHEN is_ignored THEN %s ELSE 0 END) AS ignored_metric, SUM(nxdomain_lookups) AS nxdomain_metric, SUM(successful_lookups) AS successful_metric
    FROM %s
    WHERE %s NOT IN (%s)
    GROUP BY %s
  ) collapsed_source_entities
  UNION ALL
  SELECT COUNT(*) AS entity_count, 0 AS egress_metric, COALESCE(SUM(total_metric), 0) AS ingress_metric, COALESCE(SUM(ignored_metric), 0) AS ignored_metric, COALESCE(SUM(nxdomain_metric), 0) AS nxdomain_metric, COALESCE(SUM(successful_metric), 0) AS successful_metric
  FROM (
    SELECT %s AS entity, SUM(%s) AS total_metric, SUM(CASE WHEN is_ignored THEN %s ELSE 0 END) AS ignored_metric, SUM(nxdomain_lookups) AS nxdomain_metric, SUM(successful_lookups) AS successful_metric
    FROM %s
    WHERE %s NOT IN (%s)
    GROUP BY %s
  ) collapsed_destination_entities
) collapsed_entities
`, cte, srcEntityColumn, metricExpr, metricExpr, filteredCTEName, srcEntityColumn, inPlaceholders, srcEntityColumn, dstEntityColumn, metricExpr, metricExpr, filteredCTEName, dstEntityColumn, inPlaceholders, dstEntityColumn)

	queryArgs := append(append([]any(nil), args...), stringsToAny(keepEntities)...)
	queryArgs = append(queryArgs, stringsToAny(keepEntities)...)
	row := s.db.QueryRowContext(ctx, query, queryArgs...)
	var entityCount int
	var egress int64
	var ingress int64
	var ignoredMetric int64
	var nxdomainLookups int64
	var successfulLookups int64
	if err := row.Scan(&entityCount, &egress, &ingress, &ignoredMetric, &nxdomainLookups, &successfulLookups); err != nil {
		return nil, fmt.Errorf("scan rest node %q: %w", graphRestID, err)
	}
	total := egress + ingress
	if total == 0 {
		return nil, nil
	}

	node := &Node{
		CollapsedEntityCount: entityCount,
		Egress:               egress,
		ID:                   graphRestID,
		Ignored:              ignoredMetric > 0,
		Ingress:              ingress,
		Label:                graphRestID,
		NXDomainLookups:      nxdomainLookups,
		Synthetic:            true,
		SuccessfulLookups:    successfulLookups,
		Total:                total,
	}
	node.DNSResultState = dnsResultStateForCounts(node.NXDomainLookups, node.SuccessfulLookups)
	if node.ID == state.SelectedEntity {
		node.Selected = true
	}
	return node, nil
}

func (s *Service) queryEdges(ctx context.Context, state QueryState, keepEntities []string) ([]Edge, error) {
	cte, args, err := s.filteredCTE(state)
	if err != nil {
		return nil, err
	}
	srcBucket := srcEntityColumn
	dstBucket := dstEntityColumn
	queryArgs := append([]any(nil), args...)
	if state.NodeLimit > 0 && len(keepEntities) > 0 {
		inPlaceholders := placeholders(len(keepEntities))
		srcBucket = fmt.Sprintf("CASE WHEN src_entity IN (%s) THEN src_entity ELSE %s END", inPlaceholders, quoteLiteral(graphRestID))
		dstBucket = fmt.Sprintf("CASE WHEN dst_entity IN (%s) THEN dst_entity ELSE %s END", inPlaceholders, quoteLiteral(graphRestID))
		queryArgs = append(queryArgs, stringsToAny(keepEntities)...)
		queryArgs = append(queryArgs, stringsToAny(keepEntities)...)
	}

	query := fmt.Sprintf(`%s
SELECT %s AS source_bucket, %s AS destination_bucket,
  COALESCE(SUM(bytes), 0) AS bytes_total,
  %s AS connection_total,
  COALESCE(SUM(CASE WHEN is_ignored THEN %s ELSE 0 END), 0) AS ignored_metric_total,
  COALESCE(SUM(nxdomain_lookups), 0) AS nxdomain_lookup_total,
  COALESCE(SUM(successful_lookups), 0) AS successful_lookup_total,
  COALESCE(MIN(time_start_ns), 0) AS first_seen_ns,
  COALESCE(MAX(time_end_ns), 0) AS last_seen_ns
FROM %s
GROUP BY source_bucket, destination_bucket
ORDER BY %s DESC, source_bucket, destination_bucket
`, cte, srcBucket, dstBucket, connectionTotalExpression(state.Metric), metricValueExpression(state.Metric), filteredCTEName, metricOrderExpression(state.Metric))

	rows, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query edges: %w", err)
	}
	defer rows.Close()

	edges := make([]Edge, 0, 128)
	for rows.Next() {
		var edge Edge
		var ignoredMetric int64
		if err := rows.Scan(&edge.Source, &edge.Destination, &edge.Bytes, &edge.Connections, &ignoredMetric, &edge.NXDomainLookups, &edge.SuccessfulLookups, &edge.FirstSeenNs, &edge.LastSeenNs); err != nil {
			return nil, fmt.Errorf("scan edge row: %w", err)
		}
		edge.Ignored = ignoredMetric > 0
		edge.IgnoredMetric = ignoredMetric
		edge.MetricValue = edgeMetricValue(edge, state.Metric)
		edge.DNSResultState = dnsResultStateForCounts(edge.NXDomainLookups, edge.SuccessfulLookups)
		edge.Synthetic = edge.Source == graphRestID || edge.Destination == graphRestID
		edge.Selected = state.SelectedEdgeSrc == edge.Source && state.SelectedEdgeDst == edge.Destination
		edges = append(edges, edge)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate edge rows: %w", err)
	}
	return edges, nil
}

func totalsFromEdges(edges []Edge, totalEntities, visibleEdges int, metric Metric) Totals {
	var totals Totals
	for _, edge := range edges {
		totals.Bytes += edge.Bytes
		totals.Connections += edge.Connections
		totals.Ignored += ignoredMetricValue(edge, metric)
	}
	totals.Entities = totalEntities
	totals.Edges = visibleEdges
	return totals
}

func (s *Service) selectionDetails(
	ctx context.Context,
	state QueryState,
	span TimeSpan,
	keepEntities []string,
	visibleNodeMap map[string]Node,
	visibleEdges []Edge,
) (*Node, *Edge, []DetailPeer, []HistogramBin, SelectionBreakdown, error) {
	var selectedNode *Node
	if state.SelectedEntity != "" {
		if node, ok := visibleNodeMap[state.SelectedEntity]; ok {
			selectedCopy := node
			selectedNode = &selectedCopy
		}
	}

	var selectedEdge *Edge
	if state.SelectedEdgeSrc != "" && state.SelectedEdgeDst != "" {
		for _, edge := range visibleEdges {
			if edge.Source == state.SelectedEdgeSrc && edge.Destination == state.SelectedEdgeDst {
				edgeCopy := edge
				selectedEdge = &edgeCopy
				break
			}
		}
	}

	var breakdown SelectionBreakdown
	if state.Metric != MetricDNSLookups && state.EntityActionsEnabled() {
		var err error
		breakdown, err = s.selectionBreakdown(ctx, state)
		if err != nil {
			return nil, nil, nil, nil, SelectionBreakdown{}, err
		}
	}

	if selectedNode == nil {
		return nil, selectedEdge, nil, nil, breakdown, nil
	}

	if selectedNode.Synthetic {
		topEntities, err := s.queryRestTopEntities(ctx, state, keepEntities)
		if err != nil {
			return nil, nil, nil, nil, SelectionBreakdown{}, err
		}
		peers := make([]DetailPeer, 0, len(topEntities))
		for _, entity := range topEntities {
			peers = append(peers, DetailPeer{
				Entity:      entity.ID,
				MetricValue: entity.Total,
			})
		}
		return selectedNode, selectedEdge, peers, nil, breakdown, nil
	}

	var peers []DetailPeer
	var sparkline []HistogramBin
	group, groupContext := errgroup.WithContext(ctx)
	group.Go(func() error {
		var queryErr error
		peers, queryErr = s.queryNodePeers(groupContext, state, selectedNode.ID)
		return queryErr
	})
	group.Go(func() error {
		var queryErr error
		sparkline, queryErr = s.nodeSparklineWithSpan(groupContext, state, span, selectedNode.ID)
		return queryErr
	})
	if err := group.Wait(); err != nil {
		return nil, nil, nil, nil, SelectionBreakdown{}, err
	}
	return selectedNode, selectedEdge, peers, sparkline, breakdown, nil
}

func (s *Service) selectionBreakdown(ctx context.Context, state QueryState) (SelectionBreakdown, error) {
	if state.Metric == MetricDNSLookups {
		return SelectionBreakdown{}, nil
	}

	cte, args, err := s.filteredRawFlowsCTE(state)
	if err != nil {
		return SelectionBreakdown{}, err
	}
	scopeClause, scopeArgs := selectionBreakdownScopeClause(state)
	queryArgs := append(append([]any(nil), args...), scopeArgs...)

	var protocols []BreakdownSlice
	var families []BreakdownSlice
	var ports []BreakdownSlice

	group, groupContext := errgroup.WithContext(ctx)
	group.Go(func() error {
		var err error
		protocols, err = s.queryBreakdownProtocols(groupContext, state.Metric, cte, scopeClause, queryArgs)
		return err
	})
	group.Go(func() error {
		var err error
		families, err = s.queryBreakdownFamilies(groupContext, state.Metric, cte, scopeClause, queryArgs)
		return err
	})
	group.Go(func() error {
		var err error
		ports, err = s.queryBreakdownPorts(groupContext, state.Metric, cte, scopeClause, queryArgs)
		return err
	})
	if err := group.Wait(); err != nil {
		return SelectionBreakdown{}, err
	}

	breakdown := SelectionBreakdown{}
	protocols = foldBreakdownSlices(protocols)
	families = foldBreakdownSlices(families)
	ports = foldBreakdownSlices(ports)
	if len(protocols) > 1 {
		breakdown.Protocols = &BreakdownChart{Label: "Protocols", Slices: protocols}
	}
	if len(families) > 1 {
		breakdown.Family = &BreakdownChart{Label: "IP Family", Slices: families}
	}
	if len(ports) > 1 {
		breakdown.Ports = &BreakdownChart{Label: "Popular Ports", Slices: ports}
	}
	return breakdown, nil
}

func selectionBreakdownScopeClause(state QueryState) (string, []any) {
	switch {
	case state.SelectedEdgeSrc != "" && state.SelectedEdgeDst != "":
		return fmt.Sprintf("(%s = ? AND %s = ?)", srcScopeEntityColumn, dstScopeEntityColumn), []any{state.SelectedEdgeSrc, state.SelectedEdgeDst}
	case state.SelectedEntity != "":
		return fmt.Sprintf("(%s = ? OR %s = ?)", srcScopeEntityColumn, dstScopeEntityColumn), []any{state.SelectedEntity, state.SelectedEntity}
	default:
		return "true", nil
	}
}

func (s *Service) queryBreakdownProtocols(ctx context.Context, metric Metric, cte, scopeClause string, args []any) ([]BreakdownSlice, error) {
	query := fmt.Sprintf(`%s
SELECT protocol, SUM(metric_value) AS total_metric
FROM (
  SELECT protocol, %s AS metric_value
  FROM %s
  WHERE %s
) protocol_breakdown
GROUP BY protocol
ORDER BY total_metric DESC, protocol
`, cte, metricValueExpression(metric), filteredCTEName, scopeClause)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query protocol breakdown: %w", err)
	}
	defer rows.Close()

	slices := make([]BreakdownSlice, 0, 8)
	for rows.Next() {
		var protocol int32
		var total int64
		if err := rows.Scan(&protocol, &total); err != nil {
			return nil, fmt.Errorf("scan protocol breakdown row: %w", err)
		}
		slices = append(slices, BreakdownSlice{
			FilterParam: "protocol",
			FilterValue: strconv.FormatInt(int64(protocol), 10),
			Label:       rawFlowProtocolLabel(protocol),
			Value:       total,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate protocol breakdown rows: %w", err)
	}
	return slices, nil
}

func (s *Service) queryBreakdownFamilies(ctx context.Context, metric Metric, cte, scopeClause string, args []any) ([]BreakdownSlice, error) {
	query := fmt.Sprintf(`%s
SELECT ip_version, SUM(metric_value) AS total_metric
FROM (
  SELECT ip_version, %s AS metric_value
  FROM %s
  WHERE %s
) family_breakdown
GROUP BY ip_version
ORDER BY total_metric DESC, ip_version
`, cte, metricValueExpression(metric), filteredCTEName, scopeClause)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query family breakdown: %w", err)
	}
	defer rows.Close()

	slices := make([]BreakdownSlice, 0, 2)
	for rows.Next() {
		var family int32
		var total int64
		if err := rows.Scan(&family, &total); err != nil {
			return nil, fmt.Errorf("scan family breakdown row: %w", err)
		}
		slice := BreakdownSlice{Label: ipFamilyBreakdownLabel(family), Value: total}
		if filterValue := addressFamilyFilterValue(family); filterValue != "" {
			slice.FilterParam = "family"
			slice.FilterValue = filterValue
		}
		slices = append(slices, slice)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate family breakdown rows: %w", err)
	}
	return slices, nil
}

func (s *Service) queryBreakdownPorts(ctx context.Context, metric Metric, cte, scopeClause string, args []any) ([]BreakdownSlice, error) {
	query := fmt.Sprintf(`%s
SELECT service_port, SUM(metric_value) AS total_metric
FROM (
  SELECT
    service_port,
    %s AS metric_value
  FROM %s
  WHERE %s
) port_breakdown
WHERE service_port IS NOT NULL
GROUP BY service_port
ORDER BY total_metric DESC, service_port
`, cte, metricValueExpression(metric), filteredCTEName, scopeClause)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query port breakdown: %w", err)
	}
	defer rows.Close()

	ports := make([]BreakdownSlice, 0, breakdownSliceLimit+1)
	for rows.Next() {
		var port int32
		var total int64
		if err := rows.Scan(&port, &total); err != nil {
			return nil, fmt.Errorf("scan port breakdown row: %w", err)
		}
		portLabel := strconv.FormatInt(int64(port), 10)
		ports = append(ports, BreakdownSlice{FilterParam: "port", FilterValue: portLabel, Label: portLabel, Value: total})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate port breakdown rows: %w", err)
	}
	return ports, nil
}

func foldBreakdownSlices(slices []BreakdownSlice) []BreakdownSlice {
	if len(slices) == 0 {
		return nil
	}
	var total int64
	for _, slice := range slices {
		total += slice.Value
	}
	if total <= 0 {
		return nil
	}
	ret := make([]BreakdownSlice, 0, breakdownSliceLimit)
	var restTotal int64
	for index, slice := range slices {
		if slice.Value <= 0 {
			continue
		}
		if index < breakdownSliceLimit && slice.Value*100 >= total*5 {
			ret = append(ret, slice)
			continue
		}
		restTotal += slice.Value
	}
	if restTotal > 0 {
		if len(ret) >= breakdownSliceLimit {
			restTotal += ret[len(ret)-1].Value
			ret = ret[:len(ret)-1]
		}
		ret = append(ret, BreakdownSlice{Label: breakdownRestLabel, Value: restTotal})
	}
	return ret
}

func breakdownFromTotals(protocolTotals, familyTotals, portTotals map[int32]int64) SelectionBreakdown {
	protocols := foldBreakdownSlices(breakdownSlicesFromIntTotals(protocolTotals, "protocol", rawFlowProtocolLabel, func(value int32) string {
		return strconv.FormatInt(int64(value), 10)
	}))
	families := foldBreakdownSlices(breakdownSlicesFromIntTotals(familyTotals, "family", ipFamilyBreakdownLabel, addressFamilyFilterValue))
	ports := foldBreakdownSlices(breakdownSlicesFromIntTotals(portTotals, "port", func(value int32) string {
		return strconv.FormatInt(int64(value), 10)
	}, func(value int32) string {
		return strconv.FormatInt(int64(value), 10)
	}))

	breakdown := SelectionBreakdown{}
	if len(protocols) > 1 {
		breakdown.Protocols = &BreakdownChart{Label: "Protocols", Slices: protocols}
	}
	if len(families) > 1 {
		breakdown.Family = &BreakdownChart{Label: "IP Family", Slices: families}
	}
	if len(ports) > 1 {
		breakdown.Ports = &BreakdownChart{Label: "Popular Ports", Slices: ports}
	}
	return breakdown
}

func breakdownSlicesFromIntTotals(totals map[int32]int64, filterParam string, labelFunc, filterValueFunc func(int32) string) []BreakdownSlice {
	values := make([]int32, 0, len(totals))
	for value, total := range totals {
		if total <= 0 {
			continue
		}
		values = append(values, value)
	}
	slices.SortFunc(values, func(left, right int32) int {
		leftTotal := totals[left]
		rightTotal := totals[right]
		if leftTotal == rightTotal {
			if left < right {
				return -1
			}
			if left > right {
				return 1
			}
			return 0
		}
		if leftTotal > rightTotal {
			return -1
		}
		return 1
	})

	ret := make([]BreakdownSlice, 0, len(values))
	for _, value := range values {
		filterValue := filterValueFunc(value)
		slice := BreakdownSlice{
			Label: labelFunc(value),
			Value: totals[value],
		}
		if filterValue != "" {
			slice.FilterParam = filterParam
			slice.FilterValue = filterValue
		}
		ret = append(ret, slice)
	}
	return ret
}

func ipFamilyBreakdownLabel(family int32) string {
	switch family {
	case model.IPVersion4:
		return "IPv4"
	case model.IPVersion6:
		return "IPv6"
	default:
		return strconv.FormatInt(int64(family), 10)
	}
}

func addressFamilyFilterValue(family int32) string {
	switch family {
	case model.IPVersion4:
		return string(AddressFamilyIPv4)
	case model.IPVersion6:
		return string(AddressFamilyIPv6)
	default:
		return ""
	}
}

func (s *Service) queryNodePeers(ctx context.Context, state QueryState, entity string) ([]DetailPeer, error) {
	cte, args, err := s.filteredCTE(state)
	if err != nil {
		return nil, err
	}
	metricExpr := metricValueExpression(state.Metric)
	query := fmt.Sprintf(`%s
SELECT peer_entity, SUM(metric_total) AS metric_total
FROM (
  SELECT dst_entity AS peer_entity, %s AS metric_total FROM %s WHERE src_entity = ?
  UNION ALL
  SELECT src_entity AS peer_entity, %s AS metric_total FROM %s WHERE dst_entity = ?
) peer_totals
GROUP BY peer_entity
ORDER BY metric_total DESC, peer_entity
LIMIT %d
`, cte, metricExpr, filteredCTEName, metricExpr, filteredCTEName, nodeDetailPeerLimit)

	queryArgs := append(append([]any(nil), args...), entity, entity)
	rows, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query node peers: %w", err)
	}
	defer rows.Close()

	peers := make([]DetailPeer, 0, nodeDetailPeerLimit)
	for rows.Next() {
		var peer DetailPeer
		if err := rows.Scan(&peer.Entity, &peer.MetricValue); err != nil {
			return nil, fmt.Errorf("scan peer row: %w", err)
		}
		peers = append(peers, peer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate peer rows: %w", err)
	}
	return peers, nil
}

func (s *Service) nodeSparklineWithSpan(ctx context.Context, state QueryState, span TimeSpan, entity string) ([]HistogramBin, error) {
	nodeState := state.Clone()
	nodeState.Include = []string{entity}
	nodeState.SelectedEntity = ""
	nodeState.SelectedEdgeSrc = ""
	nodeState.SelectedEdgeDst = ""
	return s.histogramWithSpan(ctx, nodeState, span)
}

func (s *Service) queryRestTopEntities(ctx context.Context, state QueryState, keepEntities []string) ([]Node, error) {
	if len(keepEntities) == 0 {
		return nil, nil
	}

	cte, args, err := s.filteredCTE(state)
	if err != nil {
		return nil, err
	}
	metricExpr := metricValueExpression(state.Metric)
	query := fmt.Sprintf(`%s
SELECT entity, SUM(total_metric) AS total_metric
FROM (
  SELECT src_entity AS entity, SUM(%s) AS total_metric
  FROM %s
  WHERE src_entity NOT IN (%s)
  GROUP BY src_entity
  UNION ALL
  SELECT dst_entity AS entity, SUM(%s) AS total_metric
  FROM %s
  WHERE dst_entity NOT IN (%s)
  GROUP BY dst_entity
) rest_entities
GROUP BY entity
ORDER BY total_metric DESC, entity
LIMIT %d
`, cte, metricExpr, filteredCTEName, placeholders(len(keepEntities)), metricExpr, filteredCTEName, placeholders(len(keepEntities)), restTopEntityLimit)

	queryArgs := append(append([]any(nil), args...), stringsToAny(keepEntities)...)
	queryArgs = append(queryArgs, stringsToAny(keepEntities)...)
	rows, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query rest top entities: %w", err)
	}
	defer rows.Close()

	nodes := make([]Node, 0, restTopEntityLimit)
	for rows.Next() {
		var node Node
		if err := rows.Scan(&node.ID, &node.Total); err != nil {
			return nil, fmt.Errorf("scan rest top entity row: %w", err)
		}
		node.Label = node.ID
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rest top entity rows: %w", err)
	}
	return nodes, nil
}

func entityExpressions(granularity Granularity) (string, string) {
	switch granularity {
	case GranularityTLD:
		return localTLDExpression("src_is_private", "ip_version", "src_host", "src_tld"), localTLDExpression("dst_is_private", "ip_version", "dst_host", "dst_tld")
	case Granularity2LD:
		return local2LDExpression("src_is_private", "ip_version", "src_host", "src_2ld", "src_tld"), local2LDExpression("dst_is_private", "ip_version", "dst_host", "dst_2ld", "dst_tld")
	case GranularityHostname:
		return coalescedEntityExpression("src_host", "src_2ld", "src_tld", "src_ip"), coalescedEntityExpression("dst_host", "dst_2ld", "dst_tld", "dst_ip")
	case GranularityIP:
		return "src_ip", "dst_ip"
	default:
		return coalescedEntityExpression("src_2ld", "src_tld", "src_ip"), coalescedEntityExpression("dst_2ld", "dst_tld", "dst_ip")
	}
}

func entityExpressionsForState(state QueryState) (string, string) {
	srcExpr, dstExpr := entityExpressions(state.Granularity)
	if state.LocalIdentity != LocalIdentityDevice {
		return srcExpr, dstExpr
	}
	return localDeviceEntityExpression("src_is_private", "src_device_label", srcExpr),
		localDeviceEntityExpression("dst_is_private", "dst_device_label", dstExpr)
}

func dnsLookupEntityExpressions(granularity Granularity) (string, string) {
	switch granularity {
	case GranularityTLD:
		return localTLDExpression("client_is_private", "client_ip_version", "client_host", "client_tld"), coalescedEntityExpression("query_tld", "query_name")
	case Granularity2LD:
		return local2LDExpression("client_is_private", "client_ip_version", "client_host", "client_2ld", "client_tld"), coalescedEntityExpression("query_2ld", "query_tld", "query_name")
	case GranularityHostname:
		return coalescedEntityExpression("client_host", "client_2ld", "client_tld", "client_ip"), "query_name"
	case GranularityIP:
		return "client_ip", "query_name"
	default:
		return local2LDExpression("client_is_private", "client_ip_version", "client_host", "client_2ld", "client_tld"), coalescedEntityExpression("query_2ld", "query_tld", "query_name")
	}
}

func dnsLookupEntityExpressionsForState(state QueryState) (string, string) {
	srcExpr, dstExpr := dnsLookupEntityExpressions(state.Granularity)
	if state.LocalIdentity != LocalIdentityDevice {
		return srcExpr, dstExpr
	}
	return localDeviceEntityExpression("client_is_private", "client_device_label", srcExpr), dstExpr
}

func localDeviceEntityExpression(privateColumn, labelColumn, fallbackExpr string) string {
	return fmt.Sprintf(
		"CASE WHEN %s AND NULLIF(%s, '') IS NOT NULL THEN %s ELSE %s END",
		privateColumn,
		labelColumn,
		labelColumn,
		fallbackExpr,
	)
}

func coalescedEntityExpression(fields ...string) string {
	return coalescedEntityExpressionWithDefault("", fields...)
}

func localTLDExpression(privateColumn, ipVersionColumn, hostColumn string, fields ...string) string {
	return fmt.Sprintf(
		"CASE WHEN %s THEN COALESCE(%s, %s) ELSE COALESCE(%s, %s) END",
		privateColumn,
		localResolvedTLDExpression(hostColumn),
		localIPVersionExpression(ipVersionColumn),
		coalescedEntityExpression(fields...),
		quoteLiteral(unknownPublicEntityLabel),
	)
}

func local2LDExpression(privateColumn, ipVersionColumn, hostColumn string, fields ...string) string {
	return fmt.Sprintf(
		"CASE WHEN %s THEN COALESCE(%s, %s) ELSE COALESCE(%s, %s) END",
		privateColumn,
		localResolved2LDExpression(hostColumn),
		localIPVersionExpression(ipVersionColumn),
		coalescedEntityExpression(fields...),
		quoteLiteral(unknownPublicEntityLabel),
	)
}

func localResolvedTLDExpression(hostColumn string) string {
	return fmt.Sprintf(
		"CASE WHEN %s THEN %s END",
		localHostPredicate(hostColumn),
		quoteLiteral(localEntityLabel),
	)
}

func localResolved2LDExpression(hostColumn string) string {
	return fmt.Sprintf(
		"CASE WHEN %s THEN SPLIT_PART(%s, '.', 1) END",
		localHostPredicate(hostColumn),
		hostColumn,
	)
}

func localHostPredicate(hostColumn string) string {
	return fmt.Sprintf(
		"NULLIF(%s, '') IS NOT NULL AND %s NOT IN (%s, %s)",
		hostColumn,
		hostColumn,
		quoteLiteral(localIPv4EntityLabel),
		quoteLiteral(localIPv6EntityLabel),
	)
}

func localIPVersionExpression(ipVersionColumn string) string {
	return fmt.Sprintf(
		"CASE WHEN %s = %d THEN %s WHEN %s = %d THEN %s ELSE %s END",
		ipVersionColumn,
		model.IPVersion4,
		quoteLiteral(localIPv4EntityLabel),
		ipVersionColumn,
		model.IPVersion6,
		quoteLiteral(localIPv6EntityLabel),
		quoteLiteral(unknownPrivateEntityLabel),
	)
}

func coalescedEntityExpressionWithDefault(defaultValue string, fields ...string) string {
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		parts = append(parts, fmt.Sprintf("NULLIF(%s, '')", field))
	}
	if defaultValue != "" {
		parts = append(parts, quoteLiteral(defaultValue))
	}
	return fmt.Sprintf("COALESCE(%s)", strings.Join(parts, ", "))
}

func filterClause(state QueryState, srcExpr, dstExpr string) (string, []any) {
	conditions := []string{
		"time_end_ns >= ?",
		"time_start_ns <= ?",
	}
	args := []any{state.FromNs, state.ToNs}

	if len(state.Include) > 0 {
		inPlaceholders := placeholders(len(state.Include))
		conditions = append(conditions, fmt.Sprintf("(%s IN (%s) OR %s IN (%s))", srcExpr, inPlaceholders, dstExpr, inPlaceholders))
		args = append(args, stringsToAny(state.Include)...)
		args = append(args, stringsToAny(state.Include)...)
	}

	if len(state.Exclude) > 0 {
		inPlaceholders := placeholders(len(state.Exclude))
		conditions = append(conditions, fmt.Sprintf("(%s NOT IN (%s) AND %s NOT IN (%s))", srcExpr, inPlaceholders, dstExpr, inPlaceholders))
		args = append(args, stringsToAny(state.Exclude)...)
		args = append(args, stringsToAny(state.Exclude)...)
	}

	if state.Search != "" {
		searchLike := "%" + strings.ToLower(state.Search) + "%"
		conditions = append(conditions, fmt.Sprintf("(LOWER(%s) LIKE ? OR LOWER(%s) LIKE ?)", srcExpr, dstExpr))
		args = append(args, searchLike, searchLike)
	}

	if state.Metric != MetricDNSLookups {
		switch state.Direction {
		case DirectionEgress:
			conditions = append(conditions, "direction = ?")
			args = append(args, directionEgressParquetValue)
		case DirectionIngress:
			conditions = append(conditions, "direction = ?")
			args = append(args, directionIngressParquetValue)
		}
		if state.Protocol > 0 {
			conditions = append(conditions, "protocol = ?")
			args = append(args, state.Protocol)
		}
		if state.Port > 0 {
			conditions = append(conditions, rawServicePortExpression()+" = ?")
			args = append(args, state.Port)
		}
	}

	switch state.AddressFamily {
	case AddressFamilyIPv4:
		conditions = append(conditions, "ip_version = ?")
		args = append(args, model.IPVersion4)
	case AddressFamilyIPv6:
		conditions = append(conditions, "ip_version = ?")
		args = append(args, model.IPVersion6)
	}

	return strings.Join(conditions, " AND "), args
}

func rawServicePortExpression() string {
	return servicePortExpression("src_port", "dst_port", "protocol", "direction")
}

func servicePortExpression(srcPortColumn, dstPortColumn, protocolColumn, directionColumn string) string {
	return fmt.Sprintf(`CASE
  WHEN %s NOT IN (6, 17) THEN NULL
  WHEN %s BETWEEN 1 AND 9999 AND %s BETWEEN 1 AND 9999 THEN
    CASE
      WHEN %s = %d THEN %s
      WHEN %s = %d THEN %s
      WHEN %s <= %s THEN %s
      ELSE %s
    END
  WHEN %s BETWEEN 1 AND 9999 THEN %s
  WHEN %s BETWEEN 1 AND 9999 THEN %s
  ELSE NULL
END`,
		protocolColumn,
		srcPortColumn,
		dstPortColumn,
		directionColumn,
		directionEgressParquetValue,
		dstPortColumn,
		directionColumn,
		directionIngressParquetValue,
		srcPortColumn,
		srcPortColumn,
		dstPortColumn,
		srcPortColumn,
		dstPortColumn,
		srcPortColumn,
		srcPortColumn,
		dstPortColumn,
		dstPortColumn,
	)
}

func metricValueExpression(metric Metric) string {
	if metric == MetricDNSLookups {
		return "lookups"
	}
	if metric == MetricConnections {
		return "1"
	}
	return "bytes"
}

func privateMetricExpression(privateColumn string, metric Metric) string {
	if metric == MetricDNSLookups {
		return fmt.Sprintf("CASE WHEN %s THEN lookups ELSE 0 END", privateColumn)
	}
	if metric == MetricConnections {
		return fmt.Sprintf("CASE WHEN %s THEN 1 ELSE 0 END", privateColumn)
	}

	return fmt.Sprintf("CASE WHEN %s THEN bytes ELSE 0 END", privateColumn)
}

func publicMetricExpression(privateColumn string, metric Metric) string {
	if metric == MetricDNSLookups {
		return fmt.Sprintf("CASE WHEN %s THEN 0 ELSE lookups END", privateColumn)
	}
	if metric == MetricConnections {
		return fmt.Sprintf("CASE WHEN %s THEN 0 ELSE 1 END", privateColumn)
	}

	return fmt.Sprintf("CASE WHEN %s THEN 0 ELSE bytes END", privateColumn)
}

func metricOrderExpression(metric Metric) string {
	if metric == MetricDNSLookups {
		return "COALESCE(SUM(lookups), 0)"
	}
	if metric == MetricConnections {
		return "COUNT(*)"
	}
	return "COALESCE(SUM(bytes), 0)"
}

func connectionTotalExpression(metric Metric) string {
	if metric == MetricDNSLookups {
		return "COALESCE(SUM(lookups), 0)"
	}
	return "COUNT(*)"
}

func chooseKeepEntities(nodeTotals []Node, state QueryState) []string {
	if state.NodeLimit == 0 || len(nodeTotals) <= state.NodeLimit {
		entities := make([]string, 0, len(nodeTotals))
		for _, node := range nodeTotals {
			entities = append(entities, node.ID)
		}
		return entities
	}

	forcedLookup := make(map[string]struct{}, len(state.Include)+1)
	for _, entity := range state.Include {
		forcedLookup[entity] = struct{}{}
	}
	if state.SelectedEntity != "" && state.SelectedEntity != graphRestID {
		forcedLookup[state.SelectedEntity] = struct{}{}
	}

	keep := make([]string, 0, state.NodeLimit)
	keepLookup := make(map[string]struct{}, state.NodeLimit)
	for _, node := range nodeTotals {
		if _, ok := forcedLookup[node.ID]; !ok {
			continue
		}
		keep = append(keep, node.ID)
		keepLookup[node.ID] = struct{}{}
	}
	for _, node := range nodeTotals {
		if len(keep) >= state.NodeLimit {
			break
		}
		if _, ok := keepLookup[node.ID]; ok {
			continue
		}
		keep = append(keep, node.ID)
		keepLookup[node.ID] = struct{}{}
	}
	return keep
}

func limitEdges(edges []Edge, edgeLimit int, selectedEntity string) ([]Edge, int) {
	if edgeLimit == 0 || len(edges) <= edgeLimit {
		return edges, 0
	}
	selectedEdges := make([]Edge, 0, min(len(edges), edgeLimit))
	selectedLookup := make(map[string]struct{}, len(edges))
	if selectedEntity != "" {
		for _, edge := range edges {
			if edge.Source != selectedEntity && edge.Destination != selectedEntity {
				continue
			}
			key := edge.Source + "\x00" + edge.Destination
			selectedLookup[key] = struct{}{}
			selectedEdges = append(selectedEdges, edge)
		}
	}

	limited := make([]Edge, 0, edgeLimit)
	for _, edge := range selectedEdges {
		limited = append(limited, edge)
		if len(limited) >= edgeLimit {
			return limited, max(0, len(edges)-len(limited))
		}
	}
	for _, edge := range edges {
		key := edge.Source + "\x00" + edge.Destination
		if _, ok := selectedLookup[key]; ok {
			continue
		}
		limited = append(limited, edge)
		if len(limited) >= edgeLimit {
			break
		}
	}
	return limited, max(0, len(edges)-len(limited))
}

func limitNodes(nodes []Node, limit int) []Node {
	if len(nodes) <= limit {
		return nodes
	}
	return nodes[:limit]
}

func limitTopEdges(edges []Edge, limit int) []Edge {
	if len(edges) <= limit {
		return edges
	}
	return edges[:limit]
}

func edgeMetricValue(edge Edge, metric Metric) int64 {
	if metric == MetricConnections || metric == MetricDNSLookups {
		return edge.Connections
	}
	return edge.Bytes
}

func ignoredMetricValue(edge Edge, metric Metric) int64 {
	if edge.IgnoredMetric > 0 {
		return edge.IgnoredMetric
	}
	if !edge.Ignored {
		return 0
	}
	return edgeMetricValue(edge, metric)
}

func dnsResultStateForCounts(nxdomainLookups, successfulLookups int64) dnsResultState {
	if nxdomainLookups <= 0 {
		return dnsResultStateSuccess
	}
	if successfulLookups > 0 {
		return dnsResultStateMixed
	}
	return dnsResultStateNXDOMAIN
}

func countNonSynthetic(nodeTotals []Node, keepLookup map[string]struct{}) int {
	total := 0
	for _, node := range nodeTotals {
		if _, ok := keepLookup[node.ID]; ok {
			total++
		}
	}
	return total
}

func buildBreadcrumbs(state QueryState) []string {
	metricLabel := "Bytes"
	switch state.Metric {
	case MetricDNSLookups:
		metricLabel = "DNS Lookups"
	case MetricConnections:
		metricLabel = "Connections"
	}
	breadcrumbs := []string{
		"All traffic",
		"Granularity: " + strings.ToUpper(string(state.Granularity)),
		"Metric: " + metricLabel,
	}
	if state.LocalIdentity == LocalIdentityDevice {
		breadcrumbs = append(breadcrumbs, "Local: Device")
	}
	if len(state.Include) > 0 {
		breadcrumbs = append(breadcrumbs, "Entity: "+strings.Join(state.Include, ", "))
	}
	if len(state.Exclude) > 0 {
		breadcrumbs = append(breadcrumbs, "Exclude: "+strings.Join(state.Exclude, ", "))
	}
	if state.Direction != "" && state.Direction != DirectionBoth {
		breadcrumbs = append(breadcrumbs, "Direction: "+directionLabel(state.Direction))
	}
	if state.Protocol > 0 {
		breadcrumbs = append(breadcrumbs, "Protocol: "+rawFlowProtocolLabel(state.Protocol))
	}
	if state.Port > 0 {
		breadcrumbs = append(breadcrumbs, "Port: "+strconv.FormatInt(int64(state.Port), 10))
	}
	if state.HideIgnored {
		breadcrumbs = append(breadcrumbs, "Ignored: hidden")
	} else {
		breadcrumbs = append(breadcrumbs, "Ignored: visible")
	}
	return breadcrumbs
}

func placeholders(count int) string {
	if count <= 0 {
		return ""
	}
	values := make([]string, 0, count)
	for range count {
		values = append(values, "?")
	}
	return strings.Join(values, ", ")
}

func stringsToAny(values []string) []any {
	args := make([]any, 0, len(values))
	for _, value := range values {
		args = append(args, value)
	}
	return args
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func readParquetExpression(path string, unionByName bool) string {
	if unionByName {
		return fmt.Sprintf("read_parquet(%s, union_by_name=true)", quoteLiteral(path))
	}
	return fmt.Sprintf("read_parquet(%s)", quoteLiteral(path))
}

func (s *Service) currentRevision() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision
}

func (s *Service) detectINETSupport(ctx context.Context) bool {
	query := "SELECT CAST('127.0.0.1' AS INET) IS NOT NULL"
	var value bool
	if err := s.db.QueryRowContext(ctx, query).Scan(&value); err != nil {
		slog.Debug("DuckDB inet support unavailable", "err", err)
		return false
	}
	return value
}

func (s *Service) hasDNSLookupData() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dnsLookupValid
}

func (s *Service) hasDNSLookupAnswer() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dnsLookupHasAnswer
}

func (s *Service) ignoreRulesSnapshot() []IgnoreRule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]IgnoreRule(nil), s.ignoreRules...)
}

func (s *Service) ignoreRuleCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.ignoreRules)
}

func (s *Service) enabledIgnoreRules() []IgnoreRule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return enabledIgnoreRules(s.ignoreRules)
}

func (s *Service) inetSupportEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inetAvailable
}

func (s *Service) SaveIgnoreRule(rule IgnoreRule) error {
	rule = normalizeIgnoreRule(rule)
	if err := validateIgnoreRule(rule); err != nil {
		return err
	}

	s.mu.Lock()
	nextRules := upsertIgnoreRule(s.ignoreRules, rule)
	s.mu.Unlock()
	if err := saveIgnoreRules(s.ignoreRulePath, nextRules); err != nil {
		return err
	}
	return s.refreshMetadataWithOptions(s.bgCtx, refreshMetadataOptions{forceSummary: true})
}

func (s *Service) DeleteIgnoreRule(id string) error {
	s.mu.Lock()
	nextRules := deleteIgnoreRule(s.ignoreRules, id)
	s.mu.Unlock()
	if err := saveIgnoreRules(s.ignoreRulePath, nextRules); err != nil {
		return err
	}
	return s.refreshMetadataWithOptions(s.bgCtx, refreshMetadataOptions{forceSummary: true})
}

func (s *Service) ToggleIgnoreRuleEnabled(id string) error {
	s.mu.Lock()
	nextRules, ok := toggleIgnoreRuleEnabled(s.ignoreRules, id)
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("ignore rule %q not found", id)
	}
	if err := saveIgnoreRules(s.ignoreRulePath, nextRules); err != nil {
		return err
	}
	return s.refreshMetadataWithOptions(s.bgCtx, refreshMetadataOptions{forceSummary: true})
}

func (s *Service) layoutPositions(ctx context.Context, state QueryState) (map[string]LayoutPoint, error) {
	cacheState := state.layoutCacheState()
	cacheKey := cacheState.cacheKey(layoutCacheKind, s.currentRevision())
	if positions, ok := s.layoutCache.Get(cacheKey); ok {
		return positions, nil
	}

	if cacheState.Metric == MetricDNSLookups {
		nodeTotals, err := s.queryNodeTotals(ctx, cacheState)
		if err != nil {
			return nil, err
		}
		keepEntities := chooseKeepEntities(nodeTotals, cacheState)
		var edges []Edge
		group, groupContext := errgroup.WithContext(ctx)
		group.Go(func() error {
			var queryErr error
			nodeTotals, queryErr = s.appendRestNodes(groupContext, cacheState, keepEntities, nodeTotals)
			return queryErr
		})
		group.Go(func() error {
			var queryErr error
			edges, queryErr = s.queryEdges(groupContext, cacheState, keepEntities)
			return queryErr
		})
		if err := group.Wait(); err != nil {
			return nil, err
		}
		visibleEdges, _ := limitEdges(edges, cacheState.EdgeLimit, cacheState.SelectedEntity)
		positions := buildSingleMetricLayoutPositions(nodeTotals, visibleEdges)
		s.layoutCache.Set(cacheKey, positions)
		return positions, nil
	}

	bytesState := cacheState.Clone()
	bytesState.Metric = MetricBytes
	connectionState := cacheState.Clone()
	connectionState.Metric = MetricConnections

	var bytesNodeTotals []Node
	var connectionNodeTotals []Node
	group, groupContext := errgroup.WithContext(ctx)
	group.Go(func() error {
		var queryErr error
		bytesNodeTotals, queryErr = s.queryNodeTotals(groupContext, bytesState)
		return queryErr
	})
	group.Go(func() error {
		var queryErr error
		connectionNodeTotals, queryErr = s.queryNodeTotals(groupContext, connectionState)
		return queryErr
	})
	if err := group.Wait(); err != nil {
		return nil, err
	}

	bytesKeepEntities := chooseKeepEntities(bytesNodeTotals, cacheState)
	connectionKeepEntities := chooseKeepEntities(connectionNodeTotals, cacheState)
	keepEntities := unionKeepEntities(bytesNodeTotals, connectionNodeTotals, cacheState)
	var bytesEdges []Edge
	var connectionEdges []Edge
	group, groupContext = errgroup.WithContext(ctx)
	group.Go(func() error {
		var queryErr error
		bytesNodeTotals, queryErr = s.appendRestNodes(groupContext, bytesState, bytesKeepEntities, bytesNodeTotals)
		return queryErr
	})
	group.Go(func() error {
		var queryErr error
		connectionNodeTotals, queryErr = s.appendRestNodes(groupContext, connectionState, connectionKeepEntities, connectionNodeTotals)
		return queryErr
	})
	group.Go(func() error {
		var queryErr error
		bytesEdges, queryErr = s.queryEdges(groupContext, bytesState, keepEntities)
		return queryErr
	})
	group.Go(func() error {
		var queryErr error
		connectionEdges, queryErr = s.queryEdges(groupContext, connectionState, keepEntities)
		return queryErr
	})
	if err := group.Wait(); err != nil {
		return nil, err
	}
	bytesVisibleEdges, _ := limitEdges(bytesEdges, cacheState.EdgeLimit, cacheState.SelectedEntity)
	connectionVisibleEdges, _ := limitEdges(connectionEdges, cacheState.EdgeLimit, cacheState.SelectedEntity)

	positions := buildStableLayoutPositions(
		bytesNodeTotals,
		connectionNodeTotals,
		bytesVisibleEdges,
		connectionVisibleEdges,
	)
	s.layoutCache.Set(cacheKey, positions)
	return positions, nil
}

func buildSingleMetricLayoutPositions(nodes []Node, edges []Edge) map[string]LayoutPoint {
	nodeRanks := nodeRankLookup(nodes)
	layoutNodesByID := make(map[string]layoutNode, len(nodes)+len(edges)*2)
	for _, node := range nodes {
		layoutNodesByID[node.ID] = layoutNode{
			ID:        node.ID,
			Score:     int64(max(1, nodeRanks[node.ID])),
			Synthetic: node.Synthetic,
		}
	}

	layoutEdges := make([]layoutEdge, 0, len(edges))
	for _, edge := range edges {
		layoutEdges = append(layoutEdges, layoutEdge{
			Bytes:       edge.Bytes,
			Connections: edge.Connections,
			Destination: edge.Destination,
			Source:      edge.Source,
		})
	}

	layoutNodes := make([]layoutNode, 0, len(layoutNodesByID))
	for _, node := range layoutNodesByID {
		layoutNodes = append(layoutNodes, node)
	}
	return computeStableNodePositions(layoutNodes, layoutEdges, graphWidthPx, graphHeightPx)
}

func (s *Service) appendRestNodes(ctx context.Context, state QueryState, keepEntities []string, nodes []Node) ([]Node, error) {
	restNode, err := s.queryRestNode(ctx, state, keepEntities)
	if err != nil {
		return nil, err
	}
	if restNode != nil {
		nodes = append(nodes, *restNode)
	}
	return nodes, nil
}

func buildStableLayoutPositions(
	bytesNodeTotals []Node,
	connectionNodeTotals []Node,
	bytesEdges []Edge,
	connectionEdges []Edge,
) map[string]LayoutPoint {
	bytesRank := nodeRankLookup(bytesNodeTotals)
	connectionRank := nodeRankLookup(connectionNodeTotals)
	layoutNodesByID := make(map[string]layoutNode, max(len(bytesNodeTotals), len(connectionNodeTotals))+2)
	for _, node := range bytesNodeTotals {
		layoutNodesByID[node.ID] = layoutNode{
			ID:        node.ID,
			Score:     nodeLayoutScore(node.ID, bytesRank, connectionRank),
			Synthetic: node.Synthetic,
		}
	}
	for _, node := range connectionNodeTotals {
		layoutNodesByID[node.ID] = layoutNode{
			ID:        node.ID,
			Score:     nodeLayoutScore(node.ID, bytesRank, connectionRank),
			Synthetic: node.Synthetic,
		}
	}

	layoutEdgesByKey := make(map[string]layoutEdge, len(bytesEdges)+len(connectionEdges))
	for _, edge := range bytesEdges {
		layoutEdgesByKey[edge.Source+"\x00"+edge.Destination] = layoutEdge{
			Bytes:       edge.Bytes,
			Connections: edge.Connections,
			Destination: edge.Destination,
			Source:      edge.Source,
		}
		layoutNodesByID[edge.Source] = layoutNode{
			ID:        edge.Source,
			Score:     nodeLayoutScore(edge.Source, bytesRank, connectionRank),
			Synthetic: edge.Source == graphRestID,
		}
		layoutNodesByID[edge.Destination] = layoutNode{
			ID:        edge.Destination,
			Score:     nodeLayoutScore(edge.Destination, bytesRank, connectionRank),
			Synthetic: edge.Destination == graphRestID,
		}
	}
	for _, edge := range connectionEdges {
		key := edge.Source + "\x00" + edge.Destination
		layoutNodesByID[edge.Source] = layoutNode{
			ID:        edge.Source,
			Score:     nodeLayoutScore(edge.Source, bytesRank, connectionRank),
			Synthetic: edge.Source == graphRestID,
		}
		layoutNodesByID[edge.Destination] = layoutNode{
			ID:        edge.Destination,
			Score:     nodeLayoutScore(edge.Destination, bytesRank, connectionRank),
			Synthetic: edge.Destination == graphRestID,
		}
		if existing, ok := layoutEdgesByKey[key]; ok {
			existing.Bytes = max(existing.Bytes, edge.Bytes)
			existing.Connections = max(existing.Connections, edge.Connections)
			layoutEdgesByKey[key] = existing
			continue
		}
		layoutEdgesByKey[key] = layoutEdge{
			Bytes:       edge.Bytes,
			Connections: edge.Connections,
			Destination: edge.Destination,
			Source:      edge.Source,
		}
	}

	layoutNodes := make([]layoutNode, 0, len(layoutNodesByID))
	for _, node := range layoutNodesByID {
		layoutNodes = append(layoutNodes, node)
	}
	layoutEdges := make([]layoutEdge, 0, len(layoutEdgesByKey))
	for _, edge := range layoutEdgesByKey {
		layoutEdges = append(layoutEdges, edge)
	}

	return computeStableNodePositions(layoutNodes, layoutEdges, graphWidthPx, graphHeightPx)
}

func nodeRankLookup(nodes []Node) map[string]int {
	ranks := make(map[string]int, len(nodes))
	for index, node := range nodes {
		ranks[node.ID] = len(nodes) - index
	}
	return ranks
}

func nodeLayoutScore(nodeID string, bytesRank, connectionRank map[string]int) int64 {
	score := bytesRank[nodeID] + connectionRank[nodeID]
	if score == 0 {
		return 1
	}
	return int64(score)
}

func unionKeepEntities(bytesNodeTotals, connectionNodeTotals []Node, state QueryState) []string {
	keepLookup := make(map[string]struct{}, max(len(bytesNodeTotals), len(connectionNodeTotals)))
	keepEntities := make([]string, 0, max(len(bytesNodeTotals), len(connectionNodeTotals)))
	appendKeepEntities := func(values []string) {
		for _, entity := range values {
			if _, ok := keepLookup[entity]; ok {
				continue
			}
			keepLookup[entity] = struct{}{}
			keepEntities = append(keepEntities, entity)
		}
	}

	appendKeepEntities(chooseKeepEntities(bytesNodeTotals, state))
	appendKeepEntities(chooseKeepEntities(connectionNodeTotals, state))
	return keepEntities
}

type resultCache[T any] struct {
	limit int
	mu    sync.Mutex
	items map[string]T
	order []string
}

func newResultCache[T any](limit int) *resultCache[T] {
	return &resultCache[T]{
		limit: limit,
		items: make(map[string]T, limit),
		order: make([]string, 0, limit),
	}
}

func (c *resultCache[T]) Get(key string) (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	value, ok := c.items[key]
	if !ok {
		var zero T
		return zero, false
	}
	c.touch(key)
	return value, true
}

func (c *resultCache[T]) Set(key string, value T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.items[key]; ok {
		c.items[key] = value
		c.touch(key)
		return
	}

	if len(c.items) >= c.limit && len(c.order) > 0 {
		evictKey := c.order[0]
		delete(c.items, evictKey)
		c.order = c.order[1:]
	}

	c.items[key] = value
	c.order = append(c.order, key)
}

func (c *resultCache[T]) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]T, c.limit)
	c.order = c.order[:0]
}

func (c *resultCache[T]) touch(key string) {
	index := slices.Index(c.order, key)
	if index < 0 {
		c.order = append(c.order, key)
		return
	}
	c.order = append(append(c.order[:index], c.order[index+1:]...), key)
}

func (s QueryState) cacheKey(kind string, revision uint64) string {
	return kind + ":" + strconv.FormatUint(revision, 10) + ":" + s.Values().Encode()
}

func collectModTimes(srcRootPath string) (map[string]time.Time, error) {
	paths, err := scan.SortedFlatParquetPaths(srcRootPath)
	if err != nil {
		return nil, fmt.Errorf("list parquet paths: %w", err)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no parquet files found in %q", srcRootPath)
	}

	modTimes := make(map[string]time.Time, len(paths))
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("stat parquet %q: %w", path, err)
		}
		modTimes[path] = info.ModTime()
	}
	return modTimes, nil
}

func collectDNSLookupModTimes(srcRootPath string) (map[string]time.Time, error) {
	entries, err := os.ReadDir(srcRootPath)
	if err != nil {
		return nil, fmt.Errorf("read dir %q: %w", srcRootPath, err)
	}

	modTimes := make(map[string]time.Time)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), dnsLookupFilenamePrefix) || !strings.HasSuffix(entry.Name(), ".parquet") {
			continue
		}
		path := filepath.Join(srcRootPath, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("stat DNS lookup parquet %q: %w", path, err)
		}
		modTimes[path] = info.ModTime()
	}
	return modTimes, nil
}

func mapsEqual(left, right map[string]time.Time) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftValue := range left {
		rightValue, ok := right[key]
		if !ok || !rightValue.Equal(leftValue) {
			return false
		}
	}
	return true
}

func validateColumns(columns []string) error {
	columnLookup := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		columnLookup[column] = struct{}{}
	}
	for _, column := range requiredColumns {
		if _, ok := columnLookup[column]; !ok {
			return fmt.Errorf("missing required enriched parquet column %q", column)
		}
	}
	return nil
}

func columnsContain(columns []string, expected string) bool {
	for _, column := range columns {
		if column == expected {
			return true
		}
	}
	return false
}

func validateEnrichmentManifests(paths []string) error {
	for _, path := range paths {
		if _, err := parquetout.ReadEnrichmentManifest(path); err != nil {
			return fmt.Errorf("parquet %q is not enriched output: %w", path, err)
		}
	}
	return nil
}

func sortedMapKeys(values map[string]time.Time) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func sortTableRows(rows []TableRow, sortKey TableSort) {
	slices.SortFunc(rows, func(a, b TableRow) int {
		switch sortKey {
		case SortConnections, SortDNSLookups:
			if a.Connections != b.Connections {
				return compareInt64Desc(a.Connections, b.Connections)
			}
		case SortFirstSeen:
			if a.FirstSeenNs != b.FirstSeenNs {
				return compareInt64Desc(a.FirstSeenNs, b.FirstSeenNs)
			}
		case SortLastSeen:
			if a.LastSeenNs != b.LastSeenNs {
				return compareInt64Desc(a.LastSeenNs, b.LastSeenNs)
			}
		case SortSource:
			if a.Source != b.Source {
				return strings.Compare(a.Source, b.Source)
			}
		case SortDestination:
			if a.Destination != b.Destination {
				return strings.Compare(a.Destination, b.Destination)
			}
		default:
			if a.Bytes != b.Bytes {
				return compareInt64Desc(a.Bytes, b.Bytes)
			}
		}
		if a.Bytes != b.Bytes {
			return compareInt64Desc(a.Bytes, b.Bytes)
		}
		if a.Connections != b.Connections {
			return compareInt64Desc(a.Connections, b.Connections)
		}
		if a.Source != b.Source {
			return strings.Compare(a.Source, b.Source)
		}
		return strings.Compare(a.Destination, b.Destination)
	})
}

func compareInt64Desc(left, right int64) int {
	if left == right {
		return 0
	}
	if left > right {
		return -1
	}
	return 1
}
