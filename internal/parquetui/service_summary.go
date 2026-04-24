package parquetui

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/fingon/homenetflow/internal/model"
)

type summaryGraphSnapshotData struct {
	edges       []summaryEdgeAggregate
	granularity Granularity
	totals      Totals
}

type summaryEdgeAggregate struct {
	Bytes                 int64
	Connections           int64
	Destination           string
	Direction             *int32
	DstPrivateBytes       int64
	DstPrivateConnections int64
	DstPublicBytes        int64
	DstPublicConnections  int64
	FirstSeenNs           int64
	IPVersion             int32
	LastSeenNs            int64
	IgnoredBytes          int64
	IgnoredConnections    int64
	NXDomainLookups       int64
	Protocol              int32
	ServicePort           *int32
	Source                string
	SrcPrivateBytes       int64
	SrcPrivateConnections int64
	SrcPublicBytes        int64
	SrcPublicConnections  int64
	SuccessfulLookups     int64
}

type summaryMetricView struct {
	edges      []Edge
	nodeTotals []Node
}

func (s *Service) canUseSummaryGraph(state QueryState, span TimeSpan) bool {
	if state.Granularity != GranularityTLD && state.Granularity != Granularity2LD {
		return false
	}
	fullRange := state.FromNs == span.StartNs && state.ToNs == span.EndNs
	if !fullRange && state.EntityActionsEnabled() {
		return false
	}
	if len(state.Include) > 0 || len(state.Exclude) > 0 || state.Search != "" {
		return false
	}
	if state.SelectedEntity != "" || state.SelectedEdgeSrc != "" || state.SelectedEdgeDst != "" {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if state.Metric == MetricDNSLookups {
		paths := s.dnsSummaryEdgePathsForStateLocked(state, fullRange)
		return len(paths) > 0 && s.summaries.spanValid
	}
	paths := s.summaryEdgePathsForStateLocked(state, fullRange)
	return len(paths) > 0 && s.summaries.spanValid
}

func (s *Service) summaryEdgePathsForStateLocked(state QueryState, fullRange bool) []string {
	if state.LocalIdentity == LocalIdentityDevice {
		if fullRange {
			return s.summaries.deviceEdgePathsByGranulariy[state.Granularity]
		}
		return s.summaries.deviceBucketedEdgePathsByGranulariy[state.Granularity]
	}
	if fullRange {
		return s.summaries.edgePathsByGranulariy[state.Granularity]
	}
	return s.summaries.bucketedEdgePathsByGranulariy[state.Granularity]
}

func (s *Service) dnsSummaryEdgePathsForStateLocked(state QueryState, fullRange bool) []string {
	if state.LocalIdentity == LocalIdentityDevice {
		if !fullRange {
			return s.summaries.deviceDNSBucketedEdgePathsByGranulariy[state.Granularity]
		}
		return s.summaries.deviceDNSEdgePathsByGranulariy[state.Granularity]
	}
	if !fullRange {
		return s.summaries.dnsBucketedEdgePathsByGranulariy[state.Granularity]
	}
	return s.summaries.dnsEdgePathsByGranulariy[state.Granularity]
}

func (s *Service) canUseSummaryHistogram(state QueryState, span TimeSpan) bool {
	if !s.canUseSummaryGraph(state, span) {
		return false
	}
	return span.EndNs-span.StartNs >= summaryBucketWidthNs*histogramBinCount
}

func (s *Service) summaryGraph(ctx context.Context, state QueryState, span TimeSpan) (GraphData, error) {
	cacheKey := state.cacheKey(graphCacheKind, s.currentRevision())
	if graph, ok := s.graphCache.Get(cacheKey); ok {
		return graph, nil
	}

	queryStart := time.Now()
	snapshot, err := s.summaryGraphSnapshotForState(ctx, state, span)
	if err != nil {
		return GraphData{}, err
	}
	snapshotLoadedAt := time.Now()

	view := buildSummaryMetricView(snapshot, state.Metric)
	viewBuiltAt := time.Now()
	keepEntities := chooseKeepEntities(view.nodeTotals, state)
	keepLookup := make(map[string]struct{}, len(keepEntities))
	for _, entity := range keepEntities {
		keepLookup[entity] = struct{}{}
	}

	visibleEdgesAll, visibleNodeMap := buildSummaryVisibleGraph(view, keepEntities, state)
	visibleEdges, hiddenEdgeCount := limitEdges(visibleEdgesAll, state.EdgeLimit, "")
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

	nodePositions, err := s.summaryLayoutPositions(ctx, state)
	if err != nil {
		return GraphData{}, err
	}
	layoutBuiltAt := time.Now()
	breakdown, err := s.summaryBreakdown(ctx, state, span)
	if err != nil {
		return GraphData{}, err
	}
	breakdownBuiltAt := time.Now()

	graph := GraphData{
		ActiveGranularity: state.Granularity,
		ActiveMetric:      state.Metric,
		Breadcrumbs:       buildBreadcrumbs(state),
		Breakdown:         breakdown,
		Edges:             visibleEdges,
		HiddenEdgeCount:   hiddenEdgeCount,
		HiddenNodeCount:   max(0, len(view.nodeTotals)-countNonSynthetic(view.nodeTotals, keepLookup)),
		Nodes:             nodes,
		NodePositions:     nodePositions,
		Span:              span,
		Totals:            viewTotalsForMetric(snapshot.totals, len(view.nodeTotals), len(visibleEdges)),
		TopEdges:          limitTopEdges(visibleEdges, summaryTopItemLimit),
		TopEntities:       limitNodes(nodes, summaryTopItemLimit),
	}
	s.graphCache.Set(cacheKey, graph)
	slog.Debug(
		"UI summary graph query complete",
		"granularity", state.Granularity,
		"snapshot_ms", snapshotLoadedAt.Sub(queryStart).Milliseconds(),
		"derive_ms", viewBuiltAt.Sub(snapshotLoadedAt).Milliseconds(),
		"layout_ms", layoutBuiltAt.Sub(viewBuiltAt).Milliseconds(),
		"breakdown_ms", breakdownBuiltAt.Sub(layoutBuiltAt).Milliseconds(),
		"duration_ms", time.Since(queryStart).Milliseconds(),
	)
	return graph, nil
}

func (s *Service) summaryBreakdown(ctx context.Context, state QueryState, span TimeSpan) (SelectionBreakdown, error) {
	if state.Metric == MetricDNSLookups {
		return SelectionBreakdown{}, nil
	}

	snapshot, err := s.summaryGraphSnapshotForState(ctx, state, span)
	if err != nil {
		return SelectionBreakdown{}, err
	}
	protocolTotals := make(map[int32]int64)
	familyTotals := make(map[int32]int64)
	portTotals := make(map[int32]int64)
	for _, edge := range snapshot.edges {
		value := summaryMetricValue(state.Metric, edge.Bytes, edge.Connections)
		if value <= 0 {
			continue
		}
		protocolTotals[edge.Protocol] += value
		familyTotals[edge.IPVersion] += value
		if edge.ServicePort != nil {
			portTotals[*edge.ServicePort] += value
		}
	}
	return breakdownFromTotals(protocolTotals, familyTotals, portTotals), nil
}

func (s *Service) summaryHistogram(ctx context.Context, state QueryState) ([]HistogramBin, error) {
	if state.ToNs <= state.FromNs {
		return nil, nil
	}

	cacheKey := state.cacheKey(histogramCacheKind, s.currentRevision())
	if bins, ok := s.histogramCache.Get(cacheKey); ok {
		return bins, nil
	}

	queryStart := time.Now()
	widthNs := max(int64(1), (state.ToNs-state.FromNs+1)/histogramBinCount)
	ignoreCondition, ignoreArgs, err := s.summaryIgnoreCondition(state)
	if err != nil {
		return nil, err
	}
	if state.HideIgnored && ignoreCondition != "" {
		return s.bucketedSummaryHistogram(ctx, state, widthNs, ignoreCondition, ignoreArgs, queryStart)
	}
	whereClause, args := summaryFilterClause(state)
	rangeClause := "WHERE bucket_start_ns BETWEEN ? AND ?"
	if whereClause != "" {
		rangeClause = whereClause + " AND bucket_start_ns BETWEEN ? AND ?"
	}
	query := fmt.Sprintf(`
SELECT CAST(FLOOR((bucket_start_ns - ?) / ?) AS BIGINT) AS bucket, SUM(%s) AS value
FROM read_parquet(%s)
%s
GROUP BY bucket
ORDER BY bucket
`, summaryMetricColumn(state.Metric), quoteLiteral(s.summaryHistogramGlob(state.Metric)), rangeClause)

	queryArgs := append([]any{state.FromNs, widthNs}, args...)
	queryArgs = append(queryArgs, summaryBucketStartNs(state.FromNs), summaryBucketStartNs(state.ToNs))
	rows, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query summary histogram: %w", err)
	}
	defer rows.Close()

	values := make(map[int]int64, histogramBinCount)
	for rows.Next() {
		var bucket int
		var value int64
		if err := rows.Scan(&bucket, &value); err != nil {
			return nil, fmt.Errorf("scan summary histogram row: %w", err)
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
		return nil, fmt.Errorf("iterate summary histogram rows: %w", err)
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
	slog.Debug("UI summary histogram query complete", "granularity", state.Granularity, "family", state.AddressFamily, "duration_ms", time.Since(queryStart).Milliseconds())
	return bins, nil
}

func (s *Service) bucketedSummaryHistogram(ctx context.Context, state QueryState, widthNs int64, ignoreCondition string, ignoreArgs []any, queryStart time.Time) ([]HistogramBin, error) {
	whereClause, args := summaryFilterClause(state)
	rangeClause := appendSummaryWhereCondition(whereClause, "NOT ("+ignoreCondition+")")
	rangeClause = appendSummaryWhereCondition(rangeClause, "bucket_start_ns BETWEEN ? AND ?")
	query := fmt.Sprintf(`
SELECT CAST(FLOOR((bucket_start_ns - ?) / ?) AS BIGINT) AS bucket, SUM(%s) AS value
FROM read_parquet(%s)
%s
GROUP BY bucket
ORDER BY bucket
`, summaryMetricColumn(state.Metric), quoteLiteral(s.summaryBucketedEdgeGlob(state)), rangeClause)

	queryArgs := append([]any{state.FromNs, widthNs}, args...)
	queryArgs = append(queryArgs, ignoreArgs...)
	queryArgs = append(queryArgs, summaryBucketStartNs(state.FromNs), summaryBucketStartNs(state.ToNs))
	rows, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query bucketed summary histogram: %w", err)
	}
	defer rows.Close()

	values := make(map[int]int64, histogramBinCount)
	for rows.Next() {
		var bucket int
		var value int64
		if err := rows.Scan(&bucket, &value); err != nil {
			return nil, fmt.Errorf("scan bucketed summary histogram row: %w", err)
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
		return nil, fmt.Errorf("iterate bucketed summary histogram rows: %w", err)
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
	s.histogramCache.Set(state.cacheKey(histogramCacheKind, s.currentRevision()), bins)
	slog.Debug("UI bucketed summary histogram query complete", "granularity", state.Granularity, "family", state.AddressFamily, "duration_ms", time.Since(queryStart).Milliseconds())
	return bins, nil
}

func (s *Service) summaryIgnoreCondition(state QueryState) (string, []any, error) {
	if state.Metric == MetricDNSLookups {
		return buildDNSSummaryIgnoreConditionSQL(s.enabledIgnoreRules(), s.inetSupportEnabled())
	}
	return buildFlowSummaryIgnoreConditionSQL(s.enabledIgnoreRules(), s.inetSupportEnabled())
}

func (s *Service) summaryLayoutPositions(ctx context.Context, state QueryState) (map[string]LayoutPoint, error) {
	cacheState := state.layoutCacheState()
	cacheKey := cacheState.cacheKey(layoutCacheKind, s.currentRevision()) + ":summary"
	if positions, ok := s.layoutCache.Get(cacheKey); ok {
		return positions, nil
	}

	if cacheState.Metric == MetricDNSLookups {
		snapshot, err := s.summaryGraphSnapshotForCurrentSpan(ctx, cacheState, MetricDNSLookups)
		if err != nil {
			return nil, err
		}
		view := buildSummaryMetricView(snapshot, MetricDNSLookups)
		keepEntities := chooseKeepEntities(view.nodeTotals, cacheState)
		edges, nodeMap := buildSummaryVisibleGraph(view, keepEntities, cacheState)
		nodes := nodesFromMapSorted(nodeMap)
		visibleEdges, _ := limitEdges(edges, cacheState.EdgeLimit, "")
		positions := buildSingleMetricLayoutPositions(nodes, visibleEdges)
		s.layoutCache.Set(cacheKey, positions)
		return positions, nil
	}

	bytesSnapshot, err := s.summaryGraphSnapshotForCurrentSpan(ctx, cacheState, MetricBytes)
	if err != nil {
		return nil, err
	}
	connectionSnapshot, err := s.summaryGraphSnapshotForCurrentSpan(ctx, cacheState, MetricConnections)
	if err != nil {
		return nil, err
	}
	bytesView := buildSummaryMetricView(bytesSnapshot, MetricBytes)
	connectionView := buildSummaryMetricView(connectionSnapshot, MetricConnections)
	bytesNodeTotals := bytesView.nodeTotals
	connectionNodeTotals := connectionView.nodeTotals
	keepEntities := unionKeepEntities(bytesNodeTotals, connectionNodeTotals, cacheState)

	bytesEdges, bytesNodeMap := buildSummaryVisibleGraph(bytesView, keepEntities, cacheState)
	connectionEdges, connectionNodeMap := buildSummaryVisibleGraph(connectionView, keepEntities, cacheState)
	bytesNodeTotals = nodesFromMapSorted(bytesNodeMap)
	connectionNodeTotals = nodesFromMapSorted(connectionNodeMap)
	bytesVisibleEdges, _ := limitEdges(bytesEdges, cacheState.EdgeLimit, "")
	connectionVisibleEdges, _ := limitEdges(connectionEdges, cacheState.EdgeLimit, "")

	positions := buildStableLayoutPositions(
		bytesNodeTotals,
		connectionNodeTotals,
		bytesVisibleEdges,
		connectionVisibleEdges,
	)
	s.layoutCache.Set(cacheKey, positions)
	return positions, nil
}

func (s *Service) summaryGraphSnapshotForCurrentSpan(ctx context.Context, state QueryState, metric Metric) (*summaryGraphSnapshotData, error) {
	s.mu.RLock()
	span := s.span
	s.mu.RUnlock()
	state.Metric = metric
	return s.summaryGraphSnapshotForState(ctx, state, span)
}

func (s *Service) summaryGraphSnapshotForState(ctx context.Context, state QueryState, span TimeSpan) (*summaryGraphSnapshotData, error) {
	if state.FromNs == span.StartNs && state.ToNs == span.EndNs {
		return s.summaryGraphSnapshot(ctx, state)
	}
	return s.bucketedSummaryGraphSnapshot(ctx, state)
}

func (s *Service) summarySnapshotFilterExpressions(state QueryState) (string, string, string, []any, error) {
	whereClause, filterArgs := summaryFilterClause(state)
	var ignoreCondition string
	var ignoreArgs []any
	var err error
	if state.Metric == MetricDNSLookups {
		ignoreCondition, ignoreArgs, err = buildDNSSummaryIgnoreConditionSQL(s.enabledIgnoreRules(), s.inetSupportEnabled())
	} else {
		ignoreCondition, ignoreArgs, err = buildFlowSummaryIgnoreConditionSQL(s.enabledIgnoreRules(), s.inetSupportEnabled())
	}
	if err != nil {
		return "", "", "", nil, err
	}
	if ignoreCondition == "" {
		return "0", "0", whereClause, filterArgs, nil
	}
	if state.HideIgnored {
		return "0", "0", appendSummaryWhereCondition(whereClause, "NOT ("+ignoreCondition+")"), append(filterArgs, ignoreArgs...), nil
	}

	ignoredBytesExpr := "COALESCE(SUM(CASE WHEN " + ignoreCondition + " THEN bytes ELSE 0 END), 0)"
	ignoredConnectionsExpr := "COALESCE(SUM(CASE WHEN " + ignoreCondition + " THEN connections ELSE 0 END), 0)"
	args := append([]any(nil), ignoreArgs...)
	args = append(args, ignoreArgs...)
	args = append(args, filterArgs...)
	return ignoredBytesExpr, ignoredConnectionsExpr, whereClause, args, nil
}

func appendSummaryWhereCondition(whereClause, condition string) string {
	if condition == "" {
		return whereClause
	}
	if whereClause == "" {
		return "WHERE " + condition
	}
	return whereClause + " AND " + condition
}

func (s *Service) summaryGraphSnapshot(ctx context.Context, state QueryState) (*summaryGraphSnapshotData, error) {
	cacheKey := summaryGraphSnapshotCacheKey(state.Granularity, state.AddressFamily, state.Direction, state.Metric, s.currentRevision()) + summaryFilterCacheSuffix(state)
	if snapshot, ok := s.summaryGraphCache.Get(cacheKey); ok {
		return snapshot, nil
	}

	queryStart := time.Now()
	ignoredBytesExpr, ignoredConnectionsExpr, whereClause, args, err := s.summarySnapshotFilterExpressions(state)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`
SELECT src_entity, dst_entity,
  COALESCE(SUM(bytes), 0) AS bytes_total,
  COALESCE(SUM(connections), 0) AS connection_total,
  %s AS ignored_bytes_total,
  %s AS ignored_connection_total,
  direction,
  COALESCE(SUM(src_private_bytes), 0) AS src_private_bytes,
  COALESCE(SUM(src_private_connections), 0) AS src_private_connections,
  COALESCE(SUM(src_public_bytes), 0) AS src_public_bytes,
  COALESCE(SUM(src_public_connections), 0) AS src_public_connections,
  COALESCE(SUM(dst_private_bytes), 0) AS dst_private_bytes,
  COALESCE(SUM(dst_private_connections), 0) AS dst_private_connections,
  COALESCE(SUM(dst_public_bytes), 0) AS dst_public_bytes,
  COALESCE(SUM(dst_public_connections), 0) AS dst_public_connections,
  COALESCE(MIN(first_seen_ns), 0) AS first_seen_ns,
  ip_version,
  COALESCE(MAX(last_seen_ns), 0) AS last_seen_ns,
  COALESCE(SUM(nxdomain_lookups), 0) AS nxdomain_lookup_total,
  protocol,
  service_port,
  COALESCE(SUM(successful_lookups), 0) AS successful_lookup_total
FROM read_parquet(%s)
%s
GROUP BY src_entity, dst_entity, direction, ip_version, protocol, service_port
`, ignoredBytesExpr, ignoredConnectionsExpr, quoteLiteral(s.summaryEdgeGlob(state)), whereClause)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query summary graph snapshot: %w", err)
	}
	defer rows.Close()

	snapshot := &summaryGraphSnapshotData{
		edges:       make([]summaryEdgeAggregate, 0, 128),
		granularity: state.Granularity,
	}
	for rows.Next() {
		var edge summaryEdgeAggregate
		var direction sql.NullInt32
		var servicePort sql.NullInt32
		if err := rows.Scan(
			&edge.Source,
			&edge.Destination,
			&edge.Bytes,
			&edge.Connections,
			&edge.IgnoredBytes,
			&edge.IgnoredConnections,
			&direction,
			&edge.SrcPrivateBytes,
			&edge.SrcPrivateConnections,
			&edge.SrcPublicBytes,
			&edge.SrcPublicConnections,
			&edge.DstPrivateBytes,
			&edge.DstPrivateConnections,
			&edge.DstPublicBytes,
			&edge.DstPublicConnections,
			&edge.FirstSeenNs,
			&edge.IPVersion,
			&edge.LastSeenNs,
			&edge.NXDomainLookups,
			&edge.Protocol,
			&servicePort,
			&edge.SuccessfulLookups,
		); err != nil {
			return nil, fmt.Errorf("scan summary graph snapshot row: %w", err)
		}
		edge.Direction = directionValue(direction)
		edge.ServicePort = nullableInt32Value(servicePort)
		snapshot.edges = append(snapshot.edges, edge)
		snapshot.totals.Bytes += edge.Bytes
		snapshot.totals.Connections += edge.Connections
		snapshot.totals.Ignored += summaryMetricValue(state.Metric, edge.IgnoredBytes, edge.IgnoredConnections)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate summary graph snapshot rows: %w", err)
	}

	s.summaryGraphCache.Set(cacheKey, snapshot)
	slog.Debug("UI summary graph snapshot built", "granularity", state.Granularity, "family", state.AddressFamily, "direction", state.Direction, "protocol", state.Protocol, "port", state.Port, "edge_count", len(snapshot.edges), "duration_ms", time.Since(queryStart).Milliseconds())
	return snapshot, nil
}

func (s *Service) bucketedSummaryGraphSnapshot(ctx context.Context, state QueryState) (*summaryGraphSnapshotData, error) {
	fromBucketStartNs := summaryBucketStartNs(state.FromNs)
	toBucketStartNs := summaryBucketStartNs(state.ToNs)
	cacheKey := summaryGraphSnapshotCacheKey(state.Granularity, state.AddressFamily, state.Direction, state.Metric, s.currentRevision()) +
		summaryFilterCacheSuffix(state) +
		fmt.Sprintf(":%d:%d", fromBucketStartNs, toBucketStartNs)
	if snapshot, ok := s.summaryGraphCache.Get(cacheKey); ok {
		return snapshot, nil
	}

	queryStart := time.Now()
	ignoredBytesExpr, ignoredConnectionsExpr, whereClause, args, err := s.summarySnapshotFilterExpressions(state)
	if err != nil {
		return nil, err
	}
	rangeClause := "WHERE bucket_start_ns BETWEEN ? AND ?"
	if whereClause != "" {
		rangeClause = whereClause + " AND bucket_start_ns BETWEEN ? AND ?"
	}
	args = append(args, fromBucketStartNs, toBucketStartNs)
	query := fmt.Sprintf(`
SELECT src_entity, dst_entity,
  COALESCE(SUM(bytes), 0) AS bytes_total,
  COALESCE(SUM(connections), 0) AS connection_total,
  %s AS ignored_bytes_total,
  %s AS ignored_connection_total,
  direction,
  COALESCE(SUM(src_private_bytes), 0) AS src_private_bytes,
  COALESCE(SUM(src_private_connections), 0) AS src_private_connections,
  COALESCE(SUM(src_public_bytes), 0) AS src_public_bytes,
  COALESCE(SUM(src_public_connections), 0) AS src_public_connections,
  COALESCE(SUM(dst_private_bytes), 0) AS dst_private_bytes,
  COALESCE(SUM(dst_private_connections), 0) AS dst_private_connections,
  COALESCE(SUM(dst_public_bytes), 0) AS dst_public_bytes,
  COALESCE(SUM(dst_public_connections), 0) AS dst_public_connections,
  COALESCE(MIN(first_seen_ns), 0) AS first_seen_ns,
  ip_version,
  COALESCE(MAX(last_seen_ns), 0) AS last_seen_ns,
  COALESCE(SUM(nxdomain_lookups), 0) AS nxdomain_lookup_total,
  protocol,
  service_port,
  COALESCE(SUM(successful_lookups), 0) AS successful_lookup_total
FROM read_parquet(%s)
%s
GROUP BY src_entity, dst_entity, direction, ip_version, protocol, service_port
`, ignoredBytesExpr, ignoredConnectionsExpr, quoteLiteral(s.summaryBucketedEdgeGlob(state)), rangeClause)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query bucketed summary graph snapshot: %w", err)
	}
	defer rows.Close()

	snapshot := &summaryGraphSnapshotData{
		edges:       make([]summaryEdgeAggregate, 0, 128),
		granularity: state.Granularity,
	}
	for rows.Next() {
		var edge summaryEdgeAggregate
		var direction sql.NullInt32
		var servicePort sql.NullInt32
		if err := rows.Scan(
			&edge.Source,
			&edge.Destination,
			&edge.Bytes,
			&edge.Connections,
			&edge.IgnoredBytes,
			&edge.IgnoredConnections,
			&direction,
			&edge.SrcPrivateBytes,
			&edge.SrcPrivateConnections,
			&edge.SrcPublicBytes,
			&edge.SrcPublicConnections,
			&edge.DstPrivateBytes,
			&edge.DstPrivateConnections,
			&edge.DstPublicBytes,
			&edge.DstPublicConnections,
			&edge.FirstSeenNs,
			&edge.IPVersion,
			&edge.LastSeenNs,
			&edge.NXDomainLookups,
			&edge.Protocol,
			&servicePort,
			&edge.SuccessfulLookups,
		); err != nil {
			return nil, fmt.Errorf("scan bucketed summary graph snapshot row: %w", err)
		}
		edge.Direction = directionValue(direction)
		edge.ServicePort = nullableInt32Value(servicePort)
		snapshot.edges = append(snapshot.edges, edge)
		snapshot.totals.Bytes += edge.Bytes
		snapshot.totals.Connections += edge.Connections
		snapshot.totals.Ignored += summaryMetricValue(state.Metric, edge.IgnoredBytes, edge.IgnoredConnections)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate bucketed summary graph snapshot rows: %w", err)
	}

	s.summaryGraphCache.Set(cacheKey, snapshot)
	slog.Debug(
		"UI bucketed summary graph snapshot built",
		"granularity", state.Granularity,
		"family", state.AddressFamily,
		"direction", state.Direction,
		"edge_count", len(snapshot.edges),
		"duration_ms", time.Since(queryStart).Milliseconds(),
	)
	return snapshot, nil
}

func buildSummaryMetricView(snapshot *summaryGraphSnapshotData, metric Metric) summaryMetricView {
	nodeTotalsByID := make(map[string]Node, len(snapshot.edges)*2)
	edges := make([]Edge, 0, len(snapshot.edges))
	for _, aggregate := range snapshot.edges {
		edge := Edge{
			Bytes:             aggregate.Bytes,
			Connections:       aggregate.Connections,
			Destination:       aggregate.Destination,
			FirstSeenNs:       aggregate.FirstSeenNs,
			LastSeenNs:        aggregate.LastSeenNs,
			MetricValue:       edgeMetricValue(Edge{Bytes: aggregate.Bytes, Connections: aggregate.Connections}, metric),
			NXDomainLookups:   aggregate.NXDomainLookups,
			Source:            aggregate.Source,
			SuccessfulLookups: aggregate.SuccessfulLookups,
		}
		edge.IgnoredMetric = summaryMetricValue(metric, aggregate.IgnoredBytes, aggregate.IgnoredConnections)
		edge.Ignored = edge.IgnoredMetric > 0
		edge.DNSResultState = dnsResultStateForCounts(edge.NXDomainLookups, edge.SuccessfulLookups)
		edges = append(edges, edge)

		sourceNode := nodeTotalsByID[aggregate.Source]
		sourceNode.ID = aggregate.Source
		sourceNode.Label = aggregate.Source
		sourceNode.PrivateMetric += summaryMetricValue(metric, aggregate.SrcPrivateBytes, aggregate.SrcPrivateConnections)
		sourceNode.PublicMetric += summaryMetricValue(metric, aggregate.SrcPublicBytes, aggregate.SrcPublicConnections)
		sourceNode.Egress += edge.MetricValue
		sourceNode.Ignored = sourceNode.Ignored || edge.Ignored
		sourceNode.NXDomainLookups += aggregate.NXDomainLookups
		sourceNode.SuccessfulLookups += aggregate.SuccessfulLookups
		sourceNode.Total += edge.MetricValue
		sourceNode.AddressClass = classifyNodeAddress(sourceNode.PrivateMetric, sourceNode.PublicMetric)
		sourceNode.DNSResultState = dnsResultStateForCounts(sourceNode.NXDomainLookups, sourceNode.SuccessfulLookups)
		nodeTotalsByID[aggregate.Source] = sourceNode

		destinationNode := nodeTotalsByID[aggregate.Destination]
		destinationNode.ID = aggregate.Destination
		destinationNode.Label = aggregate.Destination
		destinationNode.PrivateMetric += summaryMetricValue(metric, aggregate.DstPrivateBytes, aggregate.DstPrivateConnections)
		destinationNode.PublicMetric += summaryMetricValue(metric, aggregate.DstPublicBytes, aggregate.DstPublicConnections)
		destinationNode.Ingress += edge.MetricValue
		destinationNode.Ignored = destinationNode.Ignored || edge.Ignored
		destinationNode.NXDomainLookups += aggregate.NXDomainLookups
		destinationNode.SuccessfulLookups += aggregate.SuccessfulLookups
		destinationNode.Total += edge.MetricValue
		destinationNode.AddressClass = classifyNodeAddress(destinationNode.PrivateMetric, destinationNode.PublicMetric)
		destinationNode.DNSResultState = dnsResultStateForCounts(destinationNode.NXDomainLookups, destinationNode.SuccessfulLookups)
		nodeTotalsByID[aggregate.Destination] = destinationNode
	}

	nodeTotals := make([]Node, 0, len(nodeTotalsByID))
	for _, node := range nodeTotalsByID {
		nodeTotals = append(nodeTotals, node)
	}
	slices.SortFunc(nodeTotals, func(a, b Node) int {
		if a.Total == b.Total {
			return strings.Compare(a.ID, b.ID)
		}
		if a.Total > b.Total {
			return -1
		}
		return 1
	})

	slices.SortFunc(edges, func(a, b Edge) int {
		if a.MetricValue == b.MetricValue {
			if a.Source == b.Source {
				return strings.Compare(a.Destination, b.Destination)
			}
			return strings.Compare(a.Source, b.Source)
		}
		if a.MetricValue > b.MetricValue {
			return -1
		}
		return 1
	})

	return summaryMetricView{
		edges:      edges,
		nodeTotals: nodeTotals,
	}
}

func summaryMetricValue(metric Metric, bytesValue, connectionValue int64) int64 {
	if metric == MetricConnections || metric == MetricDNSLookups {
		return connectionValue
	}

	return bytesValue
}

func buildSummaryVisibleGraph(view summaryMetricView, keepEntities []string, state QueryState) ([]Edge, map[string]Node) {
	keepLookup := make(map[string]struct{}, len(keepEntities))
	for _, entity := range keepEntities {
		keepLookup[entity] = struct{}{}
	}

	visibleNodeMap := make(map[string]Node, len(keepEntities)+2)
	for _, row := range view.nodeTotals {
		if _, ok := keepLookup[row.ID]; !ok {
			continue
		}
		visibleNodeMap[row.ID] = row
	}

	restNode := summaryRestNode(view.nodeTotals, keepLookup)
	if restNode != nil {
		visibleNodeMap[restNode.ID] = *restNode
	}

	edgesByKey := make(map[string]Edge, len(view.edges))
	for _, edge := range view.edges {
		sourceBucket := edge.Source
		destinationBucket := edge.Destination
		if state.NodeLimit > 0 && len(keepEntities) > 0 {
			if _, ok := keepLookup[sourceBucket]; !ok {
				sourceBucket = graphRestID
			}
			if _, ok := keepLookup[destinationBucket]; !ok {
				destinationBucket = graphRestID
			}
		}

		key := sourceBucket + "\x00" + destinationBucket
		aggregatedEdge, ok := edgesByKey[key]
		if !ok {
			aggregatedEdge = Edge{
				Destination: destinationBucket,
				FirstSeenNs: edge.FirstSeenNs,
				LastSeenNs:  edge.LastSeenNs,
				Source:      sourceBucket,
				Synthetic:   sourceBucket == graphRestID || destinationBucket == graphRestID,
			}
		}
		aggregatedEdge.Bytes += edge.Bytes
		aggregatedEdge.Connections += edge.Connections
		aggregatedEdge.NXDomainLookups += edge.NXDomainLookups
		aggregatedEdge.SuccessfulLookups += edge.SuccessfulLookups
		if aggregatedEdge.FirstSeenNs == 0 || (edge.FirstSeenNs != 0 && edge.FirstSeenNs < aggregatedEdge.FirstSeenNs) {
			aggregatedEdge.FirstSeenNs = edge.FirstSeenNs
		}
		if edge.LastSeenNs > aggregatedEdge.LastSeenNs {
			aggregatedEdge.LastSeenNs = edge.LastSeenNs
		}
		aggregatedEdge.MetricValue = edgeMetricValue(aggregatedEdge, state.Metric)
		aggregatedEdge.DNSResultState = dnsResultStateForCounts(aggregatedEdge.NXDomainLookups, aggregatedEdge.SuccessfulLookups)
		edgesByKey[key] = aggregatedEdge
	}

	edges := make([]Edge, 0, len(edgesByKey))
	for _, edge := range edgesByKey {
		edges = append(edges, edge)
	}
	slices.SortFunc(edges, func(a, b Edge) int {
		if a.MetricValue == b.MetricValue {
			if a.Source == b.Source {
				return strings.Compare(a.Destination, b.Destination)
			}
			return strings.Compare(a.Source, b.Source)
		}
		if a.MetricValue > b.MetricValue {
			return -1
		}
		return 1
	})

	return edges, visibleNodeMap
}

func summaryRestNode(nodeTotals []Node, keepLookup map[string]struct{}) *Node {
	var collapsedEntityCount int
	var egress int64
	var ingress int64
	var nxdomainLookups int64
	var successfulLookups int64
	for _, node := range nodeTotals {
		if _, ok := keepLookup[node.ID]; ok {
			continue
		}
		if node.Egress == 0 && node.Ingress == 0 {
			continue
		}
		collapsedEntityCount++
		nxdomainLookups += node.NXDomainLookups
		successfulLookups += node.SuccessfulLookups
		egress += node.Egress
		ingress += node.Ingress
	}
	total := egress + ingress
	if total == 0 {
		return nil
	}

	node := &Node{
		CollapsedEntityCount: collapsedEntityCount,
		Egress:               egress,
		ID:                   graphRestID,
		Ingress:              ingress,
		Label:                graphRestID,
		NXDomainLookups:      nxdomainLookups,
		Synthetic:            true,
		SuccessfulLookups:    successfulLookups,
		Total:                total,
	}
	node.DNSResultState = dnsResultStateForCounts(node.NXDomainLookups, node.SuccessfulLookups)
	return node
}

func nodesFromMapSorted(nodeMap map[string]Node) []Node {
	nodes := make([]Node, 0, len(nodeMap))
	for _, node := range nodeMap {
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
	return nodes
}

func viewTotalsForMetric(totals Totals, totalEntities, visibleEdges int) Totals {
	viewTotals := totals
	viewTotals.Entities = totalEntities
	viewTotals.Edges = visibleEdges
	return viewTotals
}

func summaryGraphSnapshotCacheKey(granularity Granularity, addressFamily AddressFamily, direction DirectionFilter, metric Metric, revision uint64) string {
	return fmt.Sprintf("summary-graph:%d:%s:%s:%s:%s", revision, granularity, addressFamily, direction, metric)
}

func summaryFilterCacheSuffix(state QueryState) string {
	parts := []string{fmt.Sprintf(":hide_ignored=%t", state.HideIgnored)}
	if state.LocalIdentity != "" && state.LocalIdentity != LocalIdentityAddress {
		parts = append(parts, ":local_identity="+string(state.LocalIdentity))
	}
	if state.Metric != MetricDNSLookups && (state.Protocol != 0 || state.Port != 0) {
		parts = append(parts, fmt.Sprintf(":protocol=%d:port=%d", state.Protocol, state.Port))
	}
	return strings.Join(parts, "")
}

func summaryBucketStartNs(timestampNs int64) int64 {
	if timestampNs <= 0 {
		return 0
	}
	return (timestampNs / summaryBucketWidthNs) * summaryBucketWidthNs
}

func (s *Service) summaryEdgeGlob(state QueryState) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if state.Metric == MetricDNSLookups {
		if state.LocalIdentity == LocalIdentityDevice {
			return s.summaries.deviceDNSEdgeGlobByGranularity[state.Granularity]
		}
		return s.summaries.dnsEdgeGlobByGranularity[state.Granularity]
	}
	if state.LocalIdentity == LocalIdentityDevice {
		return s.summaries.deviceEdgeGlobByGranularity[state.Granularity]
	}
	return s.summaries.edgeGlobByGranularity[state.Granularity]
}

func (s *Service) summaryBucketedEdgeGlob(state QueryState) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if state.Metric == MetricDNSLookups {
		if state.LocalIdentity == LocalIdentityDevice {
			return s.summaries.deviceDNSBucketedEdgeGlobByGranularity[state.Granularity]
		}
		return s.summaries.dnsBucketedEdgeGlobByGranularity[state.Granularity]
	}
	if state.LocalIdentity == LocalIdentityDevice {
		return s.summaries.deviceBucketedEdgeGlobByGranularity[state.Granularity]
	}
	return s.summaries.bucketedEdgeGlobByGranularity[state.Granularity]
}

func (s *Service) summaryHistogramGlob(metric Metric) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if metric == MetricDNSLookups {
		return s.summaries.dnsHistogramGlob
	}
	return s.summaries.histogramGlob
}

func summaryMetricColumn(metric Metric) string {
	if metric == MetricConnections || metric == MetricDNSLookups {
		return "connections"
	}
	return "bytes"
}

func summaryFilterClause(state QueryState) (string, []any) {
	conditions := []string(nil)
	args := []any(nil)
	switch state.AddressFamily {
	case AddressFamilyIPv4:
		conditions = append(conditions, "ip_version = ?")
		args = append(args, model.IPVersion4)
	case AddressFamilyIPv6:
		conditions = append(conditions, "ip_version = ?")
		args = append(args, model.IPVersion6)
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
			conditions = append(conditions, "service_port = ?")
			args = append(args, state.Port)
		}
	}
	if len(conditions) == 0 {
		return "", nil
	}
	return "WHERE " + strings.Join(conditions, " AND "), args
}
