package parquetui

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
)

func (s *Service) canUseSummaryGraph(state QueryState, span TimeSpan) bool {
	if state.Granularity != GranularityTLD && state.Granularity != Granularity2LD {
		return false
	}
	if state.FromNs != span.StartNs || state.ToNs != span.EndNs {
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
	paths := s.summaries.edgePathsByGranulariy[state.Granularity]
	return len(paths) > 0 && s.summaries.spanValid
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
	nodeTotals, err := s.querySummaryNodeTotals(ctx, state)
	if err != nil {
		return GraphData{}, err
	}

	keepEntities := chooseKeepEntities(nodeTotals, state)
	keepLookup := make(map[string]struct{}, len(keepEntities))
	for _, entity := range keepEntities {
		keepLookup[entity] = struct{}{}
	}

	edges, err := s.querySummaryEdges(ctx, state, keepEntities)
	if err != nil {
		return GraphData{}, err
	}

	visibleEdges, hiddenEdgeCount := limitEdges(edges, state.EdgeLimit, "")
	visibleNodeMap := make(map[string]Node, len(keepEntities)+2)
	for _, row := range nodeTotals {
		if _, ok := keepLookup[row.ID]; !ok {
			continue
		}
		visibleNodeMap[row.ID] = row
	}

	restSourceNode, err := s.querySummaryRestNode(ctx, state, keepEntities, true)
	if err != nil {
		return GraphData{}, err
	}
	restDestinationNode, err := s.querySummaryRestNode(ctx, state, keepEntities, false)
	if err != nil {
		return GraphData{}, err
	}
	if restSourceNode != nil {
		visibleNodeMap[restSourceNode.ID] = *restSourceNode
	}
	if restDestinationNode != nil {
		visibleNodeMap[restDestinationNode.ID] = *restDestinationNode
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

	totals, err := s.querySummaryTotals(ctx, state, len(nodeTotals), len(visibleEdges))
	if err != nil {
		return GraphData{}, err
	}
	nodePositions, err := s.summaryLayoutPositions(ctx, state)
	if err != nil {
		return GraphData{}, err
	}

	graph := GraphData{
		ActiveGranularity: state.Granularity,
		ActiveMetric:      state.Metric,
		Breadcrumbs:       buildBreadcrumbs(state),
		Edges:             visibleEdges,
		HiddenEdgeCount:   hiddenEdgeCount,
		HiddenNodeCount:   max(0, len(nodeTotals)-countNonSynthetic(nodeTotals, keepLookup)),
		Nodes:             nodes,
		NodePositions:     nodePositions,
		Span:              span,
		Totals:            totals,
		TopEdges:          limitTopEdges(visibleEdges, summaryTopItemLimit),
		TopEntities:       limitNodes(nodes, summaryTopItemLimit),
	}
	s.graphCache.Set(cacheKey, graph)
	slog.Debug("UI summary graph query complete", "granularity", state.Granularity, "duration_ms", time.Since(queryStart).Milliseconds())
	return graph, nil
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
	query := fmt.Sprintf(`
SELECT CAST(FLOOR((bucket_start_ns - ?) / ?) AS BIGINT) AS bucket, SUM(%s) AS value
FROM read_parquet(%s)
GROUP BY bucket
ORDER BY bucket
`, summaryMetricColumn(state.Metric), quoteLiteral(s.summaryHistogramGlob()))

	rows, err := s.db.QueryContext(ctx, query, state.FromNs, widthNs)
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
	slog.Debug("UI summary histogram query complete", "granularity", state.Granularity, "duration_ms", time.Since(queryStart).Milliseconds())
	return bins, nil
}

func (s *Service) summaryLayoutPositions(ctx context.Context, state QueryState) (map[string]LayoutPoint, error) {
	cacheState := state.layoutCacheState()
	cacheKey := cacheState.cacheKey(layoutCacheKind, s.currentRevision()) + ":summary"
	if positions, ok := s.layoutCache.Get(cacheKey); ok {
		return positions, nil
	}

	bytesState := cacheState.Clone()
	bytesState.Metric = MetricBytes
	connectionState := cacheState.Clone()
	connectionState.Metric = MetricConnections

	bytesNodeTotals, err := s.querySummaryNodeTotals(ctx, bytesState)
	if err != nil {
		return nil, err
	}
	connectionNodeTotals, err := s.querySummaryNodeTotals(ctx, connectionState)
	if err != nil {
		return nil, err
	}

	bytesKeepEntities := chooseKeepEntities(bytesNodeTotals, cacheState)
	connectionKeepEntities := chooseKeepEntities(connectionNodeTotals, cacheState)
	keepEntities := unionKeepEntities(bytesNodeTotals, connectionNodeTotals, cacheState)
	bytesNodeTotals, err = s.appendSummaryRestNodes(ctx, bytesState, bytesKeepEntities, bytesNodeTotals)
	if err != nil {
		return nil, err
	}
	connectionNodeTotals, err = s.appendSummaryRestNodes(ctx, connectionState, connectionKeepEntities, connectionNodeTotals)
	if err != nil {
		return nil, err
	}
	bytesEdges, err := s.querySummaryEdges(ctx, bytesState, keepEntities)
	if err != nil {
		return nil, err
	}
	connectionEdges, err := s.querySummaryEdges(ctx, connectionState, keepEntities)
	if err != nil {
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

func (s *Service) appendSummaryRestNodes(ctx context.Context, state QueryState, keepEntities []string, nodes []Node) ([]Node, error) {
	restSourceNode, err := s.querySummaryRestNode(ctx, state, keepEntities, true)
	if err != nil {
		return nil, err
	}
	restDestinationNode, err := s.querySummaryRestNode(ctx, state, keepEntities, false)
	if err != nil {
		return nil, err
	}
	if restSourceNode != nil {
		nodes = append(nodes, *restSourceNode)
	}
	if restDestinationNode != nil {
		nodes = append(nodes, *restDestinationNode)
	}
	return nodes, nil
}

func (s *Service) querySummaryNodeTotals(ctx context.Context, state QueryState) ([]Node, error) {
	query := fmt.Sprintf(`
SELECT entity, SUM(total_metric) AS total_metric, SUM(inbound_metric) AS inbound_metric, SUM(outbound_metric) AS outbound_metric
FROM (
  SELECT src_entity AS entity, %s AS total_metric, 0 AS inbound_metric, %s AS outbound_metric FROM read_parquet(%s)
  UNION ALL
  SELECT dst_entity AS entity, %s AS total_metric, %s AS inbound_metric, 0 AS outbound_metric FROM read_parquet(%s)
) aggregate_nodes
GROUP BY entity
ORDER BY total_metric DESC, entity
`, summaryMetricColumn(state.Metric), summaryMetricColumn(state.Metric), quoteLiteral(s.summaryEdgeGlob(state.Granularity)), summaryMetricColumn(state.Metric), summaryMetricColumn(state.Metric), quoteLiteral(s.summaryEdgeGlob(state.Granularity)))

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query summary node totals: %w", err)
	}
	defer rows.Close()

	nodes := make([]Node, 0, 128)
	for rows.Next() {
		var node Node
		if err := rows.Scan(&node.ID, &node.Total, &node.Inbound, &node.Outbound); err != nil {
			return nil, fmt.Errorf("scan summary node total row: %w", err)
		}
		node.Label = node.ID
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate summary node totals: %w", err)
	}
	return nodes, nil
}

func (s *Service) querySummaryEdges(ctx context.Context, state QueryState, keepEntities []string) ([]Edge, error) {
	srcBucket := "src_entity"
	dstBucket := "dst_entity"
	queryArgs := make([]any, 0, len(keepEntities)*2)
	if state.NodeLimit > 0 && len(keepEntities) > 0 {
		inPlaceholders := placeholders(len(keepEntities))
		srcBucket = fmt.Sprintf("CASE WHEN src_entity IN (%s) THEN src_entity ELSE %s END", inPlaceholders, quoteLiteral(graphRestSourceID))
		dstBucket = fmt.Sprintf("CASE WHEN dst_entity IN (%s) THEN dst_entity ELSE %s END", inPlaceholders, quoteLiteral(graphRestDestination))
		queryArgs = append(queryArgs, stringsToAny(keepEntities)...)
		queryArgs = append(queryArgs, stringsToAny(keepEntities)...)
	}

	query := fmt.Sprintf(`
SELECT %s AS source_bucket, %s AS destination_bucket,
  COALESCE(SUM(bytes), 0) AS bytes_total,
  COALESCE(SUM(connections), 0) AS connection_total,
  COALESCE(MIN(first_seen_ns), 0) AS first_seen_ns,
  COALESCE(MAX(last_seen_ns), 0) AS last_seen_ns
FROM read_parquet(%s)
GROUP BY source_bucket, destination_bucket
ORDER BY %s DESC, source_bucket, destination_bucket
`, srcBucket, dstBucket, quoteLiteral(s.summaryEdgeGlob(state.Granularity)), summaryOrderExpression(state.Metric))

	rows, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query summary edges: %w", err)
	}
	defer rows.Close()

	edges := make([]Edge, 0, 128)
	for rows.Next() {
		var edge Edge
		if err := rows.Scan(&edge.Source, &edge.Destination, &edge.Bytes, &edge.Connections, &edge.FirstSeenNs, &edge.LastSeenNs); err != nil {
			return nil, fmt.Errorf("scan summary edge row: %w", err)
		}
		edge.MetricValue = edgeMetricValue(edge, state.Metric)
		edge.Synthetic = edge.Source == graphRestSourceID || edge.Destination == graphRestDestination
		edges = append(edges, edge)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate summary edge rows: %w", err)
	}
	return edges, nil
}

func (s *Service) querySummaryRestNode(ctx context.Context, state QueryState, keepEntities []string, sourceRole bool) (*Node, error) {
	if state.NodeLimit == 0 || len(keepEntities) == 0 {
		return nil, nil
	}

	entityColumn := srcEntityColumn
	nodeID := graphRestSourceID
	if !sourceRole {
		entityColumn = dstEntityColumn
		nodeID = graphRestDestination
	}

	query := fmt.Sprintf(`
SELECT COUNT(*), COALESCE(SUM(total_metric), 0)
FROM (
  SELECT %s AS entity, SUM(%s) AS total_metric
  FROM read_parquet(%s)
  WHERE %s NOT IN (%s)
  GROUP BY %s
) collapsed_entities
`, entityColumn, summaryMetricColumn(state.Metric), quoteLiteral(s.summaryEdgeGlob(state.Granularity)), entityColumn, placeholders(len(keepEntities)), entityColumn)

	row := s.db.QueryRowContext(ctx, query, stringsToAny(keepEntities)...)
	var entityCount int
	var total int64
	if err := row.Scan(&entityCount, &total); err != nil {
		return nil, fmt.Errorf("scan summary rest node %q: %w", nodeID, err)
	}
	if total == 0 {
		return nil, nil
	}

	node := &Node{
		CollapsedEntityCount: entityCount,
		ID:                   nodeID,
		Label:                nodeID,
		Synthetic:            true,
		Total:                total,
	}
	if sourceRole {
		node.Outbound = total
	} else {
		node.Inbound = total
	}
	return node, nil
}

func (s *Service) querySummaryTotals(ctx context.Context, state QueryState, totalEntities, visibleEdges int) (Totals, error) {
	query := fmt.Sprintf(`
SELECT COALESCE(SUM(bytes), 0), COALESCE(SUM(connections), 0)
FROM read_parquet(%s)
`, quoteLiteral(s.summaryEdgeGlob(state.Granularity)))

	row := s.db.QueryRowContext(ctx, query)
	var totals Totals
	if err := row.Scan(&totals.Bytes, &totals.Connections); err != nil {
		return Totals{}, fmt.Errorf("scan summary totals: %w", err)
	}
	totals.Entities = totalEntities
	totals.Edges = visibleEdges
	return totals, nil
}

func (s *Service) summaryEdgeGlob(granularity Granularity) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.summaries.edgeGlobByGranularity[granularity]
}

func (s *Service) summaryHistogramGlob() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.summaries.histogramGlob
}

func summaryMetricColumn(metric Metric) string {
	if metric == MetricConnections {
		return "connections"
	}
	return "bytes"
}

func summaryOrderExpression(metric Metric) string {
	if metric == MetricConnections {
		return "COALESCE(SUM(connections), 0)"
	}
	return "COALESCE(SUM(bytes), 0)"
}
