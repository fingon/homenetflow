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
	Source                string
	SrcPrivateBytes       int64
	SrcPrivateConnections int64
	SrcPublicBytes        int64
	SrcPublicConnections  int64
}

type summaryMetricView struct {
	edges      []Edge
	nodeTotals []Node
}

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
	if state.Metric == MetricDNSLookups {
		paths := s.summaries.dnsEdgePathsByGranulariy[state.Granularity]
		return len(paths) > 0 && s.summaries.spanValid
	}
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
	snapshot, err := s.summaryGraphSnapshot(ctx, state.Granularity, state.AddressFamily, state.Direction, state.Metric)
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

	graph := GraphData{
		ActiveGranularity: state.Granularity,
		ActiveMetric:      state.Metric,
		Breadcrumbs:       buildBreadcrumbs(state),
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
		"duration_ms", time.Since(queryStart).Milliseconds(),
	)
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
	whereClause, args := summaryFilterClause(state.AddressFamily, state.Direction)
	query := fmt.Sprintf(`
SELECT CAST(FLOOR((bucket_start_ns - ?) / ?) AS BIGINT) AS bucket, SUM(%s) AS value
FROM read_parquet(%s)
%s
GROUP BY bucket
ORDER BY bucket
`, summaryMetricColumn(state.Metric), quoteLiteral(s.summaryHistogramGlob(state.Metric)), whereClause)

	queryArgs := append([]any{state.FromNs, widthNs}, args...)
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

func (s *Service) summaryLayoutPositions(ctx context.Context, state QueryState) (map[string]LayoutPoint, error) {
	cacheState := state.layoutCacheState()
	cacheKey := cacheState.cacheKey(layoutCacheKind, s.currentRevision()) + ":summary"
	if positions, ok := s.layoutCache.Get(cacheKey); ok {
		return positions, nil
	}

	if cacheState.Metric == MetricDNSLookups {
		snapshot, err := s.summaryGraphSnapshot(ctx, cacheState.Granularity, cacheState.AddressFamily, cacheState.Direction, MetricDNSLookups)
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

	bytesSnapshot, err := s.summaryGraphSnapshot(ctx, cacheState.Granularity, cacheState.AddressFamily, cacheState.Direction, MetricBytes)
	if err != nil {
		return nil, err
	}
	connectionSnapshot, err := s.summaryGraphSnapshot(ctx, cacheState.Granularity, cacheState.AddressFamily, cacheState.Direction, MetricConnections)
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

func (s *Service) summaryGraphSnapshot(ctx context.Context, granularity Granularity, addressFamily AddressFamily, direction DirectionFilter, metric Metric) (*summaryGraphSnapshotData, error) {
	cacheKey := summaryGraphSnapshotCacheKey(granularity, addressFamily, direction, metric, s.currentRevision())
	if snapshot, ok := s.summaryGraphCache.Get(cacheKey); ok {
		return snapshot, nil
	}

	queryStart := time.Now()
	whereClause, args := summaryFilterClause(addressFamily, direction)
	query := fmt.Sprintf(`
SELECT src_entity, dst_entity,
  COALESCE(SUM(bytes), 0) AS bytes_total,
  COALESCE(SUM(connections), 0) AS connection_total,
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
  COALESCE(MAX(last_seen_ns), 0) AS last_seen_ns
FROM read_parquet(%s)
%s
GROUP BY src_entity, dst_entity, direction, ip_version
`, quoteLiteral(s.summaryEdgeGlob(granularity, metric)), whereClause)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query summary graph snapshot: %w", err)
	}
	defer rows.Close()

	snapshot := &summaryGraphSnapshotData{
		edges:       make([]summaryEdgeAggregate, 0, 128),
		granularity: granularity,
	}
	for rows.Next() {
		var edge summaryEdgeAggregate
		var direction sql.NullInt32
		if err := rows.Scan(
			&edge.Source,
			&edge.Destination,
			&edge.Bytes,
			&edge.Connections,
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
		); err != nil {
			return nil, fmt.Errorf("scan summary graph snapshot row: %w", err)
		}
		edge.Direction = directionValue(direction)
		snapshot.edges = append(snapshot.edges, edge)
		snapshot.totals.Bytes += edge.Bytes
		snapshot.totals.Connections += edge.Connections
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate summary graph snapshot rows: %w", err)
	}

	s.summaryGraphCache.Set(cacheKey, snapshot)
	slog.Debug("UI summary graph snapshot built", "granularity", granularity, "family", addressFamily, "direction", direction, "edge_count", len(snapshot.edges), "duration_ms", time.Since(queryStart).Milliseconds())
	return snapshot, nil
}

func buildSummaryMetricView(snapshot *summaryGraphSnapshotData, metric Metric) summaryMetricView {
	nodeTotalsByID := make(map[string]Node, len(snapshot.edges)*2)
	edges := make([]Edge, 0, len(snapshot.edges))
	for _, aggregate := range snapshot.edges {
		edge := Edge{
			Bytes:       aggregate.Bytes,
			Connections: aggregate.Connections,
			Destination: aggregate.Destination,
			FirstSeenNs: aggregate.FirstSeenNs,
			LastSeenNs:  aggregate.LastSeenNs,
			MetricValue: edgeMetricValue(Edge{Bytes: aggregate.Bytes, Connections: aggregate.Connections}, metric),
			Source:      aggregate.Source,
		}
		edges = append(edges, edge)

		sourceNode := nodeTotalsByID[aggregate.Source]
		sourceNode.ID = aggregate.Source
		sourceNode.Label = aggregate.Source
		sourceNode.PrivateMetric += summaryMetricValue(metric, aggregate.SrcPrivateBytes, aggregate.SrcPrivateConnections)
		sourceNode.PublicMetric += summaryMetricValue(metric, aggregate.SrcPublicBytes, aggregate.SrcPublicConnections)
		sourceNode.Egress += edge.MetricValue
		sourceNode.Total += edge.MetricValue
		sourceNode.AddressClass = classifyNodeAddress(sourceNode.PrivateMetric, sourceNode.PublicMetric)
		nodeTotalsByID[aggregate.Source] = sourceNode

		destinationNode := nodeTotalsByID[aggregate.Destination]
		destinationNode.ID = aggregate.Destination
		destinationNode.Label = aggregate.Destination
		destinationNode.PrivateMetric += summaryMetricValue(metric, aggregate.DstPrivateBytes, aggregate.DstPrivateConnections)
		destinationNode.PublicMetric += summaryMetricValue(metric, aggregate.DstPublicBytes, aggregate.DstPublicConnections)
		destinationNode.Ingress += edge.MetricValue
		destinationNode.Total += edge.MetricValue
		destinationNode.AddressClass = classifyNodeAddress(destinationNode.PrivateMetric, destinationNode.PublicMetric)
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

	restSourceNode := summaryRestNode(view.nodeTotals, keepLookup, true)
	restDestinationNode := summaryRestNode(view.nodeTotals, keepLookup, false)
	if restSourceNode != nil {
		visibleNodeMap[restSourceNode.ID] = *restSourceNode
	}
	if restDestinationNode != nil {
		visibleNodeMap[restDestinationNode.ID] = *restDestinationNode
	}

	edgesByKey := make(map[string]Edge, len(view.edges))
	for _, edge := range view.edges {
		sourceBucket := edge.Source
		destinationBucket := edge.Destination
		if state.NodeLimit > 0 && len(keepEntities) > 0 {
			if _, ok := keepLookup[sourceBucket]; !ok {
				sourceBucket = graphRestSourceID
			}
			if _, ok := keepLookup[destinationBucket]; !ok {
				destinationBucket = graphRestDestination
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
				Synthetic:   sourceBucket == graphRestSourceID || destinationBucket == graphRestDestination,
			}
		}
		aggregatedEdge.Bytes += edge.Bytes
		aggregatedEdge.Connections += edge.Connections
		if aggregatedEdge.FirstSeenNs == 0 || (edge.FirstSeenNs != 0 && edge.FirstSeenNs < aggregatedEdge.FirstSeenNs) {
			aggregatedEdge.FirstSeenNs = edge.FirstSeenNs
		}
		if edge.LastSeenNs > aggregatedEdge.LastSeenNs {
			aggregatedEdge.LastSeenNs = edge.LastSeenNs
		}
		aggregatedEdge.MetricValue = edgeMetricValue(aggregatedEdge, state.Metric)
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

func summaryRestNode(nodeTotals []Node, keepLookup map[string]struct{}, sourceRole bool) *Node {
	nodeID := graphRestSourceID
	if !sourceRole {
		nodeID = graphRestDestination
	}

	var collapsedEntityCount int
	var total int64
	for _, node := range nodeTotals {
		if _, ok := keepLookup[node.ID]; ok {
			continue
		}
		metricValue := node.Egress
		if !sourceRole {
			metricValue = node.Ingress
		}
		if metricValue == 0 {
			continue
		}
		collapsedEntityCount++
		total += metricValue
	}
	if total == 0 {
		return nil
	}

	node := &Node{
		CollapsedEntityCount: collapsedEntityCount,
		ID:                   nodeID,
		Label:                nodeID,
		Synthetic:            true,
		Total:                total,
	}
	if sourceRole {
		node.Egress = total
	} else {
		node.Ingress = total
	}
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

func (s *Service) summaryEdgeGlob(granularity Granularity, metric Metric) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if metric == MetricDNSLookups {
		return s.summaries.dnsEdgeGlobByGranularity[granularity]
	}
	return s.summaries.edgeGlobByGranularity[granularity]
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

func summaryFilterClause(addressFamily AddressFamily, direction DirectionFilter) (string, []any) {
	conditions := []string(nil)
	args := []any(nil)
	switch addressFamily {
	case AddressFamilyIPv4:
		conditions = append(conditions, "ip_version = ?")
		args = append(args, model.IPVersion4)
	case AddressFamilyIPv6:
		conditions = append(conditions, "ip_version = ?")
		args = append(args, model.IPVersion6)
	}
	switch direction {
	case DirectionEgress:
		conditions = append(conditions, "direction = ?")
		args = append(args, directionEgressParquetValue)
	case DirectionIngress:
		conditions = append(conditions, "direction = ?")
		args = append(args, directionIngressParquetValue)
	}
	if len(conditions) == 0 {
		return "", nil
	}
	return "WHERE " + strings.Join(conditions, " AND "), args
}
