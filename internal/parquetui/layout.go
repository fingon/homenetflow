package parquetui

import (
	"math"
	"math/rand/v2"
	"slices"

	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/iterator"
	"gonum.org/v1/gonum/graph/layout"
	"gonum.org/v1/gonum/graph/simple"
)

const (
	layoutCollisionIterations      = 320
	layoutDefaultSizePx            = 720
	layoutEadesRate                = 0.04
	layoutEadesRepulsion           = 1.2
	layoutEadesTheta               = 0.25
	layoutEadesUpdates             = 80
	layoutLabelCharWidthPx         = 7.2
	layoutLabelHeightPx            = 14
	layoutLabelPaddingPx           = 8
	layoutMaxSizePx                = 3200
	layoutMinNodeCenterPaddingPx   = 44
	layoutNodeGapPx                = 18
	layoutPersistentLabelGapPx     = 18
	layoutPersistentLabelYOffsetPx = 18
	layoutPersistentLabelPaddingPx = 8
	layoutScalePaddingPx           = 80
	layoutSeedStream               = 1
	layoutSeedValue                = 1
	layoutTargetNodeSeparationPx   = 128
	layoutWeightedEdgeScale        = 4
)

type LayoutPoint struct {
	X float64
	Y float64
}

type LayoutData struct {
	HeightPx  int
	Positions map[string]LayoutPoint
	WidthPx   int
}

type layoutEdge struct {
	Bytes       int64
	Connections int64
	Destination string
	Source      string
}

type layoutNode struct {
	ID        string
	Label     string
	Score     int64
	Selected  bool
	Synthetic bool
}

type layoutRect struct {
	bottom float64
	left   float64
	right  float64
	top    float64
}

type forceLayoutGraph struct {
	*simple.WeightedUndirectedGraph
	orderedNodes []graph.Node
}

func computeStableNodePositions(nodes []layoutNode, edges []layoutEdge, widthPx, heightPx int) map[string]LayoutPoint {
	return computeStableGraphLayout(nodes, edges, widthPx, heightPx).Positions
}

func computeStableGraphLayout(nodes []layoutNode, edges []layoutEdge, widthPx, heightPx int) LayoutData {
	sortedNodes := sortedLayoutNodes(nodes)
	if len(sortedNodes) == 0 {
		return LayoutData{
			HeightPx:  max(heightPx, layoutDefaultSizePx),
			Positions: map[string]LayoutPoint{},
			WidthPx:   max(widthPx, layoutDefaultSizePx),
		}
	}

	if widthPx <= 0 || heightPx <= 0 {
		sizePx := layoutSizePx(len(sortedNodes))
		widthPx = sizePx
		heightPx = sizePx
	}

	maxScore := sortedNodes[0].Score
	if maxScore <= 0 {
		maxScore = 1
	}
	nodeRadiiByID := make(map[string]float64, len(sortedNodes))
	for _, node := range sortedNodes {
		nodeRadiiByID[node.ID] = nodeRadius(node.Score, maxScore)
	}

	rawPositions := forceLayoutPositions(sortedNodes, edges)
	positions := scaleLayoutPositions(rawPositions, sortedNodes, nodeRadiiByID, widthPx, heightPx)
	relaxLayoutCollisions(positions, sortedNodes, nodeRadiiByID, widthPx, heightPx)

	return LayoutData{
		HeightPx:  heightPx,
		Positions: positions,
		WidthPx:   widthPx,
	}
}

func layoutSizePx(nodeCount int) int {
	if nodeCount <= 1 {
		return layoutDefaultSizePx
	}
	sizePx := int(math.Ceil(math.Sqrt(float64(nodeCount)) * layoutTargetNodeSeparationPx))
	return min(layoutMaxSizePx, max(layoutDefaultSizePx, sizePx))
}

func sortedLayoutNodes(nodes []layoutNode) []layoutNode {
	sortedNodes := append([]layoutNode(nil), nodes...)
	slices.SortFunc(sortedNodes, func(left, right layoutNode) int {
		if left.Score == right.Score {
			return compareLayoutSynthetic(left.Synthetic, right.Synthetic, left.ID, right.ID)
		}
		if left.Score > right.Score {
			return -1
		}
		return 1
	})
	return sortedNodes
}

func forceLayoutPositions(nodes []layoutNode, edges []layoutEdge) map[string]LayoutPoint {
	if len(nodes) == 1 {
		return map[string]LayoutPoint{
			nodes[0].ID: {X: 0, Y: 0},
		}
	}

	graphByID := &forceLayoutGraph{
		WeightedUndirectedGraph: simple.NewWeightedUndirectedGraph(0, 0),
		orderedNodes:            make([]graph.Node, 0, len(nodes)),
	}
	nodeIDsByName := make(map[string]int64, len(nodes))
	for index, node := range nodes {
		nodeID := int64(index + 1)
		nodeIDsByName[node.ID] = nodeID
		graphNode := simple.Node(nodeID)
		graphByID.AddNode(graphNode)
		graphByID.orderedNodes = append(graphByID.orderedNodes, graphNode)
	}

	for _, edge := range sortedLayoutEdges(edges) {
		sourceID, sourceOK := nodeIDsByName[edge.Source]
		destinationID, destinationOK := nodeIDsByName[edge.Destination]
		if !sourceOK || !destinationOK || sourceID == destinationID {
			continue
		}
		graphByID.SetWeightedEdge(graphByID.NewWeightedEdge(
			simple.Node(sourceID),
			simple.Node(destinationID),
			layoutGraphEdgeWeight(edge),
		))
	}

	eades := layout.EadesR2{
		Rate:      layoutEadesRate,
		Repulsion: layoutEadesRepulsion,
		Src:       rand.NewPCG(layoutSeedValue, layoutSeedStream),
		Theta:     layoutEadesTheta,
		Updates:   layoutEadesUpdates,
	}
	optimizer := layout.NewOptimizerR2(graphByID, eades.Update)
	for optimizer.Update() {
	}

	positions := make(map[string]LayoutPoint, len(nodes))
	for _, node := range nodes {
		point := optimizer.Coord2(nodeIDsByName[node.ID])
		positions[node.ID] = LayoutPoint{X: point.X, Y: point.Y}
	}
	return positions
}

func (g *forceLayoutGraph) Nodes() graph.Nodes {
	return iterator.NewOrderedNodes(g.orderedNodes)
}

func sortedLayoutEdges(edges []layoutEdge) []layoutEdge {
	sortedEdges := append([]layoutEdge(nil), edges...)
	slices.SortFunc(sortedEdges, func(left, right layoutEdge) int {
		if left.Source == right.Source {
			return stringsCompare(left.Destination, right.Destination)
		}
		return stringsCompare(left.Source, right.Source)
	})
	return sortedEdges
}

func scaleLayoutPositions(rawPositions map[string]LayoutPoint, nodes []layoutNode, nodeRadiiByID map[string]float64, widthPx, heightPx int) map[string]LayoutPoint {
	positions := make(map[string]LayoutPoint, len(rawPositions))
	if len(rawPositions) == 0 {
		return positions
	}

	minX := math.Inf(1)
	maxX := math.Inf(-1)
	minY := math.Inf(1)
	maxY := math.Inf(-1)
	for _, point := range rawPositions {
		minX = math.Min(minX, point.X)
		maxX = math.Max(maxX, point.X)
		minY = math.Min(minY, point.Y)
		maxY = math.Max(maxY, point.Y)
	}

	rawWidth := math.Max(maxX-minX, 0.0001)
	rawHeight := math.Max(maxY-minY, 0.0001)
	maxRadius := 0.0
	for _, node := range nodes {
		maxRadius = math.Max(maxRadius, nodeRadiiByID[node.ID])
	}
	paddingPx := layoutScalePaddingPx + maxRadius + maxPersistentLabelHeight(nodes, nodeRadiiByID)
	usableWidth := math.Max(float64(widthPx)-paddingPx*2, 1)
	usableHeight := math.Max(float64(heightPx)-paddingPx*2, 1)
	scale := math.Min(usableWidth/rawWidth, usableHeight/rawHeight)

	centerX := float64(widthPx) / 2
	centerY := float64(heightPx) / 2
	rawCenterX := (minX + maxX) / 2
	rawCenterY := (minY + maxY) / 2
	for _, node := range nodes {
		point := rawPositions[node.ID]
		positions[node.ID] = clampLayoutPoint(LayoutPoint{
			X: centerX + (point.X-rawCenterX)*scale,
			Y: centerY + (point.Y-rawCenterY)*scale,
		}, nodeRadiiByID[node.ID], widthPx, heightPx)
	}
	return positions
}

func maxPersistentLabelHeight(nodes []layoutNode, nodeRadiiByID map[string]float64) float64 {
	maxHeightPx := 0.0
	for _, node := range nodes {
		if !nodePersistentLabelVisible(node) {
			continue
		}
		maxHeightPx = math.Max(maxHeightPx, nodeRadiiByID[node.ID]+layoutPersistentLabelYOffsetPx+layoutLabelHeightPx)
	}
	return maxHeightPx
}

func relaxLayoutCollisions(positions map[string]LayoutPoint, nodes []layoutNode, nodeRadiiByID map[string]float64, widthPx, heightPx int) {
	nodeIDs := make([]string, 0, len(nodes))
	nodesByID := make(map[string]layoutNode, len(nodes))
	for _, node := range nodes {
		nodeIDs = append(nodeIDs, node.ID)
		nodesByID[node.ID] = node
	}
	slices.Sort(nodeIDs)

	for range layoutCollisionIterations {
		changed := false
		for leftIndex := range nodeIDs {
			leftID := nodeIDs[leftIndex]
			for rightIndex := leftIndex + 1; rightIndex < len(nodeIDs); rightIndex++ {
				rightID := nodeIDs[rightIndex]
				if separateLayoutNodes(positions, nodesByID[leftID], nodesByID[rightID], nodeRadiiByID, widthPx, heightPx, leftIndex+rightIndex) {
					changed = true
				}
			}
		}
		if !changed {
			return
		}
	}
}

func separateLayoutNodes(positions map[string]LayoutPoint, leftNode, rightNode layoutNode, nodeRadiiByID map[string]float64, widthPx, heightPx, fallbackIndex int) bool {
	leftPosition := positions[leftNode.ID]
	rightPosition := positions[rightNode.ID]
	requiredDistance := nodeRadiiByID[leftNode.ID] + nodeRadiiByID[rightNode.ID] + layoutNodeGapPx

	deltaX := rightPosition.X - leftPosition.X
	deltaY := rightPosition.Y - leftPosition.Y
	distance := math.Hypot(deltaX, deltaY)
	if distance < 0.001 {
		angle := evenlySpacedAngle(fallbackIndex, len(positions))
		deltaX = math.Cos(angle)
		deltaY = math.Sin(angle)
		distance = 1
	}

	overlapPx := math.Max(0, requiredDistance-distance)
	if labelRectsOverlap(leftNode, leftPosition, rightNode, rightPosition, nodeRadiiByID) {
		overlapPx = math.Max(overlapPx, layoutPersistentLabelGapPx)
	}
	if overlapPx <= 0 {
		return false
	}

	normalX := deltaX / distance
	normalY := deltaY / distance
	adjustmentPx := overlapPx / 2
	leftPosition.X -= normalX * adjustmentPx
	leftPosition.Y -= normalY * adjustmentPx
	rightPosition.X += normalX * adjustmentPx
	rightPosition.Y += normalY * adjustmentPx
	positions[leftNode.ID] = clampLayoutPoint(leftPosition, nodeRadiiByID[leftNode.ID], widthPx, heightPx)
	positions[rightNode.ID] = clampLayoutPoint(rightPosition, nodeRadiiByID[rightNode.ID], widthPx, heightPx)
	return true
}

func labelRectsOverlap(leftNode layoutNode, leftPosition LayoutPoint, rightNode layoutNode, rightPosition LayoutPoint, nodeRadiiByID map[string]float64) bool {
	leftRects := nodeCollisionRects(leftNode, leftPosition, nodeRadiiByID[leftNode.ID])
	rightRects := nodeCollisionRects(rightNode, rightPosition, nodeRadiiByID[rightNode.ID])
	for _, leftRect := range leftRects {
		for _, rightRect := range rightRects {
			if rectsOverlap(leftRect, rightRect) {
				return true
			}
		}
	}
	return false
}

func nodeCollisionRects(node layoutNode, position LayoutPoint, radiusPx float64) []layoutRect {
	rects := []layoutRect{{
		bottom: position.Y + radiusPx + layoutLabelPaddingPx,
		left:   position.X - radiusPx - layoutLabelPaddingPx,
		right:  position.X + radiusPx + layoutLabelPaddingPx,
		top:    position.Y - radiusPx - layoutLabelPaddingPx,
	}}
	if !nodePersistentLabelVisible(node) {
		return rects
	}

	label := node.Label
	if label == "" {
		label = node.ID
	}
	labelWidthPx := math.Max(float64(len(label))*layoutLabelCharWidthPx, radiusPx*2)
	labelTop := position.Y + radiusPx + layoutPersistentLabelYOffsetPx - layoutLabelHeightPx
	rects = append(rects, layoutRect{
		bottom: labelTop + layoutLabelHeightPx + layoutPersistentLabelPaddingPx,
		left:   position.X - labelWidthPx/2 - layoutPersistentLabelPaddingPx,
		right:  position.X + labelWidthPx/2 + layoutPersistentLabelPaddingPx,
		top:    labelTop - layoutPersistentLabelPaddingPx,
	})
	return rects
}

func nodePersistentLabelVisible(node layoutNode) bool {
	return node.Selected || node.Synthetic
}

func rectsOverlap(left, right layoutRect) bool {
	return left.left < right.right && left.right > right.left && left.top < right.bottom && left.bottom > right.top
}

func clampLayoutPoint(point LayoutPoint, nodeRadiusPx float64, widthPx, heightPx int) LayoutPoint {
	minX := float64(layoutMinNodeCenterPaddingPx) + nodeRadiusPx
	maxX := float64(widthPx-layoutMinNodeCenterPaddingPx) - nodeRadiusPx
	minY := float64(layoutMinNodeCenterPaddingPx) + nodeRadiusPx
	maxY := float64(heightPx-layoutMinNodeCenterPaddingPx) - nodeRadiusPx

	point.X = math.Max(minX, math.Min(maxX, point.X))
	point.Y = math.Max(minY, math.Min(maxY, point.Y))
	return point
}

func evenlySpacedAngle(index, count int) float64 {
	if count <= 0 {
		return 0
	}
	angle := -math.Pi/2 + (2*math.Pi*float64(index))/float64(count)
	return normalizeAngle(angle)
}

func normalizeAngle(angle float64) float64 {
	for angle < 0 {
		angle += 2 * math.Pi
	}
	for angle >= 2*math.Pi {
		angle -= 2 * math.Pi
	}
	return angle
}

func compareLayoutSynthetic(leftSynthetic, rightSynthetic bool, leftID, rightID string) int {
	if leftSynthetic != rightSynthetic {
		if leftSynthetic {
			return 1
		}
		return -1
	}
	return stringsCompare(leftID, rightID)
}

func layoutGraphEdgeWeight(edge layoutEdge) float64 {
	return 1 + math.Log10(math.Max(float64(edge.Bytes), 1)+math.Max(float64(edge.Connections), 1))/layoutWeightedEdgeScale
}

func stringsCompare(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}
