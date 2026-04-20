package parquetui

import (
	"math"
	"slices"
)

const (
	layoutInnerRingCount        = 9
	layoutMiddleRingCount       = 24
	layoutNodeGapPx             = 18
	layoutNodePaddingPx         = 44
	layoutOuterRingCount        = 48
	layoutRelaxIterations       = 20
	layoutRingCapacityFillRatio = 0.9
)

type LayoutPoint struct {
	X float64
	Y float64
}

type layoutEdge struct {
	Bytes       int64
	Connections int64
	Destination string
	Source      string
}

type layoutNode struct {
	ID        string
	Score     int64
	Synthetic bool
}

type layoutNeighbor struct {
	otherID string
	weight  float64
}

type ringNode struct {
	node        layoutNode
	targetAngle float64
}

func computeStableNodePositions(nodes []layoutNode, edges []layoutEdge, widthPx, heightPx int) map[string]LayoutPoint {
	positions := make(map[string]LayoutPoint, len(nodes))
	if len(nodes) == 0 {
		return positions
	}

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

	centerX := float64(widthPx) / 2
	centerY := float64(heightPx) / 2
	maxRadiusX := math.Max(80, centerX-float64(layoutNodePaddingPx))
	maxRadiusY := math.Max(80, centerY-float64(layoutNodePaddingPx))
	maxScore := sortedNodes[0].Score
	if maxScore <= 0 {
		maxScore = 1
	}
	nodeRadiiByID := make(map[string]float64, len(sortedNodes))
	for _, node := range sortedNodes {
		nodeRadiiByID[node.ID] = nodeRadius(node.Score, maxScore)
	}

	anchoredNodeLookup := make(map[string]struct{}, 3)
	placedAngles := make(map[string]float64, len(nodes))
	neighborsByNode := buildLayoutNeighbors(edges)
	regularNodes := make([]layoutNode, 0, len(nodes))

	for _, node := range sortedNodes {
		switch node.ID {
		case graphRestID:
			positions[node.ID] = LayoutPoint{
				X: centerX,
				Y: float64(heightPx - layoutNodePaddingPx),
			}
			placedAngles[node.ID] = math.Pi / 2
			anchoredNodeLookup[node.ID] = struct{}{}
		default:
			regularNodes = append(regularNodes, node)
		}
	}

	if len(regularNodes) == 0 {
		return positions
	}

	positions[regularNodes[0].ID] = LayoutPoint{X: centerX, Y: centerY}
	placedAngles[regularNodes[0].ID] = 0
	anchoredNodeLookup[regularNodes[0].ID] = struct{}{}

	if len(regularNodes) == 1 {
		return positions
	}

	rings := buildLayoutRings(regularNodes[1:], nodeRadiiByID, maxRadiusX, maxRadiusY)
	for ringIndex, ring := range rings {
		orderedNodes := orderLayoutRing(ring, neighborsByNode, placedAngles)
		radiusX, radiusY := layoutRingRadii(ringIndex, len(rings), maxRadiusX, maxRadiusY)
		for nodeIndex, node := range orderedNodes {
			angle := evenlySpacedAngle(nodeIndex, len(orderedNodes))
			positions[node.ID] = LayoutPoint{
				X: centerX + math.Cos(angle)*radiusX,
				Y: centerY + math.Sin(angle)*radiusY,
			}
			placedAngles[node.ID] = angle
		}
	}

	relaxLayoutCollisions(positions, nodeRadiiByID, anchoredNodeLookup, widthPx, heightPx)

	return positions
}

func buildLayoutNeighbors(edges []layoutEdge) map[string][]layoutNeighbor {
	neighborsByNode := make(map[string][]layoutNeighbor, len(edges)*2)
	for _, edge := range edges {
		weight := layoutEdgeWeight(edge)
		neighborsByNode[edge.Source] = append(neighborsByNode[edge.Source], layoutNeighbor{
			otherID: edge.Destination,
			weight:  weight,
		})
		neighborsByNode[edge.Destination] = append(neighborsByNode[edge.Destination], layoutNeighbor{
			otherID: edge.Source,
			weight:  weight,
		})
	}
	return neighborsByNode
}

func buildLayoutRings(nodes []layoutNode, nodeRadiiByID map[string]float64, maxRadiusX, maxRadiusY float64) [][]layoutNode {
	if len(nodes) == 0 {
		return nil
	}

	rings := make([][]layoutNode, 0, 4)
	remaining := append([]layoutNode(nil), nodes...)
	for ringIndex := 0; len(remaining) > 0; ringIndex++ {
		radiusX, radiusY := layoutEstimatedRingRadii(ringIndex, maxRadiusX, maxRadiusY)
		ringCapacityPx := layoutEllipseCircumference(radiusX, radiusY) * layoutRingCapacityFillRatio
		usedCapacityPx := 0.0
		ring := make([]layoutNode, 0, min(len(remaining), layoutOuterRingCount))
		for len(ring) < len(remaining) {
			nextNode := remaining[len(ring)]
			requiredCapacityPx := math.Max(2*nodeRadiiByID[nextNode.ID]+layoutNodeGapPx, 24)
			nextCount := len(ring) + 1
			hardLimit := layoutOuterRingCount
			switch ringIndex {
			case 0:
				hardLimit = layoutInnerRingCount
			case 1:
				hardLimit = layoutMiddleRingCount
			}
			if len(ring) > 0 && (usedCapacityPx+requiredCapacityPx > ringCapacityPx || nextCount > hardLimit) {
				break
			}
			ring = append(ring, nextNode)
			usedCapacityPx += requiredCapacityPx
		}
		if len(ring) == 0 {
			ring = append(ring, remaining[0])
		}
		rings = append(rings, ring)
		remaining = remaining[len(ring):]
	}
	return rings
}

func relaxLayoutCollisions(positions map[string]LayoutPoint, nodeRadiiByID map[string]float64, anchoredNodeLookup map[string]struct{}, widthPx, heightPx int) {
	nodeIDs := make([]string, 0, len(positions))
	for nodeID := range positions {
		nodeIDs = append(nodeIDs, nodeID)
	}
	slices.Sort(nodeIDs)

	centerX := float64(widthPx) / 2
	centerY := float64(heightPx) / 2

	for range layoutRelaxIterations {
		for leftIndex := range nodeIDs {
			leftID := nodeIDs[leftIndex]
			leftPosition := positions[leftID]
			leftRadius := nodeRadiiByID[leftID]

			for rightIndex := leftIndex + 1; rightIndex < len(nodeIDs); rightIndex++ {
				rightID := nodeIDs[rightIndex]
				rightPosition := positions[rightID]
				requiredDistance := leftRadius + nodeRadiiByID[rightID] + layoutNodeGapPx

				deltaX := rightPosition.X - leftPosition.X
				deltaY := rightPosition.Y - leftPosition.Y
				distance := math.Hypot(deltaX, deltaY)
				if distance >= requiredDistance {
					continue
				}

				if distance < 0.001 {
					angle := evenlySpacedAngle(leftIndex+rightIndex, len(nodeIDs))
					deltaX = math.Cos(angle)
					deltaY = math.Sin(angle)
					distance = 1
				}

				overlapPx := requiredDistance - distance
				normalX := deltaX / distance
				normalY := deltaY / distance

				_, leftAnchored := anchoredNodeLookup[leftID]
				_, rightAnchored := anchoredNodeLookup[rightID]
				switch {
				case leftAnchored && rightAnchored:
					continue
				case leftAnchored:
					rightPosition.X += normalX * overlapPx
					rightPosition.Y += normalY * overlapPx
					positions[rightID] = clampLayoutPoint(rightPosition, nodeRadiiByID[rightID], widthPx, heightPx, centerX, centerY)
				case rightAnchored:
					leftPosition.X -= normalX * overlapPx
					leftPosition.Y -= normalY * overlapPx
					positions[leftID] = clampLayoutPoint(leftPosition, leftRadius, widthPx, heightPx, centerX, centerY)
				default:
					adjustmentPx := overlapPx / 2
					leftPosition.X -= normalX * adjustmentPx
					leftPosition.Y -= normalY * adjustmentPx
					rightPosition.X += normalX * adjustmentPx
					rightPosition.Y += normalY * adjustmentPx
					positions[leftID] = clampLayoutPoint(leftPosition, leftRadius, widthPx, heightPx, centerX, centerY)
					positions[rightID] = clampLayoutPoint(rightPosition, nodeRadiiByID[rightID], widthPx, heightPx, centerX, centerY)
				}
			}
		}
	}
}

func orderLayoutRing(ring []layoutNode, neighborsByNode map[string][]layoutNeighbor, placedAngles map[string]float64) []layoutNode {
	ringNodes := make([]ringNode, 0, len(ring))
	for _, node := range ring {
		ringNodes = append(ringNodes, ringNode{
			node:        node,
			targetAngle: layoutTargetAngle(neighborsByNode[node.ID], placedAngles),
		})
	}

	slices.SortFunc(ringNodes, func(left, right ringNode) int {
		if left.targetAngle == right.targetAngle {
			if left.node.Score == right.node.Score {
				return stringsCompare(left.node.ID, right.node.ID)
			}
			if left.node.Score > right.node.Score {
				return -1
			}
			return 1
		}
		if left.targetAngle < right.targetAngle {
			return -1
		}
		return 1
	})

	orderedNodes := make([]layoutNode, 0, len(ringNodes))
	for _, ringNode := range ringNodes {
		orderedNodes = append(orderedNodes, ringNode.node)
	}
	return orderedNodes
}

func layoutTargetAngle(neighbors []layoutNeighbor, placedAngles map[string]float64) float64 {
	var sumCos float64
	var sumSin float64
	var sumWeight float64

	for _, neighbor := range neighbors {
		angle, ok := placedAngles[neighbor.otherID]
		if !ok {
			continue
		}
		sumCos += math.Cos(angle) * neighbor.weight
		sumSin += math.Sin(angle) * neighbor.weight
		sumWeight += neighbor.weight
	}

	if sumWeight == 0 {
		return math.Inf(1)
	}

	return normalizeAngle(math.Atan2(sumSin, sumCos))
}

func layoutRingRadii(ringIndex, ringCount int, maxRadiusX, maxRadiusY float64) (float64, float64) {
	if ringCount <= 0 {
		return 0, 0
	}

	const minFraction = 0.44
	const maxFraction = 0.96

	fraction := minFraction
	if ringCount > 1 {
		fraction = minFraction + (maxFraction-minFraction)*(float64(ringIndex)/float64(ringCount-1))
	}
	return maxRadiusX * fraction, maxRadiusY * fraction
}

func layoutEstimatedRingRadii(ringIndex int, maxRadiusX, maxRadiusY float64) (float64, float64) {
	const ringFractionStep = 0.16
	const minFraction = 0.44
	const maxFraction = 0.96

	fraction := minFraction + ringFractionStep*float64(ringIndex)
	if fraction > maxFraction {
		fraction = maxFraction
	}
	return maxRadiusX * fraction, maxRadiusY * fraction
}

func layoutEllipseCircumference(radiusX, radiusY float64) float64 {
	if radiusX <= 0 || radiusY <= 0 {
		return 0
	}
	return math.Pi * (3*(radiusX+radiusY) - math.Sqrt((3*radiusX+radiusY)*(radiusX+3*radiusY)))
}

func clampLayoutPoint(point LayoutPoint, nodeRadiusPx float64, widthPx, heightPx int, centerX, centerY float64) LayoutPoint {
	minX := float64(layoutNodePaddingPx) + nodeRadiusPx
	maxX := float64(widthPx-layoutNodePaddingPx) - nodeRadiusPx
	minY := float64(layoutNodePaddingPx) + nodeRadiusPx
	maxY := float64(heightPx-layoutNodePaddingPx) - nodeRadiusPx

	point.X = math.Max(minX, math.Min(maxX, point.X))
	point.Y = math.Max(minY, math.Min(maxY, point.Y))

	maxOffsetX := maxX - centerX
	maxOffsetY := maxY - centerY
	if maxOffsetX <= 0 || maxOffsetY <= 0 {
		return point
	}

	normalizedX := (point.X - centerX) / maxOffsetX
	normalizedY := (point.Y - centerY) / maxOffsetY
	distance := math.Hypot(normalizedX, normalizedY)
	if distance <= 1 {
		return point
	}

	point.X = centerX + (normalizedX/distance)*maxOffsetX
	point.Y = centerY + (normalizedY/distance)*maxOffsetY
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

func layoutEdgeWeight(edge layoutEdge) float64 {
	return 1 + math.Log10(math.Max(float64(edge.Bytes), 1)) + math.Log10(math.Max(float64(edge.Connections), 1))
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
