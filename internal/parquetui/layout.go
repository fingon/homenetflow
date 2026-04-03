package parquetui

import (
	"math"
	"slices"
)

const (
	layoutInnerRingCount   = 8
	layoutMiddleRingCount  = 24
	layoutNodePaddingPx    = 44
	layoutOuterRingCount   = 48
	layoutRestYOffsetRatio = 0.18
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
	restOffsetY := maxRadiusY * layoutRestYOffsetRatio

	placedAngles := make(map[string]float64, len(nodes))
	neighborsByNode := buildLayoutNeighbors(edges)
	regularNodes := make([]layoutNode, 0, len(nodes))

	for _, node := range sortedNodes {
		switch node.ID {
		case graphRestSourceID:
			positions[node.ID] = LayoutPoint{
				X: float64(layoutNodePaddingPx),
				Y: centerY - restOffsetY,
			}
			placedAngles[node.ID] = math.Pi
		case graphRestDestination:
			positions[node.ID] = LayoutPoint{
				X: float64(widthPx - layoutNodePaddingPx),
				Y: centerY + restOffsetY,
			}
			placedAngles[node.ID] = 0
		default:
			regularNodes = append(regularNodes, node)
		}
	}

	if len(regularNodes) == 0 {
		return positions
	}

	positions[regularNodes[0].ID] = LayoutPoint{X: centerX, Y: centerY}
	placedAngles[regularNodes[0].ID] = 0

	if len(regularNodes) == 1 {
		return positions
	}

	rings := buildLayoutRings(regularNodes[1:])
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

func buildLayoutRings(nodes []layoutNode) [][]layoutNode {
	if len(nodes) == 0 {
		return nil
	}

	rings := make([][]layoutNode, 0, 4)
	remaining := append([]layoutNode(nil), nodes...)
	appendRing := func(limit int) {
		if len(remaining) == 0 {
			return
		}
		size := min(limit, len(remaining))
		rings = append(rings, append([]layoutNode(nil), remaining[:size]...))
		remaining = remaining[size:]
	}

	appendRing(layoutInnerRingCount)
	appendRing(layoutMiddleRingCount)
	for len(remaining) > 0 {
		appendRing(layoutOuterRingCount)
	}
	return rings
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

	const minFraction = 0.28
	const maxFraction = 0.96

	fraction := minFraction
	if ringCount > 1 {
		fraction = minFraction + (maxFraction-minFraction)*(float64(ringIndex)/float64(ringCount-1))
	}
	return maxRadiusX * fraction, maxRadiusY * fraction
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
