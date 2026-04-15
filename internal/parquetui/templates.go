package parquetui

import (
	"fmt"
	"html"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	g "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html" //nolint:revive,staticcheck
)

const (
	graphHeightPx           = 560
	graphDenseNodeCount     = 36
	graphPrimaryRingCount   = 12
	graphWidthPx            = 960
	histogramAxisTickCount  = 5
	histogramBottomPadPx    = 28
	histogramHeightPx       = 132
	histogramMinBarWidthPx  = 4
	histogramRightPadPx     = 64
	histogramTopPadPx       = 8
	histogramWidthPx        = 960
	histogramYAxisMinTicks  = 3
	histogramYAxisOneDigit  = 1
	histogramYAxisTwoDigits = 2
	hxSelectAppShellValue   = "#app-shell"
	hxSwapOuterHTMLValue    = "outerHTML"
	hxTargetAppShellValue   = "#app-shell"
	nxdomainEdgeStroke      = "#8d2f20"
	nxdomainNodeFill        = "#8d2f20"
	mixedDNSEdgeStroke      = "#c4a237"
	mixedDNSNodeFill        = "#c4a237"
	mixedEntityNodeFill     = "#c4a237"
	nodeBaseRadiusPx        = 10
	nodeRadiusScalePx       = 24
	privateEntityNodeFill   = "#4d6f52"
	selectedEdgeStroke      = "#b14d24"
	selectedNodeStroke      = "#943a15"
	selectedRegularNodeFill = "#b14d24"
	syntheticNodeFill       = "#a69b84"
	unselectedEdgeStroke    = "rgba(55, 68, 87, 0.28)"
	unselectedNodeFill      = "#587ea3"
	unselectedNodeStroke    = "rgba(31, 39, 51, 0.24)"
)

func Index(dashboard DashboardData, devMode bool, devSessionToken string) g.Node {
	bodyNodes := []g.Node{
		AppShell(dashboard),
	}
	if devMode {
		bodyNodes = append([]g.Node{
			Data("dev-mode", "true"),
			Data("dev-session-token", devSessionToken),
		}, bodyNodes...)
	}

	return Doctype(
		HTML(Lang("en"),
			Head(
				Meta(Charset("utf-8")),
				Meta(Name("viewport"), Content("width=device-width, initial-scale=1")),
				TitleEl(g.Text("Netflow Browser")),
				Link(Rel("stylesheet"), Href("/static/style.css")),
				Script(Src("/static/htmx.min.js"), Defer()),
				Script(Src("/static/app.js"), Defer()),
			),
			Body(bodyNodes...),
		),
	)
}

func AppShell(dashboard DashboardData) g.Node {
	state := dashboard.State

	return Div(
		ID("app-shell"),
		Class("app-shell"),
		Data("span-start-ns", strconv.FormatInt(dashboard.Span.StartNs, 10)),
		Data("span-end-ns", strconv.FormatInt(dashboard.Span.EndNs, 10)),
		topBar(dashboard),
		Div(
			ID("loading-indicator"),
			Class("loading-indicator"),
			g.Attr("aria-live", "polite"),
		),
		Section(
			Class("histogram-panel"),
			Div(
				Class("panel-heading"),
				H2(g.Text("Timeline")),
				Span(Class("panel-subtle hover-help timeline-help"), g.Text("Drag to zoom")),
			),
			Div(
				ID("histogram"),
				Class("histogram"),
				Data("span-start-ns", strconv.FormatInt(dashboard.Span.StartNs, 10)),
				Data("span-end-ns", strconv.FormatInt(dashboard.Span.EndNs, 10)),
				renderHistogramSVG(state.Metric, dashboard.Histogram),
			),
		),
		Section(
			Class("section-panel section-block"),
			ID("graph-section"),
			sectionHeader("Graph", "graph-section-content", true),
			Div(
				Class("content-grid"),
				ID("graph-section-content"),
				Section(
					Class("graph-panel"),
					ID("graph-panel"),
					Div(
						Class("legend-line graph-legend"),
						Span(Class("legend-item"), g.Text("Node size = selected metric")),
						Span(Class("legend-item"), g.Text("Edge width = selected metric")),
						Span(Class("legend-item hover-help graph-help"), g.Text("Scroll to zoom, drag to pan")),
						Span(Class("legend-item hover-help graph-help"), g.Text("Labels expand with zoom and hover")),
					),
					Div(
						ID("graph-canvas"),
						Class("graph-canvas"),
						renderGraphSVG(state, dashboard.Graph),
					),
				),
				Aside(
					Class("side-panel"),
					ID("summary-panel"),
					SummaryPanel(state, dashboard.Graph),
				),
			),
		),
		Section(
			Class("section-panel section-block"),
			ID("rankings-section"),
			sectionHeader("Rankings", "rankings-content", true),
			Div(
				Class("rankings-section"),
				ID("rankings-content"),
				RankingsPanel(state, dashboard.Graph),
			),
		),
		Section(
			Class("section-panel section-block"),
			ID("table-panel"),
			sectionHeader("Flows Table", "table-content", true),
			Div(
				ID("table-content"),
				Class("section-content"),
				TablePanel(state, dashboard.Table),
			),
		),
	)
}

func SummaryPanel(state QueryState, graph GraphData) g.Node {
	currentAddressFamily := normalizedAddressFamily(state.AddressFamily)
	addressFamilyChip := g.Node(nil)
	if currentAddressFamily != AddressFamilyAll {
		addressFamilyChip = Span(Class("chip"), g.Text("Address Family: "+addressFamilyLabel(currentAddressFamily)))
	}
	currentDirection := normalizedDirection(state.Direction)
	directionChip := g.Node(nil)
	if currentDirection != DirectionBoth {
		directionChip = Span(Class("chip"), g.Text("Direction: "+directionLabel(currentDirection)))
	}

	return Div(
		Class("summary-panel"),
		sectionTitle("Active Filters"),
		Div(
			Class("filter-list"),
			Span(Class("chip"), g.Text("Time: "+formatNsRange(state.FromNs, state.ToNs))),
			addressFamilyChip,
			directionChip,
			renderNodes(state.Include, func(item string) g.Node {
				return Span(Class("chip"), g.Text("Entity: "+item))
			}),
			renderNodes(state.Exclude, func(item string) g.Node {
				return Span(Class("chip"), g.Text("Exclude: "+item))
			}),
		),
		sectionTitle("Totals"),
		Div(
			Class("stats-grid"),
			statBlock("Entities", strconv.Itoa(graph.Totals.Entities)),
			statBlock("Edges", strconv.Itoa(graph.Totals.Edges)),
			statBlock("Bytes", formatMetricValue(MetricBytes, graph.Totals.Bytes)),
			statBlock(connectionsTotalLabel(state.Metric), formatMetricValue(connectionsDisplayMetric(state.Metric), graph.Totals.Connections)),
		),
		selectedPanel(state, graph),
	)
}

func RankingsPanel(state QueryState, graph GraphData) g.Node {
	return g.Group([]g.Node{
		Section(
			Class("rankings-panel"),
			sectionTitle("Top Entities"),
			Ul(
				Class("rank-list"),
				renderNodes(graph.TopEntities, func(node Node) g.Node {
					return Li(navLink(selectEntityStateURL(state, node.ID), dnsResultClass("list-button", node.DNSResultState), fmt.Sprintf("%s (%s)", node.Label, formatMetricValue(graph.ActiveMetric, node.Total))))
				}),
			),
		),
		Section(
			Class("rankings-panel"),
			sectionTitle(topEdgesTitle(state.Metric)),
			Ul(
				Class("rank-list"),
				renderNodes(graph.TopEdges, func(edge Edge) g.Node {
					return Li(navLink(selectEdgeStateURL(state, edge.Source, edge.Destination), dnsResultClass("list-button", edge.DNSResultState), fmt.Sprintf("%s -> %s (%s)", edge.Source, edge.Destination, formatMetricValue(graph.ActiveMetric, edge.MetricValue))))
				}),
			),
		),
	})
}

func topEdgesTitle(metric Metric) string {
	if metric == MetricDNSLookups {
		return "Top Lookups"
	}
	return "Top Flows"
}

func connectionsTotalLabel(metric Metric) string {
	if metric == MetricDNSLookups {
		return "DNS Lookups"
	}
	return "Connections"
}

func connectionsDisplayMetric(metric Metric) Metric {
	if metric == MetricDNSLookups {
		return MetricDNSLookups
	}
	return MetricConnections
}

func connectionsSortKey(metric Metric) TableSort {
	if metric == MetricDNSLookups {
		return SortDNSLookups
	}
	return SortConnections
}

func selectedPanel(state QueryState, graph GraphData) g.Node {
	if graph.SelectedEdge != nil {
		return Div(
			sectionTitle("Selected Edge"),
			P(Class(dnsResultClass("", graph.SelectedEdge.DNSResultState)), g.Text(fmt.Sprintf("%s -> %s", graph.SelectedEdge.Source, graph.SelectedEdge.Destination))),
			P(g.Text("Bytes: "+formatMetricValue(MetricBytes, graph.SelectedEdge.Bytes))),
			P(g.Text(connectionsTotalLabel(graph.ActiveMetric)+": "+formatMetricValue(connectionsDisplayMetric(graph.ActiveMetric), graph.SelectedEdge.Connections))),
			P(g.Text("First seen: "+formatTimestamp(graph.SelectedEdge.FirstSeenNs))),
			P(g.Text("Last seen: "+formatTimestamp(graph.SelectedEdge.LastSeenNs))),
		)
	}

	if graph.SelectedNode == nil {
		return Div(
			sectionTitle("Selected Item"),
			P(Class("panel-subtle"), g.Text("Click a node to highlight it and inspect its peers.")),
		)
	}

	selectedNode := graph.SelectedNode
	entityURL := filterToEntityStateURL(state, selectedNode.ID)
	excludeURL := excludeEntityStateURL(state, selectedNode.ID)
	drillURL := drillStateURL(state, selectedNode.ID)

	return Div(
		sectionTitle("Selected Entity"),
		P(Class(dnsResultClass("", selectedNode.DNSResultState)), g.Text(selectedNode.Label)),
		P(g.Text("Ingress: "+formatMetricValue(graph.ActiveMetric, selectedNode.Ingress))),
		P(g.Text("Egress: "+formatMetricValue(graph.ActiveMetric, selectedNode.Egress))),
		Div(
			Class("button-row"),
			navLink(entityURL, "action-button", "Filter to this entity"),
			navLink(excludeURL, "action-button", "Exclude"),
			navLink(drillURL, "action-button", "Drill down granularity"),
		),
		sectionTitle("Peers"),
		Ul(
			Class("rank-list"),
			renderNodes(graph.SelectedNodePeers, func(peer DetailPeer) g.Node {
				return Li(g.Text(fmt.Sprintf("%s (%s)", peer.Entity, formatMetricValue(graph.ActiveMetric, peer.MetricValue))))
			}),
		),
	)
}

func TablePanel(state QueryState, table TableData) g.Node {
	return Div(
		Div(Class("table-meta"), Span(Class("panel-subtle"), g.Text(fmt.Sprintf("%d rows", table.TotalCount)))),
		Table(
			Class("flows-table"),
			THead(
				Tr(
					Th(sortLink(state, "Source", SortSource)),
					Th(sortLink(state, "Destination", SortDestination)),
					Th(sortLink(state, "Bytes", SortBytes)),
					Th(sortLink(state, connectionsTotalLabel(state.Metric), connectionsSortKey(state.Metric))),
					Th(sortLink(state, "First Seen", SortFirstSeen)),
					Th(sortLink(state, "Last Seen", SortLastSeen)),
				),
			),
			TBody(
				renderNodes(table.VisibleRows, func(row TableRow) g.Node {
					rowClass := dnsResultClass("", row.DNSResultState)
					if row.Synthetic {
						rowClass = strings.TrimSpace(rowClass + " synthetic-row")
					}
					return Tr(
						Class(rowClass),
						Td(g.Text(row.Source)),
						Td(g.Text(row.Destination)),
						Td(g.Text(formatMetricValue(MetricBytes, row.Bytes))),
						Td(g.Text(formatMetricValue(connectionsDisplayMetric(state.Metric), row.Connections))),
						Td(g.Text(formatTimestamp(row.FirstSeenNs))),
						Td(g.Text(formatTimestamp(row.LastSeenNs))),
					)
				}),
			),
		),
		Div(
			Class("pagination-row"),
			paginationLink(state, "Previous", max(1, table.Page-1), table.Page <= 1),
			Span(Class("panel-subtle"), g.Text(fmt.Sprintf("Page %d / %d", table.Page, table.TotalPages))),
			paginationLink(state, "Next", min(table.TotalPages, table.Page+1), table.Page >= table.TotalPages),
		),
	)
}

func topBar(dashboard DashboardData) g.Node {
	state := dashboard.State
	currentAddressFamily := normalizedAddressFamily(state.AddressFamily)
	currentDirection := normalizedDirection(state.Direction)
	directionDisabled := state.Metric == MetricDNSLookups

	return Header(
		Class("top-bar"),
		Form(
			Method("get"),
			Action("/"),
			ID("filters-form"),
			g.Attr("hx-get", "/"),
			g.Attr("hx-target", hxTargetAppShellValue),
			g.Attr("hx-select", hxSelectAppShellValue),
			g.Attr("hx-swap", hxSwapOuterHTMLValue),
			g.Attr("hx-push-url", "true"),
			g.Attr("hx-indicator", "#loading-indicator"),
			g.Attr("hx-sync", "this:replace"),
			Div(
				Class("top-bar-row"),
				hiddenStateFields(state),
				Div(
					Class("group"),
					Div(
						Class("button-row"),
						toggleRadio("preset", presetAllValue, "All", selectedPreset(state) == presetAllValue),
						toggleRadio("preset", presetHourValue, presetHourValue, selectedPreset(state) == presetHourValue),
						toggleRadio("preset", presetDayValue, presetDayValue, selectedPreset(state) == presetDayValue),
						toggleRadio("preset", presetWeekValue, presetWeekValue, selectedPreset(state) == presetWeekValue),
						toggleRadio("preset", presetMonthValue, presetMonthValue, selectedPreset(state) == presetMonthValue),
					),
				),
				Div(
					Class("group segmented"),
					Label(g.Text("Show")),
					toggleRadio("metric", string(MetricBytes), "Bytes", state.Metric == MetricBytes),
					toggleRadio("metric", string(MetricConnections), "Connections", state.Metric == MetricConnections),
					toggleRadio("metric", string(MetricDNSLookups), "DNS Lookups", state.Metric == MetricDNSLookups),
				),
				Div(
					Class("group segmented"),
					Label(g.Text("By")),
					toggleRadio("granularity", string(GranularityTLD), "TLD", state.Granularity == GranularityTLD),
					toggleRadio("granularity", string(Granularity2LD), "2TLD", state.Granularity == Granularity2LD),
					toggleRadio("granularity", string(GranularityHostname), "Hostname", state.Granularity == GranularityHostname),
					toggleRadio("granularity", string(GranularityIP), "IP", state.Granularity == GranularityIP),
				),
				Div(
					Class("group segmented"),
					Label(g.Text("Using")),
					toggleRadio("family", string(AddressFamilyAll), "All", currentAddressFamily == AddressFamilyAll),
					toggleRadio("family", string(AddressFamilyIPv4), "IPv4", currentAddressFamily == AddressFamilyIPv4),
					toggleRadio("family", string(AddressFamilyIPv6), "IPv6", currentAddressFamily == AddressFamilyIPv6),
				),
				Div(
					Class("group segmented"),
					Label(g.Text("Direction")),
					toggleRadioDisabled("direction", string(DirectionBoth), "Both", currentDirection == DirectionBoth, directionDisabled),
					toggleRadioDisabled("direction", string(DirectionEgress), "Egress", currentDirection == DirectionEgress, directionDisabled),
					toggleRadioDisabled("direction", string(DirectionIngress), "Ingress", currentDirection == DirectionIngress, directionDisabled),
				),
				Div(
					Class("group"),
					Input(
						Type("search"),
						ID("search-input"),
						Name("search"),
						Value(state.Search),
						Placeholder("Search visible entities"),
						Data("behavior", "search"),
					),
				),
				Div(
					Class("group"),
					Label(For("node-limit"), g.Text("Nodes")),
					Select(
						ID("node-limit"),
						Name("node_limit"),
						optionValue("auto", "Auto", state.NodeLimit == defaultNodeLimit(state.Granularity) || state.NodeLimit == 0),
						optionValue("100", "Top 100", state.NodeLimit == 100),
						optionValue("150", "Top 150", state.NodeLimit == 150),
						optionValue("200", "Top 200", state.NodeLimit == 200),
						optionValue("400", "Top 400", state.NodeLimit == 400),
					),
				),
				Div(
					Class("group"),
					Label(For("edge-limit"), g.Text("Edges")),
					Select(
						ID("edge-limit"),
						Name("edge_limit"),
						optionValue("100", "Important", state.EdgeLimit == 100),
						optionValue("250", "Top 250", state.EdgeLimit == 250),
						optionValue("0", "All", state.EdgeLimit == 0),
					),
				),
				navLink("/", "action-button danger", "Reset"),
			),
		),
	)
}

func hiddenStateFields(state QueryState) g.Node {
	return g.Group([]g.Node{
		Input(Type("hidden"), ID("filter-from-ns"), Name("from"), Value(strconv.FormatInt(state.FromNs, 10))),
		Input(Type("hidden"), ID("filter-to-ns"), Name("to"), Value(strconv.FormatInt(state.ToNs, 10))),
		Input(Type("hidden"), Name("sort"), Value(string(state.Sort))),
		Input(Type("hidden"), Name("page"), Value(strconv.Itoa(defaultPage))),
		Input(Type("hidden"), Name("page_size"), Value(strconv.Itoa(state.PageSize))),
		Input(Type("hidden"), Name("selected_entity"), Value(state.SelectedEntity)),
		Input(Type("hidden"), Name("selected_edge_src"), Value(state.SelectedEdgeSrc)),
		Input(Type("hidden"), Name("selected_edge_dst"), Value(state.SelectedEdgeDst)),
		renderHiddenValues("include", state.Include),
		renderHiddenValues("exclude", state.Exclude),
	})
}

func renderHiddenValues(name string, values []string) g.Node {
	return renderNodes(values, func(value string) g.Node {
		return Input(Type("hidden"), Name(name), Value(value))
	})
}

func toggleRadio(name, value, label string, checked bool) g.Node {
	return toggleRadioDisabled(name, value, label, checked, false)
}

func toggleRadioDisabled(name, value, label string, checked, disabled bool) g.Node {
	return Label(
		Class(buttonClassDisabled(checked, disabled)),
		Input(Type("radio"), Name(name), Value(value), checkedIf(checked), disabledIf(disabled)),
		Span(g.Text(label)),
	)
}

func renderGraphSVG(state QueryState, graph GraphData) g.Node {
	return g.Raw(graphSVGMarkup(state, graph))
}

func sectionHeader(title, contentID string, expanded bool) g.Node {
	return Div(
		Class("panel-heading section-heading"),
		H2(g.Text(title)),
		Button(
			Type("button"),
			Class("section-toggle"),
			g.Attr("aria-label", sectionToggleAriaLabel(title, expanded)),
			g.Attr("aria-controls", contentID),
			g.Attr("aria-expanded", strconv.FormatBool(expanded)),
			g.Attr("data-collapsible-toggle", ""),
			Data("section-title", title),
			Span(Class("section-toggle-icon"), g.Attr("aria-hidden", "true")),
		),
	)
}

func sectionToggleAriaLabel(title string, expanded bool) string {
	if expanded {
		return "Collapse " + title
	}
	return "Expand " + title
}

func graphSVGMarkup(state QueryState, graph GraphData) string {
	var builder strings.Builder

	fmt.Fprintf(&builder, `<svg class="%s" viewBox="0 0 %d %d" role="img" aria-label="Traffic graph">`, graphSVGClass(graph), graphWidthPx, graphHeightPx)
	if len(graph.Nodes) == 0 {
		builder.WriteString(labelTextMarkup(float64(graphWidthPx)/2, float64(graphHeightPx)/2, "No graph data", "middle"))
		builder.WriteString(`</svg>`)
		return builder.String()
	}

	positions := graph.NodePositions
	if len(positions) == 0 {
		positions = computeNodePositions(graph.Nodes, graphWidthPx, graphHeightPx)
	}
	builder.WriteString(`<g class="graph-scene">`)
	builder.WriteString(`<g class="graph-edges">`)
	for _, edge := range graph.Edges {
		source, sourceOK := positions[edge.Source]
		destination, destinationOK := positions[edge.Destination]
		if !sourceOK || !destinationOK {
			continue
		}

		builder.WriteString(`<a ` + navAttrString(selectEdgeStateURL(state, edge.Source, edge.Destination)) + `>`)
		fmt.Fprintf(&builder, `<path d="%s" stroke="%s" stroke-width="%0.2f" fill="none"%s>`,
			edgePathMarkup(source, destination),
			edgeStroke(edge),
			math.Max(1.5, 1+math.Log10(math.Max(float64(edge.MetricValue), 1))),
			dashArrayAttr(edge.Synthetic))
		builder.WriteString(titleMarkup(fmt.Sprintf("%s -> %s\nBytes: %s\n%s: %s",
			edge.Source,
			edge.Destination,
			formatMetricValue(MetricBytes, edge.Bytes),
			connectionsTotalLabel(graph.ActiveMetric),
			formatMetricValue(connectionsDisplayMetric(graph.ActiveMetric), edge.Connections))))
		builder.WriteString(`</path>`)
		fmt.Fprintf(&builder, `<path class="graph-edge-hitbox" d="%s" stroke="transparent" stroke-width="16" fill="none"></path>`,
			edgePathMarkup(source, destination))
		builder.WriteString(`</a>`)
	}
	builder.WriteString(`</g><g class="graph-nodes">`)

	maxTotal := maxNodeTotal(graph.Nodes)
	for _, node := range graph.Nodes {
		position := positions[node.ID]
		radius := nodeRadius(node.Total, maxTotal)
		labelPersistent := node.Selected || node.Synthetic
		builder.WriteString(`<a ` + navAttrString(selectEntityStateURL(state, node.ID)) + `>`)
		fmt.Fprintf(&builder, `<g class="%s" transform="translate(%0.2f, %0.2f)" data-node-id="%s" data-node-priority="%d" data-node-radius="%0.2f" data-label-persistent="%t">`,
			graphNodeClass(node),
			position.X,
			position.Y,
			html.EscapeString(node.ID),
			node.Total,
			radius,
			labelPersistent)
		fmt.Fprintf(&builder, `<circle r="%0.2f" fill="%s" stroke="%s" stroke-width="%s">`,
			radius,
			nodeFill(node),
			nodeStroke(node.Selected),
			nodeStrokeWidth(node.Selected))
		builder.WriteString(titleMarkup(fmt.Sprintf("%s\nIngress: %s\nEgress: %s",
			node.Label,
			formatMetricValue(state.Metric, node.Ingress),
			formatMetricValue(state.Metric, node.Egress))))
		builder.WriteString(`</circle>`)
		builder.WriteString(labelTextMarkup(0, radius+18, node.Label, "middle"))
		builder.WriteString(`</g></a>`)
	}

	builder.WriteString(`</g></g></svg>`)
	return builder.String()
}

func renderHistogramSVG(metric Metric, bins []HistogramBin) g.Node {
	return g.Raw(histogramSVGMarkup(metric, bins))
}

func histogramSVGMarkup(metric Metric, bins []HistogramBin) string {
	var builder strings.Builder

	fmt.Fprintf(&builder, `<svg class="histogram-svg" viewBox="0 0 %d %d" role="img" aria-label="Traffic timeline">`, histogramWidthPx, histogramHeightPx)
	if len(bins) == 0 {
		builder.WriteString(labelTextMarkup(float64(histogramWidthPx)/2, float64(histogramHeightPx)/2, "No timeline data", "middle"))
		builder.WriteString(`</svg>`)
		return builder.String()
	}

	maxValue := int64(1)
	for _, bin := range bins {
		maxValue = max(maxValue, bin.Value)
	}
	yAxisMaxValue, yAxisTickStep, yAxisTicks := histogramYAxisScale(maxValue)
	plotWidthPx := float64(histogramWidthPx - histogramRightPadPx)
	barWidth := math.Max(histogramMinBarWidthPx, plotWidthPx/float64(len(bins)))
	plotHeightPx := float64(histogramHeightPx - histogramBottomPadPx - histogramTopPadPx)
	builder.WriteString(`<g>`)
	for index, bin := range bins {
		barHeight := (float64(bin.Value) / float64(yAxisMaxValue)) * math.Max(1, plotHeightPx-6)
		x := float64(index) * barWidth
		y := histogramTopPadPx + plotHeightPx - barHeight
		formattedValue := formatMetricValue(metric, bin.Value)
		fromLabel := formatTimestamp(bin.FromNs)
		toLabel := formatTimestamp(bin.ToNs)
		fmt.Fprintf(&builder, `<rect class="histogram-bar" x="%0.2f" y="%0.2f" width="%0.2f" height="%0.2f" rx="2" fill="rgba(177, 77, 36, 0.62)" tabindex="0" data-bin-index="%d" data-from-ns="%d" data-to-ns="%d" data-from-label="%s" data-to-label="%s" data-value-label="%s">`,
			x,
			y,
			math.Max(2, barWidth-1),
			barHeight,
			index,
			bin.FromNs,
			bin.ToNs,
			html.EscapeString(fromLabel),
			html.EscapeString(toLabel),
			html.EscapeString(formattedValue))
		builder.WriteString(titleMarkup(fmt.Sprintf("%s - %s\nValue: %s",
			fromLabel,
			toLabel,
			formattedValue)))
		builder.WriteString(`</rect>`)
	}
	builder.WriteString(histogramAxisMarkup(metric, bins, yAxisMaxValue, yAxisTickStep, yAxisTicks))
	builder.WriteString(`</g></svg>`)
	return builder.String()
}

func computeNodePositions(nodes []Node, widthPx, heightPx int) map[string]LayoutPoint {
	positions := make(map[string]LayoutPoint, len(nodes))
	sorted := append([]Node(nil), nodes...)
	slices.SortFunc(sorted, func(left, right Node) int {
		if left.Total == right.Total {
			return strings.Compare(left.ID, right.ID)
		}
		if left.Total > right.Total {
			return -1
		}
		return 1
	})

	centerX := float64(widthPx) / 2
	centerY := float64(heightPx) / 2
	radiusX := math.Max(120, float64(widthPx)*0.34)
	radiusY := math.Max(100, float64(heightPx)*0.3)

	if len(sorted) > 0 {
		positions[sorted[0].ID] = LayoutPoint{X: centerX, Y: centerY}
	}

	for index := 1; index < len(sorted); index++ {
		angle := (2 * math.Pi * float64(index-1)) / math.Max(float64(len(sorted)-1), 1)
		ring := 1 + (index-1)/graphPrimaryRingCount
		scale := math.Min(float64(ring), 2)
		positions[sorted[index].ID] = LayoutPoint{
			X: centerX + math.Cos(angle)*radiusX*scale,
			Y: centerY + math.Sin(angle)*radiusY*scale,
		}
	}

	return positions
}

func nodeRadius(total, maxTotal int64) float64 {
	return nodeBaseRadiusPx + nodeRadiusScalePx*math.Sqrt(math.Max(float64(total), 1)/math.Max(float64(maxTotal), 1))
}

func maxNodeTotal(nodes []Node) int64 {
	maxTotal := int64(1)
	for _, node := range nodes {
		maxTotal = max(maxTotal, node.Total)
	}
	return maxTotal
}

func titleMarkup(value string) string {
	return "<title>" + html.EscapeString(value) + "</title>"
}

func labelTextMarkup(x, y float64, value, anchor string) string {
	return fmt.Sprintf(
		`<text x="%0.2f" y="%0.2f" class="graph-label" text-anchor="%s">%s</text>`,
		x,
		y,
		html.EscapeString(anchor),
		html.EscapeString(value),
	)
}

func navAttrString(href string) string {
	escapedHref := html.EscapeString(href)
	return fmt.Sprintf(`href="%s" hx-get="%s" hx-target="%s" hx-select="%s" hx-swap="%s" hx-push-url="true"`,
		escapedHref,
		escapedHref,
		hxTargetAppShellValue,
		hxSelectAppShellValue,
		hxSwapOuterHTMLValue,
	)
}

func navLink(href, className, label string) g.Node {
	return A(
		Href(href),
		Class(className),
		g.Attr("hx-get", href),
		g.Attr("hx-target", hxTargetAppShellValue),
		g.Attr("hx-select", hxSelectAppShellValue),
		g.Attr("hx-swap", hxSwapOuterHTMLValue),
		g.Attr("hx-push-url", "true"),
		g.Text(label),
	)
}

func sortLink(state QueryState, label string, sortKey TableSort) g.Node {
	nextState := state.Clone()
	nextState.Sort = sortKey
	nextState.Page = defaultPage
	return navLink(stateURL(nextState), "list-button", label)
}

func paginationLink(state QueryState, label string, page int, disabled bool) g.Node {
	className := "action-button"
	if disabled {
		className += " disabled"
		return Span(Class(className), g.Text(label))
	}

	nextState := state.Clone()
	nextState.Page = page
	return navLink(stateURL(nextState), className, label)
}

func selectEntityStateURL(state QueryState, entity string) string {
	nextState := state.Clone()
	nextState.SelectedEntity = entity
	nextState.SelectedEdgeSrc = ""
	nextState.SelectedEdgeDst = ""
	nextState.Page = defaultPage
	return stateURL(nextState)
}

func selectEdgeStateURL(state QueryState, source, destination string) string {
	nextState := state.Clone()
	nextState.SelectedEntity = ""
	nextState.SelectedEdgeSrc = source
	nextState.SelectedEdgeDst = destination
	nextState.Page = defaultPage
	return stateURL(nextState)
}

func filterToEntityStateURL(state QueryState, entity string) string {
	nextState := state.WithIncluded(entity).ResetSelection()
	nextState.Page = defaultPage
	return stateURL(nextState)
}

func excludeEntityStateURL(state QueryState, entity string) string {
	nextState := state.WithExcluded(entity).ResetSelection()
	nextState.Page = defaultPage
	return stateURL(nextState)
}

func drillStateURL(state QueryState, entity string) string {
	nextState := state.Clone()
	switch state.Granularity {
	case Granularity2LD:
		nextState.Granularity = GranularityHostname
	case GranularityHostname:
		nextState.Granularity = GranularityIP
	}
	nextState.Include = []string{entity}
	nextState.NodeLimit = 0
	nextState.Page = defaultPage
	nextState = nextState.ResetSelection()
	return stateURL(nextState)
}

func stateURL(state QueryState) string {
	query := state.Values().Encode()
	if query == "" {
		return "/"
	}
	return "/?" + query
}

func sectionTitle(title string) g.Node {
	return H3(Class("section-title"), g.Text(title))
}

func statBlock(label, value string) g.Node {
	return Div(
		Class("stat-block"),
		Span(Class("stat-label"), g.Text(label)),
		Strong(Class("stat-value"), g.Text(value)),
	)
}

func buttonClass(active bool) string {
	if active {
		return "action-button active"
	}
	return "action-button"
}

func buttonClassDisabled(active, disabled bool) string {
	className := buttonClass(active)
	if disabled {
		className += " disabled"
	}
	return className
}

func optionValue(value, label string, selected bool) g.Node {
	return Option(Value(value), selectedIf(selected), g.Text(label))
}

func selectedIf(selected bool) g.Node {
	if selected {
		return Selected()
	}
	return nil
}

func checkedIf(checked bool) g.Node {
	if checked {
		return Checked()
	}
	return nil
}

func disabledIf(disabled bool) g.Node {
	if disabled {
		return Disabled()
	}
	return nil
}

func formatTimestamp(ns int64) string {
	if ns == 0 {
		return "-"
	}
	return time.Unix(0, ns).UTC().Format(time.RFC3339)
}

func formatNsRange(fromNs, toNs int64) string {
	return formatTimestamp(fromNs) + " - " + formatTimestamp(toNs)
}

func renderNodes[T any](items []T, render func(T) g.Node) g.Node {
	nodes := make([]g.Node, 0, len(items))
	for _, item := range items {
		nodes = append(nodes, render(item))
	}
	return g.Group(nodes)
}

func selectedPreset(state QueryState) string {
	if state.FromNs == 0 || state.ToNs == 0 {
		return presetAllValue
	}

	windowNs := state.ToNs - state.FromNs
	switch {
	case windowNs <= int64(time.Hour):
		return presetHourValue
	case windowNs <= int64(24*time.Hour):
		return presetDayValue
	case windowNs <= int64(7*24*time.Hour):
		return presetWeekValue
	case windowNs <= int64(30*24*time.Hour):
		return presetMonthValue
	default:
		return presetAllValue
	}
}

func addressFamilyLabel(addressFamily AddressFamily) string {
	switch addressFamily {
	case AddressFamilyIPv4:
		return "IPv4"
	case AddressFamilyIPv6:
		return "IPv6"
	default:
		return "All"
	}
}

func directionLabel(direction DirectionFilter) string {
	switch direction {
	case DirectionEgress:
		return "Egress"
	case DirectionIngress:
		return "Ingress"
	default:
		return "Both"
	}
}

func normalizedAddressFamily(addressFamily AddressFamily) AddressFamily {
	if !addressFamily.valid() {
		return AddressFamilyAll
	}
	return addressFamily
}

func normalizedDirection(direction DirectionFilter) DirectionFilter {
	if !direction.valid() {
		return DirectionBoth
	}
	return direction
}

func graphSVGClass(graph GraphData) string {
	if len(graph.Nodes) >= graphDenseNodeCount {
		return "graph-svg is-dense"
	}
	return "graph-svg"
}

func graphNodeClass(node Node) string {
	className := "graph-node"
	if node.Selected {
		className += " is-selected"
	}
	if node.Synthetic {
		className += " is-synthetic"
	}
	className = dnsResultClass(className, node.DNSResultState)
	return className
}

func edgeStroke(edge Edge) string {
	switch edge.DNSResultState {
	case dnsResultStateNXDOMAIN:
		return nxdomainEdgeStroke
	case dnsResultStateMixed:
		return mixedDNSEdgeStroke
	}
	if edge.Selected {
		return selectedEdgeStroke
	}
	return unselectedEdgeStroke
}

func nodeStroke(selected bool) string {
	if selected {
		return selectedNodeStroke
	}
	return unselectedNodeStroke
}

func nodeStrokeWidth(selected bool) string {
	if selected {
		return "3"
	}
	return "1.5"
}

func dashArrayAttr(synthetic bool) string {
	if synthetic {
		return ` stroke-dasharray="6 4"`
	}
	return ""
}

func nodeFill(node Node) string {
	switch node.DNSResultState {
	case dnsResultStateNXDOMAIN:
		return nxdomainNodeFill
	case dnsResultStateMixed:
		return mixedDNSNodeFill
	}
	if node.Synthetic {
		return syntheticNodeFill
	}
	if node.Selected {
		return selectedRegularNodeFill
	}
	if node.AddressClass == nodeAddressClassPrivate {
		return privateEntityNodeFill
	}
	if node.AddressClass == nodeAddressClassMixed {
		return mixedEntityNodeFill
	}
	return unselectedNodeFill
}

func dnsResultClass(baseClass string, state dnsResultState) string {
	switch state {
	case dnsResultStateNXDOMAIN:
		return strings.TrimSpace(baseClass + " dns-result-nxdomain")
	case dnsResultStateMixed:
		return strings.TrimSpace(baseClass + " dns-result-mixed")
	default:
		return baseClass
	}
}

func edgePathMarkup(source, destination LayoutPoint) string {
	controlX := (source.X + destination.X) / 2
	controlY := (source.Y + destination.Y) / 2
	bend := math.Min(72, math.Abs(destination.X-source.X)*0.18+math.Abs(destination.Y-source.Y)*0.12)
	if source.X <= destination.X {
		controlY -= bend
	} else {
		controlY += bend
	}
	return fmt.Sprintf("M %0.2f %0.2f Q %0.2f %0.2f %0.2f %0.2f", source.X, source.Y, controlX, controlY, destination.X, destination.Y)
}

func histogramAxisMarkup(metric Metric, bins []HistogramBin, yAxisMaxValue, yAxisTickStep int64, yAxisTicks []int64) string {
	if len(bins) == 0 {
		return ""
	}

	spanStartNs := bins[0].FromNs
	spanEndNs := bins[len(bins)-1].ToNs
	spanWidthNs := max(int64(1), spanEndNs-spanStartNs)
	plotWidthPx := float64(histogramWidthPx - histogramRightPadPx)
	plotHeightPx := float64(histogramHeightPx - histogramBottomPadPx - histogramTopPadPx)
	baselineY := float64(histogramHeightPx - histogramBottomPadPx)
	labelY := float64(histogramHeightPx - 8)
	yAxisX := plotWidthPx + 10
	yAxisMaxValueFloat := math.Max(float64(yAxisMaxValue), 1)
	yAxisLabelScale := newTimelineYAxisLabelScale(metric, yAxisMaxValue, yAxisTickStep)

	var builder strings.Builder
	fmt.Fprintf(&builder, `<line class="histogram-axis" x1="0" y1="%0.2f" x2="%0.2f" y2="%0.2f"></line>`, baselineY, plotWidthPx, baselineY)
	for tickIndex := range histogramAxisTickCount {
		ratio := 0.0
		if histogramAxisTickCount > 1 {
			ratio = float64(tickIndex) / float64(histogramAxisTickCount-1)
		}
		x := ratio * plotWidthPx
		labelNs := spanStartNs + int64(ratio*float64(spanWidthNs))
		fmt.Fprintf(&builder, `<line class="histogram-axis-tick" x1="%0.2f" y1="%0.2f" x2="%0.2f" y2="%0.2f"></line>`,
			x,
			baselineY,
			x,
			baselineY+6,
		)
		fmt.Fprintf(&builder, `<text class="histogram-axis-label" x="%0.2f" y="%0.2f" text-anchor="%s">%s</text>`,
			x,
			labelY,
			histogramTickAnchor(tickIndex),
			html.EscapeString(formatTimelineTickLabel(labelNs, spanWidthNs)),
		)
	}
	for _, value := range yAxisTicks {
		ratio := 1 - float64(value)/yAxisMaxValueFloat
		y := histogramTopPadPx + ratio*plotHeightPx
		fmt.Fprintf(&builder, `<line class="histogram-grid-line" x1="0" y1="%0.2f" x2="%0.2f" y2="%0.2f"></line>`,
			y,
			plotWidthPx,
			y,
		)
		fmt.Fprintf(&builder, `<line class="histogram-axis-tick" x1="%0.2f" y1="%0.2f" x2="%0.2f" y2="%0.2f"></line>`,
			plotWidthPx,
			y,
			plotWidthPx+6,
			y,
		)
		fmt.Fprintf(&builder, `<text class="histogram-axis-label histogram-axis-label-y" x="%0.2f" y="%0.2f" text-anchor="start" dominant-baseline="middle">%s</text>`,
			yAxisX,
			y,
			html.EscapeString(formatTimelineYAxisMetricValue(value, yAxisLabelScale)),
		)
	}
	return builder.String()
}

func histogramYAxisScale(maxValue int64) (int64, int64, []int64) {
	yAxisMaxValue := roundUpSignificantInt(maxValue, histogramYAxisOneDigit)
	tickStep := histogramYAxisTickStep(yAxisMaxValue, histogramYAxisOneDigit)
	ticks := histogramYAxisTicks(yAxisMaxValue, tickStep)
	if histogramPositiveTickCount(ticks) >= histogramYAxisMinTicks {
		return yAxisMaxValue, tickStep, ticks
	}

	tickStep = histogramYAxisTickStep(yAxisMaxValue, histogramYAxisTwoDigits)
	ticks = histogramYAxisTicks(yAxisMaxValue, tickStep)
	return yAxisMaxValue, tickStep, ticks
}

func histogramYAxisSignificantDigits(maxValue int64) int {
	yAxisMaxValue := roundUpSignificantInt(maxValue, histogramYAxisOneDigit)
	tickStep := histogramYAxisTickStep(yAxisMaxValue, histogramYAxisOneDigit)
	if histogramPositiveTickCount(histogramYAxisTicks(yAxisMaxValue, tickStep)) >= histogramYAxisMinTicks {
		return histogramYAxisOneDigit
	}
	return histogramYAxisTwoDigits
}

func histogramYAxisTickStep(yAxisMaxValue int64, significantDigits int) int64 {
	if yAxisMaxValue <= 1 {
		return 1
	}
	if significantDigits == histogramYAxisOneDigit {
		return int64(math.Pow10(int(math.Floor(math.Log10(float64(yAxisMaxValue))))))
	}

	step := int64(math.Round(float64(yAxisMaxValue) / float64(histogramAxisTickCount-1)))
	return max(int64(1), step)
}

func histogramYAxisTicks(yAxisMaxValue, tickStep int64) []int64 {
	ticks := make([]int64, 0, histogramAxisTickCount)
	for value := yAxisMaxValue; value > 0; value -= tickStep {
		ticks = append(ticks, value)
	}
	ticks = append(ticks, 0)
	return ticks
}

func histogramPositiveTickCount(ticks []int64) int {
	count := 0
	for _, tick := range ticks {
		if tick > 0 {
			count++
		}
	}
	return count
}

func roundUpSignificantInt(value int64, significantDigits int) int64 {
	if value <= 0 {
		return 1
	}

	factor := significantFactor(float64(value), significantDigits)
	if factor < 1 {
		return value
	}
	return int64(math.Ceil(float64(value)/factor) * factor)
}

func significantFactor(value float64, significantDigits int) float64 {
	if significantDigits < 1 {
		significantDigits = 1
	}

	exponent := math.Floor(math.Log10(value))
	return math.Pow(10, exponent-float64(significantDigits)+1)
}

func histogramTickAnchor(tickIndex int) string {
	switch tickIndex {
	case 0:
		return "start"
	case histogramAxisTickCount - 1:
		return "end"
	default:
		return "middle"
	}
}
