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
	graphSizePx                     = 720
	graphHeightPx                   = graphSizePx
	graphDenseNodeCount             = 36
	graphPrimaryRingCount           = 12
	graphWidthPx                    = graphSizePx
	breakdownChartSizePx            = 136
	breakdownDonutThicknessPx       = 24
	fullTimestampFormat             = time.RFC3339
	histogramAxisTickCount          = 5
	histogramBottomPadPx            = 28
	histogramHeightPx               = 132
	histogramMinBarWidthPx          = 4
	histogramRightPadPx             = 64
	histogramTopPadPx               = 8
	histogramWidthPx                = 960
	histogramYAxisMinTicks          = 3
	histogramYAxisOneDigit          = 1
	histogramYAxisTwoDigits         = 2
	hxSelectAppShellValue           = "#app-shell"
	hxSwapOuterHTMLValue            = "outerHTML"
	hxTargetAppShellValue           = "#app-shell"
	entityActionsUnavailableMessage = "Entity actions are available for ranges up to 7 days."
	nxdomainEdgeStroke              = "#8d2f20"
	nxdomainNodeFill                = "#8d2f20"
	mixedDNSEdgeStroke              = "#c4a237"
	mixedDNSNodeFill                = "#c4a237"
	spanWidthNsDataAttr             = "data-span-width-ns"
	sameDayTimestampFormat          = "15:04:05"
	sameWeekTimestampFormat         = "Mon 15:04:05"
	sameYearTimestampFormat         = "02.01 15:04:05"
	timestampNsDataAttr             = "data-timestamp-ns"
	mixedEntityNodeFill             = "#c4a237"
	nodeBaseRadiusPx                = 10
	nodeRadiusScalePx               = 24
	privateEntityNodeFill           = "#4d6f52"
	selectedEdgeStroke              = "#b14d24"
	selectedNodeStroke              = "#943a15"
	selectedRegularNodeFill         = "#b14d24"
	syntheticNodeFill               = "#a69b84"
	unselectedEdgeStroke            = "rgba(55, 68, 87, 0.28)"
	unselectedNodeFill              = "#587ea3"
	unselectedNodeStroke            = "rgba(31, 39, 51, 0.24)"
	yearTimestampFormat             = "02.01.2006 15:04:05"
	actionButtonClass               = "action-button"
	disabledClassSuffix             = " disabled"
)

var ipProtocolNames = map[int32]string{
	1:   "ICMP",
	6:   "TCP",
	17:  "UDP",
	47:  "GRE",
	50:  "ESP",
	51:  "AH",
	58:  "ICMPv6",
	132: "SCTP",
}

var breakdownSliceColors = []string{
	"#b14d24",
	"#587ea3",
	"#4d6f52",
	"#c4a237",
	"#8d2f20",
	"#7d6f9b",
	"#3c8f87",
}

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

func FlowDetailIndex(flows FlowDetailData, devMode bool, devSessionToken string) g.Node {
	bodyNodes := []g.Node{
		FlowDetailShell(flows),
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
				TitleEl(g.Text("Selected Flows")),
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
	breakdown := BreakdownPanel(state, dashboard.Graph)
	sidebarClass := "dashboard-sidebar"
	sidebarNodes := []g.Node(nil)
	if breakdown != nil {
		sidebarClass += " has-breakdown"
		sidebarNodes = append(sidebarNodes, breakdown)
	}
	sidebarNodes = append(sidebarNodes, RankingsPanel(state, dashboard.Graph))

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
		Div(
			Class("dashboard-main"),
			Div(
				Class("dashboard-primary"),
				Section(
					Class("histogram-panel"),
					Div(
						Class("panel-heading"),
						H2(g.Text("Timeline")),
						Span(Class("panel-subtle hover-help timeline-help"), g.Text("Drag to zoom")),
					),
					Div(
						Class("timeline-layout"),
						TotalsPanel(state, dashboard.Graph),
						Div(
							ID("histogram"),
							Class("histogram"),
							Data("span-start-ns", strconv.FormatInt(dashboard.Span.StartNs, 10)),
							Data("span-end-ns", strconv.FormatInt(dashboard.Span.EndNs, 10)),
							renderHistogramSVG(state.Metric, dashboard.Histogram),
						),
					),
				),
				Section(
					Class("section-panel section-block"),
					ID("graph-section"),
					Div(Class("panel-heading section-heading"), H2(g.Text("Graph"))),
					Section(
						Class("graph-panel"),
						ID("graph-panel"),
						Div(
							ID("graph-canvas"),
							Class("graph-canvas"),
							Div(
								Class("legend-line graph-legend"),
								Span(Class("legend-item"), g.Text("Node size = selected metric")),
								Span(Class("legend-item"), g.Text("Edge width = selected metric")),
								Span(Class("legend-item hover-help graph-help"), g.Text("Scroll to zoom, drag to pan")),
								Span(Class("legend-item hover-help graph-help"), g.Text("Labels expand with zoom and hover")),
							),
							ActiveFiltersOverlay(state),
							renderGraphSVG(state, dashboard.Graph),
						),
						selectedPanel(state, dashboard.Graph),
					),
				),
			),
			Div(append([]g.Node{Class(sidebarClass)}, sidebarNodes...)...),
		),
		Section(
			Class("section-panel section-block"),
			ID("table-panel"),
			sectionHeader("Flows Table", "table-content", false),
			Div(
				ID("table-content"),
				Class("section-content is-collapsed"),
				TablePanel(state, dashboard.Table),
			),
		),
	)
}

func FlowDetailShell(flows FlowDetailData) g.Node {
	return Div(
		ID("app-shell"),
		Class("app-shell"),
		Data("span-start-ns", strconv.FormatInt(flows.Span.StartNs, 10)),
		Data("span-end-ns", strconv.FormatInt(flows.Span.EndNs, 10)),
		Section(
			Class("section-panel section-block flow-detail-page"),
			Div(
				Class("panel-heading"),
				Div(
					H2(g.Text(flowDetailTitle(flows.Query))),
					Span(Class("panel-subtle"), g.Text(flowDetailRowCountLabel(flows))),
				),
				navLink(stateURL(flows.Query.State), actionButtonClass, "Back to graph"),
			),
			Div(
				Class("section-content"),
				flowDetailFilters(flows.Query.State),
				flowDetailControls(flows),
				FlowDetailTable(flows),
			),
		),
	)
}

func SummaryPanel(state QueryState, graph GraphData) g.Node {
	return Div(
		Class("summary-panel"),
		sectionTitle("Active Filters"),
		Div(append([]g.Node{Class("filter-list")}, activeFilterChips(state)...)...),
		sectionTitle("Totals"),
		Div(append([]g.Node{Class("stats-grid")}, totalStatBlocks(state, graph)...)...),
		selectedPanel(state, graph),
	)
}

func ActiveFiltersOverlay(state QueryState) g.Node {
	return Div(
		Class("graph-filter-overlay"),
		Div(append([]g.Node{Class("filter-list")}, activeFilterChips(state)...)...),
	)
}

func TotalsPanel(state QueryState, graph GraphData) g.Node {
	return Aside(
		Class("timeline-totals-panel"),
		g.Attr("aria-label", "Totals"),
		Div(append([]g.Node{Class("stats-grid timeline-stats-grid")}, totalStatBlocks(state, graph)...)...),
	)
}

func activeFilterChips(state QueryState) []g.Node {
	nodes := []g.Node{
		Span(Class("chip"), g.Text("Time: "), timestampRangeNode(state.FromNs, state.ToNs)),
		Span(Class("chip"), g.Text(ignoreVisibilityLabel(state.HideIgnored))),
	}

	currentAddressFamily := normalizedAddressFamily(state.AddressFamily)
	if currentAddressFamily != AddressFamilyAll {
		nodes = append(nodes, Span(Class("chip"), g.Text("Address Family: "+addressFamilyLabel(currentAddressFamily))))
	}
	currentDirection := normalizedDirection(state.Direction)
	if currentDirection != DirectionBoth {
		nodes = append(nodes, Span(Class("chip"), g.Text("Direction: "+directionLabel(currentDirection))))
	}
	if state.Protocol > 0 {
		nodes = append(nodes, Span(Class("chip"), g.Text("Protocol: "+rawFlowProtocolLabel(state.Protocol))))
	}
	if state.Port > 0 {
		nodes = append(nodes, Span(Class("chip"), g.Text("Port: "+strconv.FormatInt(int64(state.Port), 10))))
	}
	for _, item := range state.Include {
		nodes = append(nodes, Span(Class("chip"), g.Text("Entity: "+item)))
	}
	for _, item := range state.Exclude {
		nodes = append(nodes, Span(Class("chip"), g.Text("Exclude: "+item)))
	}
	return nodes
}

func totalStatBlocks(state QueryState, graph GraphData) []g.Node {
	nodes := []g.Node(nil)
	if state.Metric != MetricDNSLookups || graph.Totals.Entities != graph.Totals.Edges-1 {
		nodes = append(nodes,
			statBlock("Entities", strconv.Itoa(graph.Totals.Entities)),
			statBlock("Edges", strconv.Itoa(graph.Totals.Edges)),
		)
	}
	if state.Metric != MetricDNSLookups {
		nodes = append(nodes, statBlock("Bytes", formatMetricValue(MetricBytes, graph.Totals.Bytes)))
	}
	nodes = append(nodes, statBlock(connectionsTotalLabel(state.Metric), formatMetricValue(connectionsDisplayMetric(state.Metric), graph.Totals.Connections)))
	if graph.Totals.Ignored > 0 {
		nodes = append(nodes, statBlock(ignoredTotalLabel(state.Metric), formatMetricValue(ignoredDisplayMetric(state.Metric), graph.Totals.Ignored)))
	}
	return nodes
}

func RankingsPanel(state QueryState, graph GraphData) g.Node {
	return Section(
		Class("section-panel section-block rankings-panel"),
		ID("rankings-section"),
		Div(Class("panel-heading section-heading"), H2(g.Text("Rankings"))),
		Div(
			Class("rankings-tabs"),
			g.Attr("role", "tablist"),
			Button(
				Type("button"),
				Class("list-button rankings-tab active"),
				ID("rankings-tab-entities"),
				g.Attr("role", "tab"),
				g.Attr("aria-selected", "true"),
				g.Attr("aria-controls", "rankings-panel-entities"),
				Data("rankings-tab", "entities"),
				g.Text("Top Entities"),
			),
			Button(
				Type("button"),
				Class("list-button rankings-tab"),
				ID("rankings-tab-flows"),
				g.Attr("role", "tab"),
				g.Attr("aria-selected", "false"),
				g.Attr("aria-controls", "rankings-panel-flows"),
				Data("rankings-tab", "flows"),
				g.Text(topEdgesTitle(state.Metric)),
			),
		),
		Div(
			Class("rankings-tab-panel"),
			ID("rankings-panel-entities"),
			g.Attr("role", "tabpanel"),
			g.Attr("aria-labelledby", "rankings-tab-entities"),
			Ul(
				Class("rank-list"),
				renderNodes(graph.TopEntities, func(node Node) g.Node {
					return Li(rankingItem(state, selectEntityStateURL(state, node.ID), node.DNSResultState, node.Ignored, node.Label, formatMetricValue(graph.ActiveMetric, node.Total)))
				}),
			),
		),
		Div(
			Class("rankings-tab-panel"),
			ID("rankings-panel-flows"),
			g.Attr("role", "tabpanel"),
			g.Attr("aria-labelledby", "rankings-tab-flows"),
			g.Attr("hidden", ""),
			Ul(
				Class("rank-list"),
				renderNodes(graph.TopEdges, func(edge Edge) g.Node {
					return Li(rankingItem(state, selectEdgeStateURL(state, edge.Source, edge.Destination), edge.DNSResultState, edge.Ignored, fmt.Sprintf("%s -> %s", edge.Source, edge.Destination), formatMetricValue(graph.ActiveMetric, edge.MetricValue)))
				}),
			),
		),
	)
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

func ignoredTotalLabel(metric Metric) string {
	if metric == MetricDNSLookups {
		return "Ignored DNS Lookups"
	}
	if metric == MetricConnections {
		return "Ignored Connections"
	}
	return "Ignored Bytes"
}

func ignoredDisplayMetric(metric Metric) Metric {
	if metric == MetricDNSLookups {
		return MetricDNSLookups
	}
	if metric == MetricConnections {
		return MetricConnections
	}
	return MetricBytes
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
	return selectedPanelAt(state, graph, time.Now().UTC())
}

func selectedPanelAt(state QueryState, graph GraphData, now time.Time) g.Node {
	if graph.SelectedEdge != nil {
		edge := graph.SelectedEdge
		detailNodes := []g.Node(nil)
		detailClass := strings.TrimSpace("selected-item-name " + dnsResultClass("", edge.DNSResultState))
		metricNodes := []g.Node(nil)
		if graph.SelectedEdge.Ignored {
			metricNodes = append(metricNodes, selectedItemChip("Ignored", "matches rule", "ignored-copy"))
		}
		if graph.ActiveMetric != MetricDNSLookups {
			metricNodes = append(metricNodes, selectedItemChip("Bytes", formatMetricValue(MetricBytes, edge.Bytes), ""))
		}
		metricNodes = append(metricNodes,
			selectedItemChip(connectionsTotalLabel(graph.ActiveMetric), formatMetricValue(connectionsDisplayMetric(graph.ActiveMetric), edge.Connections), ""),
			selectedItemNode("First seen", timestampNode(edge.FirstSeenNs, now), ""),
			selectedItemNode("Last seen", timestampNode(edge.LastSeenNs, now), ""),
		)
		buttons := []g.Node{navLink(deselectStateURL(state), "action-button", "Deselect")}
		if !edge.Synthetic {
			buttons = append([]g.Node{navLink(selectedEdgeIgnoreRuleURL(state, edge), actionButtonClass, "Ignore traffic like this")}, buttons...)
		}
		if flowDetailLinksEnabled(state, edge.Synthetic) {
			buttons = append(buttons, navLink(selectedFlowEdgeURL(state, edge.Source, edge.Destination), actionButtonClass, "Show matching flows"))
		}
		return selectedItemPanel(
			"Selected Edge",
			Span(Class(detailClass), g.Text(fmt.Sprintf("%s -> %s", edge.Source, edge.Destination))),
			metricNodes,
			buttons,
			detailNodes,
		)
	}

	if graph.SelectedNode == nil {
		return nil
	}
	if !state.EntityActionsEnabled() {
		selectedNode := graph.SelectedNode
		return Div(
			Class("selected-item-panel"),
			Div(
				Class("selected-item-row"),
				Div(
					Class("selected-item-main"),
					Span(Class("selected-item-kind"), g.Text("Selected Entity")),
					Span(Class(strings.TrimSpace("selected-item-name "+dnsResultClass("", selectedNode.DNSResultState))), g.Text(selectedNode.Label)),
					Span(Class("chip"), g.Text(entityActionsUnavailableMessage)),
				),
			),
		)
	}

	selectedNode := graph.SelectedNode
	entityURL := filterToEntityStateURL(state, selectedNode.ID)
	excludeURL := excludeEntityStateURL(state, selectedNode.ID)
	drillURL := drillStateURL(state, selectedNode.ID)

	metricNodes := []g.Node(nil)
	if selectedNode.Ignored {
		metricNodes = append(metricNodes, selectedItemChip("Ignored", "matches rule", "ignored-copy"))
	}
	if selectedNode.Ingress != 0 {
		metricNodes = append(metricNodes, selectedItemChip("Ingress", formatMetricValue(graph.ActiveMetric, selectedNode.Ingress), ""))
	}
	if selectedNode.Egress != 0 {
		metricNodes = append(metricNodes, selectedItemChip("Egress", formatMetricValue(graph.ActiveMetric, selectedNode.Egress), ""))
	}
	buttons := []g.Node{
		navLink(entityURL, actionButtonClass, "Filter to this entity"),
		navLink(excludeURL, actionButtonClass, "Exclude"),
		navLink(drillURL, actionButtonClass, "Drill down granularity"),
		navLink(deselectStateURL(state), actionButtonClass, "Deselect"),
		flowDetailEntityLink(state, selectedNode),
	}
	if !selectedNode.Synthetic {
		buttons = append([]g.Node{navLink(selectedNodeIgnoreRuleURL(state, selectedNode), actionButtonClass, "Ignore traffic like this")}, buttons...)
	}
	detailNodes := selectedPeerDetails(state, graph)
	return selectedItemPanel(
		"Selected Entity",
		Span(Class(strings.TrimSpace("selected-item-name "+dnsResultClass("", selectedNode.DNSResultState))), g.Text(selectedNode.Label)),
		metricNodes,
		buttons,
		detailNodes,
	)
}

func selectedItemPanel(kind string, name g.Node, metrics, buttons, detailNodes []g.Node) g.Node {
	if len(detailNodes) > 0 {
		buttons = append(buttons, Button(
			Type("button"),
			Class("action-button"),
			g.Attr("aria-label", "Expand Selected Peers"),
			g.Attr("aria-controls", "selected-peers-content"),
			g.Attr("aria-expanded", "false"),
			g.Attr("data-collapsible-toggle", ""),
			Data("section-title", "Selected Peers"),
			g.Text("Peers"),
		))
	}

	nodes := []g.Node{
		Div(
			Class("selected-item-row"),
			Div(
				Class("selected-item-main"),
				Span(Class("selected-item-kind"), g.Text(kind)),
				name,
				g.Group(metrics),
			),
			Div(append([]g.Node{Class("button-row selected-item-actions")}, buttons...)...),
		),
	}
	if len(detailNodes) > 0 {
		nodes = append(nodes, Div(
			ID("selected-peers-content"),
			Class("selected-peers-content is-collapsed"),
			g.Group(detailNodes),
		))
	}
	return Div(append([]g.Node{Class("selected-item-panel")}, nodes...)...)
}

func selectedItemChip(label, value, className string) g.Node {
	return selectedItemNode(label, g.Text(value), className)
}

func selectedItemNode(label string, value g.Node, className string) g.Node {
	return Span(
		Class(strings.TrimSpace("chip selected-item-metric "+className)),
		g.Text(label+": "),
		value,
	)
}

func selectedPeerDetails(state QueryState, graph GraphData) []g.Node {
	if len(graph.SelectedNodePeers) == 0 {
		return nil
	}
	return []g.Node{
		Ul(
			Class("rank-list"),
			renderNodes(graph.SelectedNodePeers, func(peer DetailPeer) g.Node {
				return Li(rankingItem(state, selectEntityStateURL(state, peer.Entity), dnsResultStateSuccess, false, peer.Entity, formatMetricValue(graph.ActiveMetric, peer.MetricValue)))
			}),
		),
	}
}

func BreakdownPanel(state QueryState, graph GraphData) g.Node {
	panel := breakdownPanel(state, graph.Breakdown)
	if panel == nil {
		return nil
	}
	return Aside(
		Class("section-panel section-block breakdown-sidebar"),
		ID("breakdown-section"),
		Div(Class("panel-heading"), H2(g.Text("Breakdown"))),
		panel,
	)
}

func breakdownPanel(state QueryState, breakdown SelectionBreakdown) g.Node {
	if state.Metric == MetricDNSLookups {
		return nil
	}

	charts := []g.Node(nil)
	if breakdown.Protocols != nil {
		charts = append(charts, breakdownChartNode(state, state.Metric, *breakdown.Protocols))
	}
	if breakdown.Family != nil {
		charts = append(charts, breakdownChartNode(state, state.Metric, *breakdown.Family))
	}
	if breakdown.Ports != nil {
		charts = append(charts, breakdownChartNode(state, state.Metric, *breakdown.Ports))
	}
	if len(charts) == 0 {
		return nil
	}

	return Div(
		Class("breakdown-panel"),
		Div(
			Class("breakdown-grid"),
			g.Group(charts),
		),
	)
}

func breakdownChartNode(state QueryState, metric Metric, chart BreakdownChart) g.Node {
	return Section(
		Class("breakdown-chart"),
		H3(Class("breakdown-chart-title"), g.Text(chart.Label)),
		Div(
			Class("breakdown-chart-body"),
			g.Raw(breakdownChartSVGMarkup(state, chart.Slices)),
			Ul(
				Class("breakdown-list"),
				renderNodes(chart.Slices, func(slice BreakdownSlice) g.Node {
					return Li(
						Class("breakdown-list-item"),
						breakdownSliceLabelNode(state, slice, breakdownSliceColor(sliceIndex(chart.Slices, slice.Label))),
						Span(Class("breakdown-list-value"), g.Text(formatMetricValue(metric, slice.Value))),
					)
				}),
			),
		),
	)
}

func breakdownSliceLabelNode(state QueryState, slice BreakdownSlice, color string) g.Node {
	nodes := []g.Node{
		Span(Class("breakdown-list-swatch"), Style("background:"+color)),
		Span(g.Text(slice.Label)),
	}
	if slice.FilterParam == "" || slice.FilterValue == "" || slice.Label == breakdownRestLabel {
		return Span(append([]g.Node{Class("breakdown-list-label")}, nodes...)...)
	}
	href := breakdownFilterURL(state, slice)
	return A(
		append([]g.Node{
			Href(href),
			Class("breakdown-list-label breakdown-link"),
			g.Attr("hx-get", href),
			g.Attr("hx-target", hxTargetAppShellValue),
			g.Attr("hx-select", hxSelectAppShellValue),
			g.Attr("hx-swap", hxSwapOuterHTMLValue),
			g.Attr("hx-push-url", "true"),
		}, nodes...)...,
	)
}

func breakdownFilterURL(state QueryState, slice BreakdownSlice) string {
	nextState := state.Clone()
	nextState.Page = defaultPage
	nextState = nextState.ResetSelection()
	switch slice.FilterParam {
	case "protocol":
		nextState.Protocol = parsePositiveInt32(slice.FilterValue)
	case "family":
		if family := AddressFamily(slice.FilterValue); family.valid() {
			nextState.AddressFamily = family
		}
	case "port":
		nextState.Port = parsePositiveInt32(slice.FilterValue)
	}
	return stateURL(nextState)
}

func breakdownChartSVGMarkup(state QueryState, slices []BreakdownSlice) string {
	var builder strings.Builder

	center := float64(breakdownChartSizePx) / 2
	radius := center - 4
	innerRadius := radius - breakdownDonutThicknessPx
	var total int64
	for _, slice := range slices {
		total += slice.Value
	}

	fmt.Fprintf(&builder, `<svg class="breakdown-svg" viewBox="0 0 %d %d" role="img" aria-label="Traffic breakdown">`, breakdownChartSizePx, breakdownChartSizePx)
	if total <= 0 {
		builder.WriteString(`</svg>`)
		return builder.String()
	}

	startAngle := -math.Pi / 2
	for index, slice := range slices {
		if slice.Value <= 0 {
			continue
		}
		angle := (float64(slice.Value) / float64(total)) * 2 * math.Pi
		endAngle := startAngle + angle
		path := fmt.Sprintf(`<path d="%s" fill="%s"><title>%s: %s</title></path>`,
			donutSegmentPath(center, center, radius, innerRadius, startAngle, endAngle),
			breakdownSliceColor(index),
			html.EscapeString(slice.Label),
			html.EscapeString(strconv.FormatInt(slice.Value, 10)),
		)
		if slice.FilterParam != "" && slice.FilterValue != "" && slice.Label != breakdownRestLabel {
			fmt.Fprintf(&builder, `<a %s>%s</a>`, navAttrString(breakdownFilterURL(state, slice)), path)
		} else {
			builder.WriteString(path)
		}
		startAngle = endAngle
	}

	builder.WriteString(`</svg>`)
	return builder.String()
}

func donutSegmentPath(centerX, centerY, outerRadius, innerRadius, startAngle, endAngle float64) string {
	startOuterX := centerX + outerRadius*math.Cos(startAngle)
	startOuterY := centerY + outerRadius*math.Sin(startAngle)
	endOuterX := centerX + outerRadius*math.Cos(endAngle)
	endOuterY := centerY + outerRadius*math.Sin(endAngle)
	startInnerX := centerX + innerRadius*math.Cos(startAngle)
	startInnerY := centerY + innerRadius*math.Sin(startAngle)
	endInnerX := centerX + innerRadius*math.Cos(endAngle)
	endInnerY := centerY + innerRadius*math.Sin(endAngle)
	largeArcFlag := 0
	if endAngle-startAngle > math.Pi {
		largeArcFlag = 1
	}

	return fmt.Sprintf(
		"M %0.2f %0.2f A %0.2f %0.2f 0 %d 1 %0.2f %0.2f L %0.2f %0.2f A %0.2f %0.2f 0 %d 0 %0.2f %0.2f Z",
		startOuterX, startOuterY,
		outerRadius, outerRadius, largeArcFlag, endOuterX, endOuterY,
		endInnerX, endInnerY,
		innerRadius, innerRadius, largeArcFlag, startInnerX, startInnerY,
	)
}

func breakdownSliceColor(index int) string {
	return breakdownSliceColors[index%len(breakdownSliceColors)]
}

func sliceIndex(slices []BreakdownSlice, label string) int {
	for index, slice := range slices {
		if slice.Label == label {
			return index
		}
	}
	return 0
}

func TablePanel(state QueryState, table TableData) g.Node {
	return tablePanelAt(state, table, time.Now().UTC())
}

func tablePanelAt(state QueryState, table TableData, now time.Time) g.Node {
	headerNodes := []g.Node{
		Th(sortLink(state, "Source", SortSource)),
		Th(sortLink(state, "Destination", SortDestination)),
	}
	if state.Metric != MetricDNSLookups {
		headerNodes = append(headerNodes, Th(sortLink(state, "Bytes", SortBytes)))
	}
	headerNodes = append(headerNodes,
		Th(sortLink(state, connectionsTotalLabel(state.Metric), connectionsSortKey(state.Metric))),
		Th(sortLink(state, "First Seen", SortFirstSeen)),
		Th(sortLink(state, "Last Seen", SortLastSeen)),
	)
	if flowDetailTableLinksEnabled(state) {
		headerNodes = append(headerNodes, Th(Class("flow-detail-column"), g.Text("")))
	}

	return Div(
		Div(Class("table-meta"), Span(Class("panel-subtle"), g.Text(fmt.Sprintf("%d rows", table.TotalCount)))),
		Table(
			Class("flows-table"),
			THead(
				Tr(headerNodes...),
			),
			TBody(
				renderNodes(table.VisibleRows, func(row TableRow) g.Node {
					rowClass := dnsResultClass("", row.DNSResultState)
					if row.Synthetic {
						rowClass = strings.TrimSpace(rowClass + " synthetic-row")
					}
					if row.Ignored {
						rowClass = strings.TrimSpace(rowClass + " ignored-row")
					}
					cells := []g.Node{
						Td(tableEntityNode(state, row.Source)),
						Td(destinationCellNode(state, row)),
					}
					if state.Metric != MetricDNSLookups {
						cells = append(cells, Td(g.Text(formatMetricValue(MetricBytes, row.Bytes))))
					}
					cells = append(cells,
						Td(g.Text(formatMetricValue(connectionsDisplayMetric(state.Metric), row.Connections))),
						Td(timestampNode(row.FirstSeenNs, now)),
						Td(timestampNode(row.LastSeenNs, now)),
					)
					if flowDetailTableLinksEnabled(state) {
						cells = append(cells, Td(Class("flow-detail-column"), flowDetailRowLink(state, row)))
					}
					return Tr(append([]g.Node{Class(rowClass)}, cells...)...)
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

func FlowDetailTable(flows FlowDetailData) g.Node {
	return flowDetailTableAt(flows, time.Now().UTC())
}

func flowDetailTableAt(flows FlowDetailData, now time.Time) g.Node {
	state := flows.Query.State
	sortAllColumns := state.EntityActionsEnabled()
	return Div(
		flowDetailSortNotice(sortAllColumns),
		Table(
			Class("flows-table raw-flows-table"),
			THead(
				Tr(
					Th(flowDetailSortLink(flows.Query, "Start", FlowSortStart, true)),
					Th(flowDetailSortLink(flows.Query, "End", FlowSortEnd, true)),
					Th(flowDetailSortLink(flows.Query, "Source", FlowSortSource, sortAllColumns)),
					Th(flowDetailSortLink(flows.Query, "Destination", FlowSortDestination, sortAllColumns)),
					Th(flowDetailSortLink(flows.Query, "Protocol", FlowSortProtocol, sortAllColumns)),
					Th(flowDetailSortLink(flows.Query, "Packets", FlowSortPackets, sortAllColumns)),
					Th(flowDetailSortLink(flows.Query, "Bytes", FlowSortBytes, sortAllColumns)),
					Th(Span(Class("list-button raw-flow-header-button disabled"), g.Text("Direction"))),
					Th(Class("flow-detail-column"), g.Text("")),
				),
			),
			TBody(
				renderNodes(flows.VisibleRows, func(row FlowDetailRow) g.Node {
					rowClass := ""
					if row.Ignored {
						rowClass = "ignored-row"
					}
					return Tr(
						Class(rowClass),
						Td(timestampNode(row.StartNs, now)),
						Td(timestampNode(row.EndNs, now)),
						Td(flowEndpointNode(row.Source, row.SrcIP, row.SrcPort)),
						Td(flowDetailDestinationNode(row)),
						Td(g.Text(rawFlowProtocolLabel(row.Protocol))),
						Td(g.Text(strconv.FormatInt(row.Packets, 10))),
						Td(g.Text(formatMetricValue(MetricBytes, row.Bytes))),
						Td(g.Text(rawFlowDirectionLabel(row.Direction))),
						Td(Class("flow-detail-column"), navLink(flowDetailIgnoreRuleURL(flows.Query, row), "flow-detail-link", "Ignore")),
					)
				}),
			),
		),
		Div(
			Class("pagination-row"),
			flowDetailPaginationLink(flows, "Previous", max(1, flows.Page-1), flows.Page <= 1),
			Span(Class("panel-subtle"), g.Text(flowDetailPageLabel(flows))),
			flowDetailPaginationLink(flows, "Next", min(flows.TotalPages, flows.Page+1), flows.Page >= flows.TotalPages),
		),
	)
}

func topBar(dashboard DashboardData) g.Node {
	state := dashboard.State
	currentAddressFamily := normalizedAddressFamily(state.AddressFamily)
	currentDirection := normalizedDirection(state.Direction)
	directionDisabled := state.Metric == MetricDNSLookups
	longRange := !state.EntityActionsEnabled()

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
					Label(g.Text("Local")),
					toggleRadio("local_identity", string(LocalIdentityAddress), "Address", state.LocalIdentity == LocalIdentityAddress),
					toggleRadio("local_identity", string(LocalIdentityDevice), "Device", state.LocalIdentity == LocalIdentityDevice),
				),
				Div(
					Class("group segmented"),
					Label(g.Text("By")),
					toggleRadio("granularity", string(GranularityTLD), "TLD", state.Granularity == GranularityTLD),
					toggleRadio("granularity", string(Granularity2LD), "2TLD", state.Granularity == Granularity2LD),
					toggleRadioDisabled("granularity", string(GranularityHostname), "Hostname", state.Granularity == GranularityHostname, longRange),
					toggleRadioDisabled("granularity", string(GranularityIP), "IP", state.Granularity == GranularityIP, longRange),
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
					Class("group segmented"),
					Label(g.Text("Ignored")),
					toggleRadio("hide_ignored", "true", "Hide", state.HideIgnored),
					toggleRadio("hide_ignored", "false", "Show", !state.HideIgnored),
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
						disabledIf(longRange),
					),
				),
				Div(
					Class("group"),
					Label(For("node-limit"), g.Text("Nodes")),
					Select(
						ID("node-limit"),
						Name("node_limit"),
						optionValue("10", "Top 10", state.NodeLimit == 10 || state.NodeLimit == 0),
						optionValue("50", "Top 50", state.NodeLimit == 50),
						optionValue("200", "Top 200", state.NodeLimit == 200),
						optionValue("500", "Top 500", state.NodeLimit == 500),
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
				navLink(ignoreRulesURL(stateURL(state), "Back to graph"), actionButtonClass, fmt.Sprintf("Ignored Rules (%d)", dashboard.IgnoreRuleCount)),
				navLink("/", "action-button danger", "Reset"),
			),
		),
	)
}

func hiddenStateFields(state QueryState) g.Node {
	nodes := []g.Node{
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
	}
	if state.LocalIdentity != "" && state.LocalIdentity != LocalIdentityAddress {
		nodes = append(nodes, Input(Type("hidden"), Name("local_identity"), Value(string(state.LocalIdentity))))
	}
	if state.Protocol > 0 {
		nodes = append(nodes, Input(Type("hidden"), Name("protocol"), Value(strconv.FormatInt(int64(state.Protocol), 10))))
	}
	if state.Port > 0 {
		nodes = append(nodes, Input(Type("hidden"), Name("port"), Value(strconv.FormatInt(int64(state.Port), 10))))
	}
	return g.Group(nodes)
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

	fmt.Fprintf(&builder, `<svg class="%s" viewBox="0 0 %d %d" role="img" aria-label="Traffic graph">`, graphSVGClass(state, graph), graphWidthPx, graphHeightPx)
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

		if state.EntityActionsEnabled() {
			builder.WriteString(`<a ` + navAttrString(selectEdgeStateURL(state, edge.Source, edge.Destination)) + `>`)
		}
		fmt.Fprintf(&builder, `<path d="%s" stroke="%s" stroke-width="%0.2f" fill="none"%s>`,
			edgePathMarkup(source, destination),
			edgeStroke(edge),
			math.Max(1.5, 1+math.Log10(math.Max(float64(edge.MetricValue), 1))),
			dashArrayAttr(edge.Synthetic))
		builder.WriteString(titleMarkup(edgeTitle(graph.ActiveMetric, edge)))
		builder.WriteString(`</path>`)
		fmt.Fprintf(&builder, `<path class="graph-edge-hitbox" d="%s" stroke="transparent" stroke-width="16" fill="none"></path>`,
			edgePathMarkup(source, destination))
		if state.EntityActionsEnabled() {
			builder.WriteString(`</a>`)
		}
	}
	builder.WriteString(`</g><g class="graph-nodes">`)

	maxTotal := maxNodeTotal(graph.Nodes)
	for _, node := range graph.Nodes {
		position := positions[node.ID]
		radius := nodeRadius(node.Total, maxTotal)
		labelPersistent := node.Selected || node.Synthetic
		if state.EntityActionsEnabled() {
			builder.WriteString(`<a ` + navAttrString(selectEntityStateURL(state, node.ID)) + `>`)
		}
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
		nodeTitle := fmt.Sprintf("%s\nIngress: %s\nEgress: %s",
			node.Label,
			formatMetricValue(state.Metric, node.Ingress),
			formatMetricValue(state.Metric, node.Egress))
		if node.Ignored {
			nodeTitle += "\nIgnored by rule"
		}
		builder.WriteString(titleMarkup(nodeTitle))
		builder.WriteString(`</circle>`)
		builder.WriteString(labelTextMarkup(0, radius+18, node.Label, "middle"))
		builder.WriteString(`</g>`)
		if state.EntityActionsEnabled() {
			builder.WriteString(`</a>`)
		}
	}

	builder.WriteString(`</g></g></svg>`)
	return builder.String()
}

func edgeTitle(metric Metric, edge Edge) string {
	lines := []string{
		fmt.Sprintf("%s -> %s", edge.Source, edge.Destination),
	}
	if edge.Ignored {
		lines = append(lines, "Ignored by rule")
	}
	if metric != MetricDNSLookups {
		lines = append(lines, "Bytes: "+formatMetricValue(MetricBytes, edge.Bytes))
	}
	lines = append(lines, connectionsTotalLabel(metric)+": "+formatMetricValue(connectionsDisplayMetric(metric), edge.Connections))
	return strings.Join(lines, "\n")
}

func renderHistogramSVG(metric Metric, bins []HistogramBin) g.Node {
	return g.Raw(histogramSVGMarkup(metric, bins))
}

func histogramSVGMarkup(metric Metric, bins []HistogramBin) string {
	return histogramSVGMarkupAt(metric, bins, time.Now().UTC())
}

func histogramSVGMarkupAt(metric Metric, bins []HistogramBin, now time.Time) string {
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
		fromLabel := formatTimestampAt(bin.FromNs, now)
		toLabel := formatTimestampAt(bin.ToNs, now)
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

func tableEntityNode(state QueryState, entity string) g.Node {
	if !state.EntityActionsEnabled() {
		return Span(Class("table-link disabled"), g.Text(entity))
	}
	return navLink(selectEntityStateURL(state, entity), "table-link", entity)
}

func rankingLink(href string, dnsState dnsResultState, ignored bool, label, value string) g.Node {
	return A(
		Href(href),
		Class(rankingItemClass("table-link ranking-link", dnsState, ignored)),
		g.Attr("hx-get", href),
		g.Attr("hx-target", hxTargetAppShellValue),
		g.Attr("hx-select", hxSelectAppShellValue),
		g.Attr("hx-swap", hxSwapOuterHTMLValue),
		g.Attr("hx-push-url", "true"),
		Span(Class("ranking-label"), g.Text(label)),
		ignoredBadgeNode(ignored),
		Span(Class("ranking-value"), g.Text(value)),
	)
}

func rankingItem(state QueryState, href string, dnsState dnsResultState, ignored bool, label, value string) g.Node {
	if state.EntityActionsEnabled() {
		return rankingLink(href, dnsState, ignored, label, value)
	}
	return Div(
		Class(rankingItemClass("table-link ranking-link disabled", dnsState, ignored)),
		Span(Class("ranking-label"), g.Text(label)),
		ignoredBadgeNode(ignored),
		Span(Class("ranking-value"), g.Text(value)),
	)
}

func rankingItemClass(baseClass string, dnsState dnsResultState, ignored bool) string {
	className := dnsResultClass(baseClass, dnsState)
	if ignored {
		className = strings.TrimSpace(className + " is-ignored")
	}
	return className
}

func ignoredBadgeNode(ignored bool) g.Node {
	if !ignored {
		return nil
	}
	return Span(Class("inline-badge"), g.Text("Ignored"))
}

func selectedFlowEntityURL(state QueryState, entity string) string {
	nextState := state.Clone()
	nextState.Page = defaultPage
	query := FlowQuery{
		Entity: entity,
		Scope:  FlowScopeEntity,
		State:  nextState,
	}
	return flowDetailURL(query)
}

func selectedFlowEdgeURL(state QueryState, source, destination string) string {
	nextState := state.Clone()
	nextState.Page = defaultPage
	query := FlowQuery{
		Destination: destination,
		Scope:       FlowScopeEdge,
		Source:      source,
		State:       nextState,
	}
	return flowDetailURL(query)
}

func flowDetailURL(query FlowQuery) string {
	values := query.Values().Encode()
	if values == "" {
		return "/flows"
	}
	return "/flows?" + values
}

func flowDetailLinksEnabled(state QueryState, synthetic bool) bool {
	return state.Metric != MetricDNSLookups && !synthetic && state.EntityActionsEnabled()
}

func flowDetailTableLinksEnabled(state QueryState) bool {
	return state.Metric != MetricDNSLookups && state.EntityActionsEnabled()
}

func flowDetailEntityLink(state QueryState, node *Node) g.Node {
	if node == nil || !flowDetailLinksEnabled(state, node.Synthetic) {
		return nil
	}
	return navLink(selectedFlowEntityURL(state, node.ID), actionButtonClass, "Show matching flows")
}

func flowDetailRowLink(state QueryState, row TableRow) g.Node {
	if row.Synthetic {
		return Span(Class("flow-detail-empty"), g.Text(""))
	}
	href := selectedFlowEdgeURL(state, row.Source, row.Destination)
	return A(
		Href(href),
		Class("flow-detail-link"),
		g.Attr("aria-label", fmt.Sprintf("Show flows from %s to %s", row.Source, row.Destination)),
		g.Attr("hx-get", href),
		g.Attr("hx-target", hxTargetAppShellValue),
		g.Attr("hx-select", hxSelectAppShellValue),
		g.Attr("hx-swap", hxSwapOuterHTMLValue),
		g.Attr("hx-push-url", "true"),
		g.Text(">"),
	)
}

func flowDetailPaginationLink(flows FlowDetailData, label string, page int, disabled bool) g.Node {
	className := actionButtonClass
	if disabled {
		className += disabledClassSuffix
		return Span(Class(className), g.Text(label))
	}

	nextQuery := flows.Query
	nextQuery.State.Page = page
	return navLink(flowDetailURL(nextQuery), className, label)
}

func flowDetailSortLink(query FlowQuery, label string, sortKey FlowSort, enabled bool) g.Node {
	if !enabled {
		return Span(Class("list-button raw-flow-header-button disabled"), g.Text(label))
	}
	nextQuery := query
	nextQuery.Sort = sortKey
	nextQuery.SortDir = FlowSortDesc
	if query.Sort == sortKey && sortKey.timeSort() {
		if query.SortDir == FlowSortAsc {
			nextQuery.SortDir = FlowSortDesc
		} else {
			nextQuery.SortDir = FlowSortAsc
		}
	}
	nextQuery.State.Page = defaultPage
	className := "list-button raw-flow-header-button"
	linkLabel := label
	if query.Sort == sortKey {
		className += " active"
		if sortKey.timeSort() {
			linkLabel += " " + flowSortDirectionArrow(query.SortDir)
		}
	}
	return navLink(flowDetailURL(nextQuery), className, linkLabel)
}

func flowDetailSortNotice(sortAllColumns bool) g.Node {
	if sortAllColumns {
		return nil
	}
	return P(Class("panel-subtle flow-detail-sort-note"), g.Text("Long ranges sort by time only. Choose a range up to 7 days to sort by other fields."))
}

func sortLink(state QueryState, label string, sortKey TableSort) g.Node {
	nextState := state.Clone()
	nextState.Sort = sortKey
	nextState.Page = defaultPage
	return navLink(stateURL(nextState), "list-button", label)
}

func paginationLink(state QueryState, label string, page int, disabled bool) g.Node {
	className := actionButtonClass
	if disabled {
		className += disabledClassSuffix
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

func deselectStateURL(state QueryState) string {
	nextState := state.ResetSelection()
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

func flowDetailTitle(query FlowQuery) string {
	switch query.Scope {
	case FlowScopeEntity:
		return "Flows involving " + query.Entity
	case FlowScopeEdge:
		return fmt.Sprintf("Flows from %s to %s", query.Source, query.Destination)
	default:
		return "Selected flows"
	}
}

func flowDetailRowCountLabel(flows FlowDetailData) string {
	return fmt.Sprintf("%d rows", flows.TotalCount)
}

func flowDetailPageLabel(flows FlowDetailData) string {
	return fmt.Sprintf("Page %d / %d", flows.Page, flows.TotalPages)
}

func flowDetailFilters(state QueryState) g.Node {
	currentAddressFamily := normalizedAddressFamily(state.AddressFamily)
	currentDirection := normalizedDirection(state.Direction)
	chips := []g.Node{
		Span(Class("chip"), g.Text("Time: "), timestampRangeNode(state.FromNs, state.ToNs)),
		Span(Class("chip"), g.Text("Granularity: "+strings.ToUpper(string(state.Granularity)))),
		Span(Class("chip"), g.Text(ignoreVisibilityLabel(state.HideIgnored))),
	}
	if currentAddressFamily != AddressFamilyAll {
		chips = append(chips, Span(Class("chip"), g.Text("Address Family: "+addressFamilyLabel(currentAddressFamily))))
	}
	if currentDirection != DirectionBoth {
		chips = append(chips, Span(Class("chip"), g.Text("Direction: "+directionLabel(currentDirection))))
	}
	if state.Protocol > 0 {
		chips = append(chips, Span(Class("chip"), g.Text("Protocol: "+rawFlowProtocolLabel(state.Protocol))))
	}
	if state.Port > 0 {
		chips = append(chips, Span(Class("chip"), g.Text("Port: "+strconv.FormatInt(int64(state.Port), 10))))
	}
	for _, item := range state.Include {
		chips = append(chips, Span(Class("chip"), g.Text("Entity: "+item)))
	}
	for _, item := range state.Exclude {
		chips = append(chips, Span(Class("chip"), g.Text("Exclude: "+item)))
	}
	if state.Search != "" {
		chips = append(chips, Span(Class("chip"), g.Text("Search: "+state.Search)))
	}
	return Div(append([]g.Node{Class("filter-list flow-detail-filters")}, chips...)...)
}

func flowDetailControls(flows FlowDetailData) g.Node {
	query := flows.Query
	state := query.State
	nodes := []g.Node{
		flowDetailHiddenFields(query),
		Div(
			Class("group"),
			Div(
				Class("button-row"),
				renderNodes(flows.PresetCounts, func(count FlowPresetCount) g.Node {
					label := fmt.Sprintf("%s (%d)", count.Label, count.Count)
					return toggleRadio("preset", count.Preset, label, selectedPreset(state) == count.Preset)
				}),
			),
		),
		Div(
			Class("group segmented"),
			Label(g.Text("Ignored")),
			toggleRadio("hide_ignored", "true", "Hide", state.HideIgnored),
			toggleRadio("hide_ignored", "false", "Show", !state.HideIgnored),
		),
	}
	if query.Scope == FlowScopeEdge {
		nodes = append(nodes,
			Div(
				Class("group segmented"),
				Label(g.Text("Direction")),
				toggleRadio("flow_match", string(FlowMatchBoth), "Both directions", query.Match != FlowMatchForward),
				toggleRadio("flow_match", string(FlowMatchForward), "Forward only", query.Match == FlowMatchForward),
			),
		)
	}
	return Form(
		Method("get"),
		Action("/flows"),
		ID("filters-form"),
		Class("flow-detail-controls"),
		g.Attr("hx-get", "/flows"),
		g.Attr("hx-target", hxTargetAppShellValue),
		g.Attr("hx-select", hxSelectAppShellValue),
		g.Attr("hx-swap", hxSwapOuterHTMLValue),
		g.Attr("hx-push-url", "true"),
		g.Attr("hx-indicator", "#loading-indicator"),
		g.Attr("hx-sync", "this:replace"),
		Div(append([]g.Node{Class("top-bar-row")}, nodes...)...),
	)
}

func flowDetailHiddenFields(query FlowQuery) g.Node {
	state := query.State
	nodes := []g.Node{
		Input(Type("hidden"), ID("filter-from-ns"), Name("from"), Value(strconv.FormatInt(state.FromNs, 10))),
		Input(Type("hidden"), ID("filter-to-ns"), Name("to"), Value(strconv.FormatInt(state.ToNs, 10))),
		Input(Type("hidden"), Name("metric"), Value(string(state.Metric))),
		Input(Type("hidden"), Name("granularity"), Value(string(state.Granularity))),
		Input(Type("hidden"), Name("local_identity"), Value(string(state.LocalIdentity))),
		Input(Type("hidden"), Name("sort"), Value(string(state.Sort))),
		Input(Type("hidden"), Name("page"), Value(strconv.Itoa(defaultPage))),
		Input(Type("hidden"), Name("page_size"), Value(strconv.Itoa(state.PageSize))),
		Input(Type("hidden"), Name("flow_scope"), Value(string(query.Scope))),
		Input(Type("hidden"), Name("flow_sort"), Value(string(query.Sort))),
	}
	if query.Sort.timeSort() && query.SortDir == FlowSortAsc {
		nodes = append(nodes, Input(Type("hidden"), Name(flowSortDirParam), Value(string(query.SortDir))))
	}
	if state.AddressFamily != "" && state.AddressFamily != AddressFamilyAll {
		nodes = append(nodes, Input(Type("hidden"), Name("family"), Value(string(state.AddressFamily))))
	}
	if state.Direction != "" && state.Direction != DirectionBoth {
		nodes = append(nodes, Input(Type("hidden"), Name("direction"), Value(string(state.Direction))))
	}
	if state.Protocol > 0 {
		nodes = append(nodes, Input(Type("hidden"), Name("protocol"), Value(strconv.FormatInt(int64(state.Protocol), 10))))
	}
	if state.Port > 0 {
		nodes = append(nodes, Input(Type("hidden"), Name("port"), Value(strconv.FormatInt(int64(state.Port), 10))))
	}
	if state.Search != "" {
		nodes = append(nodes, Input(Type("hidden"), Name("search"), Value(state.Search)))
	}
	switch query.Scope {
	case FlowScopeEntity:
		nodes = append(nodes, Input(Type("hidden"), Name("flow_entity"), Value(query.Entity)))
	case FlowScopeEdge:
		nodes = append(nodes,
			Input(Type("hidden"), Name("flow_source"), Value(query.Source)),
			Input(Type("hidden"), Name("flow_destination"), Value(query.Destination)),
		)
	}
	nodes = append(nodes, renderHiddenValues("include", state.Include), renderHiddenValues("exclude", state.Exclude))
	return g.Group(nodes)
}

func flowEndpointNode(entity, ip string, port int32) g.Node {
	return Div(
		Class("flow-endpoint"),
		Span(g.Text(entity)),
		Span(Class("panel-subtle"), g.Text(fmt.Sprintf("%s:%d", ip, port))),
	)
}

func destinationCellNode(state QueryState, row TableRow) g.Node {
	nodes := []g.Node{tableEntityNode(state, row.Destination)}
	if row.Ignored {
		nodes = append(nodes, Span(Class("inline-badge"), g.Text("Ignored")))
	}
	return Div(append([]g.Node{Class("table-destination-cell")}, nodes...)...)
}

func flowDetailDestinationNode(row FlowDetailRow) g.Node {
	nodes := []g.Node{flowEndpointNode(row.Destination, row.DstIP, row.DstPort)}
	if row.Ignored {
		nodes = append(nodes, Span(Class("inline-badge"), g.Text("Ignored")))
	}
	return Div(append([]g.Node{Class("table-destination-cell")}, nodes...)...)
}

func rawFlowDirectionLabel(direction *int32) string {
	if direction == nil {
		return "-"
	}
	switch *direction {
	case directionEgressParquetValue:
		return "Egress"
	case directionIngressParquetValue:
		return "Ingress"
	default:
		return strconv.FormatInt(int64(*direction), 10)
	}
}

func rawFlowProtocolLabel(protocol int32) string {
	name, ok := ipProtocolNames[protocol]
	if !ok {
		return strconv.FormatInt(int64(protocol), 10)
	}
	return fmt.Sprintf("%d (%s)", protocol, name)
}

func flowSortDirectionArrow(sortDir FlowSortDir) string {
	if sortDir == FlowSortAsc {
		return "↑"
	}
	return "↓"
}

func ignoreVisibilityLabel(hideIgnored bool) string {
	if hideIgnored {
		return "Ignored: hidden"
	}
	return "Ignored: visible"
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
	return actionButtonClass
}

func buttonClassDisabled(active, disabled bool) string {
	className := buttonClass(active)
	if disabled {
		className += disabledClassSuffix
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

func formatTimestampAt(ns int64, now time.Time) string {
	if ns == 0 {
		return "-"
	}

	timestamp := time.Unix(0, ns).UTC()
	now = now.UTC()
	if sameUTCDate(timestamp, now) {
		return timestamp.Format(sameDayTimestampFormat)
	}

	timestampYear, timestampWeek := timestamp.ISOWeek()
	nowYear, nowWeek := now.ISOWeek()
	if timestampYear == nowYear && timestampWeek == nowWeek {
		return timestamp.Format(sameWeekTimestampFormat)
	}

	if timestamp.Year() == now.Year() {
		return timestamp.Format(sameYearTimestampFormat)
	}

	return timestamp.Format(yearTimestampFormat)
}

func fullTimestamp(ns int64) string {
	if ns == 0 {
		return "-"
	}
	return time.Unix(0, ns).UTC().Format(fullTimestampFormat)
}

func timestampNode(ns int64, now time.Time) g.Node {
	if ns == 0 {
		return g.Text("-")
	}

	fullLabel := fullTimestamp(ns)
	return Time(
		g.Attr("datetime", fullLabel),
		g.Attr("title", fullLabel),
		g.Attr(timestampNsDataAttr, strconv.FormatInt(ns, 10)),
		g.Text(formatTimestampAt(ns, now)),
	)
}

func sameUTCDate(left, right time.Time) bool {
	left = left.UTC()
	right = right.UTC()
	return left.Year() == right.Year() && left.YearDay() == right.YearDay()
}

func timestampRangeNode(fromNs, toNs int64) g.Node {
	now := time.Now().UTC()
	return g.Group([]g.Node{
		timestampNode(fromNs, now),
		g.Text(" - "),
		timestampNode(toNs, now),
	})
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

func graphSVGClass(state QueryState, graph GraphData) string {
	className := "graph-svg"
	if len(graph.Nodes) >= graphDenseNodeCount {
		className += " is-dense"
	}
	if !state.EntityActionsEnabled() {
		className += " is-entity-actions-disabled"
	}
	return className
}

func graphNodeClass(node Node) string {
	className := "graph-node"
	if node.Selected {
		className += " is-selected"
	}
	if node.Ignored {
		className += " is-ignored"
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
	if edge.Ignored {
		return syntheticNodeFill
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
	if node.Ignored {
		return syntheticNodeFill
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
		fmt.Fprintf(&builder, `<text class="histogram-axis-label" x="%0.2f" y="%0.2f" text-anchor="%s" %s="%d" %s="%d">%s</text>`,
			x,
			labelY,
			histogramTickAnchor(tickIndex),
			timestampNsDataAttr,
			labelNs,
			spanWidthNsDataAttr,
			spanWidthNs,
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
