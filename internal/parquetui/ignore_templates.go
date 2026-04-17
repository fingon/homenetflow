package parquetui

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	g "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html" //nolint:revive,staticcheck
)

func IgnoreRulesIndex(page IgnoreRulePageData, devMode bool, devSessionToken string) g.Node {
	bodyNodes := []g.Node{
		IgnoreRulesShell(page),
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
				TitleEl(g.Text("Ignored Traffic Rules")),
				Link(Rel("stylesheet"), Href("/static/style.css")),
				Script(Src("/static/htmx.min.js"), Defer()),
				Script(Src("/static/app.js"), Defer()),
			),
			Body(bodyNodes...),
		),
	)
}

func IgnoreRulesShell(page IgnoreRulePageData) g.Node {
	return Div(
		ID("app-shell"),
		Class("app-shell"),
		Div(
			ID("loading-indicator"),
			Class("loading-indicator"),
			g.Attr("aria-live", "polite"),
		),
		Section(
			Class("section-panel section-block"),
			Div(
				Class("panel-heading"),
				Div(
					H2(g.Text("Ignored Traffic Rules")),
					Span(Class("panel-subtle"), g.Text(fmt.Sprintf("%d rules", len(page.Rules)))),
				),
				navLink(page.ReturnURL, actionButtonClass, page.ReturnLabel),
			),
			Div(
				Class("ignore-rules-layout"),
				ignoreRuleEditor(page),
				ignoreRuleList(page),
			),
		),
	)
}

func ignoreRuleEditor(page IgnoreRulePageData) g.Node {
	rule := page.EditRule
	return Section(
		Class("panel ignore-rule-editor"),
		sectionTitle("Rule Editor"),
		ignoreRuleError(page.ErrorMessage),
		Form(
			Method("post"),
			Action("/ignore-rules"),
			Class("ignore-rule-form"),
			g.Attr("hx-post", "/ignore-rules"),
			g.Attr("hx-target", hxTargetAppShellValue),
			g.Attr("hx-select", hxSelectAppShellValue),
			g.Attr("hx-swap", hxSwapOuterHTMLValue),
			g.Attr("hx-push-url", "true"),
			g.Attr("hx-indicator", "#loading-indicator"),
			Input(Type("hidden"), Name("action"), Value("save")),
			Input(Type("hidden"), Name("rule_id"), Value(rule.ID)),
			Input(Type("hidden"), Name("created_at_ns"), Value(strconv.FormatInt(rule.CreatedAtNs, 10))),
			Input(Type("hidden"), Name("return_to"), Value(page.ReturnURL)),
			Input(Type("hidden"), Name("return_label"), Value(page.ReturnLabel)),
			Div(
				Class("ignore-rule-grid"),
				formTextField("Name", "rule_name", rule.Name, "Optional"),
				formCheckboxField("Enabled", "enabled", rule.Enabled),
				formTextField("Any Entity", "rule_any_entity", rule.Match.AnyEntity, "host, 2LD, TLD, or IP"),
				formTextField("Source Entity", "rule_source_entity", rule.Match.SourceEntity, "alpha.lan"),
				formTextField("Destination Entity", "rule_destination_entity", rule.Match.DestinationEntity, "google.com"),
				formTextField("Source IP", "rule_source_ip", rule.Match.SourceIP, "192.168.1.10"),
				formTextField("Destination IP", "rule_destination_ip", rule.Match.DestinationIP, "8.8.8.8"),
				formTextField("Source CIDR", "rule_source_cidr", rule.Match.SourceCIDR, "192.168.1.0/24"),
				formTextField("Destination CIDR", "rule_destination_cidr", rule.Match.DestinationCIDR, "10.0.0.0/8"),
				formNumberField("Protocol", "rule_protocol", rule.Match.Protocol, "6 or 17"),
				formNumberField("Source Port", "rule_source_port", rule.Match.SourcePort, "443"),
				formNumberField("Destination Port", "rule_destination_port", rule.Match.DestinationPort, "53"),
				formSelectField("Direction", "rule_direction", string(rule.Match.Direction), []optionItem{
					{Value: "", Label: "Any"},
					{Value: string(DirectionEgress), Label: "Egress"},
					{Value: string(DirectionIngress), Label: "Ingress"},
				}),
				formSelectField("Address Family", "rule_address_family", string(rule.Match.AddressFamily), []optionItem{
					{Value: "", Label: "Any"},
					{Value: string(AddressFamilyIPv4), Label: "IPv4"},
					{Value: string(AddressFamilyIPv6), Label: "IPv6"},
				}),
			),
			Div(
				Class("button-row"),
				Button(Type("submit"), Class(actionButtonClass), g.Text("Save rule")),
				navLink(ignoreRulesURL(page.ReturnURL, page.ReturnLabel), actionButtonClass, "Clear form"),
			),
		),
		Ul(
			Class("hint-list"),
			renderNodes(page.ValidationHints, func(hint string) g.Node {
				return Li(g.Text(hint))
			}),
		),
	)
}

func ignoreRuleList(page IgnoreRulePageData) g.Node {
	return Section(
		Class("panel ignore-rule-list"),
		sectionTitle("Current Rules"),
		renderIgnoreRuleRows(page),
	)
}

func renderIgnoreRuleRows(page IgnoreRulePageData) g.Node {
	if len(page.Rules) == 0 {
		return P(Class("panel-subtle"), g.Text("No ignored-traffic rules yet."))
	}

	return Div(
		Class("rule-list"),
		renderNodes(page.Rules, func(rule IgnoreRule) g.Node {
			ruleClass := "rule-card"
			if !rule.Enabled {
				ruleClass += " is-disabled"
			}
			return Div(
				Class(ruleClass),
				Div(
					Class("rule-card-header"),
					Strong(g.Text(rule.Name)),
					Span(Class("chip"), g.Text(ignoreRuleStatus(rule))),
				),
				P(Class("panel-subtle"), g.Text(rule.Match.Summary())),
				Div(
					Class("button-row"),
					navLink(ignoreRulesEditURL(page.ReturnURL, page.ReturnLabel, rule.ID), actionButtonClass, "Edit"),
					ignoreRuleDeleteForm(page, rule),
				),
			)
		}),
	)
}

func ignoreRuleDeleteForm(page IgnoreRulePageData, rule IgnoreRule) g.Node {
	return Form(
		Method("post"),
		Action("/ignore-rules"),
		g.Attr("hx-post", "/ignore-rules"),
		g.Attr("hx-target", hxTargetAppShellValue),
		g.Attr("hx-select", hxSelectAppShellValue),
		g.Attr("hx-swap", hxSwapOuterHTMLValue),
		g.Attr("hx-push-url", "true"),
		Input(Type("hidden"), Name("action"), Value("delete")),
		Input(Type("hidden"), Name("rule_id"), Value(rule.ID)),
		Input(Type("hidden"), Name("return_to"), Value(page.ReturnURL)),
		Input(Type("hidden"), Name("return_label"), Value(page.ReturnLabel)),
		Button(Type("submit"), Class("action-button danger"), g.Text("Delete")),
	)
}

func ignoreRuleError(errorMessage string) g.Node {
	if strings.TrimSpace(errorMessage) == "" {
		return nil
	}
	return P(Class("panel-error"), g.Text(errorMessage))
}

type optionItem struct {
	Label string
	Value string
}

func formTextField(label, name, value, placeholder string) g.Node {
	return Div(
		Class("group"),
		Label(For(name), g.Text(label)),
		Input(Type("text"), ID(name), Name(name), Value(value), Placeholder(placeholder)),
	)
}

func formNumberField(label, name string, value int32, placeholder string) g.Node {
	valueText := ""
	if value > 0 {
		valueText = strconv.FormatInt(int64(value), 10)
	}
	return Div(
		Class("group"),
		Label(For(name), g.Text(label)),
		Input(Type("number"), ID(name), Name(name), Value(valueText), Placeholder(placeholder), Min("0")),
	)
}

func formSelectField(label, name, selected string, options []optionItem) g.Node {
	nodes := []g.Node{ID(name), Name(name)}
	optionNodes := make([]g.Node, 0, len(options))
	for _, option := range options {
		optionNodes = append(optionNodes, Option(Value(option.Value), selectedIf(option.Value == selected), g.Text(option.Label)))
	}
	nodes = append(nodes, optionNodes...)
	return Div(
		Class("group"),
		Label(For(name), g.Text(label)),
		Select(nodes...),
	)
}

func formCheckboxField(label, name string, checked bool) g.Node {
	return Div(
		Class("group checkbox-group"),
		Label(
			Class("checkbox-label"),
			Input(Type("hidden"), Name(name), Value("false")),
			Input(Type("checkbox"), Name(name), Value("true"), checkedIf(checked)),
			Span(g.Text(label)),
		),
	)
}

func ignoreRuleStatus(rule IgnoreRule) string {
	if rule.Enabled {
		return "Enabled"
	}
	return "Disabled"
}

func ignoreRulesURL(returnURL, returnLabel string) string {
	values := url.Values{}
	if returnURL != "" {
		values.Set("return_to", returnURL)
	}
	if returnLabel != "" {
		values.Set("return_label", returnLabel)
	}
	query := values.Encode()
	if query == "" {
		return "/ignore-rules"
	}
	return "/ignore-rules?" + query
}

func ignoreRulesEditURL(returnURL, returnLabel, ruleID string) string {
	values := url.Values{}
	values.Set("rule_id", ruleID)
	if returnURL != "" {
		values.Set("return_to", returnURL)
	}
	if returnLabel != "" {
		values.Set("return_label", returnLabel)
	}
	return "/ignore-rules?" + values.Encode()
}

func selectedNodeIgnoreRuleURL(state QueryState, node *Node) string {
	if node == nil {
		return ignoreRulesURL(stateURL(state), "Back to graph")
	}
	values := url.Values{}
	values.Set("return_to", stateURL(state))
	values.Set("return_label", "Back to graph")
	values.Set("rule_name", "Ignore "+node.ID)
	values.Set("rule_any_entity", node.ID)
	return "/ignore-rules?" + values.Encode()
}

func selectedEdgeIgnoreRuleURL(state QueryState, edge *Edge) string {
	if edge == nil {
		return ignoreRulesURL(stateURL(state), "Back to graph")
	}
	values := url.Values{}
	values.Set("return_to", stateURL(state))
	values.Set("return_label", "Back to graph")
	values.Set("rule_name", "Ignore "+edge.Source+" to "+edge.Destination)
	values.Set("rule_source_entity", edge.Source)
	values.Set("rule_destination_entity", edge.Destination)
	return "/ignore-rules?" + values.Encode()
}

func flowDetailIgnoreRuleURL(query FlowQuery, row FlowDetailRow) string {
	values := url.Values{}
	values.Set("return_to", flowDetailURL(query))
	values.Set("return_label", "Back to flows")
	values.Set("rule_name", "Ignore "+row.Source+" to "+row.Destination)
	values.Set("rule_source_entity", row.Source)
	values.Set("rule_destination_entity", row.Destination)
	values.Set("rule_protocol", strconv.FormatInt(int64(row.Protocol), 10))
	if row.SrcPort > 0 {
		values.Set("rule_source_port", strconv.FormatInt(int64(row.SrcPort), 10))
	}
	if row.DstPort > 0 {
		values.Set("rule_destination_port", strconv.FormatInt(int64(row.DstPort), 10))
	}
	switch row.IPVersion {
	case 4:
		values.Set("rule_address_family", string(AddressFamilyIPv4))
	case 6:
		values.Set("rule_address_family", string(AddressFamilyIPv6))
	}
	if row.Direction != nil {
		switch *row.Direction {
		case directionEgressParquetValue:
			values.Set("rule_direction", string(DirectionEgress))
		case directionIngressParquetValue:
			values.Set("rule_direction", string(DirectionIngress))
		}
	}
	return "/ignore-rules?" + values.Encode()
}
