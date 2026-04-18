package parquetui

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	ignoreRulesFilename       = "parquetflowui_ignored_rules.json"
	ignoreRuleIDBytes         = 8
	ignoreRulePortUnspecified = 0
)

type IgnoreRule struct {
	CreatedAtNs int64           `json:"createdAtNs"`
	Enabled     bool            `json:"enabled"`
	ID          string          `json:"id"`
	Match       IgnoreRuleMatch `json:"match"`
	Name        string          `json:"name"`
	UpdatedAtNs int64           `json:"updatedAtNs"`
}

type IgnoreRuleMatch struct {
	AddressFamily     AddressFamily   `json:"addressFamily,omitempty"`
	AnyEntity         string          `json:"anyEntity,omitempty"`
	Direction         DirectionFilter `json:"direction,omitempty"`
	DestinationCIDR   string          `json:"destinationCidr,omitempty"`
	DestinationEntity string          `json:"destinationEntity,omitempty"`
	DestinationIP     string          `json:"destinationIp,omitempty"`
	Protocol          int32           `json:"protocol,omitempty"`
	ServicePort       int32           `json:"servicePort,omitempty"`
	SourceCIDR        string          `json:"sourceCidr,omitempty"`
	SourceEntity      string          `json:"sourceEntity,omitempty"`
	SourceIP          string          `json:"sourceIp,omitempty"`
}

type IgnoreRulePageData struct {
	EditRule        IgnoreRule
	ErrorMessage    string
	FlowQuery       *FlowQuery
	ReturnLabel     string
	ReturnURL       string
	Rules           []IgnoreRule
	State           QueryState
	ValidationHints []string
}

type ignoreRuleFile struct {
	Rules []IgnoreRule `json:"rules"`
}

func ignoreRulesPath(srcParquetPath string) string {
	return filepath.Join(srcParquetPath, ignoreRulesFilename)
}

func loadIgnoreRules(path string) ([]IgnoreRule, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read ignore rules %q: %w", path, err)
	}
	if len(bytes) == 0 {
		return nil, nil
	}

	var file ignoreRuleFile
	if err := json.Unmarshal(bytes, &file); err != nil {
		return nil, fmt.Errorf("parse ignore rules %q: %w", path, err)
	}

	rules := make([]IgnoreRule, 0, len(file.Rules))
	for _, rule := range file.Rules {
		if err := validateStoredIgnoreRule(rule); err != nil {
			return nil, fmt.Errorf("validate ignore rule %q: %w", rule.ID, err)
		}
		rules = append(rules, normalizeIgnoreRule(rule))
	}
	sortIgnoreRules(rules)
	return rules, nil
}

func saveIgnoreRules(path string, rules []IgnoreRule) error {
	file := ignoreRuleFile{Rules: append([]IgnoreRule(nil), rules...)}
	bytes, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal ignore rules: %w", err)
	}
	bytes = append(bytes, '\n')

	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, bytes, 0o644); err != nil {
		return fmt.Errorf("write ignore rules temp file %q: %w", tempPath, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace ignore rules %q: %w", path, err)
	}
	return nil
}

func upsertIgnoreRule(rules []IgnoreRule, rule IgnoreRule) []IgnoreRule {
	nextRules := append([]IgnoreRule(nil), rules...)
	for index, existingRule := range nextRules {
		if existingRule.ID != rule.ID {
			continue
		}
		nextRules[index] = rule
		sortIgnoreRules(nextRules)
		return nextRules
	}
	nextRules = append(nextRules, rule)
	sortIgnoreRules(nextRules)
	return nextRules
}

func deleteIgnoreRule(rules []IgnoreRule, id string) []IgnoreRule {
	nextRules := append([]IgnoreRule(nil), rules...)
	for index, rule := range nextRules {
		if rule.ID != id {
			continue
		}
		return append(nextRules[:index], nextRules[index+1:]...)
	}
	return nextRules
}

func toggleIgnoreRuleEnabled(rules []IgnoreRule, id string) ([]IgnoreRule, bool) {
	nextRules := append([]IgnoreRule(nil), rules...)
	for index, rule := range nextRules {
		if rule.ID != id {
			continue
		}
		rule.Enabled = !rule.Enabled
		rule.UpdatedAtNs = time.Now().UTC().UnixNano()
		nextRules[index] = rule
		sortIgnoreRules(nextRules)
		return nextRules, true
	}
	return nextRules, false
}

func normalizeIgnoreRule(rule IgnoreRule) IgnoreRule {
	rule.ID = strings.TrimSpace(rule.ID)
	rule.Name = strings.TrimSpace(rule.Name)
	rule.Match.AnyEntity = strings.TrimSpace(rule.Match.AnyEntity)
	rule.Match.SourceEntity = strings.TrimSpace(rule.Match.SourceEntity)
	rule.Match.DestinationEntity = strings.TrimSpace(rule.Match.DestinationEntity)
	rule.Match.SourceIP = strings.TrimSpace(rule.Match.SourceIP)
	rule.Match.DestinationIP = strings.TrimSpace(rule.Match.DestinationIP)
	rule.Match.SourceCIDR = strings.TrimSpace(rule.Match.SourceCIDR)
	rule.Match.DestinationCIDR = strings.TrimSpace(rule.Match.DestinationCIDR)
	if !rule.Match.AddressFamily.valid() {
		rule.Match.AddressFamily = ""
	}
	if !rule.Match.Direction.valid() {
		rule.Match.Direction = ""
	}
	if rule.Name == "" {
		rule.Name = rule.Match.Summary()
	}
	return rule
}

func validateIgnoreRule(rule IgnoreRule) error {
	rule = normalizeIgnoreRule(rule)
	if rule.ID == "" {
		return errors.New("rule id is required")
	}
	if rule.Match.ServicePort < 0 || rule.Match.ServicePort > 65535 {
		return fmt.Errorf("service port must be between 0 and 65535, got %d", rule.Match.ServicePort)
	}
	if rule.Match.Protocol < 0 || rule.Match.Protocol > 255 {
		return fmt.Errorf("protocol must be between 0 and 255, got %d", rule.Match.Protocol)
	}
	if rule.Match.SourceIP != "" {
		if _, err := netip.ParseAddr(rule.Match.SourceIP); err != nil {
			return fmt.Errorf("invalid source IP %q", rule.Match.SourceIP)
		}
	}
	if rule.Match.DestinationIP != "" {
		if _, err := netip.ParseAddr(rule.Match.DestinationIP); err != nil {
			return fmt.Errorf("invalid destination IP %q", rule.Match.DestinationIP)
		}
	}
	if rule.Match.SourceCIDR != "" {
		if _, err := netip.ParsePrefix(rule.Match.SourceCIDR); err != nil {
			return fmt.Errorf("invalid source CIDR %q", rule.Match.SourceCIDR)
		}
	}
	if rule.Match.DestinationCIDR != "" {
		if _, err := netip.ParsePrefix(rule.Match.DestinationCIDR); err != nil {
			return fmt.Errorf("invalid destination CIDR %q", rule.Match.DestinationCIDR)
		}
	}
	if rule.Match.empty() {
		return errors.New("at least one ignore criterion is required")
	}
	return nil
}

func validateStoredIgnoreRule(rule IgnoreRule) error {
	if rule.ID == "" {
		return errors.New("rule id is required")
	}
	return validateIgnoreRule(rule)
}

func sortIgnoreRules(rules []IgnoreRule) {
	slices.SortFunc(rules, func(left, right IgnoreRule) int {
		if left.Enabled != right.Enabled {
			if left.Enabled {
				return -1
			}
			return 1
		}
		if left.UpdatedAtNs != right.UpdatedAtNs {
			if left.UpdatedAtNs > right.UpdatedAtNs {
				return -1
			}
			return 1
		}
		return strings.Compare(left.ID, right.ID)
	})
}

func newIgnoreRuleID() (string, error) {
	buffer := make([]byte, ignoreRuleIDBytes)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("generate ignore rule id: %w", err)
	}
	return hex.EncodeToString(buffer), nil
}

func newIgnoreRuleFromForm(values url.Values, now time.Time) (IgnoreRule, error) {
	ruleID := strings.TrimSpace(values.Get("rule_id"))
	if ruleID == "" {
		var err error
		ruleID, err = newIgnoreRuleID()
		if err != nil {
			return IgnoreRule{}, err
		}
	}
	createdAtNs := parseInt64(values.Get("created_at_ns"))
	if createdAtNs == 0 {
		createdAtNs = now.UnixNano()
	}
	rule := IgnoreRule{
		CreatedAtNs: createdAtNs,
		Enabled:     formBoolValue(values, "enabled", true),
		ID:          ruleID,
		Match: IgnoreRuleMatch{
			AddressFamily:     AddressFamily(strings.TrimSpace(values.Get("rule_address_family"))),
			AnyEntity:         strings.TrimSpace(values.Get("rule_any_entity")),
			Direction:         DirectionFilter(strings.TrimSpace(values.Get("rule_direction"))),
			DestinationCIDR:   strings.TrimSpace(values.Get("rule_destination_cidr")),
			DestinationEntity: strings.TrimSpace(values.Get("rule_destination_entity")),
			DestinationIP:     strings.TrimSpace(values.Get("rule_destination_ip")),
			Protocol:          int32(parseNonNegativeInt(values.Get("rule_protocol"))),
			ServicePort:       int32(parseNonNegativeInt(values.Get("rule_service_port"))),
			SourceCIDR:        strings.TrimSpace(values.Get("rule_source_cidr")),
			SourceEntity:      strings.TrimSpace(values.Get("rule_source_entity")),
			SourceIP:          strings.TrimSpace(values.Get("rule_source_ip")),
		},
		Name:        strings.TrimSpace(values.Get("rule_name")),
		UpdatedAtNs: now.UnixNano(),
	}
	if rule.Match.Protocol < 0 {
		rule.Match.Protocol = ignoreRulePortUnspecified
	}
	if rule.Match.ServicePort < 0 {
		rule.Match.ServicePort = ignoreRulePortUnspecified
	}
	rule = normalizeIgnoreRule(rule)
	if err := validateIgnoreRule(rule); err != nil {
		return rule, err
	}
	return rule, nil
}

func prefilledIgnoreRule(values url.Values) IgnoreRule {
	rule := IgnoreRule{
		Enabled: true,
		Match: IgnoreRuleMatch{
			AddressFamily:     AddressFamily(strings.TrimSpace(values.Get("rule_address_family"))),
			AnyEntity:         strings.TrimSpace(values.Get("rule_any_entity")),
			Direction:         DirectionFilter(strings.TrimSpace(values.Get("rule_direction"))),
			DestinationCIDR:   strings.TrimSpace(values.Get("rule_destination_cidr")),
			DestinationEntity: strings.TrimSpace(values.Get("rule_destination_entity")),
			DestinationIP:     strings.TrimSpace(values.Get("rule_destination_ip")),
			Protocol:          int32(parsePositiveInt(values.Get("rule_protocol"))),
			ServicePort:       int32(parsePositiveInt(values.Get("rule_service_port"))),
			SourceCIDR:        strings.TrimSpace(values.Get("rule_source_cidr")),
			SourceEntity:      strings.TrimSpace(values.Get("rule_source_entity")),
			SourceIP:          strings.TrimSpace(values.Get("rule_source_ip")),
		},
		Name: strings.TrimSpace(values.Get("rule_name")),
	}
	if values.Has("enabled") {
		rule.Enabled = formBoolValue(values, "enabled", true)
	}
	return normalizeIgnoreRule(rule)
}

func formBoolValue(values url.Values, name string, defaultValue bool) bool {
	rawValues, ok := values[name]
	if !ok {
		return defaultValue
	}
	for _, value := range rawValues {
		switch value {
		case trueValue:
			return true
		case falseValue:
			defaultValue = false
		}
	}
	return defaultValue
}

func ignoreRuleByID(rules []IgnoreRule, id string) (IgnoreRule, bool) {
	for _, rule := range rules {
		if rule.ID == id {
			return rule, true
		}
	}
	return IgnoreRule{}, false
}

func enabledIgnoreRules(rules []IgnoreRule) []IgnoreRule {
	filtered := make([]IgnoreRule, 0, len(rules))
	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}
		filtered = append(filtered, rule)
	}
	return filtered
}

func (m IgnoreRuleMatch) empty() bool {
	return m.AddressFamily == "" &&
		m.AnyEntity == "" &&
		m.Direction == "" &&
		m.DestinationCIDR == "" &&
		m.DestinationEntity == "" &&
		m.DestinationIP == "" &&
		m.Protocol == 0 &&
		m.ServicePort == 0 &&
		m.SourceCIDR == "" &&
		m.SourceEntity == "" &&
		m.SourceIP == ""
}

func (m IgnoreRuleMatch) Summary() string {
	parts := make([]string, 0, 8)
	if m.AnyEntity != "" {
		parts = append(parts, "entity="+m.AnyEntity)
	}
	if m.SourceEntity != "" {
		parts = append(parts, "source="+m.SourceEntity)
	}
	if m.DestinationEntity != "" {
		parts = append(parts, "destination="+m.DestinationEntity)
	}
	if m.Protocol > 0 {
		parts = append(parts, "proto="+strconv.FormatInt(int64(m.Protocol), 10))
	}
	if m.ServicePort > 0 {
		parts = append(parts, "service_port="+strconv.FormatInt(int64(m.ServicePort), 10))
	}
	if m.SourceIP != "" {
		parts = append(parts, "src_ip="+m.SourceIP)
	}
	if m.DestinationIP != "" {
		parts = append(parts, "dst_ip="+m.DestinationIP)
	}
	if m.SourceCIDR != "" {
		parts = append(parts, "src_cidr="+m.SourceCIDR)
	}
	if m.DestinationCIDR != "" {
		parts = append(parts, "dst_cidr="+m.DestinationCIDR)
	}
	if m.Direction != "" && m.Direction != DirectionBoth {
		parts = append(parts, "direction="+string(m.Direction))
	}
	if m.AddressFamily != "" && m.AddressFamily != AddressFamilyAll {
		parts = append(parts, "family="+string(m.AddressFamily))
	}
	if len(parts) == 0 {
		return "Ignored traffic"
	}
	return strings.Join(parts, ", ")
}

func buildFlowIgnoreConditionSQL(rules []IgnoreRule, inetAvailable bool) (string, []any, error) {
	return buildIgnoreConditionSQL(rules, inetAvailable, true)
}

func buildDNSIgnoreConditionSQL(rules []IgnoreRule, inetAvailable bool) (string, []any, error) {
	return buildIgnoreConditionSQL(rules, inetAvailable, false)
}

func buildFlowSummaryIgnoreConditionSQL(rules []IgnoreRule, inetAvailable bool) (string, []any, error) {
	return buildSummaryIgnoreConditionSQL(rules, inetAvailable, true)
}

func buildDNSSummaryIgnoreConditionSQL(rules []IgnoreRule, inetAvailable bool) (string, []any, error) {
	return buildSummaryIgnoreConditionSQL(rules, inetAvailable, false)
}

func buildIgnoreConditionSQL(rules []IgnoreRule, inetAvailable, flowDataset bool) (string, []any, error) {
	ruleConditions := make([]string, 0, len(rules))
	args := make([]any, 0, len(rules)*8)
	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}
		condition, conditionArgs, applies, err := ignoreRuleConditionSQL(rule, inetAvailable, flowDataset)
		if err != nil {
			return "", nil, err
		}
		if !applies || condition == "" {
			continue
		}
		ruleConditions = append(ruleConditions, "("+condition+")")
		args = append(args, conditionArgs...)
	}
	if len(ruleConditions) == 0 {
		return "", nil, nil
	}
	return strings.Join(ruleConditions, " OR "), args, nil
}

func buildSummaryIgnoreConditionSQL(rules []IgnoreRule, inetAvailable, flowDataset bool) (string, []any, error) {
	ruleConditions := make([]string, 0, len(rules))
	args := make([]any, 0, len(rules)*8)
	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}
		condition, conditionArgs, applies, err := summaryIgnoreRuleConditionSQL(rule, inetAvailable, flowDataset)
		if err != nil {
			return "", nil, err
		}
		if !applies || condition == "" {
			continue
		}
		ruleConditions = append(ruleConditions, "("+condition+")")
		args = append(args, conditionArgs...)
	}
	if len(ruleConditions) == 0 {
		return "", nil, nil
	}
	return strings.Join(ruleConditions, " OR "), args, nil
}

func ignoreRuleConditionSQL(rule IgnoreRule, inetAvailable, flowDataset bool) (string, []any, bool, error) {
	if flowDataset {
		return flowIgnoreRuleConditionSQL(rule, inetAvailable)
	}
	return dnsIgnoreRuleConditionSQL(rule, inetAvailable)
}

func summaryIgnoreRuleConditionSQL(rule IgnoreRule, inetAvailable, flowDataset bool) (string, []any, bool, error) {
	if flowDataset {
		return flowSummaryIgnoreRuleConditionSQL(rule, inetAvailable)
	}
	return dnsSummaryIgnoreRuleConditionSQL(rule, inetAvailable)
}

func flowSummaryIgnoreRuleConditionSQL(rule IgnoreRule, inetAvailable bool) (string, []any, bool, error) {
	conditions := []string(nil)
	args := []any(nil)

	appendEntity := func(value string, columns ...string) {
		if value == "" {
			return
		}
		parts := make([]string, 0, len(columns))
		for _, column := range columns {
			parts = append(parts, column+" = ?")
			args = append(args, value)
		}
		conditions = append(conditions, "("+strings.Join(parts, " OR ")+")")
	}

	appendCIDR := func(column, value string) error {
		if value == "" {
			return nil
		}
		if !inetAvailable {
			return fmt.Errorf("CIDR ignore rule %q requires DuckDB inet support", rule.Name)
		}
		conditions = append(conditions, fmt.Sprintf("(TRY_CAST(%s AS INET) <<= CAST(? AS INET))", column))
		args = append(args, value)
		return nil
	}

	appendEntity(rule.Match.AnyEntity, "src_entity", "src_host", "src_ip", "dst_entity", "dst_host", "dst_ip")
	appendEntity(rule.Match.SourceEntity, "src_entity", "src_host", "src_ip")
	appendEntity(rule.Match.DestinationEntity, "dst_entity", "dst_host", "dst_ip")
	if rule.Match.SourceIP != "" {
		conditions = append(conditions, "src_ip = ?")
		args = append(args, rule.Match.SourceIP)
	}
	if rule.Match.DestinationIP != "" {
		conditions = append(conditions, "dst_ip = ?")
		args = append(args, rule.Match.DestinationIP)
	}
	if err := appendCIDR("src_ip", rule.Match.SourceCIDR); err != nil {
		return "", nil, false, err
	}
	if err := appendCIDR("dst_ip", rule.Match.DestinationCIDR); err != nil {
		return "", nil, false, err
	}
	if rule.Match.Protocol > 0 {
		conditions = append(conditions, "protocol = ?")
		args = append(args, rule.Match.Protocol)
	}
	if rule.Match.ServicePort > 0 {
		conditions = append(conditions, "service_port = ?")
		args = append(args, rule.Match.ServicePort)
	}
	if rule.Match.Direction != "" && rule.Match.Direction != DirectionBoth {
		switch rule.Match.Direction {
		case DirectionEgress:
			conditions = append(conditions, "direction = ?")
			args = append(args, directionEgressParquetValue)
		case DirectionIngress:
			conditions = append(conditions, "direction = ?")
			args = append(args, directionIngressParquetValue)
		}
	}
	switch rule.Match.AddressFamily {
	case AddressFamilyIPv4:
		conditions = append(conditions, "ip_version = ?")
		args = append(args, 4)
	case AddressFamilyIPv6:
		conditions = append(conditions, "ip_version = ?")
		args = append(args, 6)
	}
	if len(conditions) == 0 {
		return "", nil, false, nil
	}
	return strings.Join(conditions, " AND "), args, true, nil
}

func dnsSummaryIgnoreRuleConditionSQL(rule IgnoreRule, inetAvailable bool) (string, []any, bool, error) {
	if rule.Match.Protocol > 0 || rule.Match.ServicePort > 0 || rule.Match.DestinationIP != "" || rule.Match.DestinationCIDR != "" || (rule.Match.Direction != "" && rule.Match.Direction != DirectionBoth) {
		return "", nil, false, nil
	}

	conditions := []string(nil)
	args := []any(nil)

	appendEntity := func(value string, columns ...string) {
		if value == "" {
			return
		}
		parts := make([]string, 0, len(columns))
		for _, column := range columns {
			parts = append(parts, column+" = ?")
			args = append(args, value)
		}
		conditions = append(conditions, "("+strings.Join(parts, " OR ")+")")
	}

	appendCIDR := func(column, value string) error {
		if value == "" {
			return nil
		}
		if !inetAvailable {
			return fmt.Errorf("CIDR ignore rule %q requires DuckDB inet support", rule.Name)
		}
		conditions = append(conditions, fmt.Sprintf("(TRY_CAST(%s AS INET) <<= CAST(? AS INET))", column))
		args = append(args, value)
		return nil
	}

	appendEntity(rule.Match.AnyEntity, "src_entity", "src_host", "src_ip", "dst_entity", "dst_host")
	appendEntity(rule.Match.SourceEntity, "src_entity", "src_host", "src_ip")
	appendEntity(rule.Match.DestinationEntity, "dst_entity", "dst_host")
	if rule.Match.SourceIP != "" {
		conditions = append(conditions, "src_ip = ?")
		args = append(args, rule.Match.SourceIP)
	}
	if err := appendCIDR("src_ip", rule.Match.SourceCIDR); err != nil {
		return "", nil, false, err
	}
	switch rule.Match.AddressFamily {
	case AddressFamilyIPv4:
		conditions = append(conditions, "ip_version = ?")
		args = append(args, 4)
	case AddressFamilyIPv6:
		conditions = append(conditions, "ip_version = ?")
		args = append(args, 6)
	}
	if len(conditions) == 0 {
		return "", nil, false, nil
	}
	return strings.Join(conditions, " AND "), args, true, nil
}

func flowIgnoreRuleConditionSQL(rule IgnoreRule, inetAvailable bool) (string, []any, bool, error) {
	conditions := []string(nil)
	args := []any(nil)

	appendEntity := func(value string, columns ...string) {
		if value == "" {
			return
		}
		parts := make([]string, 0, len(columns))
		for _, column := range columns {
			parts = append(parts, column+" = ?")
			args = append(args, value)
		}
		conditions = append(conditions, "("+strings.Join(parts, " OR ")+")")
	}

	appendCIDR := func(column, value string) error {
		if value == "" {
			return nil
		}
		if !inetAvailable {
			return fmt.Errorf("CIDR ignore rule %q requires DuckDB inet support", rule.Name)
		}
		conditions = append(conditions, fmt.Sprintf("(TRY_CAST(%s AS INET) <<= CAST(? AS INET))", column))
		args = append(args, value)
		return nil
	}

	appendEntity(rule.Match.AnyEntity, "src_host", "src_2ld", "src_tld", "src_ip", "dst_host", "dst_2ld", "dst_tld", "dst_ip")
	appendEntity(rule.Match.SourceEntity, "src_host", "src_2ld", "src_tld", "src_ip")
	appendEntity(rule.Match.DestinationEntity, "dst_host", "dst_2ld", "dst_tld", "dst_ip")
	if rule.Match.SourceIP != "" {
		conditions = append(conditions, "src_ip = ?")
		args = append(args, rule.Match.SourceIP)
	}
	if rule.Match.DestinationIP != "" {
		conditions = append(conditions, "dst_ip = ?")
		args = append(args, rule.Match.DestinationIP)
	}
	if err := appendCIDR("src_ip", rule.Match.SourceCIDR); err != nil {
		return "", nil, false, err
	}
	if err := appendCIDR("dst_ip", rule.Match.DestinationCIDR); err != nil {
		return "", nil, false, err
	}
	if rule.Match.Protocol > 0 {
		conditions = append(conditions, "protocol = ?")
		args = append(args, rule.Match.Protocol)
	}
	if rule.Match.ServicePort > 0 {
		conditions = append(conditions, rawServicePortExpression()+" = ?")
		args = append(args, rule.Match.ServicePort)
	}
	if rule.Match.Direction != "" && rule.Match.Direction != DirectionBoth {
		switch rule.Match.Direction {
		case DirectionEgress:
			conditions = append(conditions, "direction = ?")
			args = append(args, directionEgressParquetValue)
		case DirectionIngress:
			conditions = append(conditions, "direction = ?")
			args = append(args, directionIngressParquetValue)
		}
	}
	switch rule.Match.AddressFamily {
	case AddressFamilyIPv4:
		conditions = append(conditions, "ip_version = ?")
		args = append(args, 4)
	case AddressFamilyIPv6:
		conditions = append(conditions, "ip_version = ?")
		args = append(args, 6)
	}
	if len(conditions) == 0 {
		return "", nil, false, nil
	}
	return strings.Join(conditions, " AND "), args, true, nil
}

func dnsIgnoreRuleConditionSQL(rule IgnoreRule, inetAvailable bool) (string, []any, bool, error) {
	if rule.Match.Protocol > 0 || rule.Match.ServicePort > 0 || rule.Match.DestinationIP != "" || rule.Match.DestinationCIDR != "" || (rule.Match.Direction != "" && rule.Match.Direction != DirectionBoth) {
		return "", nil, false, nil
	}

	conditions := []string(nil)
	args := []any(nil)

	appendEntity := func(value string, columns ...string) {
		if value == "" {
			return
		}
		parts := make([]string, 0, len(columns))
		for _, column := range columns {
			parts = append(parts, column+" = ?")
			args = append(args, value)
		}
		conditions = append(conditions, "("+strings.Join(parts, " OR ")+")")
	}

	appendCIDR := func(column, value string) error {
		if value == "" {
			return nil
		}
		if !inetAvailable {
			return fmt.Errorf("CIDR ignore rule %q requires DuckDB inet support", rule.Name)
		}
		conditions = append(conditions, fmt.Sprintf("(TRY_CAST(%s AS INET) <<= CAST(? AS INET))", column))
		args = append(args, value)
		return nil
	}

	appendEntity(rule.Match.AnyEntity, "client_host", "client_2ld", "client_tld", "client_ip", "query_name", "query_2ld", "query_tld")
	appendEntity(rule.Match.SourceEntity, "client_host", "client_2ld", "client_tld", "client_ip")
	appendEntity(rule.Match.DestinationEntity, "query_name", "query_2ld", "query_tld")
	if rule.Match.SourceIP != "" {
		conditions = append(conditions, "client_ip = ?")
		args = append(args, rule.Match.SourceIP)
	}
	if err := appendCIDR("client_ip", rule.Match.SourceCIDR); err != nil {
		return "", nil, false, err
	}
	switch rule.Match.AddressFamily {
	case AddressFamilyIPv4:
		conditions = append(conditions, "client_ip_version = ?")
		args = append(args, 4)
	case AddressFamilyIPv6:
		conditions = append(conditions, "client_ip_version = ?")
		args = append(args, 6)
	}
	if len(conditions) == 0 {
		return "", nil, false, nil
	}
	return strings.Join(conditions, " AND "), args, true, nil
}
