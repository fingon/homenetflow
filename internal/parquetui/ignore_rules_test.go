package parquetui

import (
	"net/url"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestNewIgnoreRuleFromFormParsesCheckedHiddenCheckbox(t *testing.T) {
	t.Parallel()

	rule, err := newIgnoreRuleFromForm(url.Values{
		"enabled":                 {"false", "true"},
		"rule_any_entity":         {"alpha.lan"},
		"rule_id":                 {"ignore-alpha"},
		"rule_name":               {"Ignore alpha"},
		"rule_protocol":           {""},
		"rule_service_port":       {""},
		"rule_address_family":     {""},
		"rule_direction":          {""},
		"rule_destination_cidr":   {""},
		"rule_destination_entity": {""},
		"rule_destination_ip":     {""},
		"rule_source_cidr":        {""},
		"rule_source_entity":      {""},
		"rule_source_ip":          {""},
	}, time.Unix(1, 0))

	assert.NilError(t, err)
	assert.Assert(t, rule.Enabled)
}

func TestNewIgnoreRuleFromFormParsesUncheckedHiddenCheckbox(t *testing.T) {
	t.Parallel()

	rule, err := newIgnoreRuleFromForm(url.Values{
		"enabled":         {"false"},
		"rule_any_entity": {"alpha.lan"},
		"rule_id":         {"ignore-alpha"},
	}, time.Unix(1, 0))

	assert.NilError(t, err)
	assert.Assert(t, !rule.Enabled)
}

func TestNewIgnoreRuleFromFormDefaultsMissingEnabledToEnabled(t *testing.T) {
	t.Parallel()

	rule, err := newIgnoreRuleFromForm(url.Values{
		"rule_any_entity": {"alpha.lan"},
		"rule_id":         {"ignore-alpha"},
	}, time.Unix(1, 0))

	assert.NilError(t, err)
	assert.Assert(t, rule.Enabled)
}

func TestToggleIgnoreRuleEnabledFlipsRule(t *testing.T) {
	t.Parallel()

	rules, ok := toggleIgnoreRuleEnabled([]IgnoreRule{{
		ID:      "ignore-alpha",
		Enabled: false,
		Match: IgnoreRuleMatch{
			AnyEntity: "alpha.lan",
		},
	}}, "ignore-alpha")

	assert.Assert(t, ok)
	assert.Assert(t, rules[0].Enabled)
}
