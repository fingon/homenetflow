package enrich

import (
	"strings"

	"golang.org/x/net/publicsuffix"
)

const hostnameLabelSeparator = "."

type derivedNames struct {
	host string
	tld  *string
	two  *string
}

func deriveNames(host string) derivedNames {
	normalizedHost := normalizeHostname(host)
	if normalizedHost == "" {
		return derivedNames{}
	}

	labels := strings.Split(normalizedHost, hostnameLabelSeparator)
	if len(labels) < 2 {
		return derivedNames{host: normalizedHost}
	}

	tld := deriveTLD(normalizedHost, labels)
	two := derive2LD(normalizedHost, labels)
	return derivedNames{
		host: normalizedHost,
		tld:  tld,
		two:  two,
	}
}

func derive2LD(host string, labels []string) *string {
	suffix, isICANN := publicsuffix.PublicSuffix(host)
	if suffix != "" && isICANN {
		suffixLabels := strings.Split(suffix, hostnameLabelSeparator)
		if len(labels) > len(suffixLabels) {
			startIndex := len(labels) - len(suffixLabels) - 1
			value := strings.Join(labels[startIndex:], hostnameLabelSeparator)
			return &value
		}
	}

	fallback := strings.Join(labels[len(labels)-2:], hostnameLabelSeparator)
	return &fallback
}

func deriveTLD(host string, labels []string) *string {
	value, isICANN := publicsuffix.PublicSuffix(host)
	if value != "" && isICANN {
		return &value
	}

	fallback := labels[len(labels)-1]
	return &fallback
}

func normalizeHostname(host string) string {
	trimmedHost := strings.TrimSpace(strings.TrimSuffix(strings.ToLower(host), hostnameLabelSeparator))
	if trimmedHost == "" {
		return ""
	}

	return trimmedHost
}
