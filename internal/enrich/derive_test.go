package enrich

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestDeriveNames(t *testing.T) {
	t.Parallel()

	derived := deriveNames("www.fingon.iki.fi.")
	assert.Equal(t, derived.host, "www.fingon.iki.fi")
	assert.Equal(t, *derived.two, "iki.fi")
	assert.Equal(t, *derived.tld, "fi")
}

func TestDeriveNamesForPrivateSuffix(t *testing.T) {
	t.Parallel()

	derived := deriveNames("cer.lan")
	assert.Equal(t, derived.host, "cer.lan")
	assert.Equal(t, *derived.two, "cer.lan")
	assert.Equal(t, *derived.tld, "lan")
}
