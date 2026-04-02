package nfdumpparser

import (
	"testing"

	"github.com/fingon/homenetflow/internal/model"
	"gotest.tools/v3/assert"
)

func TestIPVersionForRecord(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		isIPv4        bool
		isIPv6        bool
		wantIPVersion int32
	}{
		{name: "ipv4", isIPv4: true, wantIPVersion: model.IPVersion4},
		{name: "ipv6", isIPv6: true, wantIPVersion: model.IPVersion6},
		{name: "unknown", wantIPVersion: model.IPVersionUnknown},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			record := fakeIPVersionRecord{
				isIPv4: testCase.isIPv4,
				isIPv6: testCase.isIPv6,
			}

			assert.Equal(t, ipVersionForRecord(record), testCase.wantIPVersion)
		})
	}
}

type fakeIPVersionRecord struct {
	isIPv4 bool
	isIPv6 bool
}

func (r fakeIPVersionRecord) IsIPv4() bool {
	return r.isIPv4
}

func (r fakeIPVersionRecord) IsIPv6() bool {
	return r.isIPv6
}
