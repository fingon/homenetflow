package nfdumpparser

import (
	"testing"
	"unsafe"

	"github.com/fingon/homenetflow/internal/model"
	nfdump "github.com/phaag/go-nfdump"
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

func TestMacFieldsFromRecord(t *testing.T) {
	t.Parallel()

	record := newFakeFlowRecordWithMACs(0x665544332211, 0, 0, 0x010203040506)

	macFields := macFieldsFromRecord(record)

	assert.Assert(t, macFields.inSrcMAC != nil)
	assert.Equal(t, *macFields.inSrcMAC, "11:22:33:44:55:66")
	assert.Assert(t, macFields.inDstMAC == nil)
	assert.Assert(t, macFields.outSrcMAC != nil)
	assert.Equal(t, *macFields.outSrcMAC, "06:05:04:03:02:01")
	assert.Assert(t, macFields.outDstMAC == nil)
}

func TestMacFieldsFromRecordWithoutExtension(t *testing.T) {
	t.Parallel()

	macFields := macFieldsFromRecord(&nfdump.FlowRecordV3{})

	assert.Assert(t, macFields.inSrcMAC == nil)
	assert.Assert(t, macFields.inDstMAC == nil)
	assert.Assert(t, macFields.outSrcMAC == nil)
	assert.Assert(t, macFields.outDstMAC == nil)
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

type testElementParam struct {
	offset int
	size   int
}

type testFlowRecordMirror struct {
	rawRecord      []byte
	recordHeader   unsafe.Pointer
	srcIP          [3]unsafe.Pointer
	dstIP          [3]unsafe.Pointer
	isV4           bool
	isV6           bool
	srcXlateIP     [3]unsafe.Pointer
	dstXlateIP     [3]unsafe.Pointer
	hasXlateIP     bool
	packetInterval int
	spaceInterval  int
	extOffset      [nfdump.MAXEXTENSIONS]testElementParam
}

type testMacExtension struct {
	inSrcMAC  uint64
	outDstMAC uint64
	inDstMAC  uint64
	outSrcMAC uint64
}

func newFakeFlowRecordWithMACs(inSrcMAC, outDstMAC, inDstMAC, outSrcMAC uint64) *nfdump.FlowRecordV3 {
	const macOffset = 8

	rawRecord := make([]byte, macOffset+int(unsafe.Sizeof(testMacExtension{})))
	macExtension := (*testMacExtension)(unsafe.Pointer(&rawRecord[macOffset]))
	macExtension.inSrcMAC = inSrcMAC
	macExtension.outDstMAC = outDstMAC
	macExtension.inDstMAC = inDstMAC
	macExtension.outSrcMAC = outSrcMAC

	record := &testFlowRecordMirror{
		rawRecord: rawRecord,
	}
	record.extOffset[macAddressExtensionID] = testElementParam{
		offset: macOffset,
		size:   len(rawRecord) - macOffset,
	}

	return (*nfdump.FlowRecordV3)(unsafe.Pointer(record))
}
