package nfdumpparser

import (
	"fmt"
	"net"

	"github.com/fingon/homenetflow/internal/model"
	nfdump "github.com/phaag/go-nfdump"
)

type Parser struct{}

type ipVersionRecord interface {
	IsIPv4() bool
	IsIPv6() bool
}

func (Parser) ParseFile(path string, emit func(model.FlowRecord) error) error {
	nfFile := nfdump.New()
	err := nfFile.Open(path)
	if err != nil {
		return fmt.Errorf("open %q: %w", path, err)
	}
	defer nfFile.Close()

	recordChannel, err := nfFile.AllRecords()
	if err != nil {
		return fmt.Errorf("iterate records for %q: %w", path, err)
	}

	for record := range recordChannel {
		if record == nil {
			continue
		}

		if err := emit(flowRecordFromNFDump(record)); err != nil {
			return err
		}
	}

	return nil
}

func flowRecordFromNFDump(record *nfdump.FlowRecordV3) model.FlowRecord {
	genericFlow := record.GenericFlow()
	ipAddress := record.IP()
	asRouting := record.AsRouting()
	flowMisc := record.FlowMisc()
	bgpNextHop := record.BgpNextHop()
	ipNextHop := record.IpNextHop()
	ipReceived := record.IpReceived()

	var startTimeNs int64
	var endTimeNs int64
	var durationNs int64
	ipVersion := ipVersionForRecord(record)
	var protocol int32
	var srcPort int32
	var dstPort int32
	var packets int64
	var bytes int64
	var tcpFlags *int32
	if genericFlow != nil {
		startTimeNs = int64(genericFlow.MsecFirst) * 1_000_000
		endTimeNs = int64(genericFlow.MsecLast) * 1_000_000
		durationNs = endTimeNs - startTimeNs
		protocol = int32(genericFlow.Proto)
		srcPort = int32(genericFlow.SrcPort)
		dstPort = int32(genericFlow.DstPort)
		packets = int64(genericFlow.InPackets)
		bytes = int64(genericFlow.InBytes)
		tcpFlags = optionalInt32(int32(genericFlow.TcpFlags))
	}

	var srcIP string
	var dstIP string
	if ipAddress != nil {
		srcIP = ipAddress.SrcIP.String()
		dstIP = ipAddress.DstIP.String()
	}
	var srcAS *int32
	var dstAS *int32
	if asRouting != nil {
		srcAS = optionalInt32(int32(asRouting.SrcAS))
		dstAS = optionalInt32(int32(asRouting.DstAS))
	}

	var srcMask *int32
	var dstMask *int32
	if flowMisc != nil {
		srcMask = optionalInt32(int32(flowMisc.SrcMask))
		dstMask = optionalInt32(int32(flowMisc.DstMask))
	}

	var nextHopIP *string
	if bgpNextHop != nil {
		nextHopIP = firstNonEmptyIP(nextHopIP, optionalIPString(bgpNextHop.IP))
	}
	if ipNextHop != nil {
		nextHopIP = firstNonEmptyIP(nextHopIP, optionalIPString(ipNextHop.IP))
	}

	var routerIP *string
	if ipReceived != nil {
		routerIP = optionalIPString(ipReceived.IP)
	}

	return model.FlowRecord{
		TimeStartNs: startTimeNs,
		TimeEndNs:   endTimeNs,
		DurationNs:  durationNs,
		IPVersion:   ipVersion,
		Protocol:    protocol,
		SrcIP:       srcIP,
		DstIP:       dstIP,
		SrcPort:     srcPort,
		DstPort:     dstPort,
		Packets:     packets,
		Bytes:       bytes,
		RouterIP:    routerIP,
		NextHopIP:   nextHopIP,
		SrcAS:       srcAS,
		DstAS:       dstAS,
		SrcMask:     srcMask,
		DstMask:     dstMask,
		TCPFlags:    tcpFlags,
	}
}

func ipVersionForRecord(record ipVersionRecord) int32 {
	switch {
	case record.IsIPv4():
		return model.IPVersion4
	case record.IsIPv6():
		return model.IPVersion6
	default:
		return model.IPVersionUnknown
	}
}

func optionalString(value string) *string {
	if value == "" || value == "<nil>" || value == "0.0.0.0" || value == "::" {
		return nil
	}

	return &value
}

func optionalInt32(value int32) *int32 {
	if value == 0 {
		return nil
	}

	return &value
}

func optionalIPString(ipAddress net.IP) *string {
	return optionalString(ipAddress.String())
}

func firstNonEmptyIP(values ...*string) *string {
	for _, value := range values {
		if value != nil {
			return value
		}
	}

	return nil
}
