package parquetui

type nodeAddressClass string

const (
	nodeAddressClassMixed     nodeAddressClass = "mixed"
	nodeAddressClassPrivate   nodeAddressClass = "private"
	nodeAddressClassPublic    nodeAddressClass = "public"
	unknownPrivateEntityLabel                  = "Unknown private"
	unknownPublicEntityLabel                   = "Unknown public"
)

func classifyNodeAddress(privateMetric, publicMetric int64) nodeAddressClass {
	switch {
	case privateMetric > 0 && publicMetric == 0:
		return nodeAddressClassPrivate
	case privateMetric > 0 && publicMetric > 0:
		return nodeAddressClassMixed
	default:
		return nodeAddressClassPublic
	}
}
