package enrich

import "net/netip"

var (
	ipv4PrivatePrefixes = []netip.Prefix{
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("172.16.0.0/12"),
		netip.MustParsePrefix("192.168.0.0/16"),
	}
	ipv6PrivatePrefixes = []netip.Prefix{
		netip.MustParsePrefix("fc00::/7"),
		netip.MustParsePrefix("fec0::/10"),
		netip.MustParsePrefix("fe80::/10"),
	}
)

func isPrivateIPAddress(ipAddress string) bool {
	address, err := netip.ParseAddr(ipAddress)
	if err != nil {
		return false
	}

	if address.Is4() {
		for _, prefix := range ipv4PrivatePrefixes {
			if prefix.Contains(address) {
				return true
			}
		}
		return false
	}

	if address.Is6() {
		for _, prefix := range ipv6PrivatePrefixes {
			if prefix.Contains(address) {
				return true
			}
		}
	}

	return false
}
