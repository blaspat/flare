package nat

import (
	"fmt"
	"net"
	"time"
)

// NATType represents the type of NAT detected.
type NATType int

const (
	NATUnknown          NATType = iota
	NATNone                     // No NAT — public IP matches local IP
	NATFullCone                 // Full-cone NAT (any external source can reach the mapped address)
	NATRestrictedCone           // Address-restricted cone (only traffic from a previously contacted IP can reach in)
	NATPortRestrictedCone       // Port-restricted cone (only traffic from a previously contacted IP:port can reach in)
	NATSymmetric                // Symmetric NAT (each destination gets a different mapping)
)

func (t NATType) String() string {
	switch t {
	case NATNone:
		return "none (public)"
	case NATFullCone:
		return "full-cone"
	case NATRestrictedCone:
		return "address-restricted cone"
	case NATPortRestrictedCone:
		return "port-restricted cone"
	case NATSymmetric:
		return "symmetric"
	default:
		return "unknown"
	}
}

// NATResult holds the NAT detection result.
type NATResult struct {
	NATType      NATType
	Primary      *STUNResult // result from the primary STUN server
	Secondary    *STUNResult // result from the secondary STUN server (may be nil)
	PublicIP     net.IP
	PublicPort   int
	LocalIP      net.IP
	LocalPort    int
}

// DetectNATType determines the NAT type using a simple heuristic:
//   - Runs STUN against the primary server.
//   - If no NAT (public IP matches the local IP on the primary server), returns NATNone.
//   - Runs STUN against a secondary server (if provided).
//   - If both STUN servers report the same public IP:port, it's a cone-type NAT
//     (full, restricted, or port-restricted — we can't distinguish without CHANGE-REQUEST).
//   - If they differ, it's symmetric NAT.
//
// For a more precise classification (full-cone vs restricted vs port-restricted),
// a CHANGE-REQUEST-capable STUN server is needed. The heuristic is sufficient for
// practical connectivity decisions in a mesh: cone types can accept incoming
// (with port prediction), symmetric cannot.
func DetectNATType(server1, server2 string, timeout time.Duration) (*NATResult, error) {
	// Run primary STUN discovery.
	primary, err := DiscoverAddress(server1, timeout)
	if err != nil {
		return nil, fmt.Errorf("primary STUN: %w", err)
	}

	result := &NATResult{
		Primary:    primary,
		PublicIP:   primary.PublicIP,
		PublicPort: primary.PublicPort,
		LocalIP:    primary.LocalIP,
		LocalPort:  primary.LocalPort,
	}

	// Check if we're behind a NAT by comparing public IP with the local IP
	// used for the STUN request. If the public IP is a private range, we're
	// behind a NAT that rewrites private addresses.
	if primary.PublicIP.Equal(primary.LocalIP) || primary.LocalIP.IsLoopback() ||
		primary.LocalIP.IsPrivate() {
		// Check if public and local are the same (no NAT).
		if primary.PublicIP.Equal(primary.LocalIP) {
			result.NATType = NATNone
			return result, nil
		}
	}

	// Run secondary STUN to check for symmetric NAT.
	if server2 != "" {
		secondary, err := DiscoverAddress(server2, timeout)
		if err != nil {
			// Secondary failure is non-fatal — we can still report cone/symmetric
			// based on primary alone (assumes cone for safety).
			result.NATType = NATFullCone // safest assumption: can receive incoming
			return result, nil
		}
		result.Secondary = secondary

		// Symmetric NAT: different public addresses from different servers.
		if !secondary.PublicIP.Equal(primary.PublicIP) || secondary.PublicPort != primary.PublicPort {
			result.NATType = NATSymmetric
			return result, nil
		}
	}

	// Same address from both servers — cone-type NAT.
	// Without CHANGE-REQUEST, we can't distinguish full-cone from restricted.
	// Default to full-cone (most permissive) for connectivity attempts.
	result.NATType = NATFullCone
	return result, nil
}

// CanReceiveIncoming returns true if the detected NAT type allows incoming
// connections from arbitrary peers to the STUN-discovered public address.
// Cone types (full, restricted) can; symmetric cannot without port guessing.
func (r *NATResult) CanReceiveIncoming() bool {
	switch r.NATType {
	case NATNone, NATFullCone, NATRestrictedCone, NATPortRestrictedCone:
		return true
	default:
		return false
	}
}

// PublicAddrStr returns the public address as "ip:port".
func (r *NATResult) PublicAddrStr() string {
	return net.JoinHostPort(r.PublicIP.String(), fmt.Sprintf("%d", r.PublicPort))
}
