package nat

import (
	"fmt"
	"net"
	"sort"
)

// CandidateType indicates the origin of an ICE candidate.
type CandidateType string

const (
	CandidateHost  CandidateType = "host"  // local LAN address
	CandidateSrflx CandidateType = "srflx" // server-reflexive (STUN-discovered public address)
	CandidateRelay CandidateType = "relay" // TURN relay address
)

// Candidate represents an ICE connectivity candidate.
type Candidate struct {
	IP       net.IP        `json:"ip"`
	Port     int           `json:"port"`
	Type     CandidateType `json:"type"`
	Priority int           `json:"priority"`
}

// GatherCandidates collects local host candidates from all network interfaces,
// plus a server-reflexive candidate from STUN (if available).
// The addr is the local listen address (e.g., ":9721") used to determine the port.
func GatherCandidates(listenAddr string, stunResult *STUNResult) ([]Candidate, error) {
	var candidates []Candidate

	// 1. Host candidates from local interfaces.
	hostCandidates, err := gatherHostCandidates(listenAddr)
	if err != nil {
		return nil, fmt.Errorf("gather host candidates: %w", err)
	}
	candidates = append(candidates, hostCandidates...)

	// 2. Server-reflexive candidate from STUN (if we have a public address).
	if stunResult != nil && stunResult.PublicIP != nil && !stunResult.PublicIP.IsUnspecified() {
		candidates = append(candidates, Candidate{
			IP:       stunResult.PublicIP,
			Port:     stunResult.PublicPort,
			Type:     CandidateSrflx,
			Priority: calcPriority(CandidateSrflx, len(candidates)),
		})
	}

	// Sort by priority descending (highest priority first).
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Priority > candidates[j].Priority
	})

	return candidates, nil
}

// gatherHostCandidates collects local IP addresses on all non-loopback interfaces
// and wraps them as host candidates on the given port.
func gatherHostCandidates(listenAddr string) ([]Candidate, error) {
	_, portStr, err := net.SplitHostPort(listenAddr)
	if err != nil {
		portStr = "9721"
	}
	port := 9721
	if p, err := net.LookupPort("tcp", portStr); err == nil {
		port = p
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("list interfaces: %w", err)
	}

	var candidates []Candidate
	seen := make(map[string]bool)

	for _, iface := range ifaces {
		// Skip loopback and down interfaces.
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipNet.IP

			// Skip loopback, link-local, and multicast.
			if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsMulticast() {
				continue
			}

			key := ip.String()
			if seen[key] {
				continue
			}
			seen[key] = true

			candidates = append(candidates, Candidate{
				IP:       ip,
				Port:     port,
				Type:     CandidateHost,
				Priority: calcPriority(CandidateHost, len(candidates)),
			})
		}
	}

	return candidates, nil
}

// calcPriority computes an ICE-style priority (higher = better).
// Host candidates get higher base priority than srflx, which gets higher than relay.
func calcPriority(cType CandidateType, index int) int {
	var typePref int
	switch cType {
	case CandidateHost:
		typePref = 126 // max is 127 for media, 126 is high for data
	case CandidateSrflx:
		typePref = 100
	case CandidateRelay:
		typePref = 50
	default:
		typePref = 0
	}
	// Lower index within same type = higher priority.
	localPref := 65535 - index
	if localPref < 0 {
		localPref = 0
	}
	return (typePref << 24) | (localPref << 8) | (256 - 1) // component ID 1
}
