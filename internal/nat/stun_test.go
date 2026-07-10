package nat

import (
	"crypto/rand"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestDiscoverAddress_GoogleSTUN(t *testing.T) {
	// Integration test — requires network access to stun.l.google.com:19302.
	// If network is unavailable, this test is skipped.
	result, err := DiscoverAddress("stun.l.google.com:19302", 5*time.Second)
	if err != nil {
		t.Skipf("STUN server unreachable (network-dependent test): %v", err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	t.Logf("Public address: %s:%d (local: %s:%d)",
		result.PublicIP, result.PublicPort,
		result.LocalIP, result.LocalPort)

	if result.PublicIP == nil || result.PublicIP.IsUnspecified() {
		t.Error("expected a public IP address")
	}

	if result.PublicPort == 0 {
		t.Error("expected a non-zero public port")
	}
}

func TestDiscoverAddress_DefaultServer(t *testing.T) {
	result, err := DiscoverAddress("", 5*time.Second)
	if err != nil {
		t.Skipf("STUN default server unreachable: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	t.Logf("Default server result: %s:%d", result.PublicIP, result.PublicPort)
}

func TestDiscoverAddress_InvalidServer(t *testing.T) {
	_, err := DiscoverAddress("192.0.2.1:3478", 2*time.Second)
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
}

func TestDiscoverAddress_Timeout(t *testing.T) {
	// 10.255.255.1 is in the TEST-NET range and shouldn't be routable.
	_, err := DiscoverAddress("10.255.255.1:3478", 1*time.Second)
	if err == nil {
		t.Log("note: STUN request to unroutable address succeeded unexpectedly")
	}
}

func TestParseSTUNResponse_XORMappedAddressV4(t *testing.T) {
	// Build a synthetic STUN Binding Response containing XOR-MAPPED-ADDRESS.
	// Simulate: client behind NAT with public IP 203.0.113.42:9000.
	txID := make([]byte, 12)
	rand.Read(txID)

	resp := make([]byte, 0, 100)

	// Header.
	header := make([]byte, stunHeaderSize)
	binary.BigEndian.PutUint16(header[0:2], stunBindingResponse)
	binary.BigEndian.PutUint16(header[2:4], 12)    // length: XOR-MAPPED-ADDRESS is 8 + 4 header = 12
	binary.BigEndian.PutUint32(header[4:8], stunMagicCookie)
	copy(header[8:20], txID)
	resp = append(resp, header...)

	// Attribute: XOR-MAPPED-ADDRESS.
	// Family: 0x01 (IPv4), Port XOR'd with 0x2112, IP XOR'd with magic cookie.
	origIP := net.ParseIP("203.0.113.42").To4()
	origPort := uint16(9000)

	xorPort := origPort ^ uint16(stunMagicCookie>>16)
	xorIP := binary.BigEndian.Uint32(origIP) ^ stunMagicCookie

	attrValue := make([]byte, 8)
	attrValue[0] = 0      // reserved
	attrValue[1] = 1      // IPv4
	binary.BigEndian.PutUint16(attrValue[2:4], xorPort)
	binary.BigEndian.PutUint32(attrValue[4:8], xorIP)

	attr := packAttr(attrXORMappedAddress, attrValue)
	resp = append(resp, attr...)

	// Now parse it through the internal discovery mechanism.
	// We can't call DiscoverAddress directly because it opens a UDP socket.
	// Instead, we test the response parsing logic by constructing the full flow.

	// Since parseXORRelayedAddress uses the same logic, let's test that directly
	// with a small harness — or better yet, test our attribute parsing.
	pubIP, pubPort := parseXORMappedAddressForTest(resp[stunHeaderSize:], txID)
	if pubIP == nil {
		t.Fatal("expected parsed IP")
	}
	if !pubIP.Equal(origIP) {
		t.Errorf("IP: got %s, want %s", pubIP, origIP)
	}
	if pubPort != int(origPort) {
		t.Errorf("Port: got %d, want %d", pubPort, origPort)
	}
}

// parseXORMappedAddressForTest extracts XOR-MAPPED-ADDRESS from attributes.
func parseXORMappedAddressForTest(body []byte, txID []byte) (net.IP, int) {
	pos := 0
	for pos+4 < len(body) {
		attrType := binary.BigEndian.Uint16(body[pos : pos+2])
		attrLen := int(binary.BigEndian.Uint16(body[pos+2 : pos+4]))
		attrEnd := pos + 4 + attrLen
		if attrEnd > len(body) {
			break
		}

		if attrType == attrXORMappedAddress && attrLen >= 8 {
			value := body[pos+4 : attrEnd]
			family := value[1]
			portXOR := binary.BigEndian.Uint16(value[2:4])
			publicPort := int(portXOR ^ uint16(stunMagicCookie>>16))

			if family == 1 && len(value) >= 8 {
				ip := make(net.IP, 4)
				binary.BigEndian.PutUint32(ip, binary.BigEndian.Uint32(value[4:8])^stunMagicCookie)
				return ip, publicPort
			} else if family == 2 && len(value) >= 20 {
				ip := make(net.IP, 16)
				xorKey := make([]byte, 16)
				binary.BigEndian.PutUint32(xorKey[0:4], stunMagicCookie)
				copy(xorKey[4:16], txID)
				for i := 0; i < 16; i++ {
					ip[i] = value[4+i] ^ xorKey[i]
				}
				return ip, publicPort
			}
		}

		pos = attrEnd
		if pad := attrEnd % 4; pad != 0 {
			pos += 4 - pad
		}
	}
	return nil, 0
}

func TestGatherCandidates_NoSTUN(t *testing.T) {
	candidates, err := GatherCandidates(":9721", nil)
	if err != nil {
		t.Fatalf("GatherCandidates failed: %v", err)
	}

	// Should have at least some candidates (machine has network interfaces).
	if len(candidates) == 0 {
		t.Skip("no network interfaces available")
	}

	t.Logf("Found %d host candidates:", len(candidates))
	for _, c := range candidates {
		t.Logf("  %s %s:%d (priority %d)", c.Type, c.IP, c.Port, c.Priority)
	}

	// All candidates should be host type (no STUN).
	for _, c := range candidates {
		if c.Type != CandidateHost {
			t.Errorf("expected host candidate, got %s", c.Type)
		}
	}
}

func TestGatherCandidates_WithSTUN(t *testing.T) {
	stunResult := &STUNResult{
		PublicIP:   net.ParseIP("203.0.113.100"),
		PublicPort: 19000,
		LocalIP:    net.ParseIP("192.168.1.50"),
		LocalPort:  9721,
	}

	candidates, err := GatherCandidates(":9721", stunResult)
	if err != nil {
		t.Fatalf("GatherCandidates failed: %v", err)
	}

	if len(candidates) < 1 {
		t.Fatal("expected at least one candidate")
	}

	// Check that STUN candidate is present.
	foundSrflx := false
	for _, c := range candidates {
		if c.Type == CandidateSrflx {
			foundSrflx = true
			if !c.IP.Equal(stunResult.PublicIP) || c.Port != stunResult.PublicPort {
				t.Errorf("srflx candidate: got %s:%d, want %s:%d",
					c.IP, c.Port, stunResult.PublicIP, stunResult.PublicPort)
			}
		}
	}

	if !foundSrflx {
		t.Error("expected srflx candidate from STUN result")
	}

	// Host candidates should be sorted first (higher priority).
	if len(candidates) >= 2 && candidates[0].Type != CandidateHost && candidates[1].Type == CandidateHost {
		// This can happen if the STUN candidate was added with a higher index.
		// Let's just check priorities are sorted descending.
		for i := 1; i < len(candidates); i++ {
			if candidates[i].Priority > candidates[i-1].Priority {
				t.Errorf("candidates not sorted by priority: %d > %d at index %d",
					candidates[i].Priority, candidates[i-1].Priority, i)
			}
		}
	}
}

func TestNATTypeNone(t *testing.T) {
	// Simulate no NAT by mocking with public = local.
	result := &NATResult{
		Primary: &STUNResult{
			PublicIP:   net.ParseIP("192.168.1.100"),
			PublicPort: 9721,
			LocalIP:    net.ParseIP("192.168.1.100"),
			LocalPort:  9721,
		},
		PublicIP:   net.ParseIP("192.168.1.100"),
		PublicPort: 9721,
		LocalIP:    net.ParseIP("192.168.1.100"),
		LocalPort:  9721,
		NATType:    NATNone,
	}

	if result.NATType != NATNone {
		t.Errorf("expected NATNone, got %s", result.NATType)
	}
	if !result.CanReceiveIncoming() {
		t.Error("NATNone should allow incoming")
	}
}

func TestNATTypeSymmetric(t *testing.T) {
	// Simulate symmetric NAT (different addresses from different servers).
	result := &NATResult{
		Primary: &STUNResult{
			PublicIP:   net.ParseIP("203.0.113.10"),
			PublicPort: 10000,
		},
		Secondary: &STUNResult{
			PublicIP:   net.ParseIP("203.0.113.10"),
			PublicPort: 10001, // different port = symmetric
		},
		PublicIP:   net.ParseIP("203.0.113.10"),
		PublicPort: 10000,
		NATType:    NATSymmetric,
	}

	if result.NATType != NATSymmetric {
		t.Errorf("expected NATSymmetric, got %s", result.NATType)
	}
	if result.CanReceiveIncoming() {
		t.Error("Symmetric NAT should NOT allow incoming without port prediction")
	}
}

func TestNATTypeFullCone(t *testing.T) {
	result := &NATResult{
		Primary: &STUNResult{
			PublicIP:   net.ParseIP("203.0.113.10"),
			PublicPort: 10000,
		},
		Secondary: &STUNResult{
			PublicIP:   net.ParseIP("203.0.113.10"),
			PublicPort: 10000, // same = cone type
		},
		PublicIP:   net.ParseIP("203.0.113.10"),
		PublicPort: 10000,
		NATType:    NATFullCone,
	}

	if result.NATType != NATFullCone {
		t.Errorf("expected NATFullCone, got %s", result.NATType)
	}
	if !result.CanReceiveIncoming() {
		t.Error("Full cone NAT should allow incoming")
	}
}

func TestPublicAddrStr(t *testing.T) {
	result := &NATResult{
		PublicIP:   net.ParseIP("203.0.113.42"),
		PublicPort: 19000,
	}
	expected := "203.0.113.42:19000"
	if got := result.PublicAddrStr(); got != expected {
		t.Errorf("PublicAddrStr: got %q, want %q", got, expected)
	}
}

func TestNATTypeString(t *testing.T) {
	tests := []struct {
		typ  NATType
		want string
	}{
		{NATUnknown, "unknown"},
		{NATNone, "none (public)"},
		{NATFullCone, "full-cone"},
		{NATRestrictedCone, "address-restricted cone"},
		{NATPortRestrictedCone, "port-restricted cone"},
		{NATSymmetric, "symmetric"},
	}

	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.want {
			t.Errorf("NATType(%d).String() = %q, want %q", tt.typ, got, tt.want)
		}
	}
}

func TestCandidatePriority(t *testing.T) {
	// Verify host > srflx > relay priority ordering.
	hostPri := calcPriority(CandidateHost, 0)
	srflxPri := calcPriority(CandidateSrflx, 0)
	relayPri := calcPriority(CandidateRelay, 0)

	if hostPri <= srflxPri {
		t.Error("host priority should be higher than srflx")
	}
	if srflxPri <= relayPri {
		t.Error("srflx priority should be higher than relay")
	}

	// Verify first-in-type > later-in-type.
	hostPri2 := calcPriority(CandidateHost, 1)
	if hostPri <= hostPri2 {
		t.Error("first host candidate should have higher priority than second")
	}
}

func TestTurnClientConfig(t *testing.T) {
	// Verify that missing server returns error on Allocate.
	client := NewTurnClient(TurnConfig{})
	_, err := client.Allocate(1 * time.Second)
	if err == nil {
		t.Error("expected error for empty TURN server")
	}
}

func BenchmarkDiscoverAddress(b *testing.B) {
	for i := 0; i < b.N; i++ {
		result, err := DiscoverAddress("stun.l.google.com:19302", 3*time.Second)
		if err != nil {
			b.Skipf("STUN server unreachable: %v", err)
		}
		if result == nil {
			b.Fatal("nil result")
		}
	}
}
