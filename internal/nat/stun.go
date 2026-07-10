// Package nat provides NAT traversal utilities for Flare:
// STUN client, NAT type detection, ICE candidate gathering, and TURN relay.
package nat

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

// STUN protocol constants (RFC 5389).
const (
	stunBindingRequest  = 0x0001
	stunBindingResponse = 0x0101
	stunMagicCookie     = 0x2112A442
	stunHeaderSize      = 20

	attrXORMappedAddress = 0x0020
	attrMappedAddress    = 0x0001

	defaultSTUNServer = "stun.l.google.com:19302"
	stunUDPSize       = 1500
)

// STUNResult holds the discovered public address and the local address used.
type STUNResult struct {
	PublicIP   net.IP
	PublicPort int
	LocalIP    net.IP
	LocalPort  int
	ServerAddr string
}

// DiscoverAddress sends a STUN Binding Request to the given server and returns
// the public (mapped) IP:port. If server is empty, defaults to stun.l.google.com:19302.
func DiscoverAddress(server string, timeout time.Duration) (*STUNResult, error) {
	if server == "" {
		server = defaultSTUNServer
	}

	// Resolve the STUN server address.
	serverUDPAddr, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		return nil, fmt.Errorf("resolve STUN server %q: %w", server, err)
	}

	// Create a local UDP socket.
	localAddr := &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	conn, err := net.DialUDP("udp", localAddr, serverUDPAddr)
	if err != nil {
		return nil, fmt.Errorf("dial STUN server: %w", err)
	}
	defer conn.Close()

	local := conn.LocalAddr().(*net.UDPAddr)

	// Build the Binding Request: 20-byte header, no attributes.
	req := make([]byte, stunHeaderSize)
	binary.BigEndian.PutUint16(req[0:2], stunBindingRequest) // message type
	binary.BigEndian.PutUint16(req[2:4], 0)                   // message length (no attrs)
	binary.BigEndian.PutUint32(req[4:8], stunMagicCookie)     // magic cookie

	// Generate 12 random bytes for the transaction ID.
	txID := make([]byte, 12)
	if _, err := rand.Read(txID); err != nil {
		return nil, fmt.Errorf("generate transaction ID: %w", err)
	}
	copy(req[8:20], txID)

	// Send the Binding Request.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set deadline: %w", err)
	}
	if _, err := conn.Write(req); err != nil {
		return nil, fmt.Errorf("write STUN request: %w", err)
	}

	// Read the response.
	resp := make([]byte, stunUDPSize)
	n, err := conn.Read(resp)
	if err != nil {
		return nil, fmt.Errorf("read STUN response: %w", err)
	}
	resp = resp[:n]

	// Parse and validate the response header.
	if n < stunHeaderSize {
		return nil, fmt.Errorf("STUN response too short: %d bytes", n)
	}

	msgType := binary.BigEndian.Uint16(resp[0:2])
	if msgType != stunBindingResponse {
		return nil, fmt.Errorf("unexpected STUN message type: 0x%04x", msgType)
	}

	magic := binary.BigEndian.Uint32(resp[4:8])
	if magic != stunMagicCookie {
		return nil, fmt.Errorf("invalid STUN magic cookie: 0x%08x", magic)
	}

	// Verify transaction ID matches (security check).
	respTxID := resp[8:20]
	for i := 0; i < 12; i++ {
		if respTxID[i] != txID[i] {
			return nil, fmt.Errorf("STUN transaction ID mismatch")
		}
	}

	msgLen := int(binary.BigEndian.Uint16(resp[2:4]))
	if msgLen > len(resp)-stunHeaderSize {
		return nil, fmt.Errorf("STUN message length %d exceeds buffer", msgLen)
	}

	// Parse attributes to find XOR-MAPPED-ADDRESS (preferred) or MAPPED-ADDRESS.
	var publicIP net.IP
	var publicPort int
	found := false

	pos := stunHeaderSize
	end := pos + msgLen
	for pos+4 <= end {
		attrType := binary.BigEndian.Uint16(resp[pos : pos+2])
		attrLen := int(binary.BigEndian.Uint16(resp[pos+2 : pos+4]))
		attrEnd := pos + 4 + attrLen
		if attrEnd > end {
			break
		}

		if attrType == attrXORMappedAddress || attrType == attrMappedAddress {
			value := resp[pos+4 : attrEnd]
			if len(value) < 4 {
				pos = attrEnd
				// Align to 4-byte boundary (STUN padding is to 4 bytes)
				if pad := attrEnd % 4; pad != 0 {
					pos = end // just break out
				}
				continue
			}

			family := value[1] // 0x01 = IPv4, 0x02 = IPv6
			portRaw := binary.BigEndian.Uint16(value[2:4])

			if attrType == attrXORMappedAddress {
				// XOR with the first 16 bits of the magic cookie.
				publicPort = int(portRaw ^ uint16(stunMagicCookie>>16))

				if family == 0x01 && len(value) >= 8 {
					// IPv4: XOR 4 bytes with the magic cookie.
					ip := make(net.IP, 4)
					binary.BigEndian.PutUint32(ip, binary.BigEndian.Uint32(value[4:8])^stunMagicCookie)
					publicIP = ip
				} else if family == 0x02 && len(value) >= 20 {
					// IPv6: XOR 16 bytes with (magic cookie || transaction ID).
					ip := make(net.IP, 16)
					xorKey := make([]byte, 16)
					binary.BigEndian.PutUint32(xorKey[0:4], stunMagicCookie)
					copy(xorKey[4:16], txID)
					for i := 0; i < 16; i++ {
						ip[i] = value[4+i] ^ xorKey[i]
					}
					publicIP = ip
				}
			} else {
				// MAPPED-ADDRESS (no XOR — older RFC 3489 fallback).
				publicPort = int(portRaw)
				if family == 0x01 && len(value) >= 8 {
					ip := make(net.IP, 4)
					copy(ip, value[4:8])
					publicIP = ip
				} else if family == 0x02 && len(value) >= 20 {
					ip := make(net.IP, 16)
					copy(ip, value[4:20])
					publicIP = ip
				}
			}

			if publicIP != nil {
				found = true
				break
			}
		}

		pos = attrEnd
		// Attributes are padded to 4-byte boundaries.
		if pad := attrEnd % 4; pad != 0 {
			pos += 4 - pad
		}
	}

	if !found {
		return nil, fmt.Errorf("no mapped address in STUN response from %s", server)
	}

	return &STUNResult{
		PublicIP:   publicIP,
		PublicPort: publicPort,
		LocalIP:    local.IP,
		LocalPort:  local.Port,
		ServerAddr: server,
	}, nil
}
