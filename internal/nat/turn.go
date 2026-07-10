package nat

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"
)

// TURN protocol constants (RFC 5766).
const (
	turnAllocateRequest      = 0x0003
	turnAllocateResponse     = 0x0103
	turnAllocateErrorResp    = 0x0113
	turnSendIndication       = 0x0016
	turnDataIndication       = 0x0017
	turnCreatePermRequest    = 0x0008
	turnCreatePermResponse   = 0x0108
	turnRefreshRequest       = 0x0004
	turnRefreshResponse      = 0x0104

	attrRequestedTransport   = 0x0019
	attrXORRelayedAddress    = 0x0016
	attrLifetime             = 0x000D
	attrXORPeerAddress       = 0x0022
	attrData                 = 0x0013
	attrUsername             = 0x0006
	attrRealm                = 0x0014
	attrNonce                = 0x0015
	attrMessageIntegrity     = 0x0008
	attrSoftware             = 0x8022

	transportUDP             = 17
	turnDefaultPort          = "3478"
	defaultAllocationLifetime = 600 // seconds
)

// TurnConfig configures a TURN relay client.
type TurnConfig struct {
	Server      string // "turn.example.com:3478"
	Username    string
	Password    string
	Realm       string // optional, discovered from server response
	AllocLifetime int  // seconds (default: 600)
}

// TurnClient manages a TURN relay allocation.
type TurnClient struct {
	cfg       TurnConfig
	conn      *net.UDPConn
	relayed   *STUNResult // XOR-RELAYED-ADDRESS
	mu        sync.RWMutex
	closed    bool

	// TURN auth state (learned from server).
	realm string
	nonce string

	// Data callback for incoming DataIndications.
	onData func(peerAddr string, data []byte)
}

// NewTurnClient creates a TURN client without allocating. Call Allocate() to
// create the relay allocation on the TURN server.
func NewTurnClient(cfg TurnConfig) *TurnClient {
	if cfg.AllocLifetime <= 0 {
		cfg.AllocLifetime = defaultAllocationLifetime
	}
	return &TurnClient{
		cfg: cfg,
	}
}

// SetDataHandler registers a callback for incoming relayed data from peers.
func (c *TurnClient) SetDataHandler(fn func(peerAddr string, data []byte)) {
	c.mu.Lock()
	c.onData = fn
	c.mu.Unlock()
}

// Allocate creates a TURN allocation on the configured server.
// Returns the relayed address that peers can reach this node at.
func (c *TurnClient) Allocate(timeout time.Duration) (*STUNResult, error) {
	server := c.cfg.Server
	if server == "" {
		return nil, fmt.Errorf("TURN server not configured")
	}

	// Ensure port is specified.
	if _, _, err := net.SplitHostPort(server); err != nil {
		server = net.JoinHostPort(server, turnDefaultPort)
	}

	serverUDPAddr, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		return nil, fmt.Errorf("resolve TURN server %q: %w", server, err)
	}

	localAddr := &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	conn, err := net.DialUDP("udp", localAddr, serverUDPAddr)
	if err != nil {
		return nil, fmt.Errorf("dial TURN server: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("set deadline: %w", err)
	}

	// Build the Allocate Request.
	txID := make([]byte, 12)
	if _, err := rand.Read(txID); err != nil {
		conn.Close()
		return nil, fmt.Errorf("generate tx ID: %w", err)
	}

	// Allocate Request attributes:
	//   REQUESTED-TRANSPORT (4 bytes: 17 = UDP)
	//   (optionally USERNAME, REALM, NONCE, MESSAGE-INTEGRITY if re-authenticating)
	req := c.buildSTUNMessage(turnAllocateRequest, txID, func() []byte {
		return buildAttrRequestedTransport()
	})

	if _, err := conn.Write(req); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write allocate request: %w", err)
	}

	// Read response.
	resp, err := c.readSTUNResponse(conn, txID, timeout)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("allocate response: %w", err)
	}

	msgType := binary.BigEndian.Uint16(resp[0:2])

	// Handle authentication challenge (401 Unauthorized).
	if msgType == turnAllocateErrorResp {
		errorCode := parseErrorCode(resp[20:])
		if errorCode == 401 {
			// Extract realm and nonce from response.
			realm, nonce := parseAuthAttrs(resp[20:])
			if realm == "" || nonce == "" {
				conn.Close()
				return nil, fmt.Errorf("TURN server returned 401 without realm/nonce")
			}

			c.mu.Lock()
			c.realm = realm
			c.nonce = nonce
			c.mu.Unlock()

			// Retry with authentication.
			if _, err := rand.Read(txID); err != nil {
				conn.Close()
				return nil, fmt.Errorf("generate tx ID: %w", err)
			}

			req2 := c.buildSTUNMessage(turnAllocateRequest, txID, func() []byte {
				return c.buildAllocateAttrs(txID)
			})

			if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
				conn.Close()
				return nil, fmt.Errorf("set deadline: %w", err)
			}
			if _, err := conn.Write(req2); err != nil {
				conn.Close()
				return nil, fmt.Errorf("write auth allocate: %w", err)
			}

			resp, err = c.readSTUNResponse(conn, txID, timeout)
			if err != nil {
				conn.Close()
				return nil, fmt.Errorf("auth allocate response: %w", err)
			}
			msgType = binary.BigEndian.Uint16(resp[0:2])
		} else {
			errMsg := parseErrorMsg(resp[20:])
			conn.Close()
			return nil, fmt.Errorf("TURN allocate error %d: %s", errorCode, errMsg)
		}
	}

	if msgType != turnAllocateResponse {
		conn.Close()
		return nil, fmt.Errorf("unexpected TURN response type: 0x%04x", msgType)
	}

	// Parse XOR-RELAYED-ADDRESS from the response.
	result := parseXORRelayedAddress(resp[20:], txID)
	if result == nil {
		conn.Close()
		return nil, fmt.Errorf("no relayed address in TURN response")
	}

	// Parse lifetime.
	lifetime := parseLifetime(resp[20:])
	if lifetime > 0 {
		c.mu.Lock()
		c.cfg.AllocLifetime = lifetime
		c.mu.Unlock()
	}

	c.mu.Lock()
	c.relayed = result
	c.mu.Unlock()

	// Start the data read pump.
	go c.readPump()

	return result, nil
}

// RelayedAddr returns the relayed address from the TURN server.
func (c *TurnClient) RelayedAddr() *STUNResult {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.relayed
}

// SendTo sends data to a peer through the TURN relay.
// The peerAddr is the peer's STUN-discovered (or relayed) address "ip:port".
func (c *TurnClient) SendTo(peerAddr string, data []byte) error {
	c.mu.RLock()
	conn := c.conn
	if c.closed || conn == nil {
		c.mu.RUnlock()
		return fmt.Errorf("TURN client closed or not allocated")
	}
	c.mu.RUnlock()

	// Create a Send Indication.
	txID := make([]byte, 12)
	if _, err := rand.Read(txID); err != nil {
		return fmt.Errorf("generate tx ID: %w", err)
	}

	peerUDPAddr, err := net.ResolveUDPAddr("udp", peerAddr)
	if err != nil {
		return fmt.Errorf("resolve peer address: %w", err)
	}

	msg := c.buildSTUNMessage(turnSendIndication, txID, func() []byte {
		attrs := make([]byte, 0, 20+len(data))
		attrs = append(attrs, buildXORPeerAddress(peerUDPAddr, txID)...)
		attrs = append(attrs, buildDataAttr(data)...)
		return attrs
	})

	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}
	_, err = conn.Write(msg)
	return err
}

// Close releases the TURN allocation and cleans up.
func (c *TurnClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// --- Internal helpers ---

// readPump receives incoming TURN Data Indications.
func (c *TurnClient) readPump() {
	buf := make([]byte, 65536)
	for {
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()
		if conn == nil {
			return
		}

		if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return
		}

		n, err := conn.Read(buf)
		if err != nil {
			return
		}

		packet := make([]byte, n)
		copy(packet, buf[:n])

		c.handleIncoming(packet)
	}
}

func (c *TurnClient) handleIncoming(packet []byte) {
	if len(packet) < stunHeaderSize {
		return
	}

	msgType := binary.BigEndian.Uint16(packet[0:2])
	if msgType != turnDataIndication {
		return
	}

	// Parse XOR-PEER-ADDRESS + DATA from the Data Indication.
	body := packet[20:]
	peerAddr, data := parseDataIndication(body)
	if peerAddr == "" || data == nil {
		return
	}

	c.mu.RLock()
	fn := c.onData
	c.mu.RUnlock()

	if fn != nil {
		fn(peerAddr, data)
	}
}

// buildSTUNMessage creates a STUN message with the given type, transaction ID,
// and attribute builder callback.
func (c *TurnClient) buildSTUNMessage(msgType uint16, txID []byte, buildAttrs func() []byte) []byte {
	attrs := buildAttrs()
	length := len(attrs)

	// Message integrity adds 24 bytes (attr header + 20 HMAC-SHA1).
	// Padding to 4 bytes is handled by attribute padding.

	msg := make([]byte, 20+length)
	binary.BigEndian.PutUint16(msg[0:2], msgType)
	binary.BigEndian.PutUint16(msg[2:4], uint16(length))
	binary.BigEndian.PutUint32(msg[4:8], stunMagicCookie)
	copy(msg[8:20], txID)
	copy(msg[20:], attrs)

	return msg
}

// readSTUNResponse reads a STUN message and validates the transaction ID.
func (c *TurnClient) readSTUNResponse(conn *net.UDPConn, txID []byte, timeout time.Duration) ([]byte, error) {
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set deadline: %w", err)
	}

	buf := make([]byte, 65536)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	resp := buf[:n]
	if len(resp) < stunHeaderSize {
		return nil, fmt.Errorf("response too short: %d", len(resp))
	}

	// Validate magic cookie.
	magic := binary.BigEndian.Uint32(resp[4:8])
	if magic != stunMagicCookie {
		return nil, fmt.Errorf("invalid magic cookie")
	}

	// Validate transaction ID.
	respTxID := resp[8:20]
	for i := 0; i < 12; i++ {
		if respTxID[i] != txID[i] {
			return nil, fmt.Errorf("transaction ID mismatch")
		}
	}

	return resp, nil
}

// buildAllocateAttrs builds the attributes for an authenticated Allocate Request.
func (c *TurnClient) buildAllocateAttrs(txID []byte) []byte {
	attrs := make([]byte, 0, 100)

	// REQUESTED-TRANSPORT (4 bytes, UDP = 17).
	attrs = append(attrs, packAttr(attrRequestedTransport, []byte{0, 0, 0, byte(transportUDP)})...)

	// USERNAME.
	if c.cfg.Username != "" {
		attrs = append(attrs, packAttr(attrUsername, []byte(c.cfg.Username))...)
	}

	// REALM.
	c.mu.RLock()
	realm := c.realm
	nonce := c.nonce
	c.mu.RUnlock()

	if realm != "" {
		attrs = append(attrs, packAttr(attrRealm, []byte(realm))...)
	}

	// NONCE.
	if nonce != "" {
		attrs = append(attrs, packAttr(attrNonce, []byte(nonce))...)
	}

	// MESSAGE-INTEGRITY (placeholder — computed last).
	// We need the full message length to compute HMAC.
	// Build without integrity first, then compute.
	msgNoIntegrity := c.buildSTUNMessage(turnAllocateRequest, txID, func() []byte {
		return attrs // attrs without message-integrity
	})

	// Compute MESSAGE-INTEGRITY.
	key := c.computeIntegrityKey()
	integrity := computeMessageIntegrity(key, msgNoIntegrity)

	// Append the integrity attribute.
	integrityAttr := packAttr(attrMessageIntegrity, integrity)
	attrs = append(attrs, integrityAttr...)

	return attrs
}

// computeIntegrityKey computes the TURN HMAC key: SHA1(username:realm:password).
func (c *TurnClient) computeIntegrityKey() []byte {
	c.mu.RLock()
	realm := c.realm
	c.mu.RUnlock()
	if realm == "" {
		realm = c.cfg.Realm
	}

	s := fmt.Sprintf("%s:%s:%s", c.cfg.Username, realm, c.cfg.Password)
	h := sha1.Sum([]byte(s))
	return h[:]
}

// computeMessageIntegrity computes HMAC-SHA1 over the STUN message with
// the integrity attribute filled with 0s (RFC 5389 §15.4).
func computeMessageIntegrity(key, msg []byte) []byte {
	// Create a copy and set the integrity attribute bytes to zero.
	// But we're computing integrity on the message BEFORE the integrity attr
	// is appended, so we just HMAC the message as-is.
	mac := hmac.New(sha1.New, key)
	mac.Write(msg)
	return mac.Sum(nil)
}

// --- Attribute helpers ---

func buildAttrRequestedTransport() []byte {
	return packAttr(attrRequestedTransport, []byte{0, 0, 0, transportUDP})
}

func packAttr(attrType uint16, value []byte) []byte {
	// Pad to 4-byte boundary.
	paddedLen := len(value)
	if pad := paddedLen % 4; pad != 0 {
		paddedLen += 4 - pad
	}

	buf := make([]byte, 4+paddedLen)
	binary.BigEndian.PutUint16(buf[0:2], attrType)
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(value)))
	copy(buf[4:], value)
	return buf
}

func buildXORPeerAddress(addr *net.UDPAddr, txID []byte) []byte {
	value := make([]byte, 8) // 1+1+2+4 = 8 for IPv4
	value[0] = 0             // reserved
	value[1] = 1             // family: IPv4

	// XOR port with magic cookie's first 16 bits.
	portXOR := uint16(addr.Port) ^ uint16(stunMagicCookie>>16)
	binary.BigEndian.PutUint16(value[2:4], portXOR)

	// XOR IPv4 with magic cookie.
	ip4 := addr.IP.To4()
	if ip4 != nil {
		xorIP := binary.BigEndian.Uint32(ip4) ^ stunMagicCookie
		binary.BigEndian.PutUint32(value[4:8], xorIP)
	}

	return packAttr(attrXORPeerAddress, value)
}

func buildDataAttr(data []byte) []byte {
	return packAttr(attrData, data)
}

// parseXORRelayedAddress extracts the XOR-RELAYED-ADDRESS from TURN response attributes.
func parseXORRelayedAddress(body []byte, txID []byte) *STUNResult {
	pos := 0
	for pos+4 < len(body) {
		if pos+4 > len(body) {
			break
		}
		attrType := binary.BigEndian.Uint16(body[pos : pos+2])
		attrLen := int(binary.BigEndian.Uint16(body[pos+2 : pos+4]))
		attrEnd := pos + 4 + attrLen
		if attrEnd > len(body) {
			break
		}

		if attrType == attrXORRelayedAddress {
			value := body[pos+4 : attrEnd]
			if len(value) < 8 {
				return nil
			}
			family := value[1]
			portXOR := binary.BigEndian.Uint16(value[2:4])
			publicPort := int(portXOR ^ uint16(stunMagicCookie>>16))

			var publicIP net.IP
			if family == 1 && len(value) >= 8 {
				ip := make(net.IP, 4)
				binary.BigEndian.PutUint32(ip, binary.BigEndian.Uint32(value[4:8])^stunMagicCookie)
				publicIP = ip
			} else if family == 2 && len(value) >= 20 {
				ip := make(net.IP, 16)
				xorKey := make([]byte, 16)
				binary.BigEndian.PutUint32(xorKey[0:4], stunMagicCookie)
				copy(xorKey[4:16], txID)
				for i := 0; i < 16; i++ {
					ip[i] = value[4+i] ^ xorKey[i]
				}
				publicIP = ip
			}

			if publicIP != nil {
				local := &net.UDPAddr{IP: net.IPv4zero, Port: 0}
				return &STUNResult{
					PublicIP:   publicIP,
					PublicPort: publicPort,
					LocalIP:    local.IP,
					LocalPort:  0,
				}
			}
		}

		pos = attrEnd
		if pad := attrEnd % 4; pad != 0 {
			pos += 4 - pad
		}
	}
	return nil
}

func parseLifetime(body []byte) int {
	pos := 0
	for pos+4 < len(body) {
		attrType := binary.BigEndian.Uint16(body[pos : pos+2])
		attrLen := int(binary.BigEndian.Uint16(body[pos+2 : pos+4]))
		attrEnd := pos + 4 + attrLen
		if attrEnd > len(body) {
			break
		}

		if attrType == attrLifetime && attrLen >= 4 {
			return int(binary.BigEndian.Uint32(body[pos+4 : pos+8]))
		}

		pos = attrEnd
		if pad := attrEnd % 4; pad != 0 {
			pos += 4 - pad
		}
	}
	return 0
}

func parseErrorCode(body []byte) int {
	pos := 0
	for pos+4 < len(body) {
		attrType := binary.BigEndian.Uint16(body[pos : pos+2])
		attrLen := int(binary.BigEndian.Uint16(body[pos+2 : pos+4]))
		attrEnd := pos + 4 + attrLen
		if attrEnd > len(body) {
			break
		}

		if attrType == 0x0009 { // ERROR-CODE
			if attrLen >= 4 {
				codeClass := body[pos+4+2]
				codeNumber := body[pos+4+3]
				return int(codeClass)*100 + int(codeNumber)
			}
		}

		pos = attrEnd
		if pad := attrEnd % 4; pad != 0 {
			pos += 4 - pad
		}
	}
	return 0
}

func parseErrorMsg(body []byte) string {
	pos := 0
	for pos+4 < len(body) {
		attrType := binary.BigEndian.Uint16(body[pos : pos+2])
		attrLen := int(binary.BigEndian.Uint16(body[pos+2 : pos+4]))
		attrEnd := pos + 4 + attrLen
		if attrEnd > len(body) {
			break
		}

		if attrType == 0x0009 && attrLen > 4 {
			return string(body[pos+8 : attrEnd])
		}

		pos = attrEnd
		if pad := attrEnd % 4; pad != 0 {
			pos += 4 - pad
		}
	}
	return ""
}

func parseAuthAttrs(body []byte) (realm, nonce string) {
	pos := 0
	for pos+4 < len(body) {
		attrType := binary.BigEndian.Uint16(body[pos : pos+2])
		attrLen := int(binary.BigEndian.Uint16(body[pos+2 : pos+4]))
		attrEnd := pos + 4 + attrLen
		if attrEnd > len(body) {
			break
		}

		switch attrType {
		case attrRealm:
			realm = string(body[pos+4 : attrEnd])
		case attrNonce:
			nonce = string(body[pos+4 : attrEnd])
		}

		pos = attrEnd
		if pad := attrEnd % 4; pad != 0 {
			pos += 4 - pad
		}
	}
	return
}

func parseDataIndication(body []byte) (peerAddr string, data []byte) {
	var peerIP net.IP
	var peerPort int

	pos := 0
	for pos+4 < len(body) {
		attrType := binary.BigEndian.Uint16(body[pos : pos+2])
		attrLen := int(binary.BigEndian.Uint16(body[pos+2 : pos+4]))
		attrEnd := pos + 4 + attrLen
		if attrEnd > len(body) {
			break
		}

		value := body[pos+4 : attrEnd]

		switch attrType {
		case attrXORPeerAddress:
			if len(value) >= 8 {
				family := value[1]
				portXOR := binary.BigEndian.Uint16(value[2:4])
				peerPort = int(portXOR ^ uint16(stunMagicCookie>>16))
				if family == 1 && len(value) >= 8 {
					ip := make(net.IP, 4)
					binary.BigEndian.PutUint32(ip, binary.BigEndian.Uint32(value[4:8])^stunMagicCookie)
					peerIP = ip
				}
			}
		case attrData:
			data = make([]byte, len(value))
			copy(data, value)
		}

		pos = attrEnd
		if pad := attrEnd % 4; pad != 0 {
			pos += 4 - pad
		}
	}

	if peerIP != nil && data != nil {
		peerAddr = net.JoinHostPort(peerIP.String(), fmt.Sprintf("%d", peerPort))
	}
	return
}

// Refresh sends a Refresh Request to keep the TURN allocation alive.
// Returns the new lifetime in seconds.
func (c *TurnClient) Refresh(timeout time.Duration) (int, error) {
	c.mu.RLock()
	conn := c.conn
	if c.closed || conn == nil {
		c.mu.RUnlock()
		return 0, fmt.Errorf("TURN client not allocated")
	}
	c.mu.RUnlock()

	txID := make([]byte, 12)
	if _, err := rand.Read(txID); err != nil {
		return 0, fmt.Errorf("generate tx ID: %w", err)
	}

	req := c.buildSTUNMessage(turnRefreshRequest, txID, func() []byte {
		attrs := make([]byte, 0, 50)

		if c.cfg.Username != "" {
			attrs = append(attrs, packAttr(attrUsername, []byte(c.cfg.Username))...)
		}

		c.mu.RLock()
		realm := c.realm
		nonce := c.nonce
		c.mu.RUnlock()

		if realm != "" {
			attrs = append(attrs, packAttr(attrRealm, []byte(realm))...)
		}
		if nonce != "" {
			attrs = append(attrs, packAttr(attrNonce, []byte(nonce))...)
		}

		// Message integrity.
		msgNoIntegrity := c.buildSTUNMessage(turnRefreshRequest, txID, func() []byte {
			return attrs
		})
		key := c.computeIntegrityKey()
		integrity := computeMessageIntegrity(key, msgNoIntegrity)
		attrs = append(attrs, packAttr(attrMessageIntegrity, integrity)...)

		return attrs
	})

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return 0, fmt.Errorf("set deadline: %w", err)
	}
	if _, err := conn.Write(req); err != nil {
		return 0, fmt.Errorf("write refresh: %w", err)
	}

	resp, err := c.readSTUNResponse(conn, txID, timeout)
	if err != nil {
		return 0, fmt.Errorf("refresh response: %w", err)
	}

	msgType := binary.BigEndian.Uint16(resp[0:2])
	if msgType != turnRefreshResponse {
		return 0, fmt.Errorf("unexpected refresh response type: 0x%04x", msgType)
	}

	lifetime := parseLifetime(resp[20:])
	if lifetime > 0 {
		c.mu.Lock()
		c.cfg.AllocLifetime = lifetime
		c.mu.Unlock()
	}

	return lifetime, nil
}

// IsAllocated returns true if the client has a valid TURN allocation.
func (c *TurnClient) IsAllocated() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.relayed != nil && !c.closed
}

// Ensure ioutil-style unused import suppression.
var _ = base64.StdEncoding
