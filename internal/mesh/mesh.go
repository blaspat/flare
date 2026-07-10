package mesh

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	net_url "net/url"
	"time"

	"github.com/blaspat/flare/internal/nat"
	"github.com/gorilla/websocket"
)

// NATInfo wraps NAT traversal results for use in the hello handshake.
type NATInfo struct {
	PublicAddr   string // "ip:port" of the node's STUN-discovered public address
	NATType      string // string representation of NAT type (e.g., "full-cone", "symmetric")
	NATResult    *nat.NATResult
	NatCandCache []nat.Candidate
}

// Connect establishes an outgoing WebSocket connection to a peer.
// It performs the hello handshake and registers the peer with the hub.
func Connect(ctx context.Context, addr string, nodeName string, hub *Hub, natInfo *NATInfo) (*PeerState, error) {
	u, err := net_url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("parse address: %w", err)
	}
	if u.Scheme == "" {
		u.Scheme = "ws"
	}
	if u.Path == "" {
		u.Path = "/mesh"
	}

	slog.Info("connecting to peer", "addr", u.String())

	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("dial peer: %w", err)
	}

	// Build hello with NAT info if available.
	publicAddr := ""
	natType := ""
	if natInfo != nil {
		publicAddr = natInfo.PublicAddr
		natType = natInfo.NATType
	}

	// Send hello
	hello := MustNewMessage(MsgHello, nodeName, &HelloPayload{
		NodeName:   nodeName,
		Version:    "0.1.0",
		ListenAddr: "", // we don't advertise a listen addr on outgoing connections
		PublicAddr: publicAddr,
		NATType:    natType,
	})
	data, err := hello.Marshal()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("marshal hello: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		conn.Close()
		return nil, fmt.Errorf("send hello: %w", err)
	}

	// Wait for hello response with a timeout
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, resp, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read hello response: %w", err)
	}

	msg, err := UnmarshalMessage(resp)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("unmarshal hello response: %w", err)
	}

	if msg.Type != MsgHello {
		conn.Close()
		return nil, fmt.Errorf("expected hello, got %s", msg.Type)
	}

	helloResp, err := DecodePayload[HelloPayload](msg)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("decode hello payload: %w", err)
	}

	peerName := helloResp.NodeName
	if peerName == nodeName {
		conn.Close()
		return nil, fmt.Errorf("cannot connect to self (%s)", nodeName)
	}

	// Log the peer's NAT info.
	if helloResp.PublicAddr != "" {
		slog.Debug("peer NAT info",
			"peer", peerName,
			"public_addr", helloResp.PublicAddr,
			"nat_type", helloResp.NATType)
		// Store NAT info on peer state if we extend it in the future.
		_ = helloResp.PublicAddr
		_ = helloResp.NATType
	}

	// Clear deadlines — they must not leak into the peer pumps
	conn.SetReadDeadline(time.Time{})
	conn.SetWriteDeadline(time.Time{})

	// Register with hub (sets up disconnect handler + reconnect tracking)
	peer := NewPeer(peerName, conn)
	hub.AddPeer(peerName, peer, u.String())
	slog.Info("connected to peer", "name", peerName, "addr", u.String())

	// Start peer pumps
	peer.Start(ctx, func(msg *Message) {
		HandleMessage(hub, nodeName, peer, msg)
	})

	// Share NAT candidates after connection.
	if natInfo != nil && len(natInfo.NatCandCache) > 0 {
		shareNATCandidates(peer, nodeName, natInfo.NatCandCache)
	}

	return peer, nil
}

// StartListener creates and starts the mesh WebSocket listener in a goroutine.
// Returns the listener so it can be shut down. If tlsCert and tlsKey are both
// non-empty, the listener is wrapped with TLS; otherwise it serves plain WS.
func StartListener(ctx context.Context, addr string, nodeName string, hub *Hub, tlsCert, tlsKey string, natInfo *NATInfo) *Listener {
	handler := func(conn *websocket.Conn) {
		// Read hello message
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, data, err := conn.ReadMessage()
		if err != nil {
			slog.Warn("read hello from incoming connection", "remote", conn.RemoteAddr(), "err", err)
			conn.Close()
			return
		}

		msg, err := UnmarshalMessage(data)
		if err != nil {
			slog.Warn("invalid hello from incoming connection", "remote", conn.RemoteAddr().String())
			conn.Close()
			return
		}

		if msg.Type != MsgHello {
			slog.Warn("unexpected message type from incoming connection",
				"expected", MsgHello, "got", msg.Type, "remote", conn.RemoteAddr().String())
			conn.Close()
			return
		}

		hello, err := DecodePayload[HelloPayload](msg)
		if err != nil {
			slog.Warn("invalid hello payload", "remote", conn.RemoteAddr().String(), "err", err)
			conn.Close()
			return
		}

		peerName := hello.NodeName
		if peerName == nodeName {
			slog.Warn("rejected connection to self", "remote", conn.RemoteAddr().String())
			conn.Close()
			return
		}

		// Log peer NAT info.
		if hello.PublicAddr != "" {
			slog.Debug("incoming peer NAT info",
				"peer", peerName,
				"public_addr", hello.PublicAddr,
				"nat_type", hello.NATType)
		}

		// Build hello response with our NAT info.
		publicAddr := ""
		natType := ""
		if natInfo != nil {
			publicAddr = natInfo.PublicAddr
			natType = natInfo.NATType
		}

		// Send our hello back
		resp := MustNewMessage(MsgHello, nodeName, &HelloPayload{
			NodeName:   nodeName,
			Version:    "0.1.0",
			ListenAddr: addr,
			PublicAddr: publicAddr,
			NATType:    natType,
		})
		respData, err := resp.Marshal()
		if err != nil {
			slog.Warn("marshal hello response failed", "peer", peerName, "err", err)
			conn.Close()
			return
		}
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := conn.WriteMessage(websocket.TextMessage, respData); err != nil {
			slog.Warn("send hello response failed", "peer", peerName, "err", err)
			conn.Close()
			return
		}

		conn.SetReadDeadline(time.Time{}) // clear deadline
		conn.SetWriteDeadline(time.Time{})

		peer := NewPeer(peerName, conn)
		hub.AddPeer(peerName, peer, "") // incoming — no address tracking for reconnect

		slog.Info("peer connected (incoming)", "name", peerName, "addr", conn.RemoteAddr())

		// Start the peer pumps
		go peer.readPump(ctx, func(msg *Message) {
			HandleMessage(hub, nodeName, peer, msg)
		})
		go peer.writePump(ctx)
		go peer.heartbeatLoop(ctx)

		// Share NAT candidates after handshake.
		if natInfo != nil && len(natInfo.NatCandCache) > 0 {
			shareNATCandidates(peer, nodeName, natInfo.NatCandCache)
		}
	}

	listener := NewListener(addr, handler)
	if tlsCert != "" && tlsKey != "" {
		listener = listener.WithTLS(tlsCert, tlsKey)
	}
	go func() {
		if err := listener.Start(ctx); err != nil {
			slog.Error("mesh listener stopped", "err", err)
		}
	}()
	return listener
}

// shareNATCandidates sends the node's ICE candidates to a peer.
func shareNATCandidates(peer *PeerState, nodeName string, candidates []nat.Candidate) {
	entries := make([]CandidateEntry, 0, len(candidates))
	for _, c := range candidates {
		entries = append(entries, CandidateEntry{
			IP:       c.IP.String(),
			Port:     c.Port,
			Type:     string(c.Type),
			Priority: c.Priority,
		})
	}

	msg := MustNewMessage(MsgNatCandSend, nodeName, &NatCandidatePayload{
		Candidates: entries,
	})
	data, err := msg.Marshal()
	if err != nil {
		slog.Warn("marshal NAT candidate message", "err", err)
		return
	}
	if err := peer.Send(data); err != nil {
		slog.Warn("send NAT candidates", "peer", peer.Name, "err", err)
	}
}

// HandleMessage routes incoming messages from peers.
// It first checks for registered custom handlers on the hub; if none match
// it falls through to the built-in switch.
func HandleMessage(hub *Hub, nodeName string, peer *PeerState, msg *Message) {
	// Check for registered custom handler first.
	if handler := hub.getMessageHandler(msg.Type); handler != nil {
		handler(msg, peer)
		return
	}

	switch msg.Type {
	case MsgHeartbeat:
		// heartbeat is handled implicitly by readPump updating LastHeard
		payload, err := DecodePayload[HeartbeatPayload](msg)
		if err == nil {
			_ = payload // could log seq for debugging
		}
	case MsgPong:
		// handled implicitly
	case MsgNatCandSend:
		// Received ICE candidates from peer — log them for diagnostic purposes.
		// In a full ICE implementation, these would be used for connectivity checks.
		payload, err := DecodePayload[NatCandidatePayload](msg)
		if err != nil {
			slog.Warn("decode NAT candidate payload", "from", msg.From, "err", err)
			return
		}
		slog.Debug("received NAT candidates from peer",
			"from", msg.From,
			"count", len(payload.Candidates))
		for _, c := range payload.Candidates {
			slog.Debug("  candidate", "type", c.Type, "addr", net.JoinHostPort(c.IP, fmt.Sprintf("%d", c.Port)))
		}
	case MsgNatCandAck:
		// Acknowledgment — nothing to do for now.
	default:
		slog.Debug("unhandled message type", "from", msg.From, "type", msg.Type)
	}
}
