package mesh

import (
	"testing"
)

func TestNewMessage(t *testing.T) {
	hello := &HelloPayload{NodeName: "alpha", Version: "0.1.0", ListenAddr: ":9721"}
	msg, err := NewMessage(MsgHello, "alpha", hello)
	if err != nil {
		t.Fatalf("NewMessage failed: %v", err)
	}

	if msg.Type != MsgHello {
		t.Errorf("expected type %s, got %s", MsgHello, msg.Type)
	}
	if msg.From != "alpha" {
		t.Errorf("expected from alpha, got %s", msg.From)
	}
	if msg.SentAt == 0 {
		t.Error("expected non-zero sent_at")
	}

	// Marshal and unmarshal
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	decoded, err := UnmarshalMessage(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.Type != MsgHello {
		t.Errorf("round-trip type: expected %s, got %s", MsgHello, decoded.Type)
	}

	payload, err := DecodePayload[HelloPayload](decoded)
	if err != nil {
		t.Fatalf("decode payload failed: %v", err)
	}
	if payload.NodeName != "alpha" {
		t.Errorf("expected alpha, got %s", payload.NodeName)
	}
}

func TestMessageTypes(t *testing.T) {
	tests := []struct {
		name    string
		msgType string
		payload any
	}{
		{"hello", MsgHello, &HelloPayload{NodeName: "test", Version: "1.0"}},
		{"heartbeat", MsgHeartbeat, &HeartbeatPayload{Seq: 42}},
		{"nil payload", MsgPong, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := NewMessage(tt.msgType, "node", tt.payload)
			if err != nil {
				t.Fatalf("NewMessage failed: %v", err)
			}
			if msg.Type != tt.msgType {
				t.Errorf("type: expected %s, got %s", tt.msgType, msg.Type)
			}
		})
	}
}

func TestHeartbeatPayloadRoundTrip(t *testing.T) {
	payload := &HeartbeatPayload{Seq: 100}
	msg := MustNewMessage(MsgHeartbeat, "node", payload)

	decoded, err := DecodePayload[HeartbeatPayload](msg)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.Seq != 100 {
		t.Errorf("seq: expected 100, got %d", decoded.Seq)
	}
}

func TestHubLifecycle(t *testing.T) {
	hub := NewHub(func(p *PeerState) {})
	if hub.Count() != 0 {
		t.Errorf("expected 0 peers, got %d", hub.Count())
	}

	// We can't easily create real peers without WebSocket connections,
	// but we can verify the hub doesn't panic on operations
	if peers := hub.List(); len(peers) != 0 {
		t.Errorf("expected empty list, got %d", len(peers))
	}

	hub.Remove("nonexistent") // should not panic
}
