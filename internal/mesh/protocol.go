package mesh

import (
	"encoding/json"
	"fmt"
	"time"
)

// Message types
const (
	MsgHello       = "hello"
	MsgHeartbeat   = "heartbeat"
	MsgPong        = "pong"
	MsgFileChange  = "file_change"
	MsgFileChunk   = "file_chunk"
	MsgCronTick    = "cron_tick"
	MsgCronResult  = "cron_result"
	MsgFileResume  = "file_resume"
	MsgSyncIndex   = "sync_index"
	MsgSyncRequest = "sync_request"
	MsgNatCandSend = "nat_candidate"   // share ICE candidates between peers
	MsgNatCandAck  = "nat_candidate_ack" // acknowledge candidate receipt
)

// Message is the wire format for all peer-to-peer communication.
type Message struct {
	Type    string          `json:"type"`
	From    string          `json:"from"`
	SentAt  int64           `json:"sent_at"` // Unix nanosecond timestamp
	Payload json.RawMessage `json:"payload,omitempty"`
}

// HelloPayload is sent as the first message after WebSocket handshake.
type HelloPayload struct {
	NodeName    string `json:"node_name"`
	Version     string `json:"version"`
	ListenAddr  string `json:"listen_addr"`
	PublicAddr  string `json:"public_addr,omitempty"` // STUN-discovered public address, empty if unknown
	NATType     string `json:"nat_type,omitempty"`    // detected NAT type, empty if unknown
}

// HeartbeatPayload is sent periodically to check liveness.
type HeartbeatPayload struct {
	Seq uint64 `json:"seq"`
}

// NatCandidatePayload carries ICE candidates between peers for NAT traversal.
type NatCandidatePayload struct {
	Candidates []CandidateEntry `json:"candidates"`
}

// CandidateEntry is a serialisable ICE candidate.
type CandidateEntry struct {
	IP       string `json:"ip"`
	Port     int    `json:"port"`
	Type     string `json:"type"` // "host", "srflx", "relay"
	Priority int    `json:"priority"`
}

// Message framing helpers
func NewMessage(msgType, from string, payload any) (*Message, error) {
	var raw json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal payload: %w", err)
		}
		raw = make(json.RawMessage, len(b))
		copy(raw, b)
	}
	return &Message{
		Type:    msgType,
		From:    from,
		SentAt:  time.Now().UnixNano(),
		Payload: raw,
	}, nil
}

func MustNewMessage(msgType, from string, payload any) *Message {
	msg, err := NewMessage(msgType, from, payload)
	if err != nil {
		panic(err)
	}
	return msg
}

func (m *Message) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func UnmarshalMessage(data []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal message: %w", err)
	}
	return &msg, nil
}

func DecodePayload[T any](msg *Message) (*T, error) {
	if len(msg.Payload) == 0 {
		return nil, fmt.Errorf("no payload")
	}
	var v T
	if err := json.Unmarshal([]byte(msg.Payload), &v); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	return &v, nil
}
