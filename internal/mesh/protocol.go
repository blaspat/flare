package mesh

import (
	"encoding/json"
	"fmt"
	"time"
)

// Message types
const (
	MsgHello    = "hello"
	MsgHeartbeat = "heartbeat"
	MsgPong     = "pong"
	MsgFileChange = "file_change"
	MsgFileChunk  = "file_chunk"
	MsgCronTick   = "cron_tick"
	MsgCronResult = "cron_result"
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
}

// HeartbeatPayload is sent periodically to check liveness.
type HeartbeatPayload struct {
	Seq uint64 `json:"seq"`
}

// Message framing helpers
func NewMessage(msgType, from string, payload any) (*Message, error) {
	var raw json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal payload: %w", err)
		}
		raw = raw[:0]
		raw = b
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
