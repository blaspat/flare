package mesh

import (
	"context"
	"net"
	"testing"

	"github.com/hashicorp/mdns"
)

func TestPeerAddrFromEntry(t *testing.T) {
	tests := []struct {
		name     string
		entry    *mdns.ServiceEntry
		selfName string
		want     *PeerAddr
	}{
		{
			name: "valid IPv4 entry",
			entry: &mdns.ServiceEntry{
				Info:   "node-beta",
				AddrV4: net.ParseIP("192.168.1.42"),
				Port:   9721,
			},
			selfName: "node-alpha",
			want: &PeerAddr{
				Name: "node-beta",
				Addr: "ws://192.168.1.42:9721/mesh",
			},
		},
		{
			name: "valid IPv6 entry",
			entry: &mdns.ServiceEntry{
				Info:   "node-gamma",
				AddrV6: net.ParseIP("fd00::1"),
				Port:   9722,
			},
			selfName: "node-alpha",
			want: &PeerAddr{
				Name: "node-gamma",
				Addr: "ws://[fd00::1]:9722/mesh",
			},
		},
		{
			name: "skip self (same name)",
			entry: &mdns.ServiceEntry{
				Info:   "node-alpha",
				AddrV4: net.ParseIP("192.168.1.1"),
				Port:   9721,
			},
			selfName: "node-alpha",
			want:     nil,
		},
		{
			name: "skip entry with no IP",
			entry: &mdns.ServiceEntry{
				Info: "node-delta",
				Port: 9721,
			},
			selfName: "node-alpha",
			want:     nil,
		},
		{
			name: "IPv4 takes priority over IPv6",
			entry: &mdns.ServiceEntry{
				Info:   "node-epsilon",
				AddrV4: net.ParseIP("10.0.0.5"),
				AddrV6: net.ParseIP("::1"),
				Port:   9721,
			},
			selfName: "node-alpha",
			want: &PeerAddr{
				Name: "node-epsilon",
				Addr: "ws://10.0.0.5:9721/mesh",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := peerAddrFromEntry(tt.entry, tt.selfName)
			if tt.want == nil {
				if got != nil {
					t.Errorf("expected nil, got %+v", *got)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected %+v, got nil", *tt.want)
			}
			if got.Name != tt.want.Name {
				t.Errorf("Name: expected %q, got %q", tt.want.Name, got.Name)
			}
			if got.Addr != tt.want.Addr {
				t.Errorf("Addr: expected %q, got %q", tt.want.Addr, got.Addr)
			}
		})
	}
}

func TestDiscoveryConfigDefaults(t *testing.T) {
	// Verify the config struct works as expected with typical values
	cfg := DiscoveryConfig{
		NodeName:    "node-alpha",
		ListenAddr:  ":9721",
		StaticPeers: []string{"ws://10.0.0.1:9721/mesh"},
		Mode:        "both",
	}

	if cfg.NodeName != "node-alpha" {
		t.Errorf("unexpected NodeName: %q", cfg.NodeName)
	}
	if cfg.ListenAddr != ":9721" {
		t.Errorf("unexpected ListenAddr: %q", cfg.ListenAddr)
	}
	if len(cfg.StaticPeers) != 1 {
		t.Errorf("expected 1 static peer, got %d", len(cfg.StaticPeers))
	}
	if cfg.Mode != "both" {
		t.Errorf("unexpected Mode: %q", cfg.Mode)
	}
}

func TestStartDiscoveryStaticOnly(t *testing.T) {
	// Verify no panic or hanging when using static-only mode with cancelled ctx
	hub := NewHub(func(p *PeerState) {})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	// Should return immediately with no error
	err := StartDiscovery(ctx, DiscoveryConfig{
		NodeName:    "test-node",
		ListenAddr:  ":9721",
		StaticPeers: []string{},
		Mode:        "static",
	}, hub)
	if err != nil {
		t.Errorf("StartDiscovery with cancelled ctx: expected nil, got %v", err)
	}
}

func TestStartDiscoveryEmptyConfig(t *testing.T) {
	// Static mode with no peers and cancelled context should return quickly
	hub := NewHub(func(p *PeerState) {})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := StartDiscovery(ctx, DiscoveryConfig{
		NodeName:   "test",
		ListenAddr: ":9721",
		Mode:       "static",
	}, hub)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}
