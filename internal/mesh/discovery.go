package mesh

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/mdns"
)

// PeerAddr describes a discovered peer address.
type PeerAddr struct {
	Name string
	Addr string // WebSocket URL, e.g. "ws://192.168.1.42:9721/mesh"
}

// DiscoveryConfig configures how the node discovers mesh peers.
type DiscoveryConfig struct {
	NodeName    string
	ListenAddr  string // e.g. ":9721"
	StaticPeers []string
	Mode        string // "static", "mdns", or "both"
}

// StartDiscovery starts the peer discovery subsystem.
// It starts mDNS advertising (if configured), connects to static peers,
// and runs periodic mDNS discovery (if configured).
// Blocks until ctx is cancelled.
func StartDiscovery(ctx context.Context, cfg DiscoveryConfig, hub *Hub) error {
	// Start mDNS advertiser if configured
	if cfg.Mode == "mdns" || cfg.Mode == "both" {
		stopAdvert, err := startMDNSAdvertiser(cfg.NodeName, cfg.ListenAddr)
		if err != nil {
			return fmt.Errorf("start mDNS advertiser: %w", err)
		}
		defer stopAdvert()
	}

	// Connect to static peers
	for _, addr := range cfg.StaticPeers {
		addr := addr
		go func() {
			if _, err := Connect(ctx, addr, cfg.NodeName, hub); err != nil {
				slog.Warn("static peer connect failed", "addr", addr, "err", err)
			}
		}()
	}

	// Run mDNS discovery loop if configured
	if cfg.Mode == "mdns" || cfg.Mode == "both" {
		// Run an initial discovery pass immediately
		runDiscoveryOnce(ctx, cfg.NodeName, hub)

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				runDiscoveryOnce(ctx, cfg.NodeName, hub)
			}
		}
	}

	// Static-only mode: just wait for shutdown
	<-ctx.Done()
	return nil
}

// startMDNSAdvertiser starts advertising this node's WebSocket endpoint via mDNS
// on _flare._tcp so other nodes can discover us. Returns a stop function.
func startMDNSAdvertiser(nodeName, listenAddr string) (func(), error) {
	_, portStr, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return nil, fmt.Errorf("parse listen addr %q: %w", listenAddr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("parse port %q: %w", portStr, err)
	}

	service, err := mdns.NewMDNSService(
		nodeName,      // instance name
		"_flare._tcp", // service type
		"local.",      // domain (trailing dot for FQDN)
		"",            // hostname — empty = auto-detect
		port,          // port
		nil,           // IPs — nil = auto-detect all
		[]string{nodeName}, // text records (carry node name for discovery)
	)
	if err != nil {
		return nil, fmt.Errorf("create mDNS service: %w", err)
	}

	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		return nil, fmt.Errorf("start mDNS server: %w", err)
	}

	slog.Info("mDNS advertiser started",
		"service", "_flare._tcp", "name", nodeName, "port", port)

	return func() {
		slog.Debug("stopping mDNS advertiser")
		server.Shutdown()
	}, nil
}

// runDiscoveryOnce performs a single mDNS discovery pass. It queries the LAN for
// flare nodes and connects to any found peers not already in the hub.
func runDiscoveryOnce(ctx context.Context, selfName string, hub *Hub) {
	slog.Debug("discovering peers via mDNS...")

	peers, err := discoverMDNSPeers(ctx, selfName, 5*time.Second)
	if err != nil {
		slog.Warn("mDNS discovery failed", "err", err)
		return
	}

	if len(peers) == 0 {
		return
	}

	for _, p := range peers {
		if _, exists := hub.Get(p.Name); exists {
			slog.Debug("already connected to discovered peer", "name", p.Name)
			continue
		}
		addr := p.Addr
		go func(name string) {
			slog.Info("connecting to discovered peer", "name", name, "addr", addr)
			if _, err := Connect(ctx, addr, selfName, hub); err != nil {
				slog.Warn("discovered peer connect failed",
					"name", name, "addr", addr, "err", err)
			}
		}(p.Name)
	}
}

// discoverMDNSPeers queries the local network for _flare._tcp services.
func discoverMDNSPeers(ctx context.Context, selfName string, timeout time.Duration) ([]PeerAddr, error) {
	entriesCh := make(chan *mdns.ServiceEntry, 20)

	params := &mdns.QueryParam{
		Service: "_flare._tcp",
		Domain:  "local.",
		Timeout: timeout,
		Entries: entriesCh,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- mdns.Query(params)
	}()

	var peers []PeerAddr

	// Collect entries until the channel is closed (Query has finished
	// cleaning up after the timeout).
	for entry := range entriesCh {
		addr := peerAddrFromEntry(entry, selfName)
		if addr != nil {
			peers = append(peers, *addr)
		}
	}

	// Surface any query-level error
	err := <-errCh
	return peers, err
}

// peerAddrFromEntry converts an mDNS service entry to a PeerAddr, or returns nil
// if the entry represents this node or is missing address info.
func peerAddrFromEntry(entry *mdns.ServiceEntry, selfName string) *PeerAddr {
	if entry.Info == selfName {
		return nil
	}

	var host string
	if entry.AddrV4 != nil {
		host = entry.AddrV4.String()
	} else if entry.AddrV6 != nil {
		host = "[" + entry.AddrV6.String() + "]"
	} else {
		return nil
	}

	return &PeerAddr{
		Name: entry.Info,
		Addr: fmt.Sprintf("ws://%s:%d/mesh", host, entry.Port),
	}
}
