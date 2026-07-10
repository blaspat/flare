package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Node NodeConfig `toml:"node"`
	Mesh MeshConfig `toml:"mesh"`
	Sync SyncConfig `toml:"sync"`
	Cron CronConfig `toml:"cron"`
}

type NodeConfig struct {
	Name           string `toml:"name"`
	Listen         string `toml:"listen"`
	DataDir        string `toml:"data_dir"`
	LogLevel       string `toml:"log_level"`
	WebPort        int    `toml:"web_port"`  // dashboard HTTP server port (0 = disabled)
	TLSCert        string `toml:"tls_cert"`
	TLSKey         string `toml:"tls_key"`
	EncryptionKey  string `toml:"encryption_key"`  // hex-encoded 32-byte AES-256 key (empty = disabled)
	EncryptionFile string `toml:"encryption_key_file"` // path to file with hex key (alternative to inline)
}
// MeshConfig defines mesh networking parameters.
type MeshConfig struct {
	Peers              []string      `toml:"peers"`
	Discovery          string        `toml:"discovery"`
	ReconnectInterval  time.Duration `toml:"reconnect_interval"` // used as BackoffMin fallback (deprecated)
	BackoffMin         time.Duration `toml:"backoff_min"`        // initial backoff (default: 1s)
	BackoffMax         time.Duration `toml:"backoff_max"`        // max backoff (default: 60s)
	CircuitBreakerLimit int          `toml:"circuit_breaker_limit"` // consecutive failures before circuit opens (0=disabled, default: 10)
	STUNServers        []string      `toml:"stun_servers"`       // STUN servers for NAT discovery (default: stun.l.google.com:19302)
	TurnServer         string        `toml:"turn_server"`        // TURN relay server address
	TurnUsername       string        `toml:"turn_username"`
	TurnPassword       string        `toml:"turn_password"`
}

type SyncConfig struct {
	WatchDirs      []WatchDir     `toml:"watch_dirs"`
	PollInterval   time.Duration  `toml:"poll_interval"`
	ChunkSize      int            `toml:"chunk_size"`
	BandwidthLimit int64          `toml:"bandwidth_limit"` // bytes/sec (0 = unlimited)
	BandwidthBurst int64          `toml:"bandwidth_burst"` // burst size (0 = defaults to rate)
}

type WatchDir struct {
	Path string `toml:"path"`
	Tag  string `toml:"tag"`
}

type CronConfig struct {
	Enabled         bool        `toml:"enabled"`
	HistorySize     int         `toml:"history_size"`
	CatchUpLookback string      `toml:"catch_up_lookback"` // how far back to check for missed jobs (default: "5m")
	Jobs            []CronJob   `toml:"jobs"`
}

type CronJob struct {
	Name         string        `toml:"name"`
	Schedule     string        `toml:"schedule"`
	Command      string        `toml:"command"`
	Timeout      time.Duration `toml:"timeout"`
	RetryCount   int           `toml:"retry_count"`    // max retry attempts on failure (0 = no retry)
	RetryDelay   time.Duration `toml:"retry_delay"`    // delay between retry attempts (default: 30s)
	CatchUpLimit int           `toml:"catch_up_limit"` // max missed firings to catch up on leadership change (0 = default 1)
}

func Default() *Config {
	return &Config{
		Node: NodeConfig{
			Name:     "flare-node",
			Listen:   ":9721",
			DataDir:  "./data",
			LogLevel: "info",
		},
		Mesh: MeshConfig{
			Peers:               []string{},
			Discovery:           "mdns",
			ReconnectInterval:   10 * time.Second,
			BackoffMin:          1 * time.Second,
			BackoffMax:          60 * time.Second,
			CircuitBreakerLimit: 10,
		},
		Sync: SyncConfig{
			WatchDirs:   []WatchDir{},
			PollInterval: 5 * time.Second,
			ChunkSize:   65536,
		},
		Cron: CronConfig{
			Enabled:         true,
			HistorySize:     100,
			CatchUpLookback: "5m",
			Jobs:            []CronJob{},
		},
	}
}

func Load(path string) (*Config, error) {
	cfg := Default()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	if err := toml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return cfg, nil
}

func (c *Config) ResolvePaths() error {
	base := filepath.Dir(c.Node.DataDir)
	for i, wd := range c.Sync.WatchDirs {
		if !filepath.IsAbs(wd.Path) {
			c.Sync.WatchDirs[i].Path = filepath.Join(base, wd.Path)
		}
	}
	if !filepath.IsAbs(c.Node.DataDir) {
		abs, err := filepath.Abs(c.Node.DataDir)
		if err != nil {
			return fmt.Errorf("resolve data_dir: %w", err)
		}
		c.Node.DataDir = abs
	}
	return nil
}

// EffectiveBackoffMin returns BackoffMin, falling back to ReconnectInterval
// for backward compatibility, then to 1s.
func (c *Config) EffectiveBackoffMin() time.Duration {
	if c.Mesh.BackoffMin > 0 {
		return c.Mesh.BackoffMin
	}
	if c.Mesh.ReconnectInterval > 0 {
		return c.Mesh.ReconnectInterval
	}
	return time.Second
}

// EffectiveBackoffMax returns BackoffMax, falling back to 60s.
func (c *Config) EffectiveBackoffMax() time.Duration {
	if c.Mesh.BackoffMax > 0 {
		return c.Mesh.BackoffMax
	}
	return 60 * time.Second
}

// EffectiveCircuitBreakerLimit returns CircuitBreakerLimit, falling back to 10 (0 = disabled).
func (c *Config) EffectiveCircuitBreakerLimit() int {
	if c.Mesh.CircuitBreakerLimit < 0 {
		return 0
	}
	return c.Mesh.CircuitBreakerLimit
}

// EffectiveEncryptionKey returns the AES-256 key, checking sources in order:
//  1. NodeConfig.EncryptionKey (inline hex in config)
//  2. NodeConfig.EncryptionFile (path to file containing hex key)
//  3. FLARE_ENCRYPTION_KEY env var
//
// Returns empty string if no key is configured (encryption disabled).
func (c *Config) EffectiveEncryptionKey() string {
	if c.Node.EncryptionKey != "" {
		return c.Node.EncryptionKey
	}
	if c.Node.EncryptionFile != "" {
		data, err := os.ReadFile(c.Node.EncryptionFile)
		if err == nil {
			return strings.TrimSpace(string(data))
		}
	}
	if env := os.Getenv("FLARE_ENCRYPTION_KEY"); env != "" {
		return env
	}
	return ""
}

// EffectiveBandwidthLimit returns BandwidthLimit (bytes/sec), defaulting to 0 (unlimited).
func (c *Config) EffectiveBandwidthLimit() int64 {
	if c.Sync.BandwidthLimit < 0 {
		return 0
	}
	return c.Sync.BandwidthLimit
}

// EffectiveBandwidthBurst returns BandwidthBurst, or falls back to bandwidth_limit (1 sec's worth).
func (c *Config) EffectiveBandwidthBurst() int64 {
	if c.Sync.BandwidthBurst > 0 {
		return c.Sync.BandwidthBurst
	}
	return c.EffectiveBandwidthLimit()
}

// EffectiveSTUNServers returns the configured STUN servers, or the default.
func (c *Config) EffectiveSTUNServers() []string {
	if len(c.Mesh.STUNServers) > 0 {
		return c.Mesh.STUNServers
	}
	return []string{"stun.l.google.com:19302"}
}

// EffectiveTurnServer returns the TURN server address, or empty string.
func (c *Config) EffectiveTurnServer() string {
	return c.Mesh.TurnServer
}

// EffectiveTurnUsername returns the TURN username.
func (c *Config) EffectiveTurnUsername() string {
	return c.Mesh.TurnUsername
}

// EffectiveTurnPassword returns the TURN password.
func (c *Config) EffectiveTurnPassword() string {
	return c.Mesh.TurnPassword
}

// EffectiveCatchUpLookback returns the parsed catch-up lookback duration from config.
// Defaults to 5 minutes on parse failure.
func (c *Config) EffectiveCatchUpLookback() time.Duration {
	if c.Cron.CatchUpLookback == "" {
		return 5 * time.Minute
	}
	d, err := time.ParseDuration(c.Cron.CatchUpLookback)
	if err != nil {
		return 5 * time.Minute
	}
	return d
}

// EffectiveRetryDelay returns the effective retry delay for a CronJob.
// Defaults to 30s if not set.
func EffectiveRetryDelay(d time.Duration) time.Duration {
	if d <= 0 {
		return 30 * time.Second
	}
	return d
}
