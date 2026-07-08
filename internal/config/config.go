package config

import (
	"fmt"
	"os"
	"path/filepath"
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
	Name     string `toml:"name"`
	Listen   string `toml:"listen"`
	DataDir  string `toml:"data_dir"`
	LogLevel string `toml:"log_level"`
}

type MeshConfig struct {
	Peers             []string      `toml:"peers"`
	Discovery         string        `toml:"discovery"`
	ReconnectInterval time.Duration `toml:"reconnect_interval"`
}

type SyncConfig struct {
	WatchDirs   []WatchDir     `toml:"watch_dirs"`
	PollInterval time.Duration `toml:"poll_interval"`
	ChunkSize   int            `toml:"chunk_size"`
}

type WatchDir struct {
	Path string `toml:"path"`
	Tag  string `toml:"tag"`
}

type CronConfig struct {
	Enabled     bool      `toml:"enabled"`
	HistorySize int       `toml:"history_size"`
	Jobs        []CronJob `toml:"jobs"`
}

type CronJob struct {
	Name     string        `toml:"name"`
	Schedule string        `toml:"schedule"`
	Command  string        `toml:"command"`
	Timeout  time.Duration `toml:"timeout"`
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
			Peers:             []string{},
			Discovery:         "mdns",
			ReconnectInterval: 10 * time.Second,
		},
		Sync: SyncConfig{
			WatchDirs:   []WatchDir{},
			PollInterval: 5 * time.Second,
			ChunkSize:   65536,
		},
		Cron: CronConfig{
			Enabled:     true,
			HistorySize: 100,
			Jobs:        []CronJob{},
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
