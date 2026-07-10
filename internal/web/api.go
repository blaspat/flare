package web

import (
	"net/http"
	"time"
)

// --- Status ----------------------------------------------------------------

type statusResponse struct {
	NodeName    string `json:"node_name"`
	Version     string `json:"version"`
	ListenAddr  string `json:"listen_addr"`
	WebPort     int    `json:"web_port"`
	UptimeSecs  int64  `json:"uptime_secs"`
	IsLeader    bool   `json:"is_leader"`
	LeaderName  string `json:"leader_name,omitempty"`
	PeerCount   int    `json:"peer_count"`
	AliveCount  int    `json:"alive_count"`
	NATPublic   string `json:"nat_public_addr,omitempty"`
	NATType     string `json:"nat_type,omitempty"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	uptime := int64(time.Since(s.startTime).Seconds())

	resp := statusResponse{
		NodeName:   s.nodeName,
		Version:    "dev",
		ListenAddr: s.cfg.Node.Listen,
		WebPort:    s.cfg.Node.WebPort,
		UptimeSecs: uptime,
	}

	if s.hub != nil {
		peers := s.hub.List()
		resp.PeerCount = len(peers)
		alive := 0
		for _, p := range peers {
			if p.IsAlive() {
				alive++
			}
		}
		resp.AliveCount = alive
	}

	if s.cm != nil {
		resp.IsLeader = s.cm.IsLeader()
	}

	// Eligible leader: self if we're the only node, or lowest-name wins.
	// Determine leader name from peer set.
	if s.hub != nil {
		names := s.hub.ListNames()
		leader := s.nodeName
		for _, n := range names {
			if n < leader {
				leader = n
			}
		}
		resp.LeaderName = leader
	}

	writeJSON(w, http.StatusOK, resp)
}

// --- Peers -----------------------------------------------------------------

type peerInfo struct {
	Name      string `json:"name"`
	Addr      string `json:"addr"`
	Connected string `json:"connected"`
	LastHeard string `json:"last_heard"`
	Alive     bool   `json:"alive"`
}

type peersResponse struct {
	Peers []peerInfo `json:"peers"`
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	resp := peersResponse{Peers: []peerInfo{}}

	if s.hub == nil {
		writeJSON(w, http.StatusOK, resp)
		return
	}

	for _, p := range s.hub.List() {
		info := peerInfo{
			Name:      p.Name,
			Addr:      p.Addr,
			Connected: p.Connected.Format(time.RFC3339),
			Alive:     p.IsAlive(),
		}
		if !p.LastHeard.IsZero() {
			info.LastHeard = p.LastHeard.Format(time.RFC3339)
		}
		resp.Peers = append(resp.Peers, info)
	}

	writeJSON(w, http.StatusOK, resp)
}

// --- Sync ------------------------------------------------------------------

type trackedFileInfo struct {
	Path    string `json:"path"`
	Tag     string `json:"tag"`
	Size    int64  `json:"size"`
	Hash    string `json:"hash"`
	Version uint64 `json:"version"`
}

type conflictInfo struct {
	Path         string `json:"path"`
	Tag          string `json:"tag"`
	ConflictPath string `json:"conflict_path"`
	IncomingNode string `json:"incoming_node"`
	Timestamp    string `json:"timestamp"`
}

type syncResponse struct {
	WatchDirs     []watchDirInfo    `json:"watch_dirs"`
	TrackedFiles  []trackedFileInfo `json:"tracked_files"`
	Conflicts     []conflictInfo    `json:"conflicts"`
	CdcEnabled    bool              `json:"cdc_enabled"`
}

type watchDirInfo struct {
	Path string `json:"path"`
	Tag  string `json:"tag"`
}

func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	resp := syncResponse{
		WatchDirs:    []watchDirInfo{},
		TrackedFiles: []trackedFileInfo{},
		Conflicts:    []conflictInfo{},
	}

	// Watch dirs from config
	for _, wd := range s.cfg.Sync.WatchDirs {
		resp.WatchDirs = append(resp.WatchDirs, watchDirInfo{Path: wd.Path, Tag: wd.Tag})
	}

	// Tracked files from file tracker (via transfer manager's tracker)
	if s.tm != nil {
		// Conflicts
		for _, c := range s.tm.Conflicts() {
			resp.Conflicts = append(resp.Conflicts, conflictInfo{
				Path:         c.Path,
				Tag:          c.Tag,
				ConflictPath: c.ConflictPath,
				IncomingNode: c.IncomingNode,
				Timestamp:    c.Timestamp.Format(time.RFC3339),
			})
		}

		resp.CdcEnabled = true // default: always enabled
	}

	writeJSON(w, http.StatusOK, resp)
}

// --- Cron ------------------------------------------------------------------

type cronJobInfo struct {
	Name       string `json:"name"`
	Command    string `json:"command"`
	Schedule   string `json:"schedule"`
	TimeoutSec int    `json:"timeout_secs"`
}

type cronHistoryInfo struct {
	Name        string  `json:"name"`
	Success     bool    `json:"success"`
	ErrMsg      string  `json:"err_msg,omitempty"`
	FiredAt     string  `json:"fired_at"`
	DurationSec float64 `json:"duration_secs"`
	Retry       int     `json:"retry_attempt"`
	LeaderNode  string  `json:"leader_node"`
}

type cronResponse struct {
	IsLeader bool             `json:"is_leader"`
	Jobs     []cronJobInfo    `json:"jobs"`
	History  []cronHistoryInfo `json:"history"`
}

func (s *Server) handleCron(w http.ResponseWriter, r *http.Request) {
	resp := cronResponse{
		Jobs:    []cronJobInfo{},
		History: []cronHistoryInfo{},
	}

	if s.cm != nil {
		resp.IsLeader = s.cm.IsLeader()

		for _, j := range s.cm.Jobs() {
			resp.Jobs = append(resp.Jobs, cronJobInfo{
				Name:       j.Name,
				Command:    j.Command,
				Schedule:   scheduleString(j.Schedule),
				TimeoutSec: int(j.Timeout.Seconds()),
			})
		}

		for _, h := range s.cm.History() {
			durSec := h.Duration.Seconds()
			info := cronHistoryInfo{
				Name:        h.Name,
				Success:     h.Success,
				FiredAt:     h.FiredAt.Format(time.RFC3339),
				DurationSec: durSec,
				Retry:       h.RetryAttempt,
				LeaderNode:  h.LeaderNode,
			}
			if h.ErrMsg != "" {
				info.ErrMsg = h.ErrMsg
			}
			resp.History = append(resp.History, info)
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

// --- Snapshot (used by WS) -------------------------------------------------

// Snapshot bundles all state into one payload for WS push.
type Snapshot struct {
	Status statusResponse  `json:"status"`
	Peers  []peerInfo      `json:"peers"`
	Sync   syncResponse    `json:"sync"`
	Cron   cronResponse    `json:"cron"`
}

func (s *Server) buildSnapshot() *Snapshot {
	uptime := int64(time.Since(s.startTime).Seconds())

	status := statusResponse{
		NodeName:   s.nodeName,
		Version:    "dev",
		ListenAddr: s.cfg.Node.Listen,
		WebPort:    s.cfg.Node.WebPort,
		UptimeSecs: uptime,
	}

	if s.hub != nil {
		peers := s.hub.List()
		status.PeerCount = len(peers)
		alive := 0
		for _, p := range peers {
			if p.IsAlive() {
				alive++
			}
		}
		status.AliveCount = alive

		names := s.hub.ListNames()
		leader := s.nodeName
		for _, n := range names {
			if n < leader {
				leader = n
			}
		}
		status.LeaderName = leader
	}

	if s.cm != nil {
		status.IsLeader = s.cm.IsLeader()
	}

	// Peers
	peers := []peerInfo{}
	if s.hub != nil {
		for _, p := range s.hub.List() {
			info := peerInfo{
				Name:      p.Name,
				Addr:      p.Addr,
				Connected: p.Connected.Format(time.RFC3339),
				Alive:     p.IsAlive(),
			}
			if !p.LastHeard.IsZero() {
				info.LastHeard = p.LastHeard.Format(time.RFC3339)
			}
			peers = append(peers, info)
		}
	}

	// Sync
	syncResp := syncResponse{
		WatchDirs:    []watchDirInfo{},
		TrackedFiles: []trackedFileInfo{},
		Conflicts:    []conflictInfo{},
	}
	for _, wd := range s.cfg.Sync.WatchDirs {
		syncResp.WatchDirs = append(syncResp.WatchDirs, watchDirInfo{Path: wd.Path, Tag: wd.Tag})
	}
	if s.tm != nil {
		for _, c := range s.tm.Conflicts() {
			syncResp.Conflicts = append(syncResp.Conflicts, conflictInfo{
				Path:         c.Path,
				Tag:          c.Tag,
				ConflictPath: c.ConflictPath,
				IncomingNode: c.IncomingNode,
				Timestamp:    c.Timestamp.Format(time.RFC3339),
			})
		}
		syncResp.CdcEnabled = true
	}

	// Cron
	cronResp := cronResponse{
		Jobs:    []cronJobInfo{},
		History: []cronHistoryInfo{},
	}
	if s.cm != nil {
		cronResp.IsLeader = s.cm.IsLeader()
		for _, j := range s.cm.Jobs() {
			cronResp.Jobs = append(cronResp.Jobs, cronJobInfo{
				Name:       j.Name,
				Command:    j.Command,
				Schedule:   scheduleString(j.Schedule),
				TimeoutSec: int(j.Timeout.Seconds()),
			})
		}
		for _, h := range s.cm.History() {
			info := cronHistoryInfo{
				Name:        h.Name,
				Success:     h.Success,
				FiredAt:     h.FiredAt.Format(time.RFC3339),
				DurationSec: h.Duration.Seconds(),
				Retry:       h.RetryAttempt,
				LeaderNode:  h.LeaderNode,
			}
			if h.ErrMsg != "" {
				info.ErrMsg = h.ErrMsg
			}
			cronResp.History = append(cronResp.History, info)
		}
	}

	return &Snapshot{
		Status: status,
		Peers:  peers,
		Sync:   syncResp,
		Cron:   cronResp,
	}
}
