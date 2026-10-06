package ws

import (
	"sync"
	"time"
)

// HeartbeatConfig controls ping/pong keepalive.
type HeartbeatConfig struct {
	// Interval between ping probes (default 30s).
	Interval time.Duration
	// Timeout waiting for a pong before counting a miss (default 10s).
	Timeout time.Duration
	// MaxMisses is the number of consecutive missed pongs that ends the
	// connection with ReasonHeartbeatTimeout (default 3).
	MaxMisses int
}

func (h *HeartbeatConfig) withDefaults() HeartbeatConfig {
	out := *h
	if out.Interval <= 0 {
		out.Interval = 30 * time.Second
	}
	if out.Timeout <= 0 {
		out.Timeout = 10 * time.Second
	}
	if out.MaxMisses <= 0 {
		out.MaxMisses = 3
	}
	return out
}

// heartbeat runs the keepalive loop for a Conn. All mutable state is
// guarded by mu; the loop and the read path (notePong/noteActivity) run
// in different goroutines.
type heartbeat struct {
	cfg HeartbeatConfig

	mu        sync.Mutex
	misses    int
	awaiting  bool
	deadline  time.Time
	lastPong  time.Time
	pongCount int64
	pingCount int64

	sendPing  func(payload []byte) error
	onTimeout func()
	stop      chan struct{}
	stopOnce  sync.Once
}

func newHeartbeat(cfg HeartbeatConfig, sendPing func([]byte) error, onTimeout func()) *heartbeat {
	return &heartbeat{cfg: cfg.withDefaults(), sendPing: sendPing, onTimeout: onTimeout, stop: make(chan struct{})}
}

// run is the keepalive loop; call in a goroutine. It sends a ping every
// Interval, and counts a miss when no pong (or other inbound traffic)
// arrives within Timeout. MaxMisses consecutive misses end the
// connection via onTimeout.
func (h *heartbeat) run() {
	ticker := time.NewTicker(h.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-h.stop:
			return
		case now := <-ticker.C:
			if h.tick(now) {
				return
			}
		}
	}
}

// tick performs one probe round. It reports whether the loop must stop
// because the connection timed out.
func (h *heartbeat) tick(now time.Time) bool {
	h.mu.Lock()
	if h.awaiting && now.After(h.deadline) {
		h.misses++
		h.awaiting = false
	}
	misses := h.misses
	maxMisses := h.cfg.MaxMisses
	h.mu.Unlock()

	if misses >= maxMisses {
		h.onTimeout()
		return true
	}

	payload := make([]byte, 8)
	// Use a monotonically increasing payload so pongs can be correlated
	// with pings for observability.
	h.mu.Lock()
	seq := h.pingCount
	h.mu.Unlock()
	for i := 0; i < 8; i++ {
		payload[i] = byte(seq >> (8 * uint(i%8)))
	}
	if err := h.sendPing(payload); err != nil {
		return true // write side is gone; conn teardown handles the rest
	}
	h.mu.Lock()
	h.pingCount++
	// Only arm the deadline when no ping is outstanding; otherwise a
	// ping interval shorter than the timeout would keep pushing the
	// deadline forward and a dead peer would never be detected.
	if !h.awaiting {
		h.awaiting = true
		h.deadline = now.Add(h.cfg.Timeout)
	}
	h.mu.Unlock()
	return false
}

// notePong records a received pong.
func (h *heartbeat) notePong() {
	h.mu.Lock()
	h.pongCount++
	h.lastPong = time.Now()
	h.awaiting = false
	h.misses = 0
	h.mu.Unlock()
}

// noteActivity resets the miss counter on any inbound data: a live peer
// sending data is proof of liveness even between pings.
func (h *heartbeat) noteActivity() {
	h.mu.Lock()
	h.misses = 0
	h.mu.Unlock()
}

// stopLoop terminates run.
func (h *heartbeat) stopLoop() { h.stopOnce.Do(func() { close(h.stop) }) }

// Stats returns (pings sent, pongs received, current consecutive misses).
func (h *heartbeat) Stats() (pings, pongs int64, misses int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.pingCount, h.pongCount, h.misses
}
