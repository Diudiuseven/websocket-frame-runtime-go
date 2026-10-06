package ws

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// ErrSendTimeout is returned by Send when the send queue stayed full for
// longer than Config.SendTimeout. The connection is then terminated with
// ReasonSendQueueFull.
var ErrSendTimeout = errors.New("ws: send queue full beyond configured timeout")

// Config tunes a Conn.
type Config struct {
	// MaxFramePayload bounds a single frame's payload (default 1 MiB).
	MaxFramePayload int64
	// MaxMessageSize bounds a reassembled message (default 16 MiB).
	MaxMessageSize int64
	// SendQueueSize bounds queued outbound messages (default 64).
	SendQueueSize int
	// SendTimeout bounds how long Send waits for queue space. Zero means
	// block until space or close. On expiry the send fails with
	// ErrSendTimeout and the connection ends with ReasonSendQueueFull.
	SendTimeout time.Duration
	// WriteTimeout bounds a single socket write (default 10s).
	WriteTimeout time.Duration
	// CloseTimeout bounds waiting for the peer's close reply during the
	// closing handshake (default 5s).
	CloseTimeout time.Duration
	// Heartbeat configures keepalive; nil disables it.
	Heartbeat *HeartbeatConfig
	// Compress configures permessage-deflate; nil disables it.
	Compress *CompressConfig
	// IsServer enforces mask rules: servers require masked inbound frames
	// and never mask outbound ones.
	IsServer bool
	// CompressionActive reports whether negotiation already succeeded
	// (set by the handshake layer). It applies symmetrically to both
	// directions.
	CompressionActive bool
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.MaxFramePayload <= 0 {
		out.MaxFramePayload = 1 << 20
	}
	if out.MaxMessageSize <= 0 {
		out.MaxMessageSize = 16 << 20
	}
	if out.SendQueueSize <= 0 {
		out.SendQueueSize = 64
	}
	if out.WriteTimeout <= 0 {
		out.WriteTimeout = 10 * time.Second
	}
	if out.CloseTimeout <= 0 {
		out.CloseTimeout = 5 * time.Second
	}
	return out
}

// outMsg is one logical outbound message queued for the write loop.
type outMsg struct {
	op       Opcode
	data     []byte
	fragSize int // 0 = unfragmented
}

// Conn is a concurrent-safe WebSocket connection with separated read and
// write paths. Send may be called from any number of goroutines; frames
// of one logical message are never interleaved with another's.
type Conn struct {
	cfg  Config
	rwc  net.Conn
	done chan struct{}

	parser      *FrameParser
	reassembler *Reassembler
	comp        *compressor // shared by both directions, each direction single-goroutine

	readCh chan *Message
	sendCh chan *outMsg
	stopW  chan struct{}

	writeMu sync.Mutex // serializes all socket writes

	mu        sync.Mutex
	closed    bool
	closeSent bool
	reason    CloseReason
	code      CloseCode
	text      string
	cause     error
	closeErr  *CloseError

	hb *heartbeat

	onCloseMu sync.Mutex
	onClose   func(*CloseError)
}

// NewConn wraps rwc (an established, handshaken connection) and starts
// the read and write loops.
func NewConn(rwc net.Conn, cfg Config) *Conn {
	c := &Conn{
		cfg:         cfg.withDefaults(),
		rwc:         rwc,
		done:        make(chan struct{}),
		parser:      NewFrameParser(cfg.withDefaults().MaxFramePayload),
		reassembler: NewReassembler(cfg.withDefaults().MaxMessageSize),
		readCh:      make(chan *Message, 16),
		sendCh:      make(chan *outMsg, cfg.withDefaults().SendQueueSize),
		stopW:       make(chan struct{}),
	}
	if c.cfg.CompressionActive && c.cfg.Compress != nil {
		c.comp = newCompressor(c.cfg.Compress.Level)
	}
	if c.cfg.Heartbeat != nil {
		c.hb = newHeartbeat(*c.cfg.Heartbeat,
			func(payload []byte) error {
				return c.writeFrame(&Frame{Fin: true, Opcode: OpPing, Masked: !c.cfg.IsServer, Payload: payload})
			},
			func() { c.terminate(ReasonHeartbeatTimeout, CloseGoingAway, "heartbeat timeout", nil) },
		)
		go c.hb.run()
	}
	go c.readLoop()
	go c.writeLoop()
	return c
}

// Send queues a data message (OpText or OpBinary) for transmission. It
// blocks while the queue is full, bounded by Config.SendTimeout, and
// returns the CloseError once the connection has ended.
func (c *Conn) Send(op Opcode, data []byte) error { return c.enqueue(op, data, 0) }

// SendFragmented queues a message split into fragments of fragSize bytes.
// The fragments are written as one atomic unit: no other producer's
// frames can interleave.
func (c *Conn) SendFragmented(op Opcode, data []byte, fragSize int) error {
	return c.enqueue(op, data, fragSize)
}

func (c *Conn) enqueue(op Opcode, data []byte, fragSize int) error {
	if !op.IsData() {
		return errors.New("ws: Send only accepts OpText or OpBinary")
	}
	if fragSize < 0 {
		fragSize = 0
	}
	m := &outMsg{op: op, data: data, fragSize: fragSize}
	// Fast path: once terminated, never accept new messages even if the
	// queue still has room (select would otherwise choose randomly).
	select {
	case <-c.done:
		return c.closeError()
	default:
	}
	if c.cfg.SendTimeout <= 0 {
		select {
		case c.sendCh <- m:
			return nil
		case <-c.done:
			return c.closeError()
		}
	}
	t := time.NewTimer(c.cfg.SendTimeout)
	defer t.Stop()
	select {
	case c.sendCh <- m:
		return nil
	case <-c.done:
		return c.closeError()
	case <-t.C:
		c.terminate(ReasonSendQueueFull, ClosePolicyViolation, "send queue full", ErrSendTimeout)
		return ErrSendTimeout
	}
}

// ReadMessage blocks until a full message arrives or the connection ends.
func (c *Conn) ReadMessage() (*Message, error) {
	select {
	case m := <-c.readCh:
		return m, nil
	case <-c.done:
		return nil, c.closeError()
	}
}

// Close initiates the close handshake: a close frame is sent and the
// connection terminates when the peer replies or CloseTimeout elapses.
// It is idempotent and safe to call concurrently; only the first call
// determines the close reason.
func (c *Conn) Close() error {
	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		return err
	}
	if !c.closeSent {
		c.closeSent = true
		c.reason = ReasonLocalClose
		c.code = CloseNormalClosure
	}
	c.mu.Unlock()

	// Best-effort close frame; ignore write errors, teardown follows.
	_ = c.writeFrame(&Frame{Fin: true, Opcode: OpClose, Masked: !c.cfg.IsServer,
		Payload: encodeClosePayload(CloseNormalClosure, "")})

	select {
	case <-c.done:
	case <-time.After(c.cfg.CloseTimeout):
		c.terminate(ReasonLocalClose, CloseNormalClosure, "close handshake timeout", nil)
	}
	return c.closeError()
}

// CloseReason reports why the connection ended, once it did.
func (c *Conn) CloseReason() CloseReason {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason
}

// Done closes when the connection is fully terminated.
func (c *Conn) Done() <-chan struct{} { return c.done }

// OnClose registers a callback invoked exactly once at termination.
// If the connection already ended, fn runs immediately.
func (c *Conn) OnClose(fn func(*CloseError)) {
	c.onCloseMu.Lock()
	c.onClose = fn
	c.onCloseMu.Unlock()
	select {
	case <-c.done:
		fn(c.closeError())
	default:
	}
}

// closeError returns the terminal CloseError, or nil while open.
func (c *Conn) closeError() *CloseError {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}

// terminate ends the connection exactly once. It best-effort sends a
// close frame (unless the write side already failed), stops the loops,
// closes the socket and releases blocked senders/readers.
func (c *Conn) terminate(reason CloseReason, code CloseCode, text string, cause error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	if c.reason == ReasonUnknown || reason != ReasonPeerClose {
		// A peer close never overrides a reason we already determined
		// (e.g. local close or heartbeat timeout racing the reply).
		c.reason = reason
	}
	c.code = code
	c.text = text
	c.cause = cause
	c.closeErr = &CloseError{Reason: c.reason, Code: code, Text: text, Err: cause}
	sendClose := !c.closeSent && reason != ReasonWriteError
	c.closeSent = true
	closeErr := c.closeErr
	c.mu.Unlock()

	if sendClose {
		// Best effort: if the writer is stuck (e.g. dead peer, full
		// buffers) do not block teardown behind it.
		_ = c.writeFrameTry(&Frame{Fin: true, Opcode: OpClose, Masked: !c.cfg.IsServer,
			Payload: encodeClosePayload(code, text)})
	}
	if c.hb != nil {
		c.hb.stopLoop()
	}
	close(c.done)
	close(c.stopW)
	_ = c.rwc.Close()

	c.onCloseMu.Lock()
	fn := c.onClose
	c.onCloseMu.Unlock()
	if fn != nil {
		fn(closeErr)
	}
}

// writeFrame is the single serialized socket write path. It never emits
// partial frames: EncodeFrame builds the full buffer first, and the loop
// below writes it completely or fails the connection.
func (c *Conn) writeFrame(f *Frame) error {
	data, err := EncodeFrame(f)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.rwc.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	for len(data) > 0 {
		n, err := c.rwc.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// writeFrameTry is like writeFrame but never blocks behind a stuck
// writer: if the write path is busy it reports failure immediately.
// Used for best-effort close frames during teardown.
func (c *Conn) writeFrameTry(f *Frame) error {
	data, err := EncodeFrame(f)
	if err != nil {
		return err
	}
	if !c.writeMu.TryLock() {
		return errors.New("ws: write path busy")
	}
	defer c.writeMu.Unlock()
	_ = c.rwc.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	for len(data) > 0 {
		n, err := c.rwc.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// writeLoop is the only consumer of sendCh, so outbound frames of
// different producers never interleave and compression context stays
// consistent.
func (c *Conn) writeLoop() {
	for {
		select {
		case m := <-c.sendCh:
			if err := c.writeMessage(m); err != nil {
				c.terminate(ReasonWriteError, CloseAbnormalClosure, "write failed", err)
				return
			}
		case <-c.stopW:
			// Converge: drop queued messages; blocked senders were
			// already released by done.
			return
		}
	}
}

// writeMessage compresses (if negotiated), fragments and writes one
// logical message atomically with respect to other producers.
func (c *Conn) writeMessage(m *outMsg) error {
	payload := m.data
	rsv1 := false
	if c.comp != nil && len(payload) >= c.cfg.Compress.MinSize {
		cp, err := c.comp.Compress(payload)
		if err != nil {
			return err
		}
		payload = cp
		rsv1 = true
	}

	masked := !c.cfg.IsServer
	if m.fragSize <= 0 || len(payload) <= m.fragSize {
		return c.writeFrame(&Frame{Fin: true, RSV1: rsv1, Opcode: m.op, Masked: masked, Payload: payload})
	}
	for off := 0; off < len(payload); off += m.fragSize {
		end := off + m.fragSize
		if end > len(payload) {
			end = len(payload)
		}
		f := &Frame{
			Fin:     end == len(payload),
			RSV1:    rsv1 && off == 0,
			Opcode:  OpContinuation,
			Masked:  masked,
			Payload: payload[off:end],
		}
		if off == 0 {
			f.Opcode = m.op
		}
		if err := c.writeFrame(f); err != nil {
			return err
		}
	}
	return nil
}

// readLoop owns the socket read side.
func (c *Conn) readLoop() {
	buf := make([]byte, 32*1024)
	for {
		n, err := c.rwc.Read(buf)
		if n > 0 {
			if !c.handleBytes(buf[:n]) {
				return
			}
		}
		if err != nil {
			select {
			case <-c.done:
				// Teardown already in progress; nothing to report.
			default:
				if err == io.EOF || errors.Is(err, net.ErrClosed) {
					c.terminate(ReasonReadError, CloseAbnormalClosure, "connection lost", err)
				} else {
					c.terminate(ReasonReadError, CloseAbnormalClosure, "read failed", err)
				}
			}
			return
		}
	}
}

// handleBytes feeds received bytes through parser and reassembler. It
// reports false when the connection was terminated.
func (c *Conn) handleBytes(chunk []byte) bool {
	frames, err := c.parser.Feed(chunk)
	if err != nil {
		c.terminate(ReasonReadError, CloseProtocolError, err.Error(), err)
		return false
	}
	for _, f := range frames {
		if !c.handleFrame(f) {
			return false
		}
	}
	return true
}

func (c *Conn) handleFrame(f *Frame) bool {
	// Mask rules depend on role (RFC 6455 §5.3).
	if c.cfg.IsServer && !f.Masked {
		c.terminate(ReasonReadError, CloseProtocolError, ErrMaskRequired.Error(), ErrMaskRequired)
		return false
	}
	if !c.cfg.IsServer && f.Masked {
		c.terminate(ReasonReadError, CloseProtocolError, ErrMaskForbidden.Error(), ErrMaskForbidden)
		return false
	}
	// Extension checks: RSV1 only on the first frame of a data message,
	// and only when compression was negotiated.
	if f.RSV1 {
		if !c.cfg.CompressionActive {
			c.terminate(ReasonExtensionRejected, CloseProtocolError, ErrExtensionNotNegotiated.Error(), ErrExtensionNotNegotiated)
			return false
		}
		if !f.Opcode.IsData() {
			c.terminate(ReasonReadError, CloseProtocolError, "RSV1 on non-data frame", ErrReservedBits)
			return false
		}
	}

	msg, err := c.reassembler.Push(f)
	if err != nil {
		code := CloseProtocolError
		if err == ErrMessageTooLarge {
			code = CloseMessageTooBig
		}
		c.terminate(ReasonReadError, code, err.Error(), err)
		return false
	}
	if msg == nil {
		return true // fragmented message still incomplete
	}

	switch msg.Opcode {
	case OpPing:
		if c.hb != nil {
			c.hb.noteActivity()
		}
		_ = c.writeFrame(&Frame{Fin: true, Opcode: OpPong, Masked: !c.cfg.IsServer, Payload: msg.Data})
	case OpPong:
		if c.hb != nil {
			c.hb.notePong()
		}
	case OpClose:
		code, text, derr := decodeClosePayload(msg.Data)
		if derr != nil {
			c.terminate(ReasonReadError, CloseProtocolError, derr.Error(), derr)
			return false
		}
		c.terminate(ReasonPeerClose, code, text, nil)
		return false
	default: // data message
		if c.hb != nil {
			c.hb.noteActivity()
		}
		if msg.Compressed {
			if c.comp == nil {
				c.terminate(ReasonExtensionRejected, CloseProtocolError, ErrExtensionNotNegotiated.Error(), ErrExtensionNotNegotiated)
				return false
			}
			data, derr := c.comp.Decompress(msg.Data, c.cfg.MaxMessageSize)
			if derr != nil {
				c.terminate(ReasonReadError, CloseInvalidFramePayloadData, derr.Error(), derr)
				return false
			}
			msg.Data = data
		}
		select {
		case c.readCh <- msg:
		case <-c.done:
			return false
		}
	}
	return true
}

var _ io.Closer = (*Conn)(nil)
