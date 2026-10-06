package ws

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// 连接级错误。
var (
	ErrConnClosed    = errors.New("ws: connection closed")
	ErrSendQueueFull = errors.New("ws: send queue full")
)

// 默认配置值。
const (
	defaultSendQueueSize   = 64
	defaultCloseTimeout    = 5 * time.Second
	defaultHeartbeatTimout = 10 * time.Second
	defaultHeartbeatMissed = 3
	readChunkSize          = 32 << 10
)

// Config 是连接运行时配置。
type Config struct {
	// MaxFramePayload 单帧负载上限（字节），0 用默认值 16MiB。
	// 发送侧也按该值对消息分片。
	MaxFramePayload uint64
	// MaxMessageSize 单条重组消息上限（字节），0 用默认值 64MiB。
	MaxMessageSize uint64
	// HeartbeatInterval 心跳探测周期，<=0 表示禁用心跳。
	HeartbeatInterval time.Duration
	// HeartbeatTimeout 保留字段：单次心跳响应超时（由 MaxMissed 机制覆盖）。
	HeartbeatTimeout time.Duration
	// HeartbeatMaxMissed 连续缺失响应达到该次数后终止连接，0 用默认值 3。
	HeartbeatMaxMissed int
	// SendQueueSize 发送队列容量（帧数），0 用默认值 64。
	SendQueueSize int
	// SendBlock 为 true 时队列满则阻塞直到有空间或连接关闭；
	// false 时立即返回 ErrSendQueueFull。
	SendBlock bool
	// WriteTimeout 单次底层写超时，<=0 表示不限制。
	WriteTimeout time.Duration
	// CloseTimeout 关闭握手等待对端回应的超时，0 用默认值 5s。
	CloseTimeout time.Duration
	// Compression 压缩扩展配置。
	Compression CompressConfig
	// IsClient 决定发送帧是否掩码（客户端必须掩码）。
	IsClient bool
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.MaxFramePayload == 0 {
		out.MaxFramePayload = DefaultMaxFramePayload
	}
	if out.MaxMessageSize == 0 {
		out.MaxMessageSize = DefaultMaxMessageSize
	}
	if out.SendQueueSize == 0 {
		out.SendQueueSize = defaultSendQueueSize
	}
	if out.CloseTimeout == 0 {
		out.CloseTimeout = defaultCloseTimeout
	}
	if out.HeartbeatTimeout == 0 {
		out.HeartbeatTimeout = defaultHeartbeatTimout
	}
	if out.HeartbeatMaxMissed == 0 {
		out.HeartbeatMaxMissed = defaultHeartbeatMissed
	}
	return out
}

// sendRequest 是一个待写出的已序列化帧。
// final 非空表示写出该帧后应立即以该事件终止连接
// （用于关闭握手：保证回应的关闭帧真正落盘后再终止）。
type sendRequest struct {
	data  []byte
	final *CloseEvent
}

type readResult struct {
	msg *Message
}

// Conn 是一条 WebSocket 连接运行时：读写分离，单写者循环保证
// 多生产者并发发送时帧不交叉；发送队列有界，背压行为可配置。
type Conn struct {
	cfg Config
	nc  net.Conn
	ext extNegotiation

	sendCh  chan sendRequest
	msgCh   chan readResult
	done    chan struct{}
	eventCh chan CloseEvent

	terminateOnce sync.Once
	closeSentOnce sync.Once

	pongsOutstanding atomic.Int32
	closeInitiated   atomic.Bool

	closeTimer *time.Timer
	timerMu    sync.Mutex
}

// NewConn 在已完成 WebSocket 握手的 net.Conn 上创建运行时。
// ext 为扩展协商结果（客户端用 checkExtension、服务端用 acceptExtension 获得）。
func NewConn(nc net.Conn, cfg Config, ext extNegotiation) *Conn {
	cfg = cfg.withDefaults()
	return &Conn{
		cfg:     cfg,
		nc:      nc,
		ext:     ext,
		sendCh:  make(chan sendRequest, cfg.SendQueueSize),
		msgCh:   make(chan readResult, 16),
		done:    make(chan struct{}),
		eventCh: make(chan CloseEvent, 1),
	}
}

// Run 启动写循环与心跳循环，并在当前 goroutine 执行读循环，
// 阻塞直到连接终止。
func (c *Conn) Run() {
	go c.writeLoop()
	if c.cfg.HeartbeatInterval > 0 {
		go c.heartbeatLoop()
	}
	c.readLoop()
}

// Closed 返回关闭事件通道：连接终止时收到恰好一个 CloseEvent 后通道关闭。
func (c *Conn) Closed() <-chan CloseEvent { return c.eventCh }

// Done 返回终止信号通道（用于在 select 中等待连接结束）。
func (c *Conn) Done() <-chan struct{} { return c.done }

// ---- 发送路径 ----

// Send 发送一条完整消息，必要时压缩并分片。并发安全。
func (c *Conn) Send(op OpCode, payload []byte) error {
	if !op.IsData() {
		return fmt.Errorf("ws: Send only accepts data opcodes, got 0x%x", byte(op))
	}
	if uint64(len(payload)) > c.cfg.MaxMessageSize {
		return fmt.Errorf("%w: %d > %d", ErrMessageTooLarge, len(payload), c.cfg.MaxMessageSize)
	}

	rsv1 := false
	data := payload
	if c.ext.enabled && len(payload) >= c.cfg.Compression.MinSize {
		compressed, err := compressMessage(payload)
		if err != nil {
			return err
		}
		data = compressed
		rsv1 = true
	}

	// 按 MaxFramePayload 分片。
	chunk := int(c.cfg.MaxFramePayload)
	var frames []*Frame
	for off := 0; ; {
		end := off + chunk
		fin := true
		if end < len(data) {
			fin = false
		} else {
			end = len(data)
		}
		f := &Frame{Fin: fin, OpCode: OpContinuation, Payload: data[off:end]}
		if off == 0 {
			f.OpCode = op
			f.RSV1 = rsv1
		}
		frames = append(frames, f)
		if fin {
			break
		}
		off = end
	}

	for _, f := range frames {
		raw, err := MarshalFrame(f, c.cfg.IsClient)
		if err != nil {
			return err
		}
		if err := c.enqueue(raw); err != nil {
			return err
		}
	}
	return nil
}

// Ping 发送一次心跳探测帧。响应由心跳机制跟踪。
func (c *Conn) Ping(payload []byte) error {
	return c.sendControl(OpPing, payload)
}

// sendControl 序列化并入队一个控制帧。
func (c *Conn) sendControl(op OpCode, payload []byte) error {
	raw, err := MarshalFrame(&Frame{Fin: true, OpCode: op, Payload: payload}, c.cfg.IsClient)
	if err != nil {
		return err
	}
	return c.enqueue(raw)
}

// enqueue 按背压策略入队一个已序列化帧。
func (c *Conn) enqueue(raw []byte) error {
	// 先检查终止信号，避免与已关闭的 done 竞争时误选发送分支。
	select {
	case <-c.done:
		return ErrConnClosed
	default:
	}
	req := sendRequest{data: raw}
	if c.cfg.SendBlock {
		select {
		case c.sendCh <- req:
			return nil
		case <-c.done:
			return ErrConnClosed
		}
	}
	select {
	case c.sendCh <- req:
		return nil
	case <-c.done:
		return ErrConnClosed
	default:
		return ErrSendQueueFull
	}
}

// writeLoop 是唯一写者：任一时刻不会有帧内容交叉或半帧写出。
func (c *Conn) writeLoop() {
	for {
		select {
		case req := <-c.sendCh:
			if err := c.writeRaw(req.data); err != nil {
				c.terminate(ReasonIOError, CloseAbnormal, "write error", err)
				return
			}
			if req.final != nil {
				ev := req.final
				c.terminate(ev.Reason, ev.Code, ev.Text, ev.Err)
				return
			}
		case <-c.done:
			c.drainSends()
			return
		}
	}
}

func (c *Conn) writeRaw(b []byte) error {
	if c.cfg.WriteTimeout > 0 {
		c.nc.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
		defer c.nc.SetWriteDeadline(time.Time{})
	}
	_, err := c.nc.Write(b)
	return err
}

// drainSends 在终止时收敛发送队列，保证内存有界、无残留引用。
func (c *Conn) drainSends() {
	for {
		select {
		case <-c.sendCh:
		default:
			return
		}
	}
}

// ---- 接收路径 ----

func (c *Conn) readLoop() {
	parser := NewFrameParser(c.cfg.MaxFramePayload)
	reasm := NewReassembler(c.cfg.MaxMessageSize)
	buf := make([]byte, readChunkSize)
	for {
		n, err := c.nc.Read(buf)
		if n > 0 {
			frames, perr := parser.Feed(buf[:n])
			if perr != nil {
				c.terminate(ReasonProtocolError, CloseProtocolError, perr.Error(), perr)
				return
			}
			for _, f := range frames {
				if !c.handleFrame(f, reasm) {
					return
				}
			}
		}
		if err != nil {
			select {
			case <-c.done:
			default:
				c.terminate(ReasonIOError, CloseAbnormal, "read error", err)
			}
			return
		}
	}
}

// handleFrame 处理一个帧，返回 false 表示连接已终止。
func (c *Conn) handleFrame(f *Frame, reasm *Reassembler) bool {
	// 扩展标记校验：RSV2/RSV3 一律拒绝；RSV1 仅允许已协商时的数据首帧。
	if f.RSV2 || f.RSV3 {
		c.terminate(ReasonProtocolError, CloseProtocolError, "RSV2/RSV3 set", ErrReservedOpCode)
		return false
	}
	if f.RSV1 {
		if !c.ext.enabled || !f.OpCode.IsData() {
			c.terminate(ReasonExtensionError, CloseProtocolError,
				"RSV1 set without negotiated extension", ErrExtensionNotNegotiated)
			return false
		}
	}

	switch f.OpCode {
	case OpPing:
		// 控制帧响应：回 pong。连接关闭中则忽略失败。
		_ = c.sendControl(OpPong, f.Payload)
		return true
	case OpPong:
		c.pongsOutstanding.Store(0)
		return true
	case OpClose:
		c.handleCloseFrame(f)
		return false
	}

	msg, done, err := reasm.Push(f)
	if err != nil {
		code := CloseProtocolError
		reason := ReasonProtocolError
		if errors.Is(err, ErrMessageTooLarge) {
			code = CloseMessageTooBig
			reason = ReasonMessageTooBig
		}
		c.terminate(reason, code, err.Error(), err)
		return false
	}
	if !done {
		return true
	}
	if msg.Compressed {
		payload, err := decompressMessage(msg.Payload, c.cfg.MaxMessageSize)
		if err != nil {
			c.terminate(ReasonProtocolError, CloseInvalidPayload, err.Error(), err)
			return false
		}
		msg.Payload = payload
	}
	select {
	case c.msgCh <- readResult{msg: msg}:
	case <-c.done:
		return false
	}
	return true
}

// handleCloseFrame 处理对端关闭帧：双向推进握手，幂等。
func (c *Conn) handleCloseFrame(f *Frame) {
	code, text := parseClosePayload(f.Payload)
	if c.localCloseInitiated() {
		// 本地已发起，这是对端回应：本地关闭完成。
		c.terminate(ReasonLocal, code, text, nil)
		return
	}
	// 对端主动关闭：回应关闭帧，并保证该帧写出后再终止，
	// 避免底层连接提前关闭导致回应丢失。
	ev := CloseEvent{Reason: ReasonRemote, Code: code, Text: text}
	enqueued := false
	c.closeSentOnce.Do(func() {
		payload := append([]byte{byte(code >> 8), byte(code)}, text...)
		raw, err := MarshalFrame(&Frame{Fin: true, OpCode: OpClose, Payload: payload}, c.cfg.IsClient)
		if err != nil {
			return
		}
		select {
		case c.sendCh <- sendRequest{data: raw, final: &ev}:
			enqueued = true
		case <-c.done:
		}
	})
	if !enqueued {
		c.terminate(ev.Reason, ev.Code, ev.Text, ev.Err)
	}
}

func (c *Conn) localCloseInitiated() bool { return c.closeInitiated.Load() }

// parseClosePayload 解析关闭帧负载。
func parseClosePayload(p []byte) (CloseCode, string) {
	if len(p) == 0 {
		return CloseNoStatus, ""
	}
	if len(p) == 1 {
		return CloseProtocolError, "malformed close payload"
	}
	return CloseCode(p[0])<<8 | CloseCode(p[1]), string(p[2:])
}

// ReadMessage 读取下一条重组完成的消息。
func (c *Conn) ReadMessage() (*Message, error) {
	select {
	case r := <-c.msgCh:
		return r.msg, nil
	case <-c.done:
		// 优先耗尽已排队消息。
		select {
		case r := <-c.msgCh:
			return r.msg, nil
		default:
		}
		return nil, ErrConnClosed
	}
}

// ---- 关闭与心跳 ----

// Close 发起本地关闭握手，幂等：重复或并发调用只生效一次。
func (c *Conn) Close(code CloseCode, text string) error {
	c.closeInitiated.Store(true)
	c.sendCloseFrame(code, text)
	// 启动关闭超时：对端不应答时按本地关闭收敛。
	c.timerMu.Lock()
	if c.closeTimer == nil {
		c.closeTimer = time.AfterFunc(c.cfg.CloseTimeout, func() {
			c.terminate(ReasonLocal, code, text, nil)
		})
	}
	c.timerMu.Unlock()
	return nil
}

// sendCloseFrame 恰好发送一次关闭帧。
func (c *Conn) sendCloseFrame(code CloseCode, text string) {
	c.closeSentOnce.Do(func() {
		payload := make([]byte, 0, 2+len(text))
		payload = append(payload, byte(code>>8), byte(code))
		payload = append(payload, text...)
		_ = c.sendControl(OpClose, payload)
	})
}

// heartbeatLoop 按周期发送探测，连续缺失达到阈值后终止连接。
func (c *Conn) heartbeatLoop() {
	t := time.NewTicker(c.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			missed := int(c.pongsOutstanding.Load())
			if missed >= c.cfg.HeartbeatMaxMissed {
				c.terminate(ReasonHeartbeatTimeout, CloseGoingAway,
					fmt.Sprintf("heartbeat missed %d times", missed), nil)
				return
			}
			if err := c.Ping([]byte("hb")); err != nil {
				return
			}
			c.pongsOutstanding.Add(1)
		case <-c.done:
			return
		}
	}
}

// terminate 幂等终止：只生效一次，产生唯一 CloseEvent。
func (c *Conn) terminate(reason CloseReason, code CloseCode, text string, err error) {
	c.terminateOnce.Do(func() {
		c.timerMu.Lock()
		if c.closeTimer != nil {
			c.closeTimer.Stop()
		}
		c.timerMu.Unlock()

		c.eventCh <- CloseEvent{Reason: reason, Code: code, Text: text, Err: err}
		close(c.eventCh)
		close(c.done)
		c.nc.Close()
	})
}
