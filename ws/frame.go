package ws

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
)

// OpCode 表示 WebSocket 帧操作码（RFC 6455 §5.2）。
type OpCode byte

const (
	OpContinuation OpCode = 0x0
	OpText         OpCode = 0x1
	OpBinary       OpCode = 0x2
	OpClose        OpCode = 0x8
	OpPing         OpCode = 0x9
	OpPong         OpCode = 0xA
)

// IsControl 报告操作码是否为控制帧。
func (op OpCode) IsControl() bool { return op >= 0x8 }

// IsData 报告操作码是否为数据帧（文本/二进制）。
func (op OpCode) IsData() bool { return op == OpText || op == OpBinary }

// IsReserved 报告操作码是否为保留值。
func (op OpCode) IsReserved() bool {
	switch op {
	case OpContinuation, OpText, OpBinary, OpClose, OpPing, OpPong:
		return false
	}
	return true
}

// MaxControlPayload 是控制帧负载的协议上限。
const MaxControlPayload = 125

// DefaultMaxFramePayload 是默认单帧负载上限（16 MiB）。
const DefaultMaxFramePayload = 16 << 20

// 帧解析相关错误。
var (
	ErrFrameTooLarge   = errors.New("ws: frame payload exceeds limit")
	ErrInvalidLength   = errors.New("ws: non-minimal or invalid length encoding")
	ErrReservedOpCode  = errors.New("ws: reserved opcode")
	ErrControlFragment = errors.New("ws: fragmented control frame")
	ErrControlTooLarge = errors.New("ws: control frame payload exceeds 125 bytes")
)

// Frame 是一个解析后的 WebSocket 帧。
type Frame struct {
	Fin     bool
	RSV1    bool // 压缩扩展标记（permessage-deflate）
	RSV2    bool
	RSV3    bool
	OpCode  OpCode
	Masked  bool
	MaskKey [4]byte
	Payload []byte
}

// FrameParser 增量帧解析器：支持任意截断、分次到达的输入，
// 输入不完整时等待后续字节，不越界读取、不误判。
type FrameParser struct {
	maxPayload uint64
	buf        []byte
}

// NewFrameParser 创建增量帧解析器。maxPayload 为单帧负载上限，0 用默认值。
func NewFrameParser(maxPayload uint64) *FrameParser {
	if maxPayload == 0 {
		maxPayload = DefaultMaxFramePayload
	}
	return &FrameParser{maxPayload: maxPayload}
}

// Feed 追加输入字节，返回解析出的完整帧。
// 输入不完整时返回空切片与 nil 错误，等待后续 Feed。
// 返回错误后解析器状态不可再用。
func (p *FrameParser) Feed(b []byte) ([]*Frame, error) {
	p.buf = append(p.buf, b...)
	var frames []*Frame
	for {
		f, n, err := p.tryParse()
		if err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
		frames = append(frames, f)
		// 丢弃已消费字节，保持内存有界。
		p.buf = append([]byte(nil), p.buf[n:]...)
	}
	return frames, nil
}

// Buffered 返回当前缓冲的未解析字节数（用于测试与观测）。
func (p *FrameParser) Buffered() int { return len(p.buf) }

// tryParse 尝试从 buf 头部解析一个帧。
// 返回 (nil, 0, nil) 表示数据不足，需要更多字节。
func (p *FrameParser) tryParse() (*Frame, int, error) {
	const headerLen = 2
	if len(p.buf) < headerLen {
		return nil, 0, nil
	}
	b0, b1 := p.buf[0], p.buf[1]

	f := &Frame{
		Fin:    b0&0x80 != 0,
		RSV1:   b0&0x40 != 0,
		RSV2:   b0&0x20 != 0,
		RSV3:   b0&0x10 != 0,
		OpCode: OpCode(b0 & 0x0F),
		Masked: b1&0x80 != 0,
	}
	if f.OpCode.IsReserved() {
		return nil, 0, fmt.Errorf("%w: 0x%x", ErrReservedOpCode, byte(f.OpCode))
	}

	payloadLen := uint64(b1 & 0x7F)
	offset := headerLen
	switch payloadLen {
	case 126:
		if len(p.buf) < offset+2 {
			return nil, 0, nil
		}
		payloadLen = uint64(binary.BigEndian.Uint16(p.buf[offset:]))
		offset += 2
		if payloadLen < 126 {
			return nil, 0, fmt.Errorf("%w: 16-bit length %d < 126", ErrInvalidLength, payloadLen)
		}
	case 127:
		if len(p.buf) < offset+8 {
			return nil, 0, nil
		}
		payloadLen = binary.BigEndian.Uint64(p.buf[offset:])
		offset += 8
		if payloadLen < 1<<16 {
			return nil, 0, fmt.Errorf("%w: 64-bit length %d < 65536", ErrInvalidLength, payloadLen)
		}
		if payloadLen>>63 != 0 {
			return nil, 0, fmt.Errorf("%w: 64-bit length MSB set", ErrInvalidLength)
		}
	}

	if payloadLen > p.maxPayload {
		return nil, 0, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, payloadLen, p.maxPayload)
	}
	if f.OpCode.IsControl() {
		if !f.Fin {
			return nil, 0, ErrControlFragment
		}
		if payloadLen > MaxControlPayload {
			return nil, 0, ErrControlTooLarge
		}
	}

	if f.Masked {
		if len(p.buf) < offset+4 {
			return nil, 0, nil
		}
		copy(f.MaskKey[:], p.buf[offset:offset+4])
		offset += 4
	}

	if payloadLen > uint64(len(p.buf)-offset) {
		return nil, 0, nil // 负载未收齐，等待更多字节
	}
	payload := make([]byte, payloadLen)
	copy(payload, p.buf[offset:offset+int(payloadLen)])
	offset += int(payloadLen)

	if f.Masked {
		applyMask(payload, f.MaskKey)
	}
	f.Payload = payload
	return f, offset, nil
}

// applyMask 原地异或掩码。
func applyMask(payload []byte, key [4]byte) {
	for i := range payload {
		payload[i] ^= key[i&3]
	}
}

// MarshalFrame 将帧序列化为字节流。masked 为 true 时使用随机掩码。
// 控制帧约束（不分片、负载 ≤125）在此强制校验。
func MarshalFrame(f *Frame, masked bool) ([]byte, error) {
	if f.OpCode.IsReserved() {
		return nil, fmt.Errorf("%w: 0x%x", ErrReservedOpCode, byte(f.OpCode))
	}
	if f.OpCode.IsControl() {
		if !f.Fin {
			return nil, ErrControlFragment
		}
		if len(f.Payload) > MaxControlPayload {
			return nil, ErrControlTooLarge
		}
	}

	n := len(f.Payload)
	headerLen := 2
	switch {
	case n >= 1<<16:
		headerLen += 8
	case n > 125:
		headerLen += 2
	}
	if masked {
		headerLen += 4
	}

	out := make([]byte, headerLen+n)
	b0 := byte(f.OpCode)
	if f.Fin {
		b0 |= 0x80
	}
	if f.RSV1 {
		b0 |= 0x40
	}
	if f.RSV2 {
		b0 |= 0x20
	}
	if f.RSV3 {
		b0 |= 0x10
	}
	out[0] = b0

	var maskBit byte
	if masked {
		maskBit = 0x80
	}
	offset := 2
	switch {
	case n >= 1<<16:
		out[1] = maskBit | 127
		binary.BigEndian.PutUint64(out[2:], uint64(n))
		offset = 10
	case n > 125:
		out[1] = maskBit | 126
		binary.BigEndian.PutUint16(out[2:], uint16(n))
		offset = 4
	default:
		out[1] = maskBit | byte(n)
	}

	payload := out[headerLen:]
	copy(payload, f.Payload)
	if masked {
		var key [4]byte
		if _, err := rand.Read(key[:]); err != nil {
			return nil, err
		}
		copy(out[offset:offset+4], key[:])
		applyMask(payload, key)
	}
	return out, nil
}
