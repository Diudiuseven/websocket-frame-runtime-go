// Package ws implements a WebSocket connection runtime: frame codec,
// message reassembly, heartbeat, compression negotiation and close
// handshake.
package ws

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
)

// Opcode identifies the type of a frame as defined by RFC 6455.
type Opcode byte

const (
	OpContinuation Opcode = 0x0
	OpText         Opcode = 0x1
	OpBinary       Opcode = 0x2
	OpClose        Opcode = 0x8
	OpPing         Opcode = 0x9
	OpPong         Opcode = 0xA
)

// IsControl reports whether the opcode is a control opcode.
func (o Opcode) IsControl() bool { return o >= 0x8 }

// IsData reports whether the opcode is a data opcode (text/binary).
func (o Opcode) IsData() bool { return o == OpText || o == OpBinary }

// validOpcode reports whether the opcode is defined by RFC 6455.
func validOpcode(o Opcode) bool {
	switch o {
	case OpContinuation, OpText, OpBinary, OpClose, OpPing, OpPong:
		return true
	}
	return false
}

// Protocol errors returned by the frame parser and validator.
var (
	ErrFrameTooLarge      = errors.New("ws: frame payload exceeds configured maximum")
	ErrControlFragmented  = errors.New("ws: control frame must not be fragmented")
	ErrControlPayloadSize = errors.New("ws: control frame payload exceeds 125 bytes")
	ErrReservedOpcode     = errors.New("ws: reserved opcode")
	ErrReservedBits       = errors.New("ws: reserved bits set without negotiated extension")
	ErrMaskRequired       = errors.New("ws: client-to-server frames must be masked")
	ErrMaskForbidden      = errors.New("ws: server-to-client frames must not be masked")
	ErrInvalidLength      = errors.New("ws: invalid or non-minimal length encoding")
)

// Frame is a single WebSocket protocol frame.
type Frame struct {
	Fin     bool
	RSV1    bool // set when permessage-deflate compressed this message's first frame
	Opcode  Opcode
	Masked  bool
	MaskKey [4]byte
	Payload []byte
}

const (
	maxControlPayload  = 125
	len7BitMax         = 125
	len16Indicator     = 126
	len64Indicator     = 127
	maxFrameHeaderSize = 14 // 2 + 8 + 4
)

// FrameParser incrementally decodes frames from a byte stream. It is safe
// to feed arbitrarily truncated chunks; parsing resumes where it stopped.
// The internal buffer only ever holds one incomplete frame, so memory is
// bounded by maxPayload plus one header.
type FrameParser struct {
	maxPayload int64
	buf        []byte
}

// NewFrameParser creates a parser that rejects frames whose payload length
// exceeds maxPayload.
func NewFrameParser(maxPayload int64) *FrameParser {
	return &FrameParser{maxPayload: maxPayload}
}

// Feed appends chunk to the internal buffer and returns all frames that
// became complete. Remaining bytes are kept for the next call. A protocol
// error means the stream is unrecoverable and the connection must be
// closed.
func (p *FrameParser) Feed(chunk []byte) ([]*Frame, error) {
	if len(chunk) > 0 {
		p.buf = append(p.buf, chunk...)
	}
	var frames []*Frame
	for {
		f, n, err := parseFrame(p.buf, p.maxPayload)
		if err != nil {
			return nil, err
		}
		if n == 0 { // incomplete: wait for more bytes
			break
		}
		frames = append(frames, f)
		// Advance the buffer; copy the tail down so the backing array
		// cannot grow without bound across many feeds.
		rest := len(p.buf) - n
		copy(p.buf, p.buf[n:])
		p.buf = p.buf[:rest]
	}
	return frames, nil
}

// Buffered reports how many unparsed bytes are held.
func (p *FrameParser) Buffered() int { return len(p.buf) }

// parseFrame decodes one frame from the front of buf. It returns
// (nil, 0, nil) when buf does not yet hold a complete frame.
func parseFrame(buf []byte, maxPayload int64) (*Frame, int, error) {
	if len(buf) < 2 {
		return nil, 0, nil
	}
	b0, b1 := buf[0], buf[1]

	fin := b0&0x80 != 0
	rsv1 := b0&0x40 != 0
	if b0&0x30 != 0 { // RSV2/RSV3 have no defined extension
		return nil, 0, ErrReservedBits
	}
	op := Opcode(b0 & 0x0F)
	if !validOpcode(op) {
		return nil, 0, ErrReservedOpcode
	}

	masked := b1&0x80 != 0
	l7 := b1 & 0x7F

	header := 2
	var length uint64
	switch l7 {
	case len16Indicator:
		if len(buf) < header+2 {
			return nil, 0, nil
		}
		length = uint64(binary.BigEndian.Uint16(buf[header:]))
		if length <= len7BitMax { // must use the shortest form
			return nil, 0, ErrInvalidLength
		}
		header += 2
	case len64Indicator:
		if len(buf) < header+8 {
			return nil, 0, nil
		}
		length = binary.BigEndian.Uint64(buf[header:])
		if length <= 0xFFFF || length&(1<<63) != 0 {
			return nil, 0, ErrInvalidLength
		}
		header += 8
	default:
		length = uint64(l7)
	}

	if op.IsControl() {
		if !fin {
			return nil, 0, ErrControlFragmented
		}
		if length > maxControlPayload {
			return nil, 0, ErrControlPayloadSize
		}
	}
	if maxPayload > 0 && length > uint64(maxPayload) {
		return nil, 0, ErrFrameTooLarge
	}

	if masked {
		if len(buf) < header+4 {
			return nil, 0, nil
		}
		header += 4
	}
	total := header + int(length)
	if len(buf) < total {
		return nil, 0, nil
	}

	f := &Frame{Fin: fin, RSV1: rsv1, Opcode: op, Masked: masked}
	payload := make([]byte, length)
	copy(payload, buf[header:total])
	if masked {
		copy(f.MaskKey[:], buf[header-4:header])
		applyMask(payload, f.MaskKey)
	}
	f.Payload = payload
	return f, total, nil
}

// applyMask XORs b in place with the 4-byte masking key (RFC 6455 §5.3).
func applyMask(b []byte, key [4]byte) {
	n := len(b)
	// Fast path: process 4 bytes at a time.
	for i := 0; i+4 <= n; i += 4 {
		b[i] ^= key[0]
		b[i+1] ^= key[1]
		b[i+2] ^= key[2]
		b[i+3] ^= key[3]
	}
	for i := n &^ 3; i < n; i++ {
		b[i] ^= key[i&3]
	}
}

// EncodeFrame serializes f into its wire representation. The length is
// always written in the minimal of the 7-bit, 16-bit and 64-bit forms.
// When f.Masked is set and MaskKey is all zero, a random key is generated
// and stored back into f.
func EncodeFrame(f *Frame) ([]byte, error) {
	if !validOpcode(f.Opcode) {
		return nil, ErrReservedOpcode
	}
	length := len(f.Payload)
	if f.Opcode.IsControl() {
		if !f.Fin {
			return nil, ErrControlFragmented
		}
		if length > maxControlPayload {
			return nil, ErrControlPayloadSize
		}
	}

	out := make([]byte, 0, maxFrameHeaderSize+length)
	var b0 byte
	if f.Fin {
		b0 |= 0x80
	}
	if f.RSV1 {
		b0 |= 0x40
	}
	b0 |= byte(f.Opcode)
	out = append(out, b0)

	var maskBit byte
	if f.Masked {
		maskBit = 0x80
	}
	switch {
	case length <= len7BitMax:
		out = append(out, maskBit|byte(length))
	case length <= 0xFFFF:
		out = append(out, maskBit|len16Indicator, byte(length>>8), byte(length))
	default:
		out = append(out, maskBit|len64Indicator)
		var tmp [8]byte
		binary.BigEndian.PutUint64(tmp[:], uint64(length))
		out = append(out, tmp[:]...)
	}

	if f.Masked {
		if f.MaskKey == ([4]byte{}) {
			if _, err := rand.Read(f.MaskKey[:]); err != nil {
				return nil, err
			}
		}
		out = append(out, f.MaskKey[:]...)
		payload := make([]byte, length)
		copy(payload, f.Payload)
		applyMask(payload, f.MaskKey)
		out = append(out, payload...)
	} else {
		out = append(out, f.Payload...)
	}
	return out, nil
}
