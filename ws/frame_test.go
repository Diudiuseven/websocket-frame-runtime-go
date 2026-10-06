package ws

import (
	"bytes"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		f    *Frame
	}{
		{"empty text", &Frame{Fin: true, Opcode: OpText}},
		{"small text", &Frame{Fin: true, Opcode: OpText, Payload: []byte("hello")}},
		{"binary 125", &Frame{Fin: true, Opcode: OpBinary, Payload: bytes.Repeat([]byte{1}, 125)}},
		{"binary 126 (16-bit)", &Frame{Fin: true, Opcode: OpBinary, Payload: bytes.Repeat([]byte{2}, 126)}},
		{"binary 65535 (16-bit)", &Frame{Fin: true, Opcode: OpBinary, Payload: bytes.Repeat([]byte{3}, 65535)}},
		{"binary 65536 (64-bit)", &Frame{Fin: true, Opcode: OpBinary, Payload: bytes.Repeat([]byte{4}, 65536)}},
		{"masked", &Frame{Fin: true, Opcode: OpText, Masked: true, MaskKey: [4]byte{1, 2, 3, 4}, Payload: []byte("masked payload")}},
		{"masked random key", &Frame{Fin: true, Opcode: OpText, Masked: true, Payload: []byte("random key")}},
		{"rsv1", &Frame{Fin: true, RSV1: true, Opcode: OpBinary, Payload: []byte("compressed")}},
		{"fragment", &Frame{Fin: false, Opcode: OpText, Payload: []byte("part")}},
		{"ping", &Frame{Fin: true, Opcode: OpPing, Payload: []byte("probe")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := EncodeFrame(tc.f)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			p := NewFrameParser(1 << 20)
			frames, err := p.Feed(data)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if len(frames) != 1 {
				t.Fatalf("got %d frames, want 1", len(frames))
			}
			got := frames[0]
			if got.Fin != tc.f.Fin || got.RSV1 != tc.f.RSV1 || got.Opcode != tc.f.Opcode || got.Masked != tc.f.Masked {
				t.Fatalf("header mismatch: got %+v want %+v", got, tc.f)
			}
			if !bytes.Equal(got.Payload, tc.f.Payload) {
				t.Fatalf("payload mismatch: got %d bytes, want %d", len(got.Payload), len(tc.f.Payload))
			}
			if tc.f.Masked && got.MaskKey != tc.f.MaskKey {
				t.Fatalf("mask key mismatch")
			}
			if p.Buffered() != 0 {
				t.Fatalf("parser holds %d leftover bytes", p.Buffered())
			}
		})
	}
}

func TestParserIncrementalByteByByte(t *testing.T) {
	// Three frames of different length forms, fed one byte at a time.
	var stream []byte
	for _, f := range []*Frame{
		{Fin: true, Opcode: OpText, Payload: []byte("a")},
		{Fin: true, Opcode: OpBinary, Payload: bytes.Repeat([]byte{7}, 300)},
		{Fin: true, Opcode: OpBinary, Masked: true, Payload: bytes.Repeat([]byte{9}, 70000)},
	} {
		b, err := EncodeFrame(f)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, b...)
	}
	p := NewFrameParser(1 << 20)
	var total int
	for i := 0; i < len(stream); i++ {
		frames, err := p.Feed(stream[i : i+1])
		if err != nil {
			t.Fatalf("byte %d: %v", i, err)
		}
		total += len(frames)
	}
	if total != 3 {
		t.Fatalf("parsed %d frames, want 3", total)
	}
}

func TestParserArbitraryChunking(t *testing.T) {
	var stream []byte
	payloads := [][]byte{[]byte("first"), bytes.Repeat([]byte("x"), 1000), []byte("third")}
	for _, pl := range payloads {
		b, err := EncodeFrame(&Frame{Fin: true, Opcode: OpText, Masked: true, Payload: pl})
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, b...)
	}
	// Feed in awkward chunk sizes: 1, 2, 7, 64, rest.
	p := NewFrameParser(1 << 20)
	var got [][]byte
	off := 0
	for _, size := range []int{1, 2, 7, 64, len(stream)} {
		if off >= len(stream) {
			break
		}
		end := off + size
		if end > len(stream) {
			end = len(stream)
		}
		frames, err := p.Feed(stream[off:end])
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range frames {
			got = append(got, f.Payload)
		}
		off = end
	}
	frames, err := p.Feed(stream[off:])
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range frames {
		got = append(got, f.Payload)
	}
	if len(got) != len(payloads) {
		t.Fatalf("got %d frames, want %d", len(got), len(payloads))
	}
	for i := range payloads {
		if !bytes.Equal(got[i], payloads[i]) {
			t.Fatalf("frame %d payload mismatch", i)
		}
	}
}

func TestParserTruncatedWaits(t *testing.T) {
	b, err := EncodeFrame(&Frame{Fin: true, Opcode: OpBinary, Payload: bytes.Repeat([]byte{5}, 500)})
	if err != nil {
		t.Fatal(err)
	}
	p := NewFrameParser(1 << 20)
	// Feed everything except the last byte: no frame, no error.
	frames, err := p.Feed(b[:len(b)-1])
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 0 {
		t.Fatalf("premature frame on truncated input")
	}
	frames, err = p.Feed(b[len(b)-1:])
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || len(frames[0].Payload) != 500 {
		t.Fatalf("expected completed frame after final byte, got %+v", frames)
	}
}

func TestParserRejects(t *testing.T) {
	mk := func(b0, b1 byte, tail ...byte) []byte {
		return append([]byte{b0, b1}, tail...)
	}
	cases := []struct {
		name string
		data []byte
		want error
	}{
		{"reserved opcode 3", mk(0x83, 0x00), ErrReservedOpcode},
		{"reserved opcode 0xB", mk(0x8B, 0x00), ErrReservedOpcode},
		{"rsv2 set", mk(0xA2, 0x00), ErrReservedBits},
		{"rsv3 set", mk(0x92, 0x00), ErrReservedBits},
		{"fragmented ping", mk(0x09, 0x00), ErrControlFragmented},
		{"oversized control", mk(0x88, 126, 0, 126), ErrControlPayloadSize},
		{"non-minimal 16-bit length", mk(0x82, 126, 0, 100), ErrInvalidLength},
		{"non-minimal 64-bit length", mk(0x82, 127, 0, 0, 0, 0, 0, 0, 0, 200), ErrInvalidLength},
		{"64-bit length high bit", mk(0x82, 127, 0x80, 0, 0, 0, 0, 0, 0, 0), ErrInvalidLength},
		{"frame too large", mk(0x82, 126, 0x10, 0x00), ErrFrameTooLarge}, // 4096 > 1024
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := NewFrameParser(1024)
			if tc.want == ErrInvalidLength || tc.want == ErrReservedBits || tc.want == ErrReservedOpcode {
				p = NewFrameParser(1 << 20)
			}
			_, err := p.Feed(tc.data)
			if err != tc.want {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestEncodeRejectsInvalidControl(t *testing.T) {
	if _, err := EncodeFrame(&Frame{Fin: false, Opcode: OpPing}); err != ErrControlFragmented {
		t.Fatalf("got %v, want ErrControlFragmented", err)
	}
	if _, err := EncodeFrame(&Frame{Fin: true, Opcode: OpClose, Payload: make([]byte, 126)}); err != ErrControlPayloadSize {
		t.Fatalf("got %v, want ErrControlPayloadSize", err)
	}
	if _, err := EncodeFrame(&Frame{Fin: true, Opcode: 0x4}); err != ErrReservedOpcode {
		t.Fatalf("got %v, want ErrReservedOpcode", err)
	}
}

func TestParserMemoryBound(t *testing.T) {
	// Repeatedly feed tiny chunks; the internal buffer must stay bounded
	// by one incomplete frame.
	p := NewFrameParser(1 << 20)
	b, _ := EncodeFrame(&Frame{Fin: true, Opcode: OpBinary, Payload: make([]byte, 1000)})
	for i := 0; i < 100; i++ {
		if _, err := p.Feed(b); err != nil {
			t.Fatal(err)
		}
		if p.Buffered() > len(b) {
			t.Fatalf("buffer grew to %d, frame is %d", p.Buffered(), len(b))
		}
	}
}
