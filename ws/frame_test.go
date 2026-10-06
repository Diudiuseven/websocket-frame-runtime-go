package ws

import (
	"bytes"
	"errors"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"empty", nil},
		{"7bit", bytes.Repeat([]byte("a"), 125)},
		{"16bit", bytes.Repeat([]byte("b"), 126)},
		{"16bit-max", bytes.Repeat([]byte("c"), 65535)},
		{"64bit", bytes.Repeat([]byte("d"), 65536)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, masked := range []bool{false, true} {
				in := &Frame{Fin: true, OpCode: OpBinary, Payload: tc.payload}
				raw, err := MarshalFrame(in, masked)
				if err != nil {
					t.Fatalf("marshal: %v", err)
				}
				p := NewFrameParser(0)
				frames, err := p.Feed(raw)
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				if len(frames) != 1 {
					t.Fatalf("got %d frames, want 1", len(frames))
				}
				got := frames[0]
				if !got.Fin || got.OpCode != OpBinary {
					t.Fatalf("header mismatch: %+v", got)
				}
				if got.Masked != masked {
					t.Fatalf("masked=%v want %v", got.Masked, masked)
				}
				if !bytes.Equal(got.Payload, tc.payload) {
					t.Fatalf("payload mismatch len=%d", len(got.Payload))
				}
			}
		})
	}
}

func TestFrameParserIncremental(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 1000)
	raw, err := MarshalFrame(&Frame{Fin: true, OpCode: OpText, Payload: payload}, true)
	if err != nil {
		t.Fatal(err)
	}
	// 逐字节喂入，模拟任意截断与分次到达。
	p := NewFrameParser(0)
	var got []*Frame
	for i := 0; i < len(raw); i++ {
		frames, err := p.Feed(raw[i : i+1])
		if err != nil {
			t.Fatalf("byte %d: %v", i, err)
		}
		got = append(got, frames...)
	}
	if len(got) != 1 {
		t.Fatalf("got %d frames, want 1", len(got))
	}
	if !bytes.Equal(got[0].Payload, payload) {
		t.Fatal("payload mismatch")
	}
}

func TestFrameParserMultipleFramesOneFeed(t *testing.T) {
	var stream []byte
	for i := 0; i < 5; i++ {
		raw, err := MarshalFrame(&Frame{Fin: true, OpCode: OpText, Payload: []byte{byte(i)}}, false)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, raw...)
	}
	p := NewFrameParser(0)
	frames, err := p.Feed(stream)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 5 {
		t.Fatalf("got %d frames, want 5", len(frames))
	}
	for i, f := range frames {
		if f.Payload[0] != byte(i) {
			t.Fatalf("frame %d payload %v", i, f.Payload)
		}
	}
}

func TestFrameParserRejectsNonMinimalLength(t *testing.T) {
	// 16 位长度编码了 <126 的值：非最简编码，必须拒绝。
	raw := []byte{0x82, 126, 0x00, 0x7d}
	p := NewFrameParser(0)
	if _, err := p.Feed(raw); !errors.Is(err, ErrInvalidLength) {
		t.Fatalf("got %v, want ErrInvalidLength", err)
	}
	// 64 位长度编码了 <65536 的值。
	raw = []byte{0x82, 127, 0, 0, 0, 0, 0, 0, 0, 0x7d}
	if _, err := p.Feed(raw); !errors.Is(err, ErrInvalidLength) {
		t.Fatalf("got %v, want ErrInvalidLength", err)
	}
}

func TestFrameParserRejectsOversize(t *testing.T) {
	raw, err := MarshalFrame(&Frame{Fin: true, OpCode: OpBinary, Payload: make([]byte, 1024)}, false)
	if err != nil {
		t.Fatal(err)
	}
	p := NewFrameParser(512)
	if _, err := p.Feed(raw); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("got %v, want ErrFrameTooLarge", err)
	}
}

func TestControlFrameConstraints(t *testing.T) {
	// 分片控制帧：解析与序列化两侧都必须拒绝。
	p := NewFrameParser(0)
	if _, err := p.Feed([]byte{0x09, 0x00}); !errors.Is(err, ErrControlFragment) {
		t.Fatalf("parse: got %v, want ErrControlFragment", err)
	}
	if _, err := MarshalFrame(&Frame{Fin: false, OpCode: OpPing}, false); !errors.Is(err, ErrControlFragment) {
		t.Fatalf("marshal: got %v, want ErrControlFragment", err)
	}
	// 控制帧负载 >125。
	big := make([]byte, 126)
	if _, err := MarshalFrame(&Frame{Fin: true, OpCode: OpPing, Payload: big}, false); !errors.Is(err, ErrControlTooLarge) {
		t.Fatalf("marshal: got %v, want ErrControlTooLarge", err)
	}
	raw := []byte{0x89, 126, 0x00, 0x7e}
	if _, err := NewFrameParser(0).Feed(raw); !errors.Is(err, ErrControlTooLarge) {
		t.Fatalf("parse: got %v, want ErrControlTooLarge", err)
	}
}

func TestReservedOpCodeRejected(t *testing.T) {
	p := NewFrameParser(0)
	if _, err := p.Feed([]byte{0x83, 0x00}); !errors.Is(err, ErrReservedOpCode) {
		t.Fatalf("got %v, want ErrReservedOpCode", err)
	}
	if _, err := MarshalFrame(&Frame{Fin: true, OpCode: 0x3}, false); !errors.Is(err, ErrReservedOpCode) {
		t.Fatalf("got %v, want ErrReservedOpCode", err)
	}
}
