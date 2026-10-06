package ws

import (
	"bytes"
	"testing"
)

// BenchmarkParseSmallFrames measures incremental parsing of 64-byte
// messages fed as a continuous stream.
func BenchmarkParseSmallFrames(b *testing.B) {
	frame, _ := EncodeFrame(&Frame{Fin: true, Opcode: OpText, Payload: make([]byte, 64)})
	stream := bytes.Repeat(frame, 64)
	p := NewFrameParser(1 << 20)
	b.SetBytes(int64(len(stream)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Feed(stream); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseLargeFrame measures parsing of a 1 MiB frame.
func BenchmarkParseLargeFrame(b *testing.B) {
	frame, _ := EncodeFrame(&Frame{Fin: true, Opcode: OpBinary, Payload: make([]byte, 1<<20)})
	p := NewFrameParser(2 << 20)
	b.SetBytes(int64(len(frame)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Feed(frame); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodeFrame measures serialization of a 4 KiB frame.
func BenchmarkEncodeFrame(b *testing.B) {
	f := &Frame{Fin: true, Opcode: OpBinary, Payload: make([]byte, 4096)}
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeFrame(f); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompress measures permessage-deflate of a 16 KiB text message
// with context takeover (steady state).
func BenchmarkCompress(b *testing.B) {
	c := newCompressor(0)
	msg := bytes.Repeat([]byte("benchmark payload "), 1024)
	b.SetBytes(int64(len(msg)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Compress(msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecompress measures inflation of the same message.
func BenchmarkDecompress(b *testing.B) {
	tx, rx := newCompressor(0), newCompressor(0)
	msg := bytes.Repeat([]byte("benchmark payload "), 1024)
	cp, _ := tx.Compress(msg)
	b.SetBytes(int64(len(msg)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rx.Decompress(cp, 1<<20); err != nil {
			b.Fatal(err)
		}
	}
}
