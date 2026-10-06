package ws

import (
	"bytes"
	"testing"
)

// TestParserDoesNotPreallocate 验证超大声明长度不会导致预分配：
// 只到达头部时解析器必须等待，且缓冲仅有已到达字节。
func TestParserDoesNotPreallocate(t *testing.T) {
	p := NewFrameParser(1 << 20)
	// 只发 64 位长度头部（声明 1MiB），不发负载。
	hdr := []byte{0x82, 127, 0, 0, 0, 0, 0, 0x10, 0, 0}
	frames, err := p.Feed(hdr)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 0 {
		t.Fatal("should wait for payload")
	}
	if p.Buffered() != len(hdr) {
		t.Fatalf("buffered %d, want %d", p.Buffered(), len(hdr))
	}
}

// BenchmarkFrameParser 测量持续帧流的解析吞吐。
func BenchmarkFrameParser(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 4096)
	raw, err := MarshalFrame(&Frame{Fin: true, OpCode: OpBinary, Payload: payload}, true)
	if err != nil {
		b.Fatal(err)
	}
	stream := bytes.Repeat(raw, 16)
	p := NewFrameParser(0)
	b.SetBytes(int64(len(stream)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Feed(stream); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMarshalFrame 测量序列化吞吐。
func BenchmarkMarshalFrame(b *testing.B) {
	f := &Frame{Fin: true, OpCode: OpBinary, Payload: bytes.Repeat([]byte("y"), 4096)}
	b.SetBytes(int64(len(f.Payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := MarshalFrame(f, true); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompressMessage 测量压缩路径吞吐。
func BenchmarkCompressMessage(b *testing.B) {
	payload := bytes.Repeat([]byte("benchmark-payload-"), 256)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := compressMessage(payload); err != nil {
			b.Fatal(err)
		}
	}
}
