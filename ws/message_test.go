package ws

import (
	"bytes"
	"errors"
	"testing"
)

func TestReassembleFragmentedEqualsUnfragmented(t *testing.T) {
	payload := bytes.Repeat([]byte("hello-world-"), 1000)
	// 未分片。
	r1 := NewReassembler(0)
	m1, done, err := r1.Push(&Frame{Fin: true, OpCode: OpBinary, Payload: payload})
	if err != nil || !done {
		t.Fatalf("unfragmented: done=%v err=%v", done, err)
	}
	// 分片为 7 帧，中间插入控制帧。
	r2 := NewReassembler(0)
	const nfrag = 7
	chunk := len(payload) / nfrag
	var m2 *Message
	for i := 0; i < nfrag; i++ {
		end := (i + 1) * chunk
		if i == nfrag-1 {
			end = len(payload)
		}
		fin := i == nfrag-1
		op := OpContinuation
		if i == 0 {
			op = OpBinary
		}
		// 中间插入控制帧不应影响重组。
		if i == 3 {
			if _, done, err := r2.Push(&Frame{Fin: true, OpCode: OpPing, Payload: []byte("x")}); err != nil || done {
				t.Fatalf("control interleave: done=%v err=%v", done, err)
			}
		}
		m, done, err := r2.Push(&Frame{Fin: fin, OpCode: op, Payload: payload[i*chunk : end]})
		if err != nil {
			t.Fatalf("frag %d: %v", i, err)
		}
		if done {
			m2 = m
		}
	}
	if m2 == nil {
		t.Fatal("fragmented message never completed")
	}
	if !bytes.Equal(m1.Payload, m2.Payload) || m1.OpCode != m2.OpCode {
		t.Fatal("fragmented reassembly differs from unfragmented")
	}
}

func TestReassemblerRejectsBadSequences(t *testing.T) {
	// 孤立 continuation。
	r := NewReassembler(0)
	if _, _, err := r.Push(&Frame{Fin: true, OpCode: OpContinuation, Payload: []byte("a")}); !errors.Is(err, ErrUnexpectedContinuation) {
		t.Fatalf("got %v, want ErrUnexpectedContinuation", err)
	}
	// 分片未完成时来了新的数据帧。
	r = NewReassembler(0)
	if _, _, err := r.Push(&Frame{Fin: false, OpCode: OpText, Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Push(&Frame{Fin: true, OpCode: OpBinary, Payload: []byte("b")}); !errors.Is(err, ErrNewMessageInFragment) {
		t.Fatalf("got %v, want ErrNewMessageInFragment", err)
	}
}

func TestReassemblerMessageTooLarge(t *testing.T) {
	r := NewReassembler(100)
	if _, _, err := r.Push(&Frame{Fin: false, OpCode: OpBinary, Payload: make([]byte, 60)}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Push(&Frame{Fin: true, OpCode: OpContinuation, Payload: make([]byte, 60)}); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("got %v, want ErrMessageTooLarge", err)
	}
}

func TestCompressRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("compressible-data-"), 500)
	comp, err := compressMessage(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(comp) >= len(payload) {
		t.Fatalf("compressed %d >= original %d", len(comp), len(payload))
	}
	out, err := decompressMessage(comp, uint64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, payload) {
		t.Fatal("round trip mismatch")
	}
}

func TestDecompressBombBounded(t *testing.T) {
	payload := bytes.Repeat([]byte("A"), 1<<20)
	comp, err := compressMessage(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decompressMessage(comp, 1024); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("got %v, want ErrMessageTooLarge", err)
	}
}

func TestExtensionNegotiation(t *testing.T) {
	cfg := CompressConfig{Enabled: true}
	offer := offerExtension(cfg)
	if offer == "" {
		t.Fatal("expected offer")
	}
	resp, neg := acceptExtension(cfg, offer)
	if !neg.enabled || resp == "" {
		t.Fatal("expected negotiation success")
	}
	neg2, err := checkExtension(cfg, resp)
	if err != nil || !neg2.enabled {
		t.Fatalf("client check: neg=%v err=%v", neg2, err)
	}
	// 服务端未启用：不协商。
	if _, neg := acceptExtension(CompressConfig{}, offer); neg.enabled {
		t.Fatal("expected no negotiation when server disabled")
	}
	// 客户端未启用但服务端响应了扩展：必须报错。
	if _, err := checkExtension(CompressConfig{}, resp); !errors.Is(err, ErrExtensionNotNegotiated) {
		t.Fatalf("got %v, want ErrExtensionNotNegotiated", err)
	}
}
