package ws

import (
	"bytes"
	"testing"
)

func TestReassembleFragmented(t *testing.T) {
	r := NewReassembler(1 << 20)
	parts := [][]byte{[]byte("hello, "), []byte("frag"), []byte("mented world")}
	// First frame: OpText Fin=false; middle: continuation Fin=false;
	// last: continuation Fin=true.
	if m, err := r.Push(&Frame{Fin: false, Opcode: OpText, Payload: parts[0]}); err != nil || m != nil {
		t.Fatalf("first fragment: m=%v err=%v", m, err)
	}
	if m, err := r.Push(&Frame{Fin: false, Opcode: OpContinuation, Payload: parts[1]}); err != nil || m != nil {
		t.Fatalf("middle fragment: m=%v err=%v", m, err)
	}
	m, err := r.Push(&Frame{Fin: true, Opcode: OpContinuation, Payload: parts[2]})
	if err != nil {
		t.Fatal(err)
	}
	if m == nil || m.Opcode != OpText || !bytes.Equal(m.Data, []byte("hello, fragmented world")) {
		t.Fatalf("got %+v", m)
	}
	// Reassembled result must equal the unfragmented send.
	r2 := NewReassembler(1 << 20)
	m2, err := r2.Push(&Frame{Fin: true, Opcode: OpText, Payload: []byte("hello, fragmented world")})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m.Data, m2.Data) || m.Opcode != m2.Opcode {
		t.Fatalf("fragmented and unfragmented results differ")
	}
}

func TestReassembleInterleavedControl(t *testing.T) {
	r := NewReassembler(1 << 20)
	if _, err := r.Push(&Frame{Fin: false, Opcode: OpBinary, Payload: []byte("AAA")}); err != nil {
		t.Fatal(err)
	}
	// Ping interleaved inside the fragmented message.
	m, err := r.Push(&Frame{Fin: true, Opcode: OpPing, Payload: []byte("ping")})
	if err != nil || m == nil || m.Opcode != OpPing {
		t.Fatalf("interleaved ping: m=%+v err=%v", m, err)
	}
	m, err = r.Push(&Frame{Fin: true, Opcode: OpContinuation, Payload: []byte("BBB")})
	if err != nil {
		t.Fatal(err)
	}
	if m.Opcode != OpBinary || !bytes.Equal(m.Data, []byte("AAABBB")) {
		t.Fatalf("got %+v", m)
	}
}

func TestReassembleRejects(t *testing.T) {
	t.Run("continuation without start", func(t *testing.T) {
		r := NewReassembler(1 << 20)
		if _, err := r.Push(&Frame{Fin: true, Opcode: OpContinuation, Payload: []byte("x")}); err != ErrContinuationWithoutStart {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("new data frame mid-fragment", func(t *testing.T) {
		r := NewReassembler(1 << 20)
		if _, err := r.Push(&Frame{Fin: false, Opcode: OpText, Payload: []byte("a")}); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Push(&Frame{Fin: true, Opcode: OpBinary, Payload: []byte("b")}); err != ErrNewDataFrameInFragment {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("inconsistent fragment type", func(t *testing.T) {
		// A text message started but a binary frame arrives before the
		// final continuation: type inconsistency is rejected.
		r := NewReassembler(1 << 20)
		if _, err := r.Push(&Frame{Fin: false, Opcode: OpText, Payload: []byte("a")}); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Push(&Frame{Fin: false, Opcode: OpBinary, Payload: []byte("b")}); err != ErrNewDataFrameInFragment {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("message too large", func(t *testing.T) {
		r := NewReassembler(10)
		if _, err := r.Push(&Frame{Fin: false, Opcode: OpBinary, Payload: make([]byte, 8)}); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Push(&Frame{Fin: true, Opcode: OpContinuation, Payload: make([]byte, 8)}); err != ErrMessageTooLarge {
			t.Fatalf("got %v", err)
		}
		// After the error the reassembler is reset and usable again.
		m, err := r.Push(&Frame{Fin: true, Opcode: OpText, Payload: []byte("ok")})
		if err != nil || m == nil {
			t.Fatalf("reuse after reset: m=%v err=%v", m, err)
		}
	})
	t.Run("unfragmented too large", func(t *testing.T) {
		r := NewReassembler(4)
		// Unfragmented frames bypass the reassembler size check; the
		// frame parser enforces per-frame bounds. Fragmented is checked.
		if _, err := r.Push(&Frame{Fin: false, Opcode: OpText, Payload: make([]byte, 5)}); err != ErrMessageTooLarge {
			t.Fatalf("got %v", err)
		}
	})
}

func TestReassembleCompressedFlag(t *testing.T) {
	r := NewReassembler(1 << 20)
	if _, err := r.Push(&Frame{Fin: false, RSV1: true, Opcode: OpText, Payload: []byte("ab")}); err != nil {
		t.Fatal(err)
	}
	m, err := r.Push(&Frame{Fin: true, Opcode: OpContinuation, Payload: []byte("cd")})
	if err != nil {
		t.Fatal(err)
	}
	if !m.Compressed {
		t.Fatalf("RSV1 of first fragment not propagated")
	}
}
