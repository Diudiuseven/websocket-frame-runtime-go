package ws

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestNegotiatorOffer(t *testing.T) {
	if got := NewNegotiator(CompressConfig{}).Offer(); got != "" {
		t.Fatalf("disabled: offer=%q", got)
	}
	if got := NewNegotiator(CompressConfig{Enabled: true}).Offer(); got != "permessage-deflate" {
		t.Fatalf("enabled: offer=%q", got)
	}
}

func TestNegotiatorAccept(t *testing.T) {
	n := NewNegotiator(CompressConfig{Enabled: true})

	active, err := n.Accept("permessage-deflate")
	if err != nil || !active {
		t.Fatalf("plain accept: active=%v err=%v", active, err)
	}

	active, err = n.Accept("")
	if err != nil || active {
		t.Fatalf("empty header: active=%v err=%v", active, err)
	}

	// Unsupported parameters must be rejected.
	for _, h := range []string{
		"permessage-deflate; client_no_context_takeover",
		"permessage-deflate; server_no_context_takeover",
		"permessage-deflate; server_max_window_bits=10",
		"permessage-deflate; unknown_param",
	} {
		if _, err := n.Accept(h); !errors.Is(err, ErrUnsupportedExtensionParam) {
			t.Fatalf("%q: got %v", h, err)
		}
	}

	// Compression disabled locally: peer must not enable it.
	off := NewNegotiator(CompressConfig{})
	if _, err := off.Accept("permessage-deflate"); !errors.Is(err, ErrExtensionNotNegotiated) {
		t.Fatalf("disabled accept: got %v", err)
	}
	if active, err := off.Accept(""); err != nil || active {
		t.Fatalf("disabled empty: active=%v err=%v", active, err)
	}
}

func TestCompressorRoundTrip(t *testing.T) {
	tx, rx := newCompressor(0), newCompressor(0)
	// Multiple messages to exercise context takeover across calls.
	msgs := [][]byte{
		[]byte(strings.Repeat("the quick brown fox ", 50)),
		[]byte(strings.Repeat("the quick brown fox ", 50)), // highly correlated with previous
		bytes.Repeat([]byte{0, 1, 2, 3}, 500),
		[]byte("short"),
	}
	for i, m := range msgs {
		cp, err := tx.Compress(m)
		if err != nil {
			t.Fatalf("msg %d compress: %v", i, err)
		}
		if bytes.HasSuffix(cp, deflateTrailer) {
			t.Fatalf("msg %d: trailer not stripped", i)
		}
		got, err := rx.Decompress(cp, 1<<20)
		if err != nil {
			t.Fatalf("msg %d decompress: %v", i, err)
		}
		if !bytes.Equal(got, m) {
			t.Fatalf("msg %d: round trip mismatch (%d vs %d bytes)", i, len(got), len(m))
		}
	}
	// Context takeover should make the repeated message compress well.
	cp, _ := tx.Compress(msgs[1])
	if len(cp) >= len(msgs[1]) {
		t.Fatalf("expected compression to shrink repetitive input (%d >= %d)", len(cp), len(msgs[1]))
	}
}

func TestDecompressBound(t *testing.T) {
	tx, rx := newCompressor(0), newCompressor(0)
	big := bytes.Repeat([]byte("A"), 1<<20)
	cp, err := tx.Compress(big)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rx.Decompress(cp, 1024); err != ErrMessageTooLarge {
		t.Fatalf("got %v, want ErrMessageTooLarge", err)
	}
}
