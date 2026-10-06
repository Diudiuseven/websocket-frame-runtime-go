package ws

import (
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ErrExtensionNotNegotiated is returned when an inbound frame carries RSV1
// but permessage-deflate was not negotiated.
var ErrExtensionNotNegotiated = errors.New("ws: RSV1 set but permessage-deflate not negotiated")

// ErrUnsupportedExtensionParam rejects permessage-deflate parameters this
// runtime does not implement.
var ErrUnsupportedExtensionParam = errors.New("ws: unsupported permessage-deflate parameter")

// CompressConfig controls the optional permessage-deflate extension
// (RFC 7692). This runtime always keeps compression context (context
// takeover) in both directions once negotiated, and rejects peers that
// demand no-context-takeover modes.
type CompressConfig struct {
	// Enabled advertises and accepts permessage-deflate.
	Enabled bool
	// Level is the flate compression level (see compress/flate);
	// zero selects the default.
	Level int
	// MinSize skips compression for messages smaller than this many bytes.
	MinSize int
}

func (c CompressConfig) withDefaults() CompressConfig {
	if c.Level == 0 {
		c.Level = flate.DefaultCompression
	}
	if c.MinSize < 0 {
		c.MinSize = 0
	}
	return c
}

// Negotiator computes and validates the Sec-WebSocket-Extensions header.
type Negotiator struct {
	cfg CompressConfig
}

// NewNegotiator builds a Negotiator.
func NewNegotiator(cfg CompressConfig) *Negotiator { return &Negotiator{cfg: cfg.withDefaults()} }

// extensionName is the token used in Sec-WebSocket-Extensions.
const extensionName = "permessage-deflate"

// Offer returns the Sec-WebSocket-Extensions header value to send, or ""
// when compression is disabled.
func (n *Negotiator) Offer() string {
	if !n.cfg.Enabled {
		return ""
	}
	return extensionName
}

// Accept validates the peer's extension header (a server's response to
// our offer, or a client's request we choose to accept) and reports
// whether compression is active for the connection. Parameters this
// runtime does not support are rejected with ErrUnsupportedExtensionParam.
func (n *Negotiator) Accept(peerHeader string) (active bool, err error) {
	if !n.cfg.Enabled {
		if strings.Contains(peerHeader, extensionName) {
			// We never offered it; the peer must not enable it.
			return false, ErrExtensionNotNegotiated
		}
		return false, nil
	}
	if peerHeader == "" {
		return false, nil
	}
	found := false
	for _, ext := range strings.Split(peerHeader, ",") {
		parts := strings.Split(ext, ";")
		if strings.TrimSpace(parts[0]) != extensionName {
			continue
		}
		found = true
		for _, p := range parts[1:] {
			p = strings.TrimSpace(p)
			switch {
			case p == "":
			case p == "client_no_context_takeover",
				p == "server_no_context_takeover":
				return false, fmt.Errorf("%w: %s", ErrUnsupportedExtensionParam, p)
			case strings.HasPrefix(p, "client_max_window_bits"),
				strings.HasPrefix(p, "server_max_window_bits"):
				return false, fmt.Errorf("%w: %s", ErrUnsupportedExtensionParam, p)
			default:
				return false, fmt.Errorf("%w: %s", ErrUnsupportedExtensionParam, p)
			}
		}
	}
	return found, nil
}

// deflateTrailer is removed from compressed messages and restored before
// inflation (RFC 7692 §7.2.1).
var deflateTrailer = []byte{0x00, 0x00, 0xff, 0xff}

// finalEmptyBlock terminates the deflate stream so the reader finishes
// cleanly instead of reporting an unexpected EOF.
var finalEmptyBlock = []byte{0x01, 0x00, 0x00, 0xff, 0xff}

// windowSize is the deflate sliding window kept across messages.
const windowSize = 32 * 1024

// compressor applies/removes permessage-deflate on whole messages. It is
// stateful: the flate dictionaries persist across messages (context
// takeover), so a compressor must be used from a single goroutine per
// direction. The connection layer guarantees that.
type compressor struct {
	level    int
	deflator *flate.Writer
	inflater io.ReadCloser
	defBuf   bytes.Buffer
	infBuf   bytes.Reader
	history  []byte // last windowSize decompressed bytes, used as dictionary
}

func newCompressor(level int) *compressor {
	if level == 0 {
		level = flate.DefaultCompression
	}
	return &compressor{level: level}
}

// Compress compresses msg and strips the trailing 0x00 0x00 0xff 0xff
// sync marker. The deflate context is kept across calls: the writer is
// never reset, only its output buffer is drained.
func (c *compressor) Compress(msg []byte) ([]byte, error) {
	if c.deflator == nil {
		w, err := flate.NewWriter(&c.defBuf, c.level)
		if err != nil {
			return nil, err
		}
		c.deflator = w
	}
	if _, err := c.deflator.Write(msg); err != nil {
		return nil, err
	}
	if err := c.deflator.Flush(); err != nil {
		return nil, err
	}
	out := c.defBuf.Bytes()
	if bytes.HasSuffix(out, deflateTrailer) {
		out = out[:len(out)-len(deflateTrailer)]
	}
	cp := make([]byte, len(out))
	copy(cp, out)
	c.defBuf.Reset() // drain output only; the writer keeps its dictionary
	return cp, nil
}

// Decompress restores the trailer, terminates the stream and inflates
// msg. The result is bounded by maxBytes to keep decompression bombs from
// exhausting memory. The sliding window of past output is supplied as the
// dictionary, preserving context takeover.
func (c *compressor) Decompress(msg []byte, maxBytes int64) ([]byte, error) {
	if c.inflater == nil {
		c.inflater = flate.NewReader(&c.infBuf)
	}
	full := make([]byte, 0, len(msg)+len(deflateTrailer)+len(finalEmptyBlock))
	full = append(full, msg...)
	full = append(full, deflateTrailer...)
	full = append(full, finalEmptyBlock...)
	c.infBuf.Reset(full)
	if r, ok := c.inflater.(flate.Resetter); ok {
		if err := r.Reset(&c.infBuf, c.history); err != nil {
			return nil, err
		}
	}
	limit := maxBytes
	if limit <= 0 {
		limit = 1 << 30 // hard safety ceiling: 1 GiB
	}
	out, err := io.ReadAll(io.LimitReader(c.inflater, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(out)) > limit {
		return nil, ErrMessageTooLarge
	}
	// Slide the dictionary window forward.
	c.history = append(c.history, out...)
	if len(c.history) > windowSize {
		c.history = append([]byte(nil), c.history[len(c.history)-windowSize:]...)
	}
	return out, nil
}
