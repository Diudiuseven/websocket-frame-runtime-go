package ws

import "fmt"

// CloseCode is a WebSocket close status code (RFC 6455 §7.4).
type CloseCode uint16

const (
	CloseNormalClosure           CloseCode = 1000
	CloseGoingAway               CloseCode = 1001
	CloseProtocolError           CloseCode = 1002
	CloseUnsupportedData         CloseCode = 1003
	CloseNoStatusReceived        CloseCode = 1005
	CloseAbnormalClosure         CloseCode = 1006
	CloseInvalidFramePayloadData CloseCode = 1007
	ClosePolicyViolation         CloseCode = 1008
	CloseMessageTooBig           CloseCode = 1009
	CloseMandatoryExtension      CloseCode = 1010
	CloseInternalServerErr       CloseCode = 1011
)

// CloseReason identifies why a connection ended, so that timeout,
// peer-initiated close and local close are distinguishable and observable.
type CloseReason int

const (
	// ReasonUnknown means the connection is still open.
	ReasonUnknown CloseReason = iota
	// ReasonLocalClose: Close() was called locally and the handshake completed.
	ReasonLocalClose
	// ReasonPeerClose: the peer sent a close frame and we replied.
	ReasonPeerClose
	// ReasonHeartbeatTimeout: ping/pong keepalive missed too many responses.
	ReasonHeartbeatTimeout
	// ReasonReadError: the read side failed (I/O or protocol error).
	ReasonReadError
	// ReasonWriteError: the write side failed.
	ReasonWriteError
	// ReasonSendQueueFull: the bounded send queue overflowed (backpressure policy).
	ReasonSendQueueFull
	// ReasonExtensionRejected: peer used an extension that was not negotiated.
	ReasonExtensionRejected
)

func (r CloseReason) String() string {
	switch r {
	case ReasonLocalClose:
		return "local-close"
	case ReasonPeerClose:
		return "peer-close"
	case ReasonHeartbeatTimeout:
		return "heartbeat-timeout"
	case ReasonReadError:
		return "read-error"
	case ReasonWriteError:
		return "write-error"
	case ReasonSendQueueFull:
		return "send-queue-full"
	case ReasonExtensionRejected:
		return "extension-rejected"
	default:
		return "unknown"
	}
}

// CloseError is returned from read/write paths after the connection ends.
// It carries the reason, the wire close code and an optional cause.
type CloseError struct {
	Reason CloseReason
	Code   CloseCode
	Text   string
	Err    error
}

func (e *CloseError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("ws: closed (%s, code=%d %q): %v", e.Reason, e.Code, e.Text, e.Err)
	}
	return fmt.Sprintf("ws: closed (%s, code=%d %q)", e.Reason, e.Code, e.Text)
}

func (e *CloseError) Unwrap() error { return e.Err }

// encodeClosePayload serializes a close code and reason text.
func encodeClosePayload(code CloseCode, text string) []byte {
	p := make([]byte, 2+len(text))
	p[0] = byte(code >> 8)
	p[1] = byte(code)
	copy(p[2:], text)
	return p
}

// decodeClosePayload parses a close frame payload.
func decodeClosePayload(b []byte) (CloseCode, string, error) {
	if len(b) == 0 {
		return CloseNoStatusReceived, "", nil
	}
	if len(b) == 1 {
		return 0, "", ErrControlPayloadSize
	}
	return CloseCode(b[0])<<8 | CloseCode(b[1]), string(b[2:]), nil
}
