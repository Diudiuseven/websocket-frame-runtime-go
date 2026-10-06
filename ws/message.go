package ws

import "errors"

// Message reassembly errors.
var (
	ErrContinuationWithoutStart = errors.New("ws: continuation frame without an in-flight fragmented message")
	ErrNewDataFrameInFragment   = errors.New("ws: new data frame while a fragmented message is incomplete")
	ErrInconsistentFragmentType = errors.New("ws: fragmented message changed data type")
	ErrMessageTooLarge          = errors.New("ws: reassembled message exceeds configured maximum")
)

// Message is a fully reassembled logical message.
type Message struct {
	Opcode Opcode // OpText or OpBinary
	Data   []byte
	// Compressed reports whether the message arrived with RSV1 set
	// (permessage-deflate). It is informational; decompression is handled
	// by the connection layer.
	Compressed bool
}

// Reassembler reassembles fragmented messages and validates control
// frames. Control frames interleaved inside a fragmented message are
// returned immediately as single-frame messages without disturbing the
// in-flight data message.
type Reassembler struct {
	maxMessage int64
	inFlight   bool
	opcode     Opcode
	compressed bool
	size       int64
	buf        []byte
}

// NewReassembler creates a Reassembler rejecting messages larger than
// maxMessage bytes.
func NewReassembler(maxMessage int64) *Reassembler {
	return &Reassembler{maxMessage: maxMessage}
}

// Push consumes one frame. It returns a complete Message when one is
// finished (a control frame, an unfragmented data frame, or the final
// fragment of a message), and nil while a fragmented message is still
// incomplete. A protocol error means the connection must be closed.
func (r *Reassembler) Push(f *Frame) (*Message, error) {
	// Control frames are never fragmented (enforced by the parser) and
	// may be interleaved anywhere; pass them through untouched.
	if f.Opcode.IsControl() {
		return &Message{Opcode: f.Opcode, Data: f.Payload}, nil
	}

	switch f.Opcode {
	case OpText, OpBinary:
		if r.inFlight {
			// A new data frame may not start until the previous
			// fragmented message is complete.
			return nil, ErrNewDataFrameInFragment
		}
		if f.Fin {
			// Unfragmented message: deliver as-is, no copy.
			return &Message{Opcode: f.Opcode, Data: f.Payload, Compressed: f.RSV1}, nil
		}
		r.inFlight = true
		r.opcode = f.Opcode
		r.compressed = f.RSV1
		r.size = int64(len(f.Payload))
		if r.maxMessage > 0 && r.size > r.maxMessage {
			r.Reset()
			return nil, ErrMessageTooLarge
		}
		r.buf = append(r.buf[:0], f.Payload...)
		return nil, nil

	case OpContinuation:
		if !r.inFlight {
			return nil, ErrContinuationWithoutStart
		}
		r.size += int64(len(f.Payload))
		if r.maxMessage > 0 && r.size > r.maxMessage {
			r.Reset()
			return nil, ErrMessageTooLarge
		}
		r.buf = append(r.buf, f.Payload...)
		if !f.Fin {
			return nil, nil
		}
		msg := &Message{Opcode: r.opcode, Data: r.buf, Compressed: r.compressed}
		r.inFlight = false
		r.buf = nil // ownership transferred to msg
		r.size = 0
		return msg, nil
	}
	return nil, ErrReservedOpcode
}

// Reset aborts any in-flight fragmented message (used on connection
// teardown).
func (r *Reassembler) Reset() {
	r.inFlight = false
	r.buf = nil
	r.size = 0
	r.compressed = false
}
