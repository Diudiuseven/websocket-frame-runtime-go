package ws

import (
	"errors"
	"fmt"
)

// DefaultMaxMessageSize 是默认单条消息上限（64 MiB）。
const DefaultMaxMessageSize = 64 << 20

// 分片重组相关错误。
var (
	ErrMessageTooLarge        = errors.New("ws: message exceeds size limit")
	ErrUnexpectedContinuation = errors.New("ws: continuation frame without active message")
	ErrNewMessageInFragment   = errors.New("ws: new data frame while fragmented message incomplete")
)

// Message 是一条重组完成的逻辑消息。
type Message struct {
	OpCode  OpCode // OpText 或 OpBinary
	Payload []byte
	// Compressed 表示该消息在传输时使用了压缩扩展（首帧 RSV1 置位）。
	Compressed bool
}

// Reassembler 负责消息分片重组与协议校验。
// 控制帧直接透传（返回 nil, false, nil），由上层处理；
// 控制帧可插入分片序列中间，不影响重组状态。
type Reassembler struct {
	maxMessage uint64

	active     bool   // 是否有进行中的分片消息
	op         OpCode // 分片消息首帧操作码
	compressed bool   // 首帧 RSV1
	buf        []byte
}

// NewReassembler 创建重组器，maxMessage 为单条消息的最大字节数（0 用默认值）。
func NewReassembler(maxMessage uint64) *Reassembler {
	if maxMessage == 0 {
		maxMessage = DefaultMaxMessageSize
	}
	return &Reassembler{maxMessage: maxMessage}
}

// Push 输入一个数据帧或控制帧。
// 返回 (msg, true, nil) 表示一条消息重组完成；
// 控制帧与中间分片返回 (nil, false, nil)；协议违规返回错误。
func (r *Reassembler) Push(f *Frame) (*Message, bool, error) {
	switch {
	case f.OpCode.IsControl():
		// 控制帧不参与重组，直接透传。
		return nil, false, nil

	case f.OpCode == OpContinuation:
		if !r.active {
			return nil, false, ErrUnexpectedContinuation
		}
		if err := r.append(f.Payload); err != nil {
			return nil, false, err
		}
		if !f.Fin {
			return nil, false, nil
		}
		msg := &Message{OpCode: r.op, Payload: r.buf, Compressed: r.compressed}
		r.reset()
		return msg, true, nil

	default: // OpText / OpBinary
		if r.active {
			return nil, false, fmt.Errorf("%w: got opcode 0x%x", ErrNewMessageInFragment, byte(f.OpCode))
		}
		if f.Fin {
			// 未分片消息，直接完成，不做多余拷贝。
			return &Message{OpCode: f.OpCode, Payload: f.Payload, Compressed: f.RSV1}, true, nil
		}
		r.active = true
		r.op = f.OpCode
		r.compressed = f.RSV1
		r.buf = nil
		if err := r.append(f.Payload); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
}

func (r *Reassembler) append(p []byte) error {
	if uint64(len(r.buf))+uint64(len(p)) > r.maxMessage {
		return fmt.Errorf("%w: %d + %d > %d", ErrMessageTooLarge, len(r.buf), len(p), r.maxMessage)
	}
	r.buf = append(r.buf, p...)
	return nil
}

func (r *Reassembler) reset() {
	r.active = false
	r.op = 0
	r.compressed = false
	r.buf = nil
}

// Reset 丢弃进行中的分片状态（连接重置时使用）。
func (r *Reassembler) Reset() { r.reset() }
