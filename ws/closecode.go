package ws

// CloseCode 是关闭帧中的状态码（RFC 6455 §7.4）。
type CloseCode uint16

const (
	CloseNormal          CloseCode = 1000
	CloseGoingAway       CloseCode = 1001
	CloseProtocolError   CloseCode = 1002
	CloseUnsupportedData CloseCode = 1003
	CloseNoStatus        CloseCode = 1005
	CloseAbnormal        CloseCode = 1006
	CloseInvalidPayload  CloseCode = 1007
	ClosePolicyViolation CloseCode = 1008
	CloseMessageTooBig   CloseCode = 1009
	CloseMandatoryExt    CloseCode = 1010
	CloseInternalError   CloseCode = 1011
	CloseServiceRestart  CloseCode = 1012
	CloseTryAgainLater   CloseCode = 1013
)

// CloseReason 区分连接终止的可观测原因。
type CloseReason int

const (
	ReasonNone CloseReason = iota
	// ReasonLocal 本地主动关闭。
	ReasonLocal
	// ReasonRemote 对端发起关闭握手并完成。
	ReasonRemote
	// ReasonHeartbeatTimeout 心跳连续缺失达到阈值。
	ReasonHeartbeatTimeout
	// ReasonProtocolError 协议违规（帧格式、分片、控制帧约束等）。
	ReasonProtocolError
	// ReasonExtensionError 扩展协商不一致（未协商却出现 RSV 标记等）。
	ReasonExtensionError
	// ReasonMessageTooBig 消息或帧超过配置上限。
	ReasonMessageTooBig
	// ReasonIOError 底层读写错误（弱网断链等）。
	ReasonIOError
	// ReasonWriteTimeout 发送侧超时或被背压策略终止。
	ReasonWriteTimeout
)

func (r CloseReason) String() string {
	switch r {
	case ReasonLocal:
		return "local"
	case ReasonRemote:
		return "remote"
	case ReasonHeartbeatTimeout:
		return "heartbeat_timeout"
	case ReasonProtocolError:
		return "protocol_error"
	case ReasonExtensionError:
		return "extension_error"
	case ReasonMessageTooBig:
		return "message_too_big"
	case ReasonIOError:
		return "io_error"
	case ReasonWriteTimeout:
		return "write_timeout"
	}
	return "none"
}

// CloseEvent 是连接关闭的可观测事件，只产生一次。
type CloseEvent struct {
	Reason CloseReason
	Code   CloseCode
	Text   string
	Err    error // 触发关闭的底层错误（如有）
}
