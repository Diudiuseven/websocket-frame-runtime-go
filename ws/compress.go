package ws

import (
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"strings"
)

// CompressConfig 配置 permessage-deflate 压缩扩展。
type CompressConfig struct {
	// Enabled 是否启用压缩扩展协商。
	Enabled bool
	// MinSize 小于该字节数的消息不压缩（压缩无收益）。
	MinSize int
}

// extNegotiation 记录扩展协商结果，收发两个方向共用同一结果，
// 保证压缩开关在双向一致。
type extNegotiation struct {
	enabled bool
}

// deflateTail 是 permessage-deflate 消息尾部标记（RFC 7692 §7.2.1）。
var deflateTail = []byte{0x00, 0x00, 0xff, 0xff}

// ErrExtensionNotNegotiated 表示未协商扩展却出现扩展标记。
var ErrExtensionNotNegotiated = errors.New("ws: RSV1 set but permessage-deflate not negotiated")

// offerExtension 生成客户端握手时发送的扩展协商头。
// 总是声明 no_context_takeover，压缩上下文不跨消息保留，保证行为可复现。
func offerExtension(c CompressConfig) string {
	if !c.Enabled {
		return ""
	}
	return "permessage-deflate; client_no_context_takeover; server_no_context_takeover"
}

// acceptExtension 服务端根据客户端 offer 生成响应头。
// 仅当本地启用且对端 offer 包含 permessage-deflate 时协商成功。
func acceptExtension(c CompressConfig, offer string) (string, extNegotiation) {
	if !c.Enabled || !strings.Contains(offer, "permessage-deflate") {
		return "", extNegotiation{}
	}
	return "permessage-deflate; client_no_context_takeover; server_no_context_takeover",
		extNegotiation{enabled: true}
}

// checkExtension 客户端校验服务端响应，返回协商结果。
// 本地未启用而服务端响应了扩展，视为协商不一致错误。
func checkExtension(c CompressConfig, resp string) (extNegotiation, error) {
	if resp == "" {
		return extNegotiation{}, nil
	}
	if !c.Enabled {
		return extNegotiation{}, fmt.Errorf("%w: server responded %q but compression disabled locally",
			ErrExtensionNotNegotiated, resp)
	}
	if !strings.Contains(resp, "permessage-deflate") {
		return extNegotiation{}, fmt.Errorf("ws: unsupported extension in response %q", resp)
	}
	return extNegotiation{enabled: true}, nil
}

// compressMessage 压缩一条消息负载，返回去掉尾部 0x00 0x00 0xff 0xff 的字节。
// 每条消息使用独立的 flate 上下文（no_context_takeover）。
func compressMessage(payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(payload); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	out := buf.Bytes()
	// Flush 保证输出以 0x00 0x00 0xff 0xff 结尾，按协议去掉。
	if bytes.HasSuffix(out, deflateTail) {
		out = out[:len(out)-len(deflateTail)]
	}
	return out, nil
}

// decompressMessage 解压一条消息负载（补回尾部后 inflate）。
// maxOut 限制解压输出上限，防止解压炸弹导致无界内存分配。
func decompressMessage(payload []byte, maxOut uint64) ([]byte, error) {
	if maxOut == 0 {
		maxOut = DefaultMaxMessageSize
	}
	src := make([]byte, 0, len(payload)+len(deflateTail))
	src = append(src, payload...)
	src = append(src, deflateTail...)

	r := flate.NewReader(bytes.NewReader(src))
	defer r.Close()
	// 多读 1 字节用于检测超限。
	limited := io.LimitReader(r, int64(maxOut)+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("ws: decompress: %w", err)
	}
	if uint64(len(out)) > maxOut {
		return nil, fmt.Errorf("%w: decompressed %d > %d", ErrMessageTooLarge, len(out), maxOut)
	}
	return out, nil
}
