package ws

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// connPair 在 net.Pipe 上创建一对已运行的连接。
func connPair(t *testing.T, ccfg, scfg Config, cext, sext extNegotiation) (*Conn, *Conn) {
	t.Helper()
	p1, p2 := net.Pipe()
	ccfg.IsClient = true
	client := NewConn(p1, ccfg, cext)
	server := NewConn(p2, scfg, sext)
	go client.Run()
	go server.Run()
	t.Cleanup(func() {
		client.Close(CloseNormal, "")
		server.Close(CloseNormal, "")
	})
	return client, server
}

func waitEvent(t *testing.T, c *Conn, timeout time.Duration) CloseEvent {
	t.Helper()
	select {
	case ev, ok := <-c.Closed():
		if !ok {
			t.Fatal("event channel closed without event")
		}
		return ev
	case <-time.After(timeout):
		t.Fatal("timeout waiting for close event")
		return CloseEvent{}
	}
}

func TestConnEchoRoundTrip(t *testing.T) {
	client, server := connPair(t, Config{}, Config{}, extNegotiation{}, extNegotiation{})
	// 服务端回声。
	go func() {
		for {
			m, err := server.ReadMessage()
			if err != nil {
				return
			}
			_ = server.Send(m.OpCode, m.Payload)
		}
	}()
	payload := []byte("hello websocket")
	if err := client.Send(OpText, payload); err != nil {
		t.Fatal(err)
	}
	m, err := client.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if m.OpCode != OpText || !bytes.Equal(m.Payload, payload) {
		t.Fatalf("got %v/%q", m.OpCode, m.Payload)
	}
}

func TestConnFragmentedLargeMessage(t *testing.T) {
	// 小帧上限强制发送侧分片，接收侧重组结果必须与原消息一致。
	cfg := Config{MaxFramePayload: 100}
	client, server := connPair(t, cfg, cfg, extNegotiation{}, extNegotiation{})
	payload := bytes.Repeat([]byte("fragment-me-"), 500) // 6000 字节，61 帧
	if err := client.Send(OpBinary, payload); err != nil {
		t.Fatal(err)
	}
	m, err := server.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m.Payload, payload) {
		t.Fatalf("reassembled %d bytes, want %d", len(m.Payload), len(payload))
	}
}

func TestConnCloseHandshakeLocal(t *testing.T) {
	client, server := connPair(t, Config{}, Config{}, extNegotiation{}, extNegotiation{})
	if err := client.Close(CloseNormal, "bye"); err != nil {
		t.Fatal(err)
	}
	cev := waitEvent(t, client, 2*time.Second)
	sev := waitEvent(t, server, 2*time.Second)
	if cev.Reason != ReasonLocal {
		t.Fatalf("client reason=%v, want local", cev.Reason)
	}
	if sev.Reason != ReasonRemote {
		t.Fatalf("server reason=%v, want remote", sev.Reason)
	}
	if sev.Code != CloseNormal || sev.Text != "bye" {
		t.Fatalf("server close=%v/%q", sev.Code, sev.Text)
	}
}

func TestConnCloseHandshakeRemoteInitiated(t *testing.T) {
	client, server := connPair(t, Config{}, Config{}, extNegotiation{}, extNegotiation{})
	if err := server.Close(CloseGoingAway, "shutdown"); err != nil {
		t.Fatal(err)
	}
	sev := waitEvent(t, server, 2*time.Second)
	cev := waitEvent(t, client, 2*time.Second)
	if sev.Reason != ReasonLocal || cev.Reason != ReasonRemote {
		t.Fatalf("server=%v client=%v", sev.Reason, cev.Reason)
	}
	if cev.Code != CloseGoingAway {
		t.Fatalf("client code=%v", cev.Code)
	}
}

func TestConnCloseIdempotentConcurrent(t *testing.T) {
	client, _ := connPair(t, Config{}, Config{}, extNegotiation{}, extNegotiation{})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = client.Close(CloseNormal, fmt.Sprintf("c%d", i))
		}(i)
	}
	wg.Wait()
	ev := waitEvent(t, client, 2*time.Second)
	if ev.Reason != ReasonLocal {
		t.Fatalf("reason=%v", ev.Reason)
	}
	// 事件通道必须恰好产生一个事件后关闭。
	if _, ok := <-client.Closed(); ok {
		t.Fatal("expected exactly one close event")
	}
}

func TestConnHeartbeatTimeout(t *testing.T) {
	p1, p2 := net.Pipe()
	// 对端只读不回 pong，模拟弱网对端失联。
	go func() {
		_, _ = io.Copy(io.Discard, p2)
	}()
	cfg := Config{
		HeartbeatInterval:  30 * time.Millisecond,
		HeartbeatMaxMissed: 2,
	}
	c := NewConn(p1, cfg, extNegotiation{})
	go c.Run()
	ev := waitEvent(t, c, 3*time.Second)
	if ev.Reason != ReasonHeartbeatTimeout {
		t.Fatalf("reason=%v, want heartbeat_timeout", ev.Reason)
	}
	if ev.Code != CloseGoingAway {
		t.Fatalf("code=%v", ev.Code)
	}
}

func TestConnHeartbeatHealthy(t *testing.T) {
	cfg := Config{HeartbeatInterval: 20 * time.Millisecond, HeartbeatMaxMissed: 2}
	client, server := connPair(t, cfg, cfg, extNegotiation{}, extNegotiation{})
	// 双方互相应答 ping/pong，300ms 内不得有心跳超时。
	time.Sleep(300 * time.Millisecond)
	select {
	case ev := <-client.Closed():
		t.Fatalf("client closed unexpectedly: %+v", ev)
	default:
	}
	select {
	case ev := <-server.Closed():
		t.Fatalf("server closed unexpectedly: %+v", ev)
	default:
	}
}

func TestConnCompressionRoundTrip(t *testing.T) {
	ccfg := Config{Compression: CompressConfig{Enabled: true, MinSize: 10}}
	client, server := connPair(t, ccfg, ccfg, extNegotiation{enabled: true}, extNegotiation{enabled: true})
	payload := bytes.Repeat([]byte("highly-compressible-payload-"), 200)
	if err := client.Send(OpText, payload); err != nil {
		t.Fatal(err)
	}
	m, err := server.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m.Payload, payload) {
		t.Fatalf("decompressed %d bytes, want %d", len(m.Payload), len(payload))
	}
}

func TestConnRejectsRSV1WithoutNegotiation(t *testing.T) {
	p1, p2 := net.Pipe()
	server := NewConn(p2, Config{}, extNegotiation{}) // 未协商压缩
	go server.Run()
	// 手工构造带 RSV1 的文本帧。
	raw, err := MarshalFrame(&Frame{Fin: true, RSV1: true, OpCode: OpText, Payload: []byte("x")}, true)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = p1.Write(raw)
		_, _ = io.Copy(io.Discard, p1)
	}()
	ev := waitEvent(t, server, 2*time.Second)
	if ev.Reason != ReasonExtensionError {
		t.Fatalf("reason=%v, want extension_error", ev.Reason)
	}
	if !errors.Is(ev.Err, ErrExtensionNotNegotiated) {
		t.Fatalf("err=%v", ev.Err)
	}
}

func TestConnConcurrentSenders(t *testing.T) {
	cfg := Config{SendBlock: true}
	client, server := connPair(t, cfg, cfg, extNegotiation{}, extNegotiation{})
	const producers = 8
	const perProducer = 100
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				// 负载编码生产者与序号，便于校验帧不交叉。
				payload := []byte(fmt.Sprintf("p%02d-%04d-%s", p, i, bytes.Repeat([]byte{byte('a' + p)}, 50)))
				if err := client.Send(OpBinary, payload); err != nil {
					return
				}
			}
		}(p)
	}
	received := make(map[string]int)
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < producers*perProducer; i++ {
			m, err := server.ReadMessage()
			if err != nil {
				return
			}
			var p, seq int
			if _, err := fmt.Sscanf(string(m.Payload), "p%02d-%04d-", &p, &seq); err != nil {
				t.Errorf("corrupted payload: %q", m.Payload[:min(20, len(m.Payload))])
				return
			}
			// 校验负载尾部完整（无帧交叉）。头部 "p%02d-%04d-" 占 9 字节。
			for _, b := range m.Payload[9:] {
				if b != byte('a'+p) {
					t.Errorf("interleaved payload from p%d", p)
					return
				}
			}
			mu.Lock()
			received[fmt.Sprintf("p%02d-%04d", p, seq)]++
			mu.Unlock()
		}
	}()
	wg.Wait()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(received) != producers*perProducer {
		t.Fatalf("received %d unique messages, want %d", len(received), producers*perProducer)
	}
	for k, v := range received {
		if v != 1 {
			t.Fatalf("message %s received %d times", k, v)
		}
	}
}

func TestConnBackpressureQueueFull(t *testing.T) {
	p1, p2 := net.Pipe()
	// 对端永不读取：发送队列必然填满。
	cfg := Config{SendQueueSize: 4, SendBlock: false}
	c := NewConn(p1, cfg, extNegotiation{})
	go c.Run()
	defer func() {
		c.Close(CloseNormal, "")
		_ = p2.Close()
	}()
	var fullErr error
	for i := 0; i < 100; i++ {
		if err := c.Send(OpBinary, []byte("payload")); err != nil {
			fullErr = err
			break
		}
	}
	if !errors.Is(fullErr, ErrSendQueueFull) {
		t.Fatalf("got %v, want ErrSendQueueFull", fullErr)
	}
}

func TestConnSendBlockConvergesOnClose(t *testing.T) {
	p1, p2 := net.Pipe()
	cfg := Config{SendQueueSize: 2, SendBlock: true}
	c := NewConn(p1, cfg, extNegotiation{})
	go c.Run()
	// 对端不读取，阻塞式 Send 最终阻塞在队列满。
	blocked := make(chan error, 1)
	go func() {
		for i := 0; i < 1000; i++ {
			if err := c.Send(OpBinary, make([]byte, 10)); err != nil {
				blocked <- err
				return
			}
		}
		blocked <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	_ = p2.Close() // 模拟对端断链
	select {
	case err := <-blocked:
		if !errors.Is(err, ErrConnClosed) {
			t.Fatalf("blocked sender got %v, want ErrConnClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked sender did not converge after close")
	}
	waitEvent(t, c, 2*time.Second)
}

func TestConnReadAfterCloseFails(t *testing.T) {
	client, _ := connPair(t, Config{}, Config{}, extNegotiation{}, extNegotiation{})
	client.Close(CloseNormal, "")
	waitEvent(t, client, 2*time.Second)
	if _, err := client.ReadMessage(); !errors.Is(err, ErrConnClosed) {
		t.Fatalf("got %v, want ErrConnClosed", err)
	}
	if err := client.Send(OpText, []byte("x")); !errors.Is(err, ErrConnClosed) {
		t.Fatalf("send got %v, want ErrConnClosed", err)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
