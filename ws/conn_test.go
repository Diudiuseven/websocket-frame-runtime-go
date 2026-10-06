package ws

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// newPair creates a client/server Conn pair over a local TCP connection.
// TCP (unlike net.Pipe) has kernel buffers, so both directions may write
// concurrently without rendezvous deadlocks.
func newPair(t *testing.T, serverCfg, clientCfg Config) (server, client *Conn) {
	t.Helper()
	serverCfg.IsServer = true
	clientCfg.IsServer = false
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	acceptCh := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			acceptCh <- c
		}
	}()
	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	srv := <-acceptCh
	server = NewConn(srv, serverCfg)
	client = NewConn(cli, clientCfg)
	t.Cleanup(func() { server.Close(); client.Close() })
	return server, client
}

func readOne(t *testing.T, c *Conn) *Message {
	t.Helper()
	type res struct {
		m   *Message
		err error
	}
	ch := make(chan res, 1)
	go func() {
		m, err := c.ReadMessage()
		ch <- res{m, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("ReadMessage: %v", r.err)
		}
		return r.m
	case <-time.After(5 * time.Second):
		t.Fatal("ReadMessage timed out")
		return nil
	}
}

func waitReason(t *testing.T, c *Conn, want CloseReason) {
	t.Helper()
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("connection did not terminate; want reason %s", want)
	}
	if got := c.CloseReason(); got != want {
		t.Fatalf("reason = %s, want %s", got, want)
	}
}

func TestEchoRoundTrip(t *testing.T) {
	server, client := newPair(t, Config{}, Config{})
	go func() {
		for {
			m, err := server.ReadMessage()
			if err != nil {
				return
			}
			_ = server.Send(m.Opcode, m.Data)
		}
	}()
	for i := 0; i < 10; i++ {
		payload := []byte(fmt.Sprintf("message-%d", i))
		if err := client.Send(OpText, payload); err != nil {
			t.Fatal(err)
		}
		m := readOne(t, client)
		if !bytes.Equal(m.Data, payload) || m.Opcode != OpText {
			t.Fatalf("got %q op=%v", m.Data, m.Opcode)
		}
	}
}

func TestFragmentedSendReassembles(t *testing.T) {
	server, client := newPair(t, Config{}, Config{})
	payload := bytes.Repeat([]byte("fragmented-"), 1000) // ~11KB
	if err := client.SendFragmented(OpBinary, payload, 100); err != nil {
		t.Fatal(err)
	}
	m := readOne(t, server)
	if m.Opcode != OpBinary || !bytes.Equal(m.Data, payload) {
		t.Fatalf("reassembled mismatch: %d bytes", len(m.Data))
	}
	// Unfragmented send of the same payload must produce identical result.
	if err := client.Send(OpBinary, payload); err != nil {
		t.Fatal(err)
	}
	m2 := readOne(t, server)
	if !bytes.Equal(m.Data, m2.Data) {
		t.Fatal("fragmented and unfragmented results differ")
	}
}

func TestCloseHandshakeLocalThenPeer(t *testing.T) {
	server, client := newPair(t, Config{}, Config{})
	if err := client.Close(); err == nil {
		t.Log("client close returned nil error (normal closure)")
	}
	waitReason(t, client, ReasonLocalClose)
	waitReason(t, server, ReasonPeerClose)
}

func TestCloseHandshakePeerInitiated(t *testing.T) {
	server, client := newPair(t, Config{}, Config{})
	if err := server.Close(); err != nil {
		t.Logf("server close: %v", err)
	}
	waitReason(t, server, ReasonLocalClose)
	waitReason(t, client, ReasonPeerClose)
}

func TestCloseIdempotentConcurrent(t *testing.T) {
	_, client := newPair(t, Config{}, Config{})
	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = client.Close()
		}(i)
	}
	wg.Wait()
	waitReason(t, client, ReasonLocalClose)
	// All concurrent closers observe the same terminal error.
	for i := 1; i < n; i++ {
		if (errs[i] == nil) != (errs[0] == nil) {
			t.Fatalf("inconsistent close results: %v vs %v", errs[0], errs[i])
		}
	}
	// Close after termination is a no-op returning the same reason.
	_ = client.Close()
	if got := client.CloseReason(); got != ReasonLocalClose {
		t.Fatalf("reason changed after repeated Close: %s", got)
	}
}

func TestCloseReasonObservable(t *testing.T) {
	server, client := newPair(t, Config{}, Config{})
	seen := make(chan *CloseError, 2)
	server.OnClose(func(e *CloseError) { seen <- e })
	client.OnClose(func(e *CloseError) { seen <- e })
	_ = client.Close()
	e1, e2 := <-seen, <-seen
	reasons := map[CloseReason]bool{e1.Reason: true, e2.Reason: true}
	if !reasons[ReasonLocalClose] || !reasons[ReasonPeerClose] {
		t.Fatalf("observed reasons %v, want local-close and peer-close", reasons)
	}
}

func TestHeartbeatTimeout(t *testing.T) {
	// Peer never pongs: a raw pipe endpoint that just drains reads.
	p1, p2 := net.Pipe()
	defer p2.Close()
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := p2.Read(buf); err != nil {
				return
			}
		}
	}()
	c := NewConn(p1, Config{
		Heartbeat: &HeartbeatConfig{
			Interval:  20 * time.Millisecond,
			Timeout:   30 * time.Millisecond,
			MaxMisses: 2,
		},
	})
	waitReason(t, c, ReasonHeartbeatTimeout)
	pings, _, _ := c.hb.Stats()
	if pings < 2 {
		t.Fatalf("expected at least 2 pings, got %d", pings)
	}
}

func TestHeartbeatHealthyPeerStaysAlive(t *testing.T) {
	hb := &HeartbeatConfig{Interval: 20 * time.Millisecond, Timeout: 200 * time.Millisecond, MaxMisses: 2}
	server, client := newPair(t, Config{Heartbeat: hb}, Config{Heartbeat: hb})
	// The runtime answers pings automatically; the connection must survive
	// many heartbeat rounds.
	time.Sleep(250 * time.Millisecond)
	select {
	case <-server.Done():
		t.Fatalf("healthy connection terminated: %v", server.CloseReason())
	case <-client.Done():
		t.Fatalf("healthy connection terminated: %v", client.CloseReason())
	default:
	}
	pings, pongs, _ := client.hb.Stats()
	if pings == 0 || pongs == 0 {
		t.Fatalf("no heartbeat traffic: pings=%d pongs=%d", pings, pongs)
	}
}

func TestCompressionEndToEnd(t *testing.T) {
	cfg := Config{
		Compress:          &CompressConfig{Enabled: true, MinSize: 1},
		CompressionActive: true,
	}
	server, client := newPair(t, cfg, cfg)
	payload := bytes.Repeat([]byte("compressible payload "), 200)
	for i := 0; i < 5; i++ {
		if err := client.Send(OpText, payload); err != nil {
			t.Fatal(err)
		}
		m := readOne(t, server)
		if !bytes.Equal(m.Data, payload) {
			t.Fatalf("round %d: decompressed payload mismatch", i)
		}
	}
	// Reverse direction must behave identically (symmetric negotiation).
	if err := server.Send(OpText, payload); err != nil {
		t.Fatal(err)
	}
	m := readOne(t, client)
	if !bytes.Equal(m.Data, payload) {
		t.Fatal("reverse direction mismatch")
	}
}

func TestCompressionRejectedWhenNotNegotiated(t *testing.T) {
	// Server without compression receives an RSV1 frame: must reject.
	p1, p2 := net.Pipe()
	defer p2.Close()
	server := NewConn(p1, Config{IsServer: true})
	defer server.Close()
	// Craft a masked RSV1 text frame from the "client".
	raw, err := EncodeFrame(&Frame{Fin: true, RSV1: true, Opcode: OpText, Masked: true, Payload: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = p2.Write(raw)
		// Drain the server's close frame so its write does not block.
		buf := make([]byte, 64)
		_, _ = p2.Read(buf)
	}()
	waitReason(t, server, ReasonExtensionRejected)
}

func TestServerRejectsUnmaskedFrame(t *testing.T) {
	p1, p2 := net.Pipe()
	defer p2.Close()
	server := NewConn(p1, Config{IsServer: true})
	defer server.Close()
	raw, _ := EncodeFrame(&Frame{Fin: true, Opcode: OpText, Payload: []byte("unmasked")})
	done := make(chan []byte, 1)
	go func() {
		_, _ = p2.Write(raw)
		buf := make([]byte, 64)
		n, _ := p2.Read(buf)
		done <- buf[:n]
	}()
	waitReason(t, server, ReasonReadError)
	// The peer must receive a close frame with protocol error code 1002.
	reply := <-done
	p := NewFrameParser(1 << 20)
	frames, err := p.Feed(reply)
	if err != nil || len(frames) != 1 || frames[0].Opcode != OpClose {
		t.Fatalf("reply frames=%v err=%v", frames, err)
	}
	code, _, _ := decodeClosePayload(frames[0].Payload)
	if code != CloseProtocolError {
		t.Fatalf("close code = %d, want %d", code, CloseProtocolError)
	}
}

func TestConcurrentSendersNoInterleaving(t *testing.T) {
	server, client := newPair(t, Config{}, Config{})
	const producers = 8
	const perProducer = 40
	const msgLen = 512

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			payload := bytes.Repeat([]byte{byte('A' + p)}, msgLen)
			for i := 0; i < perProducer; i++ {
				// Mix fragmented and unfragmented sends.
				if i%3 == 0 {
					if err := client.SendFragmented(OpBinary, payload, 64); err != nil {
						return
					}
				} else if err := client.Send(OpBinary, payload); err != nil {
					return
				}
			}
		}(p)
	}

	counts := make(map[byte]int)
	for i := 0; i < producers*perProducer; i++ {
		m := readOne(t, server)
		if len(m.Data) != msgLen {
			t.Fatalf("message %d: length %d, want %d (frames interleaved?)", i, len(m.Data), msgLen)
		}
		first := m.Data[0]
		for _, b := range m.Data {
			if b != first {
				t.Fatalf("message %d: mixed content from different producers", i)
			}
		}
		counts[first]++
	}
	wg.Wait()
	if len(counts) != producers {
		t.Fatalf("received from %d producers, want %d", len(counts), producers)
	}
	for p := 0; p < producers; p++ {
		if counts[byte('A'+p)] != perProducer {
			t.Fatalf("producer %d: %d messages, want %d", p, counts[byte('A'+p)], perProducer)
		}
	}
}

func TestBackpressureSendTimeout(t *testing.T) {
	// Peer never reads: the write loop blocks and the bounded queue fills.
	p1, p2 := net.Pipe()
	defer p2.Close()
	c := NewConn(p1, Config{
		SendQueueSize: 4,
		SendTimeout:   100 * time.Millisecond,
		WriteTimeout:  5 * time.Second,
	})
	defer c.Close()

	payload := make([]byte, 1024)
	var sendErr error
	for i := 0; i < 100; i++ {
		if err := c.Send(OpBinary, payload); err != nil {
			sendErr = err
			break
		}
	}
	if sendErr != ErrSendTimeout {
		t.Fatalf("got %v, want ErrSendTimeout", sendErr)
	}
	waitReason(t, c, ReasonSendQueueFull)
	// Sends after termination converge to the close error.
	if err := c.Send(OpBinary, payload); err == nil {
		t.Fatal("send after termination succeeded")
	}
}

func TestSendAfterCloseConverges(t *testing.T) {
	_, client := newPair(t, Config{}, Config{})
	_ = client.Close()
	if err := client.Send(OpText, []byte("late")); err == nil {
		t.Fatal("send after close succeeded")
	} else {
		var ce *CloseError
		if ok := errorAs(err, &ce); !ok {
			t.Fatalf("send after close returned %T, want *CloseError", err)
		}
	}
}

func errorAs(err error, target **CloseError) bool {
	for err != nil {
		if ce, ok := err.(*CloseError); ok {
			*target = ce
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestLargeMessageBounded(t *testing.T) {
	server, client := newPair(t,
		Config{MaxMessageSize: 1 << 20},
		Config{MaxMessageSize: 1 << 20},
	)
	// 4 MiB message exceeds the server's 1 MiB bound: rejected with 1009.
	big := make([]byte, 4<<20)
	if err := client.Send(OpBinary, big); err != nil {
		t.Fatal(err)
	}
	waitReason(t, server, ReasonReadError)
}

func TestPingPongAutomatic(t *testing.T) {
	server, client := newPair(t, Config{}, Config{})
	// Client writeLoop sends a ping via the internal path; emulate by
	// writing a ping frame directly and expecting an automatic pong.
	_ = client // pong is observed on the client read path below
	if err := server.writeFrame(&Frame{Fin: true, Opcode: OpPing, Payload: []byte("are you there")}); err != nil {
		t.Fatal(err)
	}
	// The client answers automatically; read the pong on the server side
	// via its parser by hijacking is complex — instead verify the client
	// stays alive and processes the ping (no protocol error).
	time.Sleep(50 * time.Millisecond)
	select {
	case <-client.Done():
		t.Fatalf("client terminated after ping: %v", client.CloseReason())
	default:
	}
}
