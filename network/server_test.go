package network

import (
	"bufio"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/LaiJunda666/snailmq/broker"
	"github.com/LaiJunda666/snailmq/protocol"
)

// connCount 返回当前活跃连接数(测试用于等待连接处理 goroutine 退出)。
func (s *Server) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// startServer 起一个监听随机端口的 Server,并注册清理;返回地址、服务端与内核。
func startServer(t *testing.T) (string, *Server, *broker.Broker) {
	t.Helper()
	b := broker.New()
	srv := NewServer(b)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = ln.Close()
	})
	return ln.Addr().String(), srv, b
}

// dial 建立测试连接并设定读 deadline,避免用例挂死。
func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	return conn
}

// readFrame 读取一个完整帧,返回 opcode 与 payload。
func readFrame(t *testing.T, br *bufio.Reader) (protocol.Opcode, []byte) {
	t.Helper()
	op, payload, err := protocol.ReadFrame(br)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	return op, payload
}

// TestServerRejectsBadMagic 裸连发送坏帧头,验证服务端按协议拒绝并断开连接。
func TestServerRejectsBadMagic(t *testing.T) {
	addr, _, _ := startServer(t)
	conn := dial(t, addr)

	if _, err := conn.Write([]byte{0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00}); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 1)
	if _, err := conn.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("read after bad magic = %v; want io.EOF (server closed)", err)
	}
}

// TestServerPublishSubscribe 覆盖正常路径:订阅后连接转为推送流,能收到发布的消息。
func TestServerPublishSubscribe(t *testing.T) {
	addr, _, b := startServer(t)
	if err := b.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}

	subConn := dial(t, addr)
	subReader := bufio.NewReader(subConn)
	if _, err := subConn.Write(protocol.EncodeFrame(protocol.OpSubscribe, []byte("t"))); err != nil {
		t.Fatal(err)
	}
	if op, body := readFrame(t, subReader); op != protocol.OpSubscribe || len(body) != 0 {
		t.Fatalf("subscribe resp op=%d body=%v", op, body)
	}

	pubConn := dial(t, addr)
	pubReader := bufio.NewReader(pubConn)
	if _, err := pubConn.Write(protocol.EncodeFrame(protocol.OpPublish, protocol.EncodePublish("t", []byte("hello")))); err != nil {
		t.Fatal(err)
	}
	op, body := readFrame(t, pubReader)
	if op != protocol.OpPublish {
		t.Fatalf("publish resp op=%d", op)
	}
	if off, err := protocol.UnmarshalOffset(body); err != nil || off != 0 {
		t.Fatalf("publish offset = %d, %v; want 0, nil", off, err)
	}

	op, body = readFrame(t, subReader)
	if op != protocol.OpMessage {
		t.Fatalf("push op=%d; want OpMessage", op)
	}
	off, payload, err := protocol.DecodeMessage(body)
	if err != nil || off != 0 || string(payload) != "hello" {
		t.Fatalf("pushed msg off=%d payload=%q err=%v", off, payload, err)
	}
}

// TestServerCloseReturnsWithIdleConn 验证空闲连接下 Close 不会挂起(H1 回归)。
func TestServerCloseReturnsWithIdleConn(t *testing.T) {
	addr, srv, _ := startServer(t)
	_ = dial(t, addr) // 连上后不发任何字节

	done := make(chan error, 1)
	go func() { done <- srv.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close err = %v; want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Server.Close hung on idle connection")
	}
}

// TestServerReadTimeoutClosesIdleConn 验证空闲连接会因读超时被关闭(M2 缓解回归)。
func TestServerReadTimeoutClosesIdleConn(t *testing.T) {
	b := broker.New()
	srv := NewServer(b, WithReadTimeout(50*time.Millisecond))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = ln.Close()
	})

	conn := dial(t, ln.Addr().String())
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("read on idle conn = %v; want io.EOF (server closed on read timeout)", err)
	}
}

// TestServerWriteDeadlineFreshPerBatch 验证空闲超过 writeTimeout 后收到大批量推送仍能送达(H1 回归):
// 写 deadline 必须在整批写入前刷新,而非写完之后。
func TestServerWriteDeadlineFreshPerBatch(t *testing.T) {
	b := broker.New()
	srv := NewServer(b, WithWriteTimeout(80*time.Millisecond))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = ln.Close()
	})
	addr := ln.Addr().String()

	if err := b.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}

	subConn := dial(t, addr)
	subReader := bufio.NewReader(subConn)
	if _, err := subConn.Write(protocol.EncodeFrame(protocol.OpSubscribe, []byte("t"))); err != nil {
		t.Fatal(err)
	}
	if op, _ := readFrame(t, subReader); op != protocol.OpSubscribe {
		t.Fatalf("subscribe resp op=%d", op)
	}

	// 空闲超过 writeTimeout,使上一轮 deadline 过期。
	time.Sleep(250 * time.Millisecond)

	payload := make([]byte, 16<<10) // 超过 bufio 4KiB 缓冲,触发批内底层写
	pub := mustDial(t, addr)
	if _, err := pub.Publish("t", payload); err != nil {
		t.Fatal(err)
	}

	op, body := readFrame(t, subReader)
	if op != protocol.OpMessage {
		t.Fatalf("push op=%d; want OpMessage", op)
	}
	_, got, err := protocol.DecodeMessage(body)
	if err != nil || len(got) != len(payload) {
		t.Fatalf("pushed payload len=%d err=%v; want %d", len(got), err, len(payload))
	}
}

// stubAddr 是 singleConnListener 的占位地址。
type stubAddr struct{}

func (stubAddr) Network() string { return "pipe" }
func (stubAddr) String() string  { return "pipe" }

// singleConnListener 只返回一条预置连接,之后的 Accept 阻塞到 Close。
type singleConnListener struct {
	conn net.Conn
	done chan struct{}
}

func newSingleConnListener(conn net.Conn) *singleConnListener {
	return &singleConnListener{conn: conn, done: make(chan struct{})}
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	if l.conn != nil {
		conn := l.conn
		l.conn = nil
		return conn, nil
	}
	<-l.done
	return nil, errors.New("network: listener closed")
}

func (l *singleConnListener) Close() error {
	close(l.done)
	return nil
}

func (l *singleConnListener) Addr() net.Addr { return stubAddr{} }

// TestServerMaxConns 验证连接数达上限时,新连接被拒绝且不计入活跃连接。
func TestServerMaxConns(t *testing.T) {
	b := broker.New()
	srv := NewServer(b, WithMaxConns(1))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = ln.Close()
	})
	addr := ln.Addr().String()
	if err := b.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}

	first, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	if err := first.CreateTopic("t2"); err != nil {
		t.Fatal(err)
	}

	second, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	// 满员连接会被服务端回 OpError 后断开(也可能先收到连接复位),两种都算拒绝。
	if err := second.CreateTopic("t3"); err == nil {
		t.Fatal("second connection CreateTopic succeeded; want rejection")
	}
	if got := srv.connCount(); got != 1 {
		t.Fatalf("connCount = %d; want 1 (second must not be registered)", got)
	}
}

// TestRemoteErrorMapping 验证客户端把协议错误码映射回本地哨兵错误。
func TestRemoteErrorMapping(t *testing.T) {
	if !errors.Is(remoteError(protocol.CodeOverloaded, "x"), ErrOverloaded) {
		t.Fatal("CodeOverloaded should map to ErrOverloaded")
	}
	if !errors.Is(remoteError(protocol.CodeTooLarge, "x"), protocol.ErrTooLarge) {
		t.Fatal("CodeTooLarge should map to protocol.ErrTooLarge")
	}
	if !errors.Is(remoteError(protocol.CodeClosed, "x"), broker.ErrClosed) {
		t.Fatal("CodeClosed should map to broker.ErrClosed")
	}
}

// TestServerRequestWriteTimeoutClosesStalledClient 验证请求-响应阶段也有写超时:
// 客户端不读响应时,服务端写阻塞会因 writeTimeout 被清退,不会永久钉住 goroutine(M1 回归)。
// 用 net.Pipe 制造确定性写阻塞(TCP 缓冲会自适应放大,不可靠)。
func TestServerRequestWriteTimeoutClosesStalledClient(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })

	b := broker.New()
	srv := NewServer(b, WithWriteTimeout(100*time.Millisecond))
	ln := newSingleConnListener(serverConn)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = ln.Close()
	})
	if err := b.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}

	// 发一个请求但从不读响应:服务端 respond 的管道写会阻塞。
	bw := bufio.NewWriter(clientConn)
	_ = protocol.WriteFrame(bw, protocol.OpPublish, protocol.EncodePublish("t", []byte("x")))
	_ = bw.Flush()

	deadline := time.Now().Add(2 * time.Second)
	for srv.connCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("server kept stalled client: no write deadline in request phase")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSubscriptionClosedOnClientDisconnect 验证客户端断开后订阅被注销,不泄漏(H2 回归)。
func TestSubscriptionClosedOnClientDisconnect(t *testing.T) {
	addr, srv, b := startServer(t)
	if err := b.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}

	conn := dial(t, addr)
	br := bufio.NewReader(conn)
	if _, err := conn.Write(protocol.EncodeFrame(protocol.OpSubscribe, []byte("t"))); err != nil {
		t.Fatal(err)
	}
	if op, _ := readFrame(t, br); op != protocol.OpSubscribe {
		t.Fatalf("subscribe resp op=%d", op)
	}

	_ = conn.Close()

	// 先等连接处理 goroutine 退出(其 defer 会注销订阅),再读内部计数,避免与其写并发。
	deadline := time.Now().Add(2 * time.Second)
	for srv.connCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("connection handler did not exit after client disconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got, err := b.SubscriberCount("t"); err != nil || got != 0 {
		t.Fatalf("subscription leaked after client disconnect: count = %d, %v; want 0, nil", got, err)
	}
}
