package network

import (
	"bufio"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/LaiJunda666/mq-lite/broker"
	"github.com/LaiJunda666/mq-lite/protocol"
)

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
	op, n, err := protocol.ReadHeader(br)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	body := make([]byte, n)
	if n > 0 {
		if _, err := br.Read(body); err != nil {
			t.Fatalf("read body: %v", err)
		}
	}
	return op, body
}

// TestServerRejectsBadMagic 裸连发送坏帧头,验证服务端按协议拒绝并断开连接。
func TestServerRejectsBadMagic(t *testing.T) {
	addr, _, _ := startServer(t)
	conn := dial(t, addr)

	if _, err := conn.Write([]byte{0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00}); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected connection close after bad magic")
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
	srv := NewServer(b)
	srv.readTimeout = 50 * time.Millisecond
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
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected idle connection to be closed by read timeout")
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
	if got := subscriptionCount(t, b); got != 0 {
		t.Fatalf("subscription leaked after client disconnect: subs = %d", got)
	}
}

// subscriptionCount 只读地统计 Broker 各分区上的订阅数(用于泄漏回归,测试专用)。
func subscriptionCount(t *testing.T, b *broker.Broker) int {
	t.Helper()
	v := reflect.ValueOf(b).Elem()
	topics := v.FieldByName("topics")
	total := 0
	for _, key := range topics.MapKeys() {
		partition := topics.MapIndex(key).Elem().FieldByName("partition").Elem()
		total += partition.FieldByName("subs").Len()
	}
	return total
}
