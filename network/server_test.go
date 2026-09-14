package network

import (
	"net"
	"testing"
	"time"

	"github.com/LaiJunda666/mq-lite/broker"
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

// TestServerRejectsBadMagic 裸连发送坏帧头,验证服务端按协议拒绝并断开连接。
func TestServerRejectsBadMagic(t *testing.T) {
	addr, _, _ := startServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.Write([]byte{0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00}); err != nil {
		t.Fatal(err)
	}

	// 服务端应断开连接:随后的读取应失败(EOF / reset)。
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected connection close after bad magic")
	}
}
