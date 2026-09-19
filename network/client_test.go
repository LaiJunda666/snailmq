package network

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/LaiJunda666/snailmq/broker"
	"github.com/LaiJunda666/snailmq/protocol"
)

// mustDial 连接服务端并在测试结束时关闭。
func mustDial(t *testing.T, addr string) *Client {
	t.Helper()
	c, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestClientEndToEnd 覆盖完整闭环:建 topic → 发布 → 订阅 → 读到消息。
func TestClientEndToEnd(t *testing.T) {
	addr, _, _ := startServer(t)
	c := mustDial(t, addr)

	if err := c.CreateTopic("greetings"); err != nil {
		t.Fatal(err)
	}
	off, err := c.Publish("greetings", []byte("hello"))
	if err != nil || off != 0 {
		t.Fatalf("Publish = %d, %v; want 0, nil", off, err)
	}

	sub, err := c.Subscribe("greetings")
	if err != nil {
		t.Fatal(err)
	}
	m, err := sub.Read()
	if err != nil {
		t.Fatal(err)
	}
	if m.Offset != 0 || string(m.Payload) != "hello" {
		t.Fatalf("got %+v; want offset 0 payload hello", m)
	}
}

// TestClientTwoSubscribersBroadcast 验证两个订阅者都能收到同一 topic 的广播。
// 发布使用独立连接:订阅成功后该连接已转为推送流,不能再发布。
func TestClientTwoSubscribersBroadcast(t *testing.T) {
	addr, _, _ := startServer(t)

	pub := mustDial(t, addr)
	if err := pub.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}
	sub1, err := mustDial(t, addr).Subscribe("t")
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := mustDial(t, addr).Subscribe("t")
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range []string{"a", "b", "c"} {
		if _, err := pub.Publish("t", []byte(p)); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 3; i++ {
		m1, err := sub1.Read()
		if err != nil {
			t.Fatal(err)
		}
		m2, err := sub2.Read()
		if err != nil {
			t.Fatal(err)
		}
		if string(m1.Payload) != string(m2.Payload) {
			t.Fatalf("sub1=%q sub2=%q", m1.Payload, m2.Payload)
		}
	}
}

// TestClientPublishMissingTopicError 验证发布到不存在的 topic 返回结构化错误(可 errors.Is)。
func TestClientPublishMissingTopicError(t *testing.T) {
	addr, _, _ := startServer(t)
	c := mustDial(t, addr)
	if _, err := c.Publish("nope", []byte("x")); !errors.Is(err, broker.ErrTopicNotFound) {
		t.Fatalf("err = %v; want broker.ErrTopicNotFound", err)
	}
}

// TestClientSubscribeMissingTopicError 验证订阅不存在的 topic 返回结构化错误(可 errors.Is)。
func TestClientSubscribeMissingTopicError(t *testing.T) {
	addr, _, _ := startServer(t)
	c := mustDial(t, addr)
	if _, err := c.Subscribe("nope"); !errors.Is(err, broker.ErrTopicNotFound) {
		t.Fatalf("err = %v; want broker.ErrTopicNotFound", err)
	}
}

// TestClientSubscriptionReceivesLateMessage 验证先订阅、后发布时,阻塞的 Read 能被唤醒。
func TestClientSubscriptionReceivesLateMessage(t *testing.T) {
	addr, _, _ := startServer(t)
	pub := mustDial(t, addr)
	if err := pub.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}
	sub, err := mustDial(t, addr).Subscribe("t")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = pub.Publish("t", []byte("late"))
	}()

	done := make(chan broker.Message, 1)
	errCh := make(chan error, 1)
	go func() {
		m, err := sub.Read()
		if err != nil {
			errCh <- err
			return
		}
		done <- m
	}()

	select {
	case m := <-done:
		if string(m.Payload) != "late" {
			t.Fatalf("payload=%q; want late", m.Payload)
		}
	case err := <-errCh:
		t.Fatalf("Read err: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for late message")
	}
}

// TestSocketBufferOptionsEndToEnd 验证服务端与客户端的内核缓冲选项下闭环仍正常。
func TestSocketBufferOptionsEndToEnd(t *testing.T) {
	b := broker.New()
	srv := NewServer(b, WithReadBuffer(64<<10), WithWriteBuffer(64<<10))
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

	subClient, err := Dial(addr, WithClientReadBuffer(64<<10), WithClientWriteBuffer(64<<10))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = subClient.Close() })
	sub, err := subClient.Subscribe("t")
	if err != nil {
		t.Fatal(err)
	}

	pub, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	if _, err := pub.Publish("t", []byte("hello")); err != nil {
		t.Fatal(err)
	}

	m, err := sub.Read()
	if err != nil || string(m.Payload) != "hello" {
		t.Fatalf("Read = %+v, %v; want hello", m, err)
	}
}

// TestClientPublishBatch 验证批量发布返回首条 offset 且订阅者按序收到全部消息。
func TestClientPublishBatch(t *testing.T) {
	addr, _, _ := startServer(t)
	setup := mustDial(t, addr)
	if err := setup.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}
	sub, err := mustDial(t, addr).Subscribe("t")
	if err != nil {
		t.Fatal(err)
	}
	pub := mustDial(t, addr)

	base, err := pub.PublishBatch("t", [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil || base != 0 {
		t.Fatalf("PublishBatch = %d, %v; want 0, nil", base, err)
	}
	for i, want := range []string{"a", "b", "c"} {
		m, err := sub.Read()
		if err != nil || string(m.Payload) != want || m.Offset != int64(i) {
			t.Fatalf("msg[%d] = %+v, %v; want %q offset %d", i, m, err, want, i)
		}
	}
}

// TestClientPublishAsync 验证 fire-and-forget 发布:不等 ack,但消息仍最终到达订阅者。
func TestClientPublishAsync(t *testing.T) {
	addr, _, _ := startServer(t)
	setup := mustDial(t, addr)
	if err := setup.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}
	sub, err := mustDial(t, addr).Subscribe("t")
	if err != nil {
		t.Fatal(err)
	}
	pub := mustDial(t, addr)
	for i := range 3 {
		if err := pub.PublishAsync("t", fmt.Appendf(nil, "m%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 3 {
		m, err := sub.Read()
		if err != nil || string(m.Payload) != fmt.Sprintf("m%d", i) {
			t.Fatalf("msg[%d] = %+v, %v", i, m, err)
		}
	}
}

// TestBatcher 验证攒批发布:达到 maxBatch 或 Close 时 flush,offset 连续。
func TestBatcher(t *testing.T) {
	addr, _, _ := startServer(t)
	setup := mustDial(t, addr)
	if err := setup.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}
	sub, err := mustDial(t, addr).Subscribe("t")
	if err != nil {
		t.Fatal(err)
	}
	pub := mustDial(t, addr)

	b := NewBatcher(pub, "t", 2, 50*time.Millisecond)
	if err := b.Add([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := b.Add([]byte("b")); err != nil { // 达到 maxBatch,立即 flush
		t.Fatal(err)
	}
	if err := b.Add([]byte("c")); err != nil {
		t.Fatal(err)
	}
	base, count, err := b.Flush()
	if err != nil || base != 2 || count != 1 {
		t.Fatalf("Flush = %d, %d, %v; want 2, 1, nil", base, count, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	for i, want := range []string{"a", "b", "c"} {
		m, err := sub.Read()
		if err != nil || string(m.Payload) != want || m.Offset != int64(i) {
			t.Fatalf("msg[%d] = %+v, %v; want %q", i, m, err, want)
		}
	}
}

// TestSubscriptionReadBatch 验证批量读:循环 ReadBatch 能收满全部消息且保序。
func TestSubscriptionReadBatch(t *testing.T) {
	addr, _, _ := startServer(t)
	setup := mustDial(t, addr)
	if err := setup.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}
	sub, err := mustDial(t, addr).Subscribe("t")
	if err != nil {
		t.Fatal(err)
	}
	pub := mustDial(t, addr)

	const total = 10
	payloads := make([][]byte, total)
	for i := range payloads {
		payloads[i] = fmt.Appendf(nil, "m%d", i)
	}
	if _, err := pub.PublishBatch("t", payloads); err != nil {
		t.Fatal(err)
	}

	got := 0
	for got < total {
		msgs, err := sub.ReadBatch(4)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			if string(m.Payload) != fmt.Sprintf("m%d", got) {
				t.Fatalf("msg[%d] = %q; want m%d", got, m.Payload, got)
			}
			got++
		}
	}
}

// TestDialTimeout 验证带超时的 Dial 可正常连接,且连接失败会返回错误。
func TestDialTimeout(t *testing.T) {
	addr, _, _ := startServer(t)
	c, err := DialTimeout(addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}

	if _, err := DialTimeout("127.0.0.1:1", 100*time.Millisecond); err == nil {
		t.Fatal("expected error dialing closed port")
	}
}

// TestClientRejectsRequestsAfterSubscribe 验证进入推送流后,再次订阅/发布/建 topic
// 返回 ErrStreaming,而不是把推送帧当成响应吞掉(M2 回归)。
func TestClientRejectsRequestsAfterSubscribe(t *testing.T) {
	addr, _, _ := startServer(t)
	setup := mustDial(t, addr)
	if err := setup.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}

	c := mustDial(t, addr)
	if _, err := c.Subscribe("t"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Subscribe("t"); !errors.Is(err, ErrStreaming) {
		t.Fatalf("2nd Subscribe err = %v; want ErrStreaming", err)
	}
	if _, err := c.Publish("t", []byte("x")); !errors.Is(err, ErrStreaming) {
		t.Fatalf("Publish after Subscribe err = %v; want ErrStreaming", err)
	}
	if err := c.CreateTopic("t2"); !errors.Is(err, ErrStreaming) {
		t.Fatalf("CreateTopic after Subscribe err = %v; want ErrStreaming", err)
	}
}

// TestClientPublishTooLargeRejectedLocally 验证超大 payload 在本地被拒(ErrTooLarge),
// 不写出非法帧、不断连,之后仍可正常发布(L1 回归)。
func TestClientPublishTooLargeRejectedLocally(t *testing.T) {
	addr, _, _ := startServer(t)
	c := mustDial(t, addr)
	if err := c.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Publish("t", make([]byte, protocol.MaxMessage+1)); !errors.Is(err, protocol.ErrTooLarge) {
		t.Fatalf("oversized Publish err = %v; want protocol.ErrTooLarge", err)
	}
	if _, err := c.Publish("t", []byte("ok")); err != nil {
		t.Fatalf("Publish after rejection err = %v; want nil (connection still usable)", err)
	}
}

// TestClientClosedReturnsErrClosed 验证 Close 后各方法返回明确的 ErrClosed(而非底层 conn 错误)。
func TestClientClosedReturnsErrClosed(t *testing.T) {
	addr, _, _ := startServer(t)
	c := mustDial(t, addr)
	if err := c.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Subscribe("t"); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Publish("t", []byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish after Close err = %v; want ErrClosed", err)
	}
	if err := c.CreateTopic("t2"); !errors.Is(err, ErrClosed) {
		t.Fatalf("CreateTopic after Close err = %v; want ErrClosed", err)
	}
	if _, err := c.Subscribe("t"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe after Close err = %v; want ErrClosed", err)
	}
}
