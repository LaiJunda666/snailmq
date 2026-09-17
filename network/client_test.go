package network

import (
	"errors"
	"testing"
	"time"

	"github.com/LaiJunda666/mq-lite/broker"
	"github.com/LaiJunda666/mq-lite/protocol"
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

// TestClientPublishMissingTopicError 验证发布到不存在的 topic 返回错误。
func TestClientPublishMissingTopicError(t *testing.T) {
	addr, _, _ := startServer(t)
	c := mustDial(t, addr)
	if _, err := c.Publish("nope", []byte("x")); err == nil {
		t.Fatal("want error for missing topic")
	}
}

// TestClientSubscribeMissingTopicError 验证订阅不存在的 topic 返回错误。
func TestClientSubscribeMissingTopicError(t *testing.T) {
	addr, _, _ := startServer(t)
	c := mustDial(t, addr)
	if _, err := c.Subscribe("nope"); err == nil {
		t.Fatal("want error for missing topic")
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

	if _, err := c.Publish("t", make([]byte, protocol.MaxPayload+1)); !errors.Is(err, protocol.ErrTooLarge) {
		t.Fatalf("oversized Publish err = %v; want protocol.ErrTooLarge", err)
	}
	if _, err := c.Publish("t", []byte("ok")); err != nil {
		t.Fatalf("Publish after rejection err = %v; want nil (connection still usable)", err)
	}
}
