package broker

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// TestPublishThenReadOrdering 验证先发布多条、后订阅的订阅者
// 从 offset 0 读到全量且保持发布顺序。
func TestPublishThenReadOrdering(t *testing.T) {
	p := newPartition(newMemoryLog())
	for _, payload := range []string{"m0", "m1", "m2"} {
		if _, err := p.Publish([]byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	s, err := p.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	got := readAll(t, s, 3, 0)
	want := []string{"m0", "m1", "m2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("read = %v; want %v", got, want)
	}
}

// TestSubscribeBeforePublishBlocksThenReads 验证先订阅、后发布时,
// 阻塞中的 Read 应在新消息到达后被唤醒并返回。
func TestSubscribeBeforePublishBlocksThenReads(t *testing.T) {
	p := newPartition(newMemoryLog())
	s, err := p.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		_, _ = p.Publish([]byte("late"))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msgs, err := s.Read(ctx, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].Payload) != "late" || msgs[0].Offset != 0 {
		t.Fatalf("got %+v; want single 'late' offset 0", msgs)
	}
}

// TestBroadcastIndependentCursors 验证多个订阅者游标互相独立:
// 各自从头消费不受他人进度影响,且新消息广播给所有订阅者。
func TestBroadcastIndependentCursors(t *testing.T) {
	p := newPartition(newMemoryLog())
	for _, payload := range []string{"a", "b", "c", "d", "e"} {
		_, _ = p.Publish([]byte(payload))
	}
	s1, _ := p.Subscribe()
	s2, _ := p.Subscribe()
	if got := readAll(t, s1, 2, 0); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("s1 = %v", got)
	}
	// s2 从头读全量,证明游标独立
	if got := readAll(t, s2, 5, 0); !reflect.DeepEqual(got, []string{"a", "b", "c", "d", "e"}) {
		t.Fatalf("s2 = %v", got)
	}
	// 新消息两订阅者都收到(广播)
	_, _ = p.Publish([]byte("f"))
	if got := readAll(t, s1, 4, 0); !reflect.DeepEqual(got, []string{"c", "d", "e", "f"}) {
		t.Fatalf("s1 tail = %v", got)
	}
	if got := readAll(t, s2, 1, 0); !reflect.DeepEqual(got, []string{"f"}) {
		t.Fatalf("s2 tail = %v", got)
	}
}

// TestReadContextCancelledWhenIdle 验证空闲阻塞的 Read 在 ctx 超时后
// 返回 context.DeadlineExceeded。
func TestReadContextCancelledWhenIdle(t *testing.T) {
	p := newPartition(newMemoryLog())
	s, _ := p.Subscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := s.Read(ctx, 10); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v; want DeadlineExceeded", err)
	}
}

// TestCloseWakesBlockedRead 验证订阅者 Close 会唤醒阻塞中的 Read 并返回 ErrClosed。
func TestCloseWakesBlockedRead(t *testing.T) {
	p := newPartition(newMemoryLog())
	s, _ := p.Subscribe()
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = s.Close()
	}()
	if _, err := s.Read(context.Background(), 10); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v; want ErrClosed", err)
	}
}

// TestPartitionCloseWakesBlockedRead 验证分区 close 会唤醒阻塞中的 Read 并返回 ErrClosed。
func TestPartitionCloseWakesBlockedRead(t *testing.T) {
	p := newPartition(newMemoryLog())
	s, _ := p.Subscribe()
	go func() {
		time.Sleep(20 * time.Millisecond)
		p.close()
	}()
	if _, err := s.Read(context.Background(), 10); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v; want ErrClosed", err)
	}
}

// TestPublishAfterCloseErrors 验证分区关闭后 Publish 返回 ErrClosed。
func TestPublishAfterCloseErrors(t *testing.T) {
	p := newPartition(newMemoryLog())
	p.close()
	if _, err := p.Publish([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v; want ErrClosed", err)
	}
}

// readAll 读取 total 条消息并返回 payload;timeout 秒超时(0 表示默认 2 秒)。
func readAll(t *testing.T, s *Subscription, total int, timeout time.Duration) []string {
	t.Helper()
	if timeout == 0 {
		timeout = 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout*time.Second)
	defer cancel()
	var out []string
	for len(out) < total {
		msgs, err := s.Read(ctx, total-len(out))
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		for _, m := range msgs {
			out = append(out, string(m.Payload))
		}
	}
	return out
}
