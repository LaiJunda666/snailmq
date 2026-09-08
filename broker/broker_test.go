package broker

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// TestBrokerLifecycle 验证 broker 门面基本流程:建 topic、重复建报错、
// Publish/Subscribe/Read 闭环,以及操作不存在 topic 时报 ErrTopicNotFound。
func TestBrokerLifecycle(t *testing.T) {
	b := New()
	if err := b.CreateTopic("t1"); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("t1"); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("dup create err = %v; want ErrTopicExists", err)
	}
	off, err := b.Publish("t1", []byte("hi"))
	if err != nil || off != 0 {
		t.Fatalf("Publish = %d, %v; want 0, nil", off, err)
	}
	s, err := b.Subscribe("t1")
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := s.Read(context.Background(), 10)
	if err != nil || len(msgs) != 1 || string(msgs[0].Payload) != "hi" {
		t.Fatalf("Read = %v, %v", msgs, err)
	}
	if _, err := b.Publish("nope", []byte("x")); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("err = %v; want ErrTopicNotFound", err)
	}
	if _, err := b.Subscribe("nope"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("err = %v; want ErrTopicNotFound", err)
	}
}

// TestBrokerTwoTopicsIsolated 验证不同 topic 的消息互不串扰,订阅互不影响。
func TestBrokerTwoTopicsIsolated(t *testing.T) {
	b := New()
	_ = b.CreateTopic("a")
	_ = b.CreateTopic("b")
	_, _ = b.Publish("a", []byte("only-a"))
	s, _ := b.Subscribe("b")
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Close() // b 上无消息,订阅立即关闭即可,避免泄漏
	if _, err := b.Subscribe("a"); err != nil {
		t.Fatal(err)
	}
	// a 的订阅者读得到 a 的消息
	sa, _ := b.Subscribe("a")
	msgs, err := sa.Read(context.Background(), 10)
	if err != nil || len(msgs) != 1 || string(msgs[0].Payload) != "only-a" {
		t.Fatalf("a read = %v, %v", msgs, err)
	}
}

// TestBrokerClose 验证 Close 的关闭语义:第一次成功,第二次及之后的操作返回 ErrClosed。
func TestBrokerClose(t *testing.T) {
	b := New()
	_ = b.CreateTopic("t")
	if _, err := b.Publish("t", []byte("x")); err != nil {
		t.Fatal(err)
	}
	s, _ := b.Subscribe("t")
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("2nd Close err = %v; want ErrClosed", err)
	}
	if _, err := b.Publish("t", []byte("y")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish after close = %v; want ErrClosed", err)
	}
	if _, err := s.Read(context.Background(), 10); !errors.Is(err, ErrClosed) {
		t.Fatalf("Read after close = %v; want ErrClosed", err)
	}
}

// TestBrokerConcurrentPublishConsistency 让多个生产者并发发布到同一 topic,
// 验证订阅者读到的 offset 恰好一次、无重复无遗漏。
func TestBrokerConcurrentPublishConsistency(t *testing.T) {
	b := New()
	_ = b.CreateTopic("t")
	const producers, perProducer = 8, 500
	s, _ := b.Subscribe("t")
	var wg sync.WaitGroup
	for range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perProducer {
				if _, err := b.Publish("t", []byte(strings.Repeat("x", 4))); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	total := producers * perProducer
	seen := make([]int64, total)
	for i := range seen {
		seen[i] = -1
	}
	got := 0
	for got < total {
		msgs, err := s.Read(context.Background(), 64)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			if m.Offset < 0 || int(m.Offset) >= total {
				t.Fatalf("offset %d out of range", m.Offset)
			}
			seen[m.Offset]++
			got++
		}
	}
	for i, n := range seen {
		if n != 0 {
			t.Fatalf("offset %d seen %d times; want once", i, n)
		}
	}
	if !reflect.DeepEqual(int64(len(seen)), int64(got)) {
		t.Fatalf("received %d; want %d", got, total)
	}
}
