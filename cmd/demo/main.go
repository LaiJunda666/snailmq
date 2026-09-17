// Command demo 一键演示 mq-lite 的 V0 闭环:
// 起 server → 建 topic → 两个订阅者各自读满全量 → 发布者顺序发 N 条 → 打印吞吐。
package main

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/LaiJunda666/mq-lite/broker"
	"github.com/LaiJunda666/mq-lite/network"
)

const (
	topicName = "greetings"
	total     = 100_000
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(1)
	}
}

// readerResult 是单个订阅者的消费统计。
type readerResult struct {
	id      int
	last    int64
	elapsed time.Duration
}

func run() error {
	b := broker.New()
	srv := network.NewServer(b)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	addr := ln.Addr().String()
	fmt.Printf("mq-lite demo server on %s\n", addr)

	// 发布者用独立连接:订阅成功后连接会转为推送流,不能再发布。
	pub, err := network.Dial(addr)
	if err != nil {
		return err
	}
	defer func() { _ = pub.Close() }()
	if err := pub.CreateTopic(topicName); err != nil {
		return err
	}

	ready := make(chan struct{}, 2)
	results := make(chan readerResult, 2)
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	for id := 1; id <= 2; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r, err := consume(addr, id, ready)
			if err != nil {
				errCh <- fmt.Errorf("subscriber %d: %w", id, err)
				return
			}
			results <- r
		}(id)
	}

	// 等两个订阅者都订阅成功再开始发布。
	<-ready
	<-ready

	start := time.Now()
	for j := range total {
		if _, err := pub.Publish(topicName, fmt.Appendf(nil, "msg-%d", j)); err != nil {
			return err
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("publisher: %d messages in %v (%.0f msg/s end-to-end)\n",
		total, elapsed.Round(time.Millisecond), rate(total, elapsed))

	wg.Wait()
	close(results)
	for r := range results {
		fmt.Printf("subscriber %d: read %d messages (last offset %d) in %v (%.0f msg/s)\n",
			r.id, total, r.last, r.elapsed.Round(time.Millisecond), rate(total, r.elapsed))
	}

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// consume 连接、订阅并读满 total 条消息;订阅成功即向 ready 报信号。
func consume(addr string, id int, ready chan<- struct{}) (readerResult, error) {
	c, err := network.Dial(addr)
	if err != nil {
		return readerResult{}, err
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe(topicName)
	if err != nil {
		return readerResult{}, err
	}
	ready <- struct{}{}

	start := time.Now()
	var last int64
	for range total {
		m, err := sub.Read()
		if err != nil {
			return readerResult{}, err
		}
		last = m.Offset
	}
	return readerResult{id: id, last: last, elapsed: time.Since(start)}, nil
}

// rate 返回每秒消息数。
func rate(n int, d time.Duration) float64 {
	return float64(n) / d.Seconds()
}
