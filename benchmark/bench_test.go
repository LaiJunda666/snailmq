// Package benchmark 提供 mq-lite 的内核与端到端基准。
//
// 说明:内存日志无界,直接压 64KiB 大消息会 OOM,故内核/协议路径基准
// 通过 broker.WithStoreFactory 注入不保留消息的测试 Store(只测锁/通知/协议开销);
// 需要真实存储语义的场景在 V3 引入磁盘后端后再补。
package benchmark

import (
	"context"
	"net"
	"testing"

	"github.com/LaiJunda666/mq-lite/broker"
	"github.com/LaiJunda666/mq-lite/network"
)

// sinkStore 只分配 offset,不保存消息,用于无界增长场景的发布路径基准。
type sinkStore struct{ n int64 }

func (s *sinkStore) Append(p []byte) (int64, error)            { off := s.n; s.n++; return off, nil }
func (s *sinkStore) Read(from int64, max int) []broker.Message { return nil }
func (s *sinkStore) Len() int64                                { return s.n }

// slotStore 只保留最近一条消息,用于"发布 + 订阅读"的闭环基准(读者始终在队首)。
type slotStore struct {
	off int64
	msg broker.Message
}

func (s *slotStore) Append(p []byte) (int64, error) {
	off := s.off
	s.off++
	s.msg = broker.Message{Offset: off, Payload: p}
	return off, nil
}

func (s *slotStore) Read(from int64, max int) []broker.Message {
	if from == s.msg.Offset && s.off > 0 && from >= s.off-1 {
		return []broker.Message{s.msg}
	}
	return nil
}

func (s *slotStore) Len() int64 { return s.off }

type size struct {
	name string
	n    int
}

var sizes = []size{{"10B", 10}, {"1KiB", 1 << 10}, {"64KiB", 64 << 10}}

// BenchmarkBrokerPublish 内核发布路径(锁 + offset + 订阅通知,不含存储/网络)。
func BenchmarkBrokerPublish(b *testing.B) {
	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			br := broker.New(broker.WithStoreFactory(func() broker.Store { return &sinkStore{} }))
			if err := br.CreateTopic("t"); err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, s.n)
			b.SetBytes(int64(s.n))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := br.Publish("t", payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkBrokerPublishParallel 并发发布,暴露分区锁争用。
func BenchmarkBrokerPublishParallel(b *testing.B) {
	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			br := broker.New(broker.WithStoreFactory(func() broker.Store { return &sinkStore{} }))
			if err := br.CreateTopic("t"); err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, s.n)
			b.SetBytes(int64(s.n))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := br.Publish("t", payload); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkBrokerPublishSubscribe 内核闭环:一轮 = 1 发布 + 1 订阅读。
func BenchmarkBrokerPublishSubscribe(b *testing.B) {
	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			br := broker.New(broker.WithStoreFactory(func() broker.Store { return &slotStore{} }))
			if err := br.CreateTopic("t"); err != nil {
				b.Fatal(err)
			}
			sub, err := br.Subscribe("t")
			if err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, s.n)
			b.SetBytes(int64(s.n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := br.Publish("t", payload); err != nil {
					b.Fatal(err)
				}
				if _, err := sub.Read(context.Background(), 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkEndToEndSync 端到端逐条发布(客户端等 ack),测请求-响应路径。
func BenchmarkEndToEndSync(b *testing.B) {
	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			_, srv, c := startServer(b)
			defer func() { _ = srv.Close() }()
			defer func() { _ = c.Close() }()
			payload := make([]byte, s.n)
			b.SetBytes(int64(s.n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := c.Publish("t", payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkEndToEndBatch 端到端批量发布(单帧 n 条 + 单帧 ack),测批量路径。
func BenchmarkEndToEndBatch(b *testing.B) {
	for _, s := range sizes {
		batch := batchFor(s.n)
		b.Run(s.name, func(b *testing.B) {
			_, srv, c := startServer(b)
			defer func() { _ = srv.Close() }()
			defer func() { _ = c.Close() }()
			payloads := make([][]byte, batch)
			for i := range payloads {
				payloads[i] = make([]byte, s.n)
			}
			b.SetBytes(int64(s.n * batch))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := c.PublishBatch("t", payloads); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(batch), "msg/op")
		})
	}
}

// batchFor 选择使批量 body 不超过 ~4MiB 的批大小。
func batchFor(size int) int {
	batch := (4 << 20) / size
	if batch < 1 {
		return 1
	}
	if batch > 1024 {
		return 1024
	}
	return batch
}

// startServer 起一个使用 sinkStore 的服务端并返回已建 topic 的客户端。
func startServer(b *testing.B) (string, *network.Server, *network.Client) {
	b.Helper()
	br := broker.New(broker.WithStoreFactory(func() broker.Store { return &sinkStore{} }))
	if err := br.CreateTopic("t"); err != nil {
		b.Fatal(err)
	}
	srv := network.NewServer(br)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	c, err := network.Dial(ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	return ln.Addr().String(), srv, c
}
