# Benchmark 结果(V0)

- 机器:Apple M4(darwin/arm64)
- Go:`go1.27.1`
- 命令:`make bench`(等价 `go test ./benchmark -bench=. -benchmem -run=^$`)
- 消息大小参数化:10B / 1KiB / 64KiB;`SetBytes` 使 `-benchmem` 直接给出 MB/s

> 说明:内核路径基准通过 `WithStoreFactory` 注入不保留消息的测试 Store,避免内存日志无界增长导致 OOM,
> 因此其"MB/s"不代表带宽(内核发布为 `~14ns/op`,与消息大小无关);端到端基准使用 sink 服务端(无订阅者),
> 衡量的是"发布 + ack"路径。真实内存存储与广播消费见 `cmd/demo`。

## 内核(内存)

| 基准 | 10B | 1KiB | 64KiB |
|---|---|---|---|
| Publish(ns/op, 1 allocs=0) | 14.0 | 13.8 | 13.9 |
| PublishParallel(ns/op) | 99.9 | 98.3 | 98.7 |
| PublishSubscribe(ns/op, 1 alloc) | 51.8 | 50.9 | 51.3 |

- 内核发布约 13–14 ns/op,与消息大小基本无关;并发因分区锁串行,约 100 ns/op。
- 闭环(发布 + 读)约 51 ns/op。

## 端到端(loopback TCP,发布→ack)

| 基准 | 10B | 1KiB | 64KiB |
|---|---|---|---|
| Sync Publish(ns/op) | 13312 | 13956 | 23244 |
| Sync msg/s | ~75k | ~72k | ~43k |
| Sync MB/s | 0.75 | 73 | 2820 |
| Batch Publish(ns/op) | 47452 | 417517 | 531750 |
| Batch 批大小 | 1024 | 1024 | 64 |
| Batch msg/s | ~21.6M | ~2.45M | ~120k |
| Batch MB/s | 216 | 2511 | 7888 |

- 逐条(`Publish`)受每消息往返限制:小消息约 13 µs/op。
- 批量(`PublishBatch`,单帧多消息 + 单帧 ack)摊薄往返开销:小消息吞吐提升约两个数量级;
  64KiB 时趋于 loopback 带宽上限(约 7.9 GB/s)。
- `cmd/demo`(真实内存日志 + 双订阅者广播 + `Batcher` 攒批 + `ReadBatch`):
  约 220–270 万 msg/s / 19–23 MB/s(8.9B 消息,10 万条)。

## 备注

- 小消息(10B)受每消息往返与系统调用影响,msg/s 高但单条字节少,MB/s 偏低;批量化可降低往返开销。
- 大消息(64KiB)接近 loopback 带宽上限,批量收益减小。
- 数据受机器、Go 版本与并发负载影响,以上为量级参考。
