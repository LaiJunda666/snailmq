<p align="center">
  <img src="assets/logo.png" alt="SnailMQ logo" width="150">
</p>

# SnailMQ

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.27-00ADD8.svg)](go.mod)
[![dependencies](https://img.shields.io/badge/runtime%20deps-0-success.svg)](go.mod)

SnailMQ 是一个轻量单机消息队列,以单个 Go 模块提供「发布 → 广播消费」的完整闭环,适用于学习与轻量场景。
其核心模型为**共享有序日志 + 每订阅者独立游标**:消息仅追加一次,各订阅者按自身 offset 尾随读取;
存储、协议与传输层均预留可替换接缝。

**设计取舍**

- **零第三方运行时依赖**:只用标准库,便于审计、部署与教学;代价是自带协议与并发实现。
- **薄协议、低拷贝**:定长帧 + 小端整数;解码零拷贝、编码直写,支持批量发布 + 单帧批量 ack、推送端 writev。
- **两种用法**:内嵌 `broker` 库,或运行标准库 TCP 服务并使用官方 `network` 客户端。
- **可演进**:`Store` 接口通向 WAL;游标模型通向 ack/消费组;传输预留 netpoll。

**当前状态**:V0 全内存广播已闭环,`make demo` 一键跑通;端到端吞吐随消息大小与批大小变化
(小消息批量下最高,详见 [benchmark/RESULT.md](benchmark/RESULT.md))。下一步 ack / 消费组 / WAL / raft,详见 [docs/ROADMAP.md](docs/ROADMAP.md)。

**快速开始**:`make demo` · **协议**:[docs/PROTOCOL.md](docs/PROTOCOL.md) · **架构**:[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · **贡献**:[CONTRIBUTING.md](CONTRIBUTING.md)

## 特性

- **广播 + 独立游标**:每个 topic 一个分区,订阅者各自维护 offset,互不影响。
- **零三方依赖**:仅用 Go 标准库(唯一例外是 V4 的 `etcd/raft`,尚未引入)。
- **自研二进制协议**:定长头 + payload,纯编解码;解码零拷贝,畸形输入返回错误而非 panic(fuzz 覆盖)。
- **标准库 TCP 服务**:每连接一个 goroutine;订阅成功后连接转为单向推送流;读/写超时、连接上限、panic 隔离、慢消费者丢弃。
- **批量能力**:批量发布 + 单帧批量 ack、无 ack 发布、客户端攒批(linger)、批量读、推送端 writev 批量写。
- **可靠工程**:`-race` 单测、codec fuzz、`golangci-lint`、GitHub Actions CI、内嵌基准 + 报告。

## 状态与路线

当前 **V0:最小闭环 — 全内存广播**,已实现协议 5/6/7 号 opcode、批量与端到端。

| 版本 | 主题 |
|---|---|
| **V0** | 最小闭环:全内存广播 + 自研协议 + TCP server/client + demo + bench |
| V1 | 可靠消费:ack / at-least-once、重投 |
| V2 | consumer group 竞争消费、多分区 |
| V3 | WAL 持久化(磁盘 Store、retention) |
| V4 | raft 集群 |
| V5 | 打磨发布 v1.0 |

详见 [docs/ROADMAP.md](docs/ROADMAP.md)。

## 架构

```
broker/     内核:共享有序 log + 独立游标,per-partition 锁 + ready 通知
protocol/   自研二进制帧,纯编解码(吃 io.Reader / []byte)
network/    TCP 服务端与客户端,订阅后转推送流
cmd/demo/   一键演示
benchmark/  基准与报告
```

- **并发模型(方案 A)**:每个 topic 一个 `Partition`,持有有序不可变日志(经 `broker.Store` 接口);
  `Publish` 持分区锁追加,并向每个订阅者非阻塞发 `ready` 信号;订阅者循环读 `[offset, …)`,无消息时阻塞在 `ready` / `done` / `ctx`。
- **依赖方向**:`broker` 不 import `network`;`network` 依赖 `broker + protocol`。内核可单独测试、可内嵌。
- **关闭语义**:`Close` 幂等(重复返回 nil);关闭后的其它操作返回包装后的 `ErrClosed`(用 `errors.Is` 判断)。
- **错误语义**:协议 `OpError` 携带结构化错误码,客户端可跨线用 `errors.Is` 判定(`ErrClosed`/`ErrTopicNotFound` 等)。

详见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

## 安装与引入

前置条件:**Go 1.27+**;除 Go 工具链外无任何运行时依赖。

在已有项目中使用:

```bash
go get github.com/LaiJunda666/snailmq@latest
# 发布版本后建议锁定 tag:
go get github.com/LaiJunda666/snailmq@v0.1.0
```

按需引入:

```go
import (
	"github.com/LaiJunda666/snailmq/broker"   // 内嵌内核
	"github.com/LaiJunda666/snailmq/network"  // TCP 服务/客户端(可选)
	"github.com/LaiJunda666/snailmq/protocol" // 直接用线协议(可选)
)
```

快速体验:克隆仓库后执行 `make demo`(或 `go run ./cmd/demo`)。

> 尚未发布 tag 时,`@latest` 会解析到默认分支的最新提交;正式使用请锁定版本 tag。

## 快速开始

### 一键演示

```bash
make demo        # 等价于 go run ./cmd/demo
```

启动服务端并创建主题,两个订阅者各自消费全量,发布 10 万条消息,输出吞吐(msg/s 与 MB/s):

```
SnailMQ demo server on 127.0.0.1:54825
publisher: 100000 messages / 888890 B in 40ms (2500000 msg/s, 21.5 MB/s end-to-end)
subscriber 1: read 100000 messages / 888890 B (last offset 99999) in 40ms (2495000 msg/s, 21.5 MB/s)
subscriber 2: read 100000 messages / 888890 B (last offset 99999) in 40ms (2494000 msg/s, 21.5 MB/s)
```

### 内嵌内核库

不经过网络,直接把 broker 当作库使用:

```go
import (
	"context"
	"github.com/LaiJunda666/snailmq/broker"
)

b := broker.New()
defer b.Close()

_ = b.CreateTopic("orders")
off, _ := b.Publish("orders", []byte("hi"))

sub, _ := b.Subscribe("orders")   // 游标从 0 起,可读全量历史
msgs, _ := sub.Read(context.Background(), 1)
// msgs[0].Offset == off, msgs[0].Payload == []byte("hi")

_ = sub.Close()
```

### TCP 服务 + 官方客户端

```go
import (
	"net"
	"github.com/LaiJunda666/snailmq/broker"
	"github.com/LaiJunda666/snailmq/network"
)

// 服务端
b := broker.New()
srv := network.NewServer(b, network.WithMaxConns(1000))
ln, _ := net.Listen("tcp", "127.0.0.1:0")
go srv.Serve(ln)
defer srv.Close()

// 客户端
c, _ := network.Dial(ln.Addr().String())
defer c.Close()

_ = c.CreateTopic("orders")
_, _ = c.Publish("orders", []byte("hi"))

sub, _ := c.Subscribe("orders")
m, _ := sub.Read()               // 阻塞直到收到推送
// m.Offset == 0, m.Payload == []byte("hi")
```

> 注意:一条连接**同一时刻只能订阅一个 topic**;`Subscribe` 成功后该连接进入推送流,不能再 `Publish`(会返回 `ErrStreaming`)。发布请使用另一条连接。

## API 摘录

### broker(内嵌内核)

| 方法 | 说明 |
|---|---|
| `broker.New(opts ...Option) *Broker` | 创建;`WithStoreFactory` 可注入日志后端 |
| `(*Broker) CreateTopic(name) error` | 建 topic;空名/重名/过长/已关闭返回对应错误 |
| `(*Broker) Publish(topic, payload) (int64, error)` | 发布并返回 offset;payload 以引用保存,发布后不得修改 |
| `(*Broker) Subscribe(topic) (*Subscription, error)` | 新订阅者(游标从 0,可读全量历史) |
| `(*Broker) SubscriberCount(topic) (int, error)` | 只读订阅数(观测/测试) |
| `(*Broker) Close() error` | 幂等关闭 |
| `(*Subscription) Read(ctx, max) ([]Message, error)` | 阻塞读;关闭返回 `ErrClosed`,取消返回 `ctx.Err()` |

### network(服务端与客户端)

| 方法 | 说明 |
|---|---|
| `network.NewServer(b, opts...) *Server` | 选项:`WithMaxConns` / `WithReadTimeout` / `WithWriteTimeout` / `WithReadBuffer` / `WithWriteBuffer` |
| `(*Server) Serve(ln) error` / `Close() error` | accept 循环 / 幂等关闭(不关 listener) |
| `network.Dial(addr, opts...) (*Client, error)` | 默认 5s 拨号超时;`DialTimeout` 可自定义 |
| `(*Client) CreateTopic / Publish / Subscribe / Close` | 请求-响应;关闭后返回 `ErrClosed` |
| `(*Client) PublishBatch(topic, payloads) (base int64, err error)` | 批量发布,单帧批量 ack,offset 连续 `[base, base+n)` |
| `(*Client) PublishAsync(topic, payload) error` | fire-and-forget,不等 ack |
| `(*Subscription) Read() (Message, error)` / `ReadBatch(max) ([]Message, error)` | 逐条 / 批量读推送流 |

客户端选项:`WithClientReadTimeout` / `WithClientWriteTimeout`(请求相位,默认 10s)、`WithClientReadBuffer` / `WithClientWriteBuffer`。

### 攒批发布(linger)

```go
batcher := network.NewBatcher(c, "orders", 512, time.Millisecond) // 满 512 条或 1ms 触发
defer batcher.Close()

for _, p := range payloads {
    if err := batcher.Add(p); err != nil { /* ... */ }
}
// Close 会 flush 剩余;Flush 返回 (base offset, count)
```

## 协议

帧格式(定长头 + payload):

```
byte 0-1   magic   uint16 LE = 0x4D51 ("MQ")
byte 2     version uint8  = 1
byte 3     opcode  uint8
byte 4-7   payloadLen uint32 LE
byte 8+    payload
```

opcode:1 CreateTopic · 2 Publish · 3 Subscribe · 4 Message(S→C) · 5 Error(S→C) · 6 PublishBatch · 7 PublishNoAck。
错误码:0 未知 · 1 已关闭 · 2 topic 已存在 · 3 topic 不存在 · 4 空名 · 5 超限 · 6 过载。
长度上限:单帧 `16 MiB`;单条消息 `MaxMessage = MaxPayload - 12`;topic 名 `255 B`。

详见 [docs/PROTOCOL.md](docs/PROTOCOL.md)。

## 性能

参数化 10B / 1KiB / 64KiB,`-benchmem` 直接给出 MB/s;完整结果见 [benchmark/RESULT.md](benchmark/RESULT.md)。Apple M4 / go1.27.1 摘要:

| 场景 | 结果 |
|---|---|
| 内核 Publish | ~14 ns/op |
| 端到端逐条(10B) | ~13.3 µs/op(受每消息往返限制) |
| 端到端批量(10B×1024/批) | ≈2160 万 msg/s |
| `cmd/demo`(双订阅者) | ≈220–270 万 msg/s / ~19–23 MB/s |

> 口径:小消息的 msg/s 高但单条字节少(MB/s 不高),大消息受带宽限制;内核发布与消息大小基本无关。以上仅为量级参考,以 [benchmark/RESULT.md](benchmark/RESULT.md) 为准。内核基准注入不保留消息的测试 Store 以避免内存无界增长。

## 开发

统一入口为 Makefile:

```bash
make help       # 查看所有目标
make fmt        # gofmt 检查(CI 用)
make fmt-fix    # gofmt -w .
make vet        # go vet
make test       # go test -race ./...
make build      # go build ./...
make bench      # 基准 → benchmark/RESULT.txt
make demo       # go run ./cmd/demo
```

fuzz(协议解码):

```bash
go test ./protocol -fuzz=FuzzDecodePublish -fuzztime=10s
go test ./protocol -fuzz=FuzzReadFrame   -fuzztime=10s
```

代码约定见 [AGENTS.md](AGENTS.md):零三方运行时依赖、依赖方向、提交用 Conventional Commits、**commit/push 需先经用户同意**。
贡献流程(分支/PR/发布)见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 文档索引

| 文档 | 内容 |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | 分层、并发模型、服务端生命周期、演进接缝 |
| [docs/PROTOCOL.md](docs/PROTOCOL.md) | 帧格式、opcode、错误码、长度上限 |
| [docs/ROADMAP.md](docs/ROADMAP.md) | V0–V5 版本规划 |
| [benchmark/RESULT.md](benchmark/RESULT.md) | 基准结果 |
| [CONTRIBUTING.md](CONTRIBUTING.md) | 开发流程、分支策略、提交与发布规范 |
| [CHANGELOG.md](CHANGELOG.md) | 变更记录 |

## License

[MIT](LICENSE)
