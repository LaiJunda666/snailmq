# Architecture

## 概览

SnailMQ 是单机消息队列内核 + 可选 TCP 服务的分层实现。核心理念:

- **零三方运行时依赖**(仅 Go 标准库),内核可内嵌、可单测;
- **广播 + 独立游标**:消息只追加一次,每个订阅者各自维护读取进度;
- **薄协议 + 可替换接缝**:线协议、存储、传输都留有替换点(WAL / netpoll / 多路复用)。

## 分层与依赖

```
broker/     内核:共享有序 log + 独立游标,per-partition 锁 + ready 通知
protocol/   自研二进制帧,纯编解码(吃 io.Reader / []byte,不依赖 broker/网络)
network/    TCP 服务端与客户端:请求-响应 + 订阅推送流
cmd/demo/   一键演示
benchmark/  内核与端到端基准 + 报告
```

依赖方向(硬约束):

```
network ──► broker
   │
   └─────► protocol
broker ──► (无外部依赖)
protocol ──► (无依赖)
```

- `broker` **不 import network**;网络细节不泄漏进内核。
- `protocol` 保持纯函数、无状态,便于 fuzz 与跨语言实现。

## 内核数据模型

### Message / Store

- `broker.Message{Offset int64, Payload []byte}`:不可变消息。`Payload` 在订阅者之间**共享**,
  发布后不得修改(约定式不可变,换取零拷贝;内嵌用户若需隔离可自行拷贝)。
- `broker.Store`:分区日志抽象,最小接口 `Append / Read / Len`。
  - **不自带并发安全**,由 `Partition` 持锁串行调用(单一所有权)。
  - 当前实现 `memoryLog`:无界 `[]Message`,只增不删;不变量 **`msgs[i].Offset == i`**。
  - `Read` 返回消息切片副本(`Message` 值拷贝),payload 仍共享。

### Partition

`Partition` 是「一段有序日志 + 它的广播订阅者集合」,也是并发控制的核心:

- 字段:`mu sync.Mutex`、`store Store`、`subs map[*Subscription]struct{}`、`closed bool`。
- `Publish`:锁内 `store.Append` → 对每个订阅者向 `ready`(cap-1)**非阻塞**发信号。
- `Subscribe`:注册新订阅者(游标从 0)。
- `close` / `unsubscribe`:唤醒并移除订阅者(关闭其 `done`)。

### Subscription

`Subscription` 是单个订阅者的读取视角:私有游标 `offset` + `ready`/`done` 通道。

- `offset` 逻辑上只被**单个读 goroutine** 访问;其读写与「读日志、判断关闭态」一起在 `partition.mu` 下完成。
- `Read(ctx, max)`:循环读 `[offset, …)`;无消息时 `select` 阻塞于 `ready` / `done` / `ctx`。
- `Close` 幂等;关闭后 `Read` 立即返回 `ErrClosed`(不排空积压)。

## 并发模型(方案 A)

```
Publisher ──► Partition.mu ──► store.Append ──► 向每个 sub.ready 非阻塞发信号
                                                        │
Subscriber ──► Read: p.mu 下 store.Read ──► 有数据: 推进游标返回
                                        └─► 无数据: select ready / done / ctx
```

- **无丢失唤醒**:`ready` cap-1 + 循环回读日志;即使信号被合并/丢弃,订阅者醒来后仍会以游标读到最新数据。
- **单读者约定**:同一 `Subscription` 同时只能被一个 goroutine `Read`(否则会重复投递/游标错乱,但不 data race)。
- **消费与关闭解耦**:读阻塞期间不持有任何锁;`done` 通道用于关闭唤醒。

## 关闭与生命周期

- `Broker.Close`:幂等;关闭所有分区 → 唤醒订阅者;关闭后其它操作返回包装后的 `ErrClosed`。
- `Partition.close`:置 `closed`、唤醒并移除所有订阅者,保证之后 `Close` 不二次关闭通道。
- `Subscription.Close`:注销 + 唤醒阻塞读;幂等。

服务端连接生命周期(`network`):

1. **请求相位**:每连接一个 goroutine,`ReadFrame` → 按 opcode 分派 → `respond`;读/写各有超时。
2. **推送相位**:`OpSubscribe` 成功后连接转单向推送流;并发启动「对端监视」goroutine,
   通过读连接感知 FIN/EOF 或协议违规,取消订阅上下文并清理。
3. **收尾**:所有返回路径都会关闭连接并 join 监视 goroutine;连接 goroutine 与监视 goroutine 均有 `recover` 隔离。
4. `Server.Close`:幂等;取消并关闭所有连接、等待全部 goroutine 退出;不关闭 listener(归调用方)。

## 错误模型

- 内核哨兵:`ErrClosed` / `ErrTopicExists` / `ErrTopicNotFound` / `ErrTopicNameEmpty`。
- 门面可对关闭类错误用 `fmt.Errorf("%w")` 附加「操作 + topic」上下文,判断一律用 `errors.Is`。
- **跨线**:协议 `OpError` 携带 `[u16 code][u32 len][msg]`;服务端把哨兵映射为 `Code`,
  客户端再把 `Code` 映射回本地哨兵,从而在网络两端都能 `errors.Is`。
- 未知/未分类错误回 `CodeUnknown` 并保留文本。

## 网络与协议接缝

- 传输:标准库 TCP,每个连接一个 goroutine;请求相位使用 `bufio` 读写,推送相位批量 `writev`;预留替换 netpoll/io_uring 的接缝。
- 背压/防护:读写超时、最大连接数、socket 缓冲可配、超大帧拒收、慢消费者写超时丢弃。
- 性能:解码零拷贝、编码直写、批量发布 + 单帧批量 ack、推送 writev + 头缓冲池。

详见 [PROTOCOL.md](PROTOCOL.md)。

## 演进接缝

- `Store` 接口 = WAL 落地点;V3 需补 `Close`/`FirstOffset` 并去掉 `offset == index` 假设。
- `Partition`/游标模型通向 V1 ack(读游标 vs 提交游标)与 V2 消费组(分区分配)。
- `Topic` 单分区 → V2 多分区 + key 路由;`OpMessage` 预留 partition 标识。
- 客户端单连接单订阅 → V1 多路复用。
- 传输层预留给 netpoll。
