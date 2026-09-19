# Roadmap

## 整体原则

- **工程化不后置**:文档、测试、CI、基准与代码同步维护,不留「以后补」。
- **每版可验收**:结束态可运行、可演示、可测、有基准与明确验收线。
- **零三方默认**:仅标准库;唯一例外 V4 的 `etcd/raft`。
- **改动走分支**:所有改动一律新分支 → 验证 → 合并 main(见 [CONTRIBUTING.md](../CONTRIBUTING.md))。

## V0 — 最小闭环(全内存 · 广播) · 已完成

目标:一条命令跑通「生产 → 广播消费」并量化吞吐。

- 内核:`New / CreateTopic / Publish / Subscribe / Close`;per-partition 锁 + `ready` 通知。
- 方案 A:共享有序 log + 独立游标;最小 `Store` 接口(内存实现,无界)。
- 协议:自研二进制帧与 payload 编解码(定长头、opcode、错误码、长度上限),fuzz 覆盖。
- 网络:标准库 TCP server/client;订阅后转单向推送流;读写超时、连接上限、socket 缓冲、panic 隔离。
- 性能:零拷贝解码、直写帧、批量发布 + 单帧批量 ack、无 ack 发布、客户端攒批(linger)、批量读、writev 推送。
- 工程:单测 + `-race`、fuzz、CI(fmt/vet/test/build)、`cmd/demo`、`benchmark` + `RESULT.md`。
- **验收**:`make demo` 跑通;吞吐与资源数据见 `benchmark/RESULT.md`。

## V1 — 可靠消费:ack / at-least-once

目标:订阅者显式确认,未确认超时重投。

- 内核:读游标与提交游标分离;`Subscription.Ack(offset)` / `Commit`;in-flight 窗口与可见性超时;at-least-once 语义文档化。
- 重构:读逻辑收进 `Partition.read`,便于挂载 ack/in-flight 状态。
- 协议:ack/commit opcode;协议版本协商(握手帧,`ReadHeader` 放宽为已知版本集合);错误码扩展(Internal/Backpressure/Unsupported/ProtocolViolation/SlowConsumer)。
- 客户端:单连接多路复用(一连接多订阅);`Subscription.Read(ctx)` 支持取消/限时。
- 生产韧性:心跳/半开连接检测、慢消费者策略(Drop|Disconnect|Block)、优雅排空关闭(`CloseWithContext`/drain)。
- 可观测:可选注入 Logger/Metrics + `Server.Stats()`(含订阅滞后指标),保持零依赖。

## V2 — consumer group 竞争消费

目标:组内竞争、组间广播;支持按 key 分区。

- 内核:多分区 `Topic`(`CreateTopic(name, WithPartitions(n))`);`Publish(topic, key, payload)` 按 key 路由。
- 协议:订阅/推送带 partition 标识;组协调 opcode。
- 组管理:成员心跳、分区分配、超时接管;组位点用独立 `OffsetStore`/`MetaStore`,不复用消息 `Store`。
- 安全:多租户/公网前补 TLS 与认证授权。

## V3 — WAL 持久化

目标:落盘、可恢复、可控保留。

- `Store` 磁盘后端:append-only 顺序写、分段文件 + 索引、尾部截断恢复、fsync 策略、段清理。
- 接口:`Store` 补 `Close` / `FirstOffset`,`Read` 返回 error(retention 缺口 `ErrOffsetOutOfRetention`);工厂改为 `func(topic) (Store, error)`。
- 兼容:去掉 `memoryLog` 的 `offset == index` 假设(以 `Message.Offset` 为准);`CreateTopic` 的 store 构造移出全局锁。
- 资源:每 topic 字节/条数上限 + `ErrBackpressure`;retention 策略。

## V4 — raft 集群

目标:多节点一致性与故障恢复。

- 引入 `etcd/raft` 做协调层;分区 leader/follower;日志复制。
- 验收:3 节点,kill leader 自动恢复,消息不丢。

## V5 — 打磨发布 v1.0

- 功能/文档/基准收口;release CI;仓库转 public;打 `v1.0.0`。

---

当前进度:**V0 完成**,下一步 V1。
