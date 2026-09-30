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

## V1 — 可靠消费:ack / at-least-once(分阶段)

目标:订阅者显式确认,未确认超时重投;分 V1a/V1b/V1c 三步交付(设计与任务见 `docs/superpowers/`)。

### V1a — 核心可靠消费(进行中)

- 内核:`cursor`/`committed` 分离,累积 `Ack(upTo)`;可见性超时重投(默认关闭,`WithVisibilityTimeout` 选入)。
- 协议:`OpAck`;`OpSubscribe` 携带可见性字段(兼容旧客户端)。
- 网络:订阅相位升级为双向(推送 + ack 读取协程);客户端拆分写锁,支持读/ack 并发。
- 文档:at-least-once 语义与边界(内存态、重启丢失)。

### V1b — 协议演进与客户端能力

- 协议版本协商/握手(未知 opcode 明确拒绝语义);错误码扩展(Internal/Backpressure/Unsupported/ProtocolViolation/SlowConsumer)。
- 客户端单连接多路复用(一连接多订阅);`Subscription.Read(ctx)` 取消/限时。
- 推送读侧与请求路径彻底解耦(读不再持客户端状态锁,仅在单订阅模型下成立的问题一并消除)。

### V1c — 生产韧性与可观测

- 心跳/半开连接检测;慢消费者策略(Drop|Disconnect|Block);优雅排空关闭(`CloseWithContext`/drain)。
- 可观测:可选注入 Logger/Metrics + `Server.Stats()`(含订阅滞后指标),保持零依赖。
- API 规范:`Option` 改为可校验(`func(*config) error` 或 `Config + Validate()`);`NoTimeout`/`Unlimited` 常量替代魔法 0。
- 性能/健壮 backlog:
  - `memoryLog.Read` 支持 caller-buffer,减少热路径分配;
  - 缩小 `Read` 的锁范围(锁内取偏移、锁外拷贝),批量大时不再阻塞 `Publish`;
  - 推流批量大小可配(当前硬编码 64);
  - `WithCopyOnPublish` 可选拷贝,缓解 payload 别名 footgun;
  - `Store` 契约测试套件/可选校验包装,校验自定义实现不回乱序/负 offset;
  - `ErrClosed` 语义拆分(写拒绝 vs 读终态)或 typed error;
  - 单读者重入守卫(调试构建可选)。

## V2 — consumer group 竞争消费

目标:组内竞争、组间广播;支持按 key 分区。

- 内核:多分区 `Topic`(`CreateTopic(name, WithPartitions(n))`);`Publish(topic, key, payload)` 按 key 路由。
- 协议:订阅/推送带 partition 标识;组协调 opcode。
- 组管理:成员心跳、分区分配、超时接管;组位点用独立 `OffsetStore`/`MetaStore`,不复用消息 `Store`。
- topic 生命周期:删除 topic(与 retention 协同设计)。
- 广播唤醒优化:`Partition.Publish` 当前在锁内对每个订阅者发信号(O(subs));考虑单通道/分片降低开销。
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
