# Roadmap

> **版本以 git tag + [CHANGELOG](../CHANGELOG.md) 为准**(SemVer);本文件只描述**方向与主题**,不排版本号,避免与 tag 混淆。

## 原则

- 工程化不后置:文档、测试、CI、基准与代码同步维护。
- 每个可发布版本都可运行、可演示、可测、有基准。
- 默认零三方运行时依赖,三方为**受控例外**(判据见 [AGENTS.md](../AGENTS.md))。
- 所有改动走分支 → PR → 合并 `main`(见 [CONTRIBUTING.md](../CONTRIBUTING.md))。

## 已发布

| tag | 摘要 |
|---|---|
| **v0.2.0** | 主题名校验/数量上限/`ListTopics`;选项负值归一(详见 CHANGELOG) |
| **v0.1.0** | 全内存广播闭环:内核(共享日志 + 独立游标)、自研二进制协议、TCP server/client、`cmd/demo`、`benchmark` + 报告 |

## 后续方向(主题)

### 可靠消费:ack / at-least-once(进行中)

- 已实现(分支待合并):`cursor`/`committed` 分离与累积 `Ack`;可见性超时重投;协议 `OpAck` 与 `OpSubscribe` 可见性字段;订阅相位双向(读 ack)。
- 待做:协议版本协商与未知 op 语义、客户端单连接多路复用、`Read(ctx)`、心跳/半开检测、慢消费者策略、优雅排空关闭。
- 可观测:自研 `Logger`/`Metrics` 接口(默认 no-op)+ **自写 Prometheus 文本端点**(0 依赖);`client_golang` 仅按需作**可选嵌套 module**。
- API 规范:`Option` 可校验(`func(*config) error` 或 `Config + Validate()`);`NoTimeout`/`Unlimited` 常量替代魔法 0。
- 性能/健壮 backlog:缩小读锁范围、caller-buffer 读、推流批量可配、`WithCopyOnPublish`、`Store` 契约测试套件、`ErrClosed` 语义拆分、单读者重入守卫。

### 消费组与多分区

- 内核:组内竞争 / 组间广播;`CreateTopic(name, WithPartitions(n))`;`Publish(topic, key, payload)` 按 key 路由。
- 协议:订阅 / 推送带 partition 标识;组协调 opcode。
- 组管理:成员心跳、分区分配、超时接管;组位点用独立 `OffsetStore`/`MetaStore`,不复用消息 `Store`。
- topic 生命周期:删除 topic(与 retention 协同设计)。
- 广播唤醒优化:`Partition.Publish` 当前在锁内对每个订阅者发信号(O(subs));考虑单通道 / 分片降低开销。
- 安全:多租户 / 公网前补 TLS 与认证授权。

### 持久化(WAL)

- 实现选型:**首选 `tidwall/wal`**(纯 Go、零依赖、分段 + LSN + `TruncateFront`),藏在 `Store` 接口后;备选手写分段 + 索引(零依赖但需自证崩溃恢复)。二者都需处理 LSN(1-based)↔ offset(0-based)映射、fsync 策略、段大小与 retention。
- 前置:`Store` 接口扩展(`Close` / `FirstOffset` / `Read` 返回 error,retention 缺口 `ErrOffsetOutOfRetention`);工厂改为 `func(topic) (Store, error)`。
- 磁盘后端:append-only 顺序写、分段文件 + 索引、尾部截断恢复、段清理。
- 兼容:去掉 `memoryLog` 的 `offset == index` 假设(以 `Message.Offset` 为准);`CreateTopic` 的 store 构造移出全局锁。
- 资源:每 topic 字节 / 条数上限 + `ErrBackpressure`;retention 策略。

### 集群(raft)

- `go.etcd.io/raft/v3`——**唯一已批准的三方运行时例外**,子包隔离。
- 分区 leader/follower;验收:3 节点 kill leader 自动恢复、消息不丢。

### 发布 v1.0

- 功能 / 文档 / 基准收口;release CI;打 `v1.0.0`。
