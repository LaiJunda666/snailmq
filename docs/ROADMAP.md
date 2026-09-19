# Roadmap

整体原则:

- 工程化不后置:文档、测试、CI、基准在每个版本同步维护。
- 每版可验收:结束态可运行、可演示、可测、有基准。
- 依赖例外:默认零三方;V4 引入 `etcd/raft`。

## V0 — 最小闭环(全内存 · 广播)

- 内核:New / CreateTopic / Publish / Subscribe / Close;per-partition 锁 + ready 通知
- 方案:共享有序 log + 独立游标尾随读;最小 `store` 接口(内存实现)
- 自研二进制协议;标准库 TCP server + client;cmd/demo 一键跑通
- 协议:批量发布 / 批量 ack、无 ack 发布;客户端攒批(`Batcher`)与批量读;推送 writev + 头缓冲池
- 单测 + -race、codec fuzz、README 示例、bench + RESULT 报告
- 验收:demo 跑通;吞吐达万级

## V1 — 可靠消费:ack / at-least-once

每订阅者维护已确认游标 + 待确认窗口;Ack/Commit;超时重投;at-least-once 语义文档化。

- 接缝:新增 `Subscription.Ack(offset)`(读游标与提交游标分离);读逻辑收进 `Partition.read`,便于挂载 ack/in-flight 状态。
- 客户端:单连接多路复用(支持一连接多订阅),消除当前"一连接一订阅"限制(见 plan 记录 E3/E4)。
- 生产韧性:心跳/半开连接检测、慢消费者策略、优雅排空关闭、协议版本协商。
- 可观测性:可选注入 Logger/Metrics + `Server.Stats()`(含订阅滞后等关键指标),保持零三方依赖。

## V2 — consumer group 竞争消费

无组广播 与 组竞争并存;分区在组成员间分配;心跳超时触发分区接管。

- 接缝:预留 key 分区能力(`CreateTopic(name, WithPartitions(n))`、`Publish(topic, key, payload)`);组位点用独立 `OffsetStore`/`MetaStore`,不复用消息 `Store`。
- 安全:多租户 / 公网场景前补 TLS 与认证授权。

## V3 — WAL 持久化

store 磁盘后端:append-only 顺序写、分段文件 + 索引、尾部截断恢复;fsync 策略;段清理。

- `Store` 接口扩为含 `Close`/`FirstOffset`,工厂改为 `func() (Store, error)`(open 失败可在创建期返回)。
- 去掉 `offset == index` 假设(截断/retention 前必须完成);`CreateTopic` 的 store 构造移出 broker 全局锁。
- 资源上限与 retention:每 topic 字节 / 条数上限(`WithMaxTopicBytes`/`WithMaxTopicMessages` + `ErrBackpressure`),内存不再依赖 OOM 兜底。

## V4 — raft 集群

引入 etcd/raft 做协调层;分区 leader/follower;3 节点 kill leader 自动恢复。

## V5 — 打磨发布 v1.0

功能/文档/基准收口;release CI;转 public;打 v1.0.0。
