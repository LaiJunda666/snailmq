# Roadmap

整体原则:

- 工程化不后置:文档、测试、CI、基准在每个版本同步维护。
- 每版可验收:结束态可运行、可演示、可测、有基准。
- 依赖例外:默认零三方;V4 引入 `etcd/raft`。

## V0 — 最小闭环(全内存 · 广播)

- 内核:New / CreateTopic / Publish / Subscribe / Close;per-partition 锁 + ready 通知
- 方案:共享有序 log + 独立游标尾随读;最小 `store` 接口(内存实现)
- 自研二进制协议;标准库 TCP server + client;cmd/demo 一键跑通
- 单测 + -race、codec fuzz、README 示例、bench + RESULT 报告
- 验收:demo 跑通;吞吐达万级

## V1 — 可靠消费:ack / at-least-once

每订阅者维护已确认游标 + 待确认窗口;Ack/Commit;超时重投;at-least-once 语义文档化。

## V2 — consumer group 竞争消费

无组广播 与 组竞争并存;分区在组成员间分配;心跳超时触发分区接管。

## V3 — WAL 持久化

store 磁盘后端:append-only 顺序写、分段文件 + 索引、尾部截断恢复;fsync 策略;段清理。

## V4 — raft 集群

引入 etcd/raft 做协调层;分区 leader/follower;3 节点 kill leader 自动恢复。

## V5 — 打磨发布 v1.0

功能/文档/基准收口;release CI;转 public;打 v1.0.0。
