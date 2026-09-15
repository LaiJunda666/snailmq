# mq-lite

轻量单机消息队列,使用 Go 标准库实现,零第三方运行时依赖。

## 状态

V0 最小闭环进行中;工程骨架(Makefile / CI / AGENTS / 架构·协议·路线文档)已补齐,尚未打 tag:

- 已完成:
  - `broker` 内核(单元测试 + `-race` 通过)——
    - 消息模型 `Message`、日志 `Store` 接口与内存实现 `memoryLog`
    - 分区 `Partition` 与广播订阅 `Subscription`(共享日志 + 独立游标 + ready 通知 + 关闭语义)
    - 线程安全门面 `Broker`(`New`/`CreateTopic`/`Publish`/`Subscribe`/`Close`)管理多主题
  - `protocol`:自研二进制帧与 payload 编解码(含 fuzz)
  - `network`:标准库 TCP 服务端(每连接 goroutine;订阅后转推送流;读写超时)
- 待做:TCP 客户端(network)→ cmd/demo → benchmark → V0 收口(含 tag v0.1.0)

## 设计目标

- 广播 + 独立游标尾随读(每订阅者维护自己的 offset)
- 可插拔存储接口(内存实现起步,WAL 持久化后置)
- 自研二进制帧协议(纯编解码,与网络解耦)
- 内嵌内核库,或独立 TCP 服务(标准库实现)两种用法

## Roadmap

单机核心(广播) → ack / 消费组 → WAL 持久化 → raft 集群。

## License

[MIT](LICENSE)
