# mq-lite

轻量单机消息队列,使用 Go 标准库实现,零第三方运行时依赖。

## 状态

V0 最小闭环进行中;工程骨架(Makefile / CI / 设计文档)按指令暂缓、后置到代码任务之后:

- 已完成:`broker` 内核 —— `Message` 模型、`Store` 接口与内存日志实现 `memoryLog`(单元测试 + `-race` 通过)
- 待做:协议编解码 → TCP server/client → cmd/demo → benchmark → V0 收口

## 设计目标

- 广播 + 独立游标尾随读(每订阅者维护自己的 offset)
- 可插拔存储接口(内存实现起步,WAL 持久化后置)
- 自研二进制帧协议(纯编解码,与网络解耦)
- 内嵌内核库,或独立 TCP 服务(标准库实现)两种用法

## Roadmap

单机核心(广播) → ack / 消费组 → WAL 持久化 → raft 集群。

## License

[MIT](LICENSE)
