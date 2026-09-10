# Architecture

## 分层

```
broker/     内核(共享有序 log + 独立游标,per-partition 锁 + ready 通知)
protocol/   自研二进制帧,纯编解码(吃 io.Reader/Writer)
network/    TCP 服务与客户端(纯标准库),订阅后推送流
cmd/demo/   一键跑通演示
benchmark/  基准与报告
```

## 并发模型

- 每个 topic 一个 partition,持有有序不可变 log(经由 `broker.Store` 接口)
- Publish:取 partition 锁 → store.Append → 对每个订阅者非阻塞发 `ready`(cap-1)信号
- 订阅者:循环读 `[offset, len)`;无新消息则阻塞在 `ready` / `done` / ctx
- 优雅关闭:Broker.Close → partition.close → close 所有订阅者 `done` → 读方收到 `ErrClosed`
- 关闭语义:`Close` 幂等,重复调用返回 nil;关闭后的其它操作返回包装后的 `ErrClosed`(判断用 `errors.Is`)

## 依赖方向

broker 不 import network;network 依赖 broker + protocol。内核可单独测试、可内嵌。

## 演进接缝

- `Store` 接口 = WAL 落地点;`transport`(现为标准库)预留换 netpoll;分区/offset 模型通向 ack/消费组/raft。
