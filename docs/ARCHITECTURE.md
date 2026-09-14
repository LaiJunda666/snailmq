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

## 服务端生命周期(network)

- 每连接一个 goroutine:请求相位"读帧 → 调 broker → 回响应";收到 `OpSubscribe` 成功后转为单向推送流(只发 `OpMessage`)。
- 对端监视:推送相位并发读连接以感知客户端关闭(FIN/EOF),据此取消订阅上下文并注销订阅,避免 `Partition.subs` 泄漏。
- 读写超时(默认 30s):空闲请求连接被关闭;推送写入超时的慢订阅者被丢弃。
- `Server.Close`:幂等返回 nil,取消并关闭所有连接、等待 goroutine 退出;不关闭 listener(归调用方)。

## 依赖方向

broker 不 import network;network 依赖 broker + protocol。内核可单独测试、可内嵌。

## 演进接缝

- `Store` 接口 = WAL 落地点;`transport`(现为标准库)预留换 netpoll;分区/offset 模型通向 ack/消费组/raft。
