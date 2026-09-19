# Changelog

本项目所有值得记录的变更都会记录在此文件中。

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 添加

- 项目骨架:Go 模块、MIT License、README、CHANGELOG
- 工程骨架:Makefile、GitHub Actions CI、AGENTS、架构 / 协议 / 路线文档
- broker:消息模型 `Message`、日志 `Store` 接口与内存实现 `memoryLog`
- broker:分区 `Partition` 与广播订阅 `Subscription`(共享日志 + 独立游标 + ready 通知 + 关闭语义)
- broker:线程安全门面 `Broker`(`New`/`CreateTopic`/`Publish`/`Subscribe`/`Close`)管理多主题
- protocol:二进制帧与 payload 编解码(Magic/Version/Opcode、防截断与长度上限,含 fuzz)
- broker:空主题名哨兵 `ErrTopicNameEmpty`
- network:标准库 TCP server(每连接 goroutine,订阅成功后连接转为单向推送流)
- network:标准库 TCP 客户端 `Dial`/`CreateTopic`/`Publish`/`Subscribe`/`Close` 与端到端测试
- cmd/demo:一键演示(起 server + 双订阅者广播 10 万条,打印吞吐)
- protocol:`OpError` 结构化错误码(`[u16 code][u32 len][msg]`)与 `Code` 常量
- network:客户端请求相位读写超时选项(`WithClientReadTimeout`/`WithClientWriteTimeout`)与 `network.ErrClosed`
- network:`Server` `WithMaxConns` 连接数上限(超限回 `CodeOverloaded`);`broker.MaxTopicNameLen` 主题名上限

### 变更

- 项目更名 SnailMQ,Go module 路径迁移为 `github.com/LaiJunda666/snailmq`
- broker:`Close` 统一为幂等语义,重复调用返回 nil
- broker:门面返回的 `ErrClosed` 附加操作与 topic 上下文(`errors.Is` 判定仍成立)
- network:连接读 / 写超时(默认 30s):空闲连接自动关闭,写入超时的慢订阅者被丢弃
- network:`Dial` 增加默认 5s 连接超时,并提供 `DialTimeout`
- protocol:`OpError` payload 由纯文本改为"错误码 + 文本";新增 `MaxMessage`(单条消息体上限,与帧上限区分)
- CI 增加 lint 与 fuzz 冒烟;移除未使用的 `examples/` 占位目录
- cmd/demo:吞吐同时打印 msg/s 与 MB/s(以消息 payload 字节计),避免小消息下 msg/s 误导
- protocol:解码零拷贝(`payload` 为输入子切片);新增直写 `WriteMessage`/`WritePublish`(免整帧拼接拷贝)
- network:服务端 / 客户端内核读写缓冲可配(`WithReadBuffer`/`WithWriteBuffer`、`WithClientReadBuffer`/`WithClientWriteBuffer`),默认 256 KiB
- protocol:批量发布 `OpPublishBatch`(单帧批量 ack)与无 ack 发布 `OpPublishNoAck`
- network:`Client.PublishBatch` / `PublishAsync` / `Batcher`(linger 攒批)/ `Subscription.ReadBatch`
- protocol:`WriteMessageBatch` 用 writev 一次写多条推送,头缓冲池化(sync.Pool)
- benchmark:内核 / 端到端基准(参数化 10B/1KiB/64KiB,含 MB/s)与 `benchmark/RESULT.md`
- docs:详细化 README / ARCHITECTURE / PROTOCOL / ROADMAP;新增 `CONTRIBUTING.md`(分支、提交、发布规范)
- 项目 logo 与 social preview(`assets/`)

### 修复

- broker:`Store.Read` 在超大 `max` 下整数溢出导致 panic,现按日志剩余夹紧
- broker:`memoryLog` 移除内部锁,并发安全统一由分区持锁串行保证,消除双重加锁
- broker:`New` 对 `WithStoreFactory(nil)` 回退默认内存后端,不再 nil 调用 panic
- network:`Server.Close` 现在关闭连接,空闲 / 慢客户端下不再挂起
- network:订阅连接结束(客户端断开、写失败、Close)时自动注销订阅,不再泄漏 `Partition.subs`
- network:`Serve` 与 `Close` 的 WaitGroup 登记移入临界区,消除登记与等待的竞态
- network:推送写超时改为在整批写入前刷新,避免空闲后收大批量消息被误断丢消息
- network:请求-响应阶段的响应写也受写超时保护,不读响应的客户端不再钉住 goroutine
- network:客户端进入推送流后再次请求返回 `ErrStreaming`,不再静默吞掉一条推送
- protocol:`WriteFrame` 本地拒绝超过 `MaxPayload` 的帧(`ErrTooLarge`),服务端回 `OpError` 后再断
- broker:订阅 `Close` 后 `Read` 立即返回 `ErrClosed`,不再投递积压消息
- network:发布/推送长度口径统一,消除"发布成功却推送超限、静默断连丢消息"
- network:`Server.Close` 并发调用现在都等到同一完成点;连接监视 goroutine 纳入等待
- network:客户端请求相位读写超时;`Client.Close` 后各方法返回 `ErrClosed`;响应/推送解析错误补上下文
- broker:工厂返回 nil 时 `CreateTopic` 返回错误,不再在 `Publish` 时空指针 panic
- network:`DialTimeout` 负值按"不设超时"处理,与文档一致
- cmd/demo:订阅者失败时立即返回错误,不再永久阻塞
- network:连接处理与对端监视 goroutine 增加 panic 隔离;监视 goroutine 在所有返回路径 join
- network:推送相位收到客户端数据视为协议违规并断开;超长 topic 名在 broker/client 侧拒绝
