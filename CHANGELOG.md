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

### 修复

- broker:`Store.Read` 在超大 `max` 下整数溢出导致 panic,现按日志剩余夹紧
- broker:`memoryLog` 移除内部锁,并发安全统一由分区持锁串行保证,消除双重加锁
- broker:`New` 对 `WithStoreFactory(nil)` 回退默认内存后端,不再 nil 调用 panic
