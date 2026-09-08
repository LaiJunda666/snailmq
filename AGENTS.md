# 给 AI Agent 的项目约定

构建 / 测试 / 格式 / 压测统一走 Makefile 入口。

- 格式检查:`make fmt`(先 `make fmt-fix` 再提交)
- 测试(含 `-race`):`make test`
- 静态检查:`make vet`
- 构建:`make build`
- 压测:`make bench`
- 演示:`make demo`

代码约定:

- 零三方运行时依赖(唯一例外 `etcd/raft`,属 V4 集群)
- 依赖方向:broker 不 import network;network 依赖 broker + protocol
- 提交前必须 `make test` 与 `make fmt` 通过
- 提交信息用 Conventional Commits
- **任何 commit / push 必须先经用户明确同意**

路线:V0 全内存广播闭环 → V1 ack/重投 → V2 消费组 → V3 WAL → V4 raft → V5 发布 v1.0(详见 docs/ROADMAP.md)。
