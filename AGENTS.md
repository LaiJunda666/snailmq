# 面向 AI Agent 的项目约定

构建 / 测试 / 格式 / 压测统一走 Makefile 入口。

- 格式检查:`make fmt`(先 `make fmt-fix` 再提交)
- 测试(含 `-race`):`make test`
- 静态检查:`make vet`
- 构建:`make build`
- 压测:`make bench`
- 演示:`make demo`

代码约定:

- 零三方运行时依赖(唯一例外 `etcd/raft`,用于集群 / raft)
- 依赖方向:broker 不 import network;network 依赖 broker + protocol
- 提交前必须 `make test` 与 `make fmt` 通过
- 提交信息用 Conventional Commits
- **任何 commit / push 必须先经用户明确同意**

依赖策略(零依赖为默认档,三方为受控例外):

- 默认:运行时仅用标准库;**主构建路径不 import 三方运行时依赖**。
- 开例外须**同时**满足:① 能被我们自己的接口隔离(三方类型不泄漏进公共 API);
  ② 该领域"库本身难做对"(共识、持久化/崩溃恢复、高性能事件循环),且实现成熟、可审计、维护活跃。
- **一次只开一个例外**;新增依赖须先与所有者讨论获批。
- 已批准例外:`go.etcd.io/raft/v3`(集群 / raft,子包隔离)。
- 候选待评:持久化(WAL)存储库(如 `tidwall/wal`),须藏在 `Store` 接口后。
- 可观测:优先**自研接口 + 自写 Prometheus 文本端点**;不默认引入 `client_golang`(要用走可选嵌套 module)。
- 明确不接受:三方 logger 替换 `log`、三方 map、仅为好看的 errors 包装;替换标准库已做好的事(HTTP/编码/sync/map)。
- 测试 / lint 工具(dev-only)不受此限,不计入运行时依赖。
- 若需根 `go.mod` 保持零依赖:重依赖放**嵌套独立 module**;注意 **build tag 无法把依赖从 `go.mod` 移除**。

分支与合并:

- **所有改动必须开新分支 → 验证通过 → 经确认后再合并到 `main`;禁止直接 push `main`。**
- 分支命名 `feat/` `fix/` `perf/` `docs/` `chore/` `refactor/` + 简短描述。
- 协议等敏感改动需额外通过 `make test`(含 `-race`)与 fuzz 冒烟。
- 完整流程见 CONTRIBUTING.md。

路线:**版本以 CHANGELOG / git tag 为准**;后续方向以主题呈现,详见 docs/ROADMAP.md。
