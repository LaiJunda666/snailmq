# Contributing to SnailMQ

感谢参与贡献。本文规定开发流程、质量门与提交规范。**AI Agent 同样适用**(另见 [AGENTS.md](AGENTS.md))。

## 开发环境

- Go **1.27+**;零三方运行时依赖(新增依赖须先讨论获批)。
- 克隆后即可用 Makefile 入口:

```bash
make help     # 目标列表
make test     # go test -race ./...
make fmt      # gofmt 检查
make vet
make build
make bench
make demo
```

## 分支策略(重要)

- **`main` 受保护,禁止直接推送**:所有改动(含文档、小修)一律从最新 `main` 开新分支 → 开发 → 本地/CI 验证通过 → 经 review 确认后,再合并回 `main`。
- 分支命名建议:

```
feat/<short-desc>     新功能(如 feat/consumer-groups)
fix/<short-desc>      修复(如 fix/server-close-hang)
perf/<short-desc>     性能(如 perf/batch-publish)
docs/<short-desc>     文档
chore/<short-desc>    杂项/构建/CI
refactor/<short-desc> 重构
```

- AI Agent 的**任何 commit/push 都必须先获得所有者明确同意**。

典型流程:

```bash
git switch main && git pull
git switch -c feat/xxx
# 开发 + 验证
make fmt && make vet && make test && make build
git add -A && git commit -m "feat(broker): ..."
git push -u origin feat/xxx
# 开 PR → CI 绿 → review → 合并 main
```

## 提交规范

使用 [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>
```

- **type**:`feat` `fix` `perf` `refactor` `docs` `test` `chore` `bench` `ci`。
- **scope**(建议):`broker` / `protocol` / `network` / `cmd` / `benchmark` / `docs` / `ci`。
- subject 用中文简要描述;一次提交只做一件事。
- 示例:`feat(protocol): 批量发布与单帧批量 ack`、`fix(network): 修正推送写超时位置`。

## 质量门(合并前必须通过)

```bash
make fmt     # gofmt 必须干净
make vet
make test    # 全部单测,含 -race
make build
```

- **协议改动**额外跑 fuzz:

```bash
go test ./protocol -fuzz=FuzzDecodePublish -fuzztime=10s
go test ./protocol -fuzz=FuzzReadFrame   -fuzztime=10s
```

- 静态检查:`golangci-lint run ./...`(CI 与本地均要求 0 issues)。
- **新功能/修复必须带测试**:单元测试;跨层行为补 `network` 端到端;并发路径确保 `-race` 覆盖。
- 性能相关改动请在 `benchmark/` 补/更新基准,并把结果写入 `benchmark/RESULT.md`。

## 代码风格

- 仅标准库;遵守依赖方向:**`broker` 不 import `network`**;`network` 依赖 `broker + protocol`。
- 导出标识符写中文 godoc;关键不变量(锁边界、所有权、生命周期)在注释中说明。
- 错误用哨兵 + `errors.Is`;跨线错误走协议 `Code` 映射。
- 不提交注释掉的死代码;不留未处理的错误(errcheck 应干净)。

## 文档同步

改动涉及以下内容时同步更新对应文档:

| 变更 | 更新 |
|---|---|
| 行为/接口/性能 | `CHANGELOG.md`(Keep a Changelog) |
| 协议(帧/opcode/错误码/上限) | `docs/PROTOCOL.md` |
| 架构/并发/生命周期 | `docs/ARCHITECTURE.md` |
| 用法/命令/项目结构 | `README.md` |
| 版本范围/接缝 | `docs/ROADMAP.md` |

## Pull Request

1. 标题用 Conventional Commits 风格,正文说明**动机 / 方案 / 影响 / 测试**。
2. CI(fmt → vet → test → build,外加 lint 与 fuzz 冒烟)必须全绿。
3. 至少一位维护者 review;讨论结论与后续项在 PR 记录。
4. 合并策略:**squash merge** 到 `main`,保持线性历史。

## 发布流程

- 发布由维护者执行;**打 tag 前必须经所有者确认**。
- 步骤:更新 `CHANGELOG.md`(版本段 + 日期)→ 全量验证(`make fmt/vet/test/build`、`make bench`、demo)→ `git tag -a vX.Y.Z` → push tag。
- 版本号遵循 [SemVer](https://semver.org/lang/zh-CN/):V0 阶段为 `0.x.y`。

## 报告问题

- 说明:版本/commit、复现步骤、期望 vs 实际、日志;协议/并发问题尽量附最小复现。
- 安全相关问题请私下联系维护者,请勿公开披露。
