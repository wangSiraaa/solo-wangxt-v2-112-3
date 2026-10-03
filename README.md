# incbackup — 可验证的本地增量备份服务（仅 API）

“备份任务显示成功，恢复时却缺了一段文件”——本服务用一个硬规则拦住它：

> **快照在提交之前，必须逐个核对清单引用的每个内容块在内容仓中真实存在且长度正确；
> 恢复时再对流过的每个块和每个整文件做 SHA-256 与长度核对。**

任何一块对不上，快照就是 `failed`（或崩溃留下的 `pending`，重启后自动复验），
永远不会出现“成功”的快照恢复出残缺目录。

## 技术栈

| 组件 | 选择 | 用途 |
|---|---|---|
| 分块 | `github.com/restic/chunker`（Rabin 指纹内容定义分块） | 小改动只产生 1 个新块，其余块哈希相同直接复用 |
| 清单 | SQLite（`modernc.org/sqlite`，纯 Go，无 CGO） | 快照、条目、块索引、错误记录 |
| 内容仓 | 独立目录 `<repo>/chunks/ab/cdef…` | SHA-256 内容寻址、去重、只读不可变 blob |
| 接口 | 本地 HTTP API（默认 `127.0.0.1:8090`） | 无 UI、无鉴权，设计为只监听本地 |

分块多项式持久化在 `meta` 表中，跨快照/跨重启保持一致——否则边界漂移会让增量失效。

## 目录结构

```
cmd/backupd/main.go          HTTP 服务（启动时自动复验 pending 快照）
cmd/demo/main.go             端到端演示（走真实 HTTP API，含 23 项断言）
internal/repo/
  contentstore.go            内容寻址块仓（原子写、读时校验摘要、分片目录）
  manifest.go                SQLite schema 与快照状态机（pending/committed/failed）
  manifest_write.go          条目/块写入、缺块诊断查询、快照引用块清单
  scrub.go                   巡检作业：水位/冻结工作集/游标/每块结果/每快照结论的持久化
  meta.go                    分块多项式持久化
internal/backup/
  scan.go                    不跟随链接的目录扫描、分块、整文件摘要、写入中重读
  engine.go                  快照编排、提交前逐块验证、恢复与全部安全约束
  scrub.go                   可中断续扫的流式巡检执行器、suspect 恢复预检
  util_linux.go              O_EXCL|O_NOFOLLOW 建文件（阻止沿预置符号链接写出）
internal/api/server.go       HTTP 路由（快照/恢复）
internal/api/scrub.go        HTTP 路由（巡检启动/进度/按快照/受影响路径）
```

## 快速开始

```bash
go run ./cmd/demo            # 端到端演示（临时目录，自动清理）
go test ./...                # 单元测试
go run ./cmd/backupd --repo ./backup-repo --addr 127.0.0.1:8090
```

## HTTP API

| 方法与路径 | 说明 |
|---|---|
| `POST /v1/snapshots` | 扫描 `root` → 落块 → **逐块验证** → 提交。`finish:false`、`lose_chunks:N` 为故障演练参数 |
| `GET  /v1/snapshots` | 列出全部快照（含 failed，失败记录不删除） |
| `GET  /v1/snapshots/{id}` | 单个快照状态 |
| `GET  /v1/snapshots/{id}/missing` | **维护入口**：列出每个缺块的文件路径、SHA-256、期望磁盘路径与原因 |
| `GET  /v1/snapshots/{id}/errors` | 扫描/验证阶段的逐条错误（stage、rel_path、chunk_digest） |
| `POST /v1/snapshots/{id}/verify` | 对 pending 快照重新执行逐块验证并提交/判失败 |
| `POST /v1/snapshots/{id}/restore` | 恢复到**全新**目录，返回逐文件长度+摘要+块数报告 |
| `GET  /v1/snapshots/{id}/chunks` | 维护入口：列出快照引用的每个块及使用它的文件路径 |
| `POST /v1/recover` | 复验所有 pending 快照（服务启动时也会自动执行） |
| `POST /v1/scrubs` | **启动全仓完整性巡检**（已中断则从稳定游标续扫；可选 `throttle_ms` / `gate_ms`） |
| `POST /v1/scrubs/interrupt` | 请求中断当前巡检（已检结果与游标保留） |
| `GET  /v1/scrubs/latest` | 最近一次巡检的进度：水位、游标、块计数、逐快照状态汇总 |
| `GET  /v1/scrubs/{id}` | 指定巡检作业的进度 |
| `GET  /v1/scrubs/latest/snapshots/{sid}` | **按快照查询独立完整性状态**（可用 `{id}` 换具体作业） |
| `GET  /v1/scrubs/latest/chunks/{digest}/affected` | **受影响路径反查**：列出引用某块的全部快照与文件路径 |

### 典型请求

```bash
curl -s -XPOST localhost:8090/v1/snapshots \
  -d '{"root":"/srv/data","message":"nightly"}'
# 201 {"snapshot_id":7,"status":"committed","chunks_new":1,"chunks_referenced":5}

curl -s localhost:8090/v1/snapshots/7/missing
# {"snapshot_id":7,"status":"failed","missing":[
#   {"rel_path":"app.log",
#    "chunk_digest":"d2a8d66b…",
#    "expected_blob_path":"/…/chunks/d2/a8d66b…",
#    "reason":"chunk blob missing or length mismatch in content store"}]}

curl -s -XPOST localhost:8090/v1/snapshots/7/restore \
  -d '{"target":"/restore/2026-09-29"}'

# 启动全仓完整性巡检（异步），随后查看进度与某快照结论
curl -s -XPOST localhost:8090/v1/scrubs -d '{}'
curl -s localhost:8090/v1/scrubs/latest
curl -s localhost:8090/v1/scrubs/latest/snapshots/7
# 某个坏块影响了哪些快照和文件？
curl -s localhost:8090/v1/scrubs/latest/chunks/<64位hex摘要>/affected
```

## 关键正确性保证

1. **完成前验证所有内容块**：`snapshots.status` 只有 pending→committed/failed。
   提交前 `FindMissingChunks` 同时检查（a）清单里是否有块行、（b）blob 是否存在且长度一致；
   每块在恢复读取时再做流式 SHA-256 校验，每个文件组装后比对整文件摘要与总长度。
2. **扫描中正在写入的文件**：读取前后比对 size+mtime，并增加读后置静窗口
   （防止小文件恰好在两次 append 之间被整文件读完）。检测到变化→整块重读（最多 3 次）；
   仍在变→快照 `failed`，错误明确点名文件，绝不猜测版本。
3. **权限与符号链接本身保留**：保存并恢复目录/文件权限位、属主（root 时）、mtime；
   符号链接存的是链接本身与目标字符串，扫描与恢复均不跟随。
4. **不越界**：恢复前校验清单路径无绝对路径/`..`；符号链接目标按词法解析，解析后必须仍在恢复根内；
   任何现存祖先目录是符号链接一律拒绝；Linux 下用 `O_EXCL|O_NOFOLLOW` 建文件。
5. **不覆盖**：恢复目标已存在（任何类型）直接 `409`；恢复中途失败自动删除半成品目录。
6. **空文件**：长度 0、整文件摘要 `e3b0c442…`、0 个内容块，正常备份与恢复。
7. **失败可定位**：failed/pending 快照永久保留，`/missing` 直接给出“哪个文件的哪个块该在哪个路径”，
   而不是只看到队列空了。
8. **可恢复的全仓完整性巡检**：见下节，主动发现磁盘静默损坏，而不是等恢复失败。

## 全仓完整性巡检（scrub）

提交成功只代表当时逐块验证通过；磁盘上的 blob 之后可能被篡改或静默损坏。巡检作业主动扫描
**全部快照共享的去重内容仓**，在 SQLite 中持久化四类状态：

| 表 | 内容 |
|---|---|
| `scrub_runs` | 每次巡检：**开始水位**（启动时 `max(snapshots.id)`）、**稳定游标**（最后处理完的块摘要）、块计数、状态 |
| `scrub_work` | 启动瞬间冻结的工作集（水位内快照引用的去重块 + 当时清单声明长度），之后提交的快照不能扩大它 |
| `scrub_chunks` | **每块的流式摘要结果**：`ok / missing_catalog / missing_blob / length_mismatch / digest_mismatch / read_error` |
| `scrub_snapshots` | **每个快照独立的完整性状态**：`clean / suspect / unscanned / excluded / uncovered` |

关键规则：

- **历史不被改写**：`snapshots.status`（pending/committed/failed）仍是当时的提交事实；
  巡检结论存在独立的 `integrity` 字段，快照列表同时返回两者。
- **独立完整性状态**：`clean` = 水位内已提交且引用块全部复核通过；`suspect` = 有坏块；
  `unscanned` = 巡检未结束、尚未扫到（**绝不会因为扫了一半就显示 clean**）；
  `excluded` = 启动时还是 pending/failed（提交状态与 `snapshot_errors` 原故障记录保持权威）；
  `uncovered` = 水位之后才提交的快照，明确标为未覆盖。
- **共享块反查**：一个共享 blob 不符时，`…/chunks/{digest}/affected` 经清单反查，
  列出**所有受影响快照和文件路径**（一个块可能出现在多个快照的多个文件里）。
- **可中断、可续扫、不重复**：每批结果与游标在同一事务落盘；中断后再次 `POST /v1/scrubs`
  复用同一作业（返回 `200 resumed`，不生成重复报告），`INSERT OR IGNORE` 保证不重复记录；
  daemon 重启时自动从游标续扫。续扫结果与一次完整巡检逐块一致。
- **恢复前预检**：已知 `suspect` 的快照调用恢复会在流式读取**之前**被拒绝
  （`409 snapshot_suspect`），错误直接给出坏块摘要、`rel_paths` 和期望磁盘路径；
  未被巡检覆盖的块仍由恢复时的逐块流式 SHA-256 校验兜底。

## 演示会依次证明

1. 基线快照 → 恢复到新目录，逐文件核对摘要与长度（含空文件、0750 脚本、符号链接）；
2. 在 160KB 文件中部改 8 字节：**新块=1，复用旧块=4**，恢复结果与源一致；
3. 恢复到已存在目录 → `409 target_exists`；
4. 写入在重读窗口内停止 → 重读后成功；持续写入 → 3 次重读后拒绝并点名；
5. `lose_chunks:1` 模拟提交中断 → `failed` + `/missing` 给出精确缺块，旧快照仍可恢复；
6. 指向根目录外的符号链接 → 恢复 `422`，半成品目录回滚，外部文件不被触及；
7. `finish:false` 制造 pending → 重启服务后自动复验为 committed。
8. **巡检**：健康基线 clean → 静默损坏两个快照共享的块 → 两者均 suspect、清单反查列出全部
   快照与路径、恢复被提前拒绝；唯一块损坏只影响所属快照；巡检中断在第一个块前后续扫，
   结果与完整巡检一致且不重复报告。
9. **水位**：巡检期间新建的快照标为 uncovered（不假装已检查），下一次巡检才覆盖；
   既有 failed 快照始终 excluded，故障记录不丢失。
