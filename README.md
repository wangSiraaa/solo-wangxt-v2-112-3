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

快照**提交成功之后**磁盘上的块仍可能被篡改或静默损坏，因此服务还提供**可恢复的全仓完整性巡检（scrub）**：
后台逐块流式重算 SHA-256，把水位、游标、每块结果和每个快照的独立巡检结论都写进 SQLite；
`committed` 仍是不可改写的历史提交事实，巡检另以 `clean / suspect / unscanned / excluded / uncovered` 呈现。

## 目录结构

```
cmd/backupd/main.go          HTTP 服务（启动时自动复验 pending 快照，自动续跑未完成巡检）
cmd/demo/main.go             端到端演示（走真实 HTTP API，含 38 项断言）
internal/repo/
  contentstore.go            内容寻址块仓（原子写、读时校验摘要、分片目录）
  manifest.go                SQLite schema 与快照状态机（pending/committed/failed）
  manifest_write.go          条目/块写入、缺块诊断查询
  meta.go                    分块多项式持久化
  scrub.go                   巡检 schema 操作：水位/冻结队列/游标、每块结果、每快照 verdict、清单反查
internal/backup/
  scan.go                    不跟随链接的目录扫描、分块、整文件摘要、写入中重读
  engine.go                  快照编排、提交前逐块验证、恢复与全部安全约束
  scrub.go                   巡检 worker（分批流式摘要、中断续扫）、suspect 恢复前置拦截
  util_linux.go              O_EXCL|O_NOFOLLOW 建文件（阻止沿预置符号链接写出）
internal/api/server.go       HTTP 路由（快照/恢复）
internal/api/scrub.go        HTTP 路由（巡检启动/进度/按快照查询/受影响路径查询）
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
| `POST /v1/snapshots/{id}/restore` | 恢复到**全新**目录，返回逐文件长度+摘要+块数报告；已知 suspect 提前 `409` |
| `POST /v1/recover` | 复验所有 pending 快照（服务启动时也会自动执行） |
| `GET  /v1/snapshots/{id}/integrity` | 该快照相对最近一次巡检的结论（clean/suspect/unscanned/excluded/uncovered/never_scrubbed） |
| `POST /v1/scrubs` | 启动巡检（后台）；存在未完成作业时默认从游标续跑，`{"force":true}` 另起一次 |
| `GET  /v1/scrubs` | 列出全部巡检作业 |
| `GET  /v1/scrubs/{id}` | 巡检进度：水位、总块数/已扫/坏块、字节数、逐快照结论 |
| `POST /v1/scrubs/{id}/cancel` | 在当前批的稳定边界暂停（可再 `POST /v1/scrubs` 续跑） |
| `GET  /v1/scrubs/{id}/snapshots` | 按快照查询巡检状态（水位后的快照显式为 `uncovered`） |
| `GET  /v1/scrubs/{id}/affected` | **维护入口**：每个坏块经清单反查列出全部受影响快照与文件路径、磁盘 blob 路径 |

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
8. **主动巡检而非等恢复失败**：`POST /v1/scrubs` 对全仓引用块逐块**流式**重算 SHA-256 与长度。
   作业开始时冻结水位（最高快照 id）、快照集合与去重块队列（稳定游标）；每批一个短事务，
   中断后只续扫 `done=0` 的块，报告按 `(scrub, digest)` UPSERT——**不重复出报告，也不把未扫数据标成 clean**。
9. **历史事实与巡检结论分离**：巡检只写 `scrub_snapshots`，永不改 `snapshots.status`。
   每个快照的结论独立呈现：`clean`（引用块全部流式校验通过）、`suspect`（至少一块 bad/missing）、
   `unscanned`（尚未扫完）、`excluded`（水位时仍是 pending/failed，不重贴 clean 标签、故障记录原样保留）、
   `uncovered`（水位之后才提交，本次未覆盖，绝不假装检查过）。
10. **共享块一处损坏、全部受影响者可列**：块是全局去重的，一个坏块的 `/affected` 通过
    `entry_chunks` 反查列出**所有**引用它的快照与文件路径（含水位外快照）。
11. **已知 suspect 提前拒绝恢复**：恢复在写任何文件之前先查最近巡检结论，suspect 快照直接
    `409 snapshot_suspect`，错误携带每个坏块的摘要、观测摘要/长度、磁盘 blob 路径与全部受影响路径，
    可定位而不是恢复到一半才失败。

## 演示会依次证明

1. 基线快照 → 恢复到新目录，逐文件核对摘要与长度（含空文件、0750 脚本、符号链接）；
2. 在 160KB 文件中部改 8 字节：**新块=1，复用旧块=4**，恢复结果与源一致；
3. 恢复到已存在目录 → `409 target_exists`；
4. 写入在重读窗口内停止 → 重读后成功；持续写入 → 3 次重读后拒绝并点名；
5. `lose_chunks:1` 模拟提交中断 → `failed` + `/missing` 给出精确缺块，旧快照仍可恢复；
6. 指向根目录外的符号链接 → 恢复 `422`，半成品目录回滚，外部文件不被触及；
7. `finish:false` 制造 pending → 重启服务后自动复验为 committed；
8. 全仓巡检：篡改两个快照共享的块 → 两者均 `suspect`、`/affected` 列出全部受影响快照与路径、
   恢复提前 `409` 且错误可定位；无关节点快照仍 `clean` 且可恢复；水位后新建快照为 `uncovered`。
