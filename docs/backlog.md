# 待办事项 / Backlog

> 本文汇总从「Ververica Platform 产品视角」review 后**尚未完成**的改进项。
> 已完成的 P0 安全项（Secure cookie、WS 同源校验、审计中间件、redeploy nonce 单调化、
> handoff CAS）不在此列。每项标注优先级、动机、代码依据与建议做法,便于逐项认领。

优先级约定:
- **P1** — 迈向「平台」的核心能力 / 存在真实越权面,应尽快排期。
- **P2** — 体验与健壮性打磨,可择机进行。

---

## P1-1 Flink UI 反代限制为只读（安全 + 一致性）

**动机**：`api.Any("/jobs/:name/ui/*path", h.flinkUIProxy)` 把整个 JobManager REST 透传出去,
`Any` 含 POST/PATCH/DELETE。任何登录用户都能经代理直接调用 JM 原生变更接口
（`POST /jobs/:id/stop`、触发 savepoint、改配置等）,**绕过** per-deployment 串行锁与异步
operation 语义;审计中间件虽记录一条,但 `operationFromPath` 只取末段路径,记录语义失真。

**代码依据**：
- `internal/api/server.go` — `api.Any(".../ui/*path", h.flinkUIProxy)`
- `internal/api/proxy.go` — `flinkUIProxy` 直接透传所有方法
- `internal/api/audit.go` — `operationFromPath` 仅取路径末段

**建议做法**：代理只放行 `GET`/`HEAD`（只读浏览）,对写方法返回 405;补一个「写方法被拒」
的测试。可同时消除「绕过锁」与「审计失真」两个问题。

**验收**：经 `/ui/` 的 POST/PATCH/DELETE 返回 405;GET 仍可正常浏览 Flink UI。

---

## P1-2 高危操作的 RBAC / 分权

**动机**：目前是单账号 basic auth。`force Promote(ackDataLoss)` 这类可致运行期双跑的操作,
现在有审计（记录谁干的）但**无授权**（谁都能干）。

**代码依据**：
- `internal/auth/auth.go` — 单 `username`/`password`,无角色概念
- `internal/failover/switch.go` — `Promote(name, force, ackDataLoss)` 无角色校验

**建议做法**：先落地最简两级角色（admin / viewer）,把 mutating 端点（尤其
promote / release / rollback / restart）限制为 admin;角色可先来自配置或多账号 secret。
审计已记录 `user`,与角色天然衔接。

**验收**：viewer 调用高危端点返回 403;admin 正常;审计含角色维度。

---

## P1-3 Savepoint 作为一等资源（catalog）

**动机**：当前 savepoint 触发即返回一个 location 字符串,rollback 靠扫 S3 目录反推
（列 `savepoint-*` 目录与 `chk-N/_metadata`）。缺元数据（触发人/时间/类型 manual|periodic/
关联 job/状态）、周期性 savepoint、保留清理。这是与 VVP 差距最大处,做了会同时反哺审计与
rollback 选择器。

**代码依据**：
- `internal/flink/savepoint.go` — `Savepoint` 仅返回 `SavepointResult{Location}`
- `internal/store/s3.go` — `ListRecoveryPoints` 从对象存储路径反推,无元数据

**建议做法**：引入 savepoint catalog（CR annotation 或独立 S3 `index.json`）,每次
`StartSavepoint` 落一条元数据;rollback 选择器改读结构化列表;后续叠加周期触发与保留策略
（retain N / TTL）。

**验收**：savepoint 列表带触发人/时间/类型;rollback 从 catalog 而非目录扫描取值。

---

## P1-4 多租户 / 多集群 / 分页

**动机**：`ClusterConfig` 仍是单个,`listJobs` 全量返回无分页。VVP 的组织单元是
namespace(逻辑租户) + deployment target(物理 ns),一个平台管多个;几十上百个 deployment
时前端会吃力。

**代码依据**：
- `internal/config/config.go` — `Cluster ClusterConfig`（单集群,注释已预留 list 演进）
- `internal/api/handlers.go` — `listJobs` 直接全量 `svc.List` 无分页/过滤

**建议做法**：即使暂不做多集群,先把 API 路径预留集群维度
（如 `/api/clusters/:cluster/jobs`）,List 支持分页 / label selector / 状态过滤。

**验收**：List 支持 `limit`/`offset`（或游标）与按 label 过滤;路径为多集群预留。

---

## P1-5 异步 operation / HA task 状态持久化

**动机**：`operationStore` 与 `taskStore` 均为内存 + bounded retention,进程重启即丢。
savepoint 可能跑 2 分钟,滚动升级控制台会让 UI 永远等不到结果;handoff task 也会「重启丢半途」。

**代码依据**：
- `internal/flink/operations.go` — `operationStore` in-memory
- `internal/failover/switch.go` — `taskStore` in-memory
- `docs/failover-decentralized-design.md` §11 — 已坦白该限制

**建议做法**：进行中的 operation/task 落 S3 或 CR;或让视图从集群实际状态 + S3 token/handoff
重建（设计文档已指出可重建）。

**验收**：控制台重启后,进行中/刚完成的操作状态可恢复展示。

---

## P1-6 Promote 中 token 写入与 handoff CAS 非原子（代码层收敛）

**动机**：文档已记录该中间态（`docs/failover-decentralized-design.md` §6/§11):Promote 先
`WriteHandoffCAS` 再无条件 `WriteToken`,两步之间崩溃会留下 `handoff=self 但 token≠self`。
该态 fail-closed（不双跑）且可重跑恢复,但代价是 epoch 虚增 + 需人工介入。**文档已完成,
代码层收敛仍待做。**

**代码依据**：
- `internal/failover/switch.go` — `doPromote`：`WriteHandoffCAS` 成功后才 `WriteToken`（无条件）

**建议做法**：将两步做成幂等可续跑序列——重跑 Promote 时若发现 handoff 已是 self/更高 epoch,
则**不再 epoch+1**,直接补写 token 收敛;或给 token 写入也加条件写。并让 `deriveRole` 对
「handoff=self 但 token≠self」给出明确的「重试 Promote」提示。

**验收**：模拟两步之间失败后重跑 Promote,epoch 不虚增,最终 token/handoff 一致。

---

## P2-1 可观测性:作业内部指标 ✅ 已完成

**动机**：控制台只看到 K8s 层的 pod/event/status 与 `Health` 派生态,看不到作业内部指标
（records in/out、checkpoint duration/size、失败计数）。

**实现**：
- 后端 `internal/flink/metrics.go` — `Metrics()` 经 pod exec curl `localhost:8081` 拉取
  `GET /jobs/{jid}` 与 `GET /jobs/{jid}/checkpoints`,聚合为 `JobMetrics`(状态/uptime、
  各 vertex records/bytes 汇总、checkpoint 完成/失败/进行中计数与最近一次大小/时长/时间)。
  与 Savepoint 一致走 Exec,in/out-of-cluster 均可用;只读、不加锁;checkpoint 为 best-effort。
- API `GET /api/jobs/:name/metrics`（`internal/api/handlers.go` + `server.go`）。
- 前端 `frontend/app/job/page.tsx` 新增 Metrics 卡片(仅在作业运行时查询,5s 刷新);
  类型/客户端在 `frontend/lib/types.ts`、`api.ts`。
- 测试 `internal/flink/metrics_test.go`(聚合 / 无 jobId / checkpoint best-effort)。

**readyz degraded**：已由早前提交完成（`readyz` 返回 `degraded` + `clusterReachable`,
仍恒 200;`svc.Reachable()` 带 5s TTL 缓存的 live 探测）。

**验收**：✅ 作业卡片展示 checkpoint / 吞吐 / uptime;`go build/test/vet` 与 `pnpm build` 全绿。

---

## P2-2 Flink UI 反代:root-absolute 资源

**动机**：Flink Web UI 部分版本以 root-absolute 路径（`/assets/...`）加载静态资源,
prefix-stripping 代理无法在不解析 HTML/JS 的前提下重写,某些版本会在代理下 404。

**代码依据**：
- `internal/api/proxy.go` — 顶部注释已标注该已知限制

**建议做法**：以「深链原生 UI」为可靠路径（`flinkUIInfo.target`）;若要代理内可用,需针对
各 Flink 版本实测,必要时重写 base href / HTML。优先级低。

**验收**：明确各支持版本在代理下的资源可用性,或在 UI 上引导用户使用深链。

---

## 建议排期顺序

1. **P1-1**（只读代理）与 **P1-2**（高危操作分权）——改动小、直接消除越权面。
2. **P1-3**（savepoint catalog）——中等体量、收益最广（审计 + rollback 双受益）。
3. **P1-4 / P1-5**（多租户分页 / 状态持久化）——平台化基础设施。
4. **P1-6**、**P2-\***——健壮性与体验打磨,择机进行。
