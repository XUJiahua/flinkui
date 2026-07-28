# 按时间戳重放设计（Replay From Timestamp）

> 让 flinkui 在**直连 K8s** 的前提下，一站式完成「Flink SQL 作业从指定时间点重新消费
> Kafka 并重放」及其收尾回退，替代
> [`replay-from-timestamp-runbook.md`](../../fko-demo/docs/replay-from-timestamp-runbook.md)
> 里的手工步骤。本设计对应「flinkui 直连模型内扩展」这条路线（最小能力集），**不**引入
> GitOps 渲染/提交能力。

## 1. 背景与目标

改了 SQL 逻辑后需要用历史数据回填、修数据、验证新逻辑时，要让作业**从 N 小时前的
Kafka 位点无状态重放**。runbook 已证明这需要两件事**缺一不可**：

1. Kafka 源改 `scan.startup.mode=timestamp`（+ `timestamp-millis` + `auto.offset.reset=earliest`）——位于 **SQL/ConfigMap**。
2. `spec.job.upgradeMode=stateless`——位于 **FlinkDeployment CR**。

再加**一次重新部署**让 operator 用新 SQL + stateless 重拉。

**目标**：把上面三步 + 追平后的回退，做成 flinkui 上的一个受控、可观测、可审计的流程。

**非目标（明确排除）**：

- 不做 GitOps 渲染 / `kustomize` / 写回 git（那是另一条更大的「部署工具」路线）。
- 不做破坏性的残留 HA 元数据清理（删 CR / 删 ConfigMap / `mc rm ha/`）——检测到并告警，不自动执行。
- 不改作业拓扑之外的部署参数（镜像、并行度、资源等）。

## 2. 适用边界（务必先读）

本能力是**带外（out-of-band）变更**：flinkui 直接改线上 ConfigMap 与 CR，不经过 git。

- ✅ 适用：手工 `kubectl apply` 管理、或没有持续调谐（no Argo/Flux auto-sync + self-heal）的环境（dev / uat / 部分 sit）。
- ⚠️ 不适用：由 Argo/Flux **持续同步**的 GitOps 环境——重放期间 ConfigMap 会被 git 版本
  **回滚覆盖**，可能重放到一半被打回旧 SQL。这类环境应走 runbook 的 GitOps 流程，或未来的
  「部署工具」路线。

因此本能力**默认关闭**，需显式开启（`FKO_REPLAY_ENABLED=true`）；并提供
`FKO_REPLAY_GITOPS_MANAGED=true` 开关，置真时 UI 强制显示漂移告警并要求二次确认（或直接禁用
提交，仅保留预览）。

## 3. 核心机理（为什么三要素缺一不可）

| 事实 | 设计含义 |
|------|---------|
| SQL 只在作业启动时经 `--sqlfile` 读一次 | 改 ConfigMap 不会让运行中的作业重读，必须重启 |
| operator 只在 **spec 变化**时调谐/升级作业 | 单改 ConfigMap 是静默的；必须让 spec 变（改 upgradeMode + 注入 checksum）才会触发 |
| `disableNameSuffixHash: true`，SQL ConfigMap 名稳定 | 可按固定名 GET/patch；但名字是 `flink-sql-<job>`，与 CR 名 `flink-sql-job-<job>` 前缀不同 |

> **SQL ConfigMap 的定位方式**：不靠命名硬拼，而是从 CR 读
> `spec.podTemplate.spec.volumes[?name=="sql-scripts"].configMap.name`（数据键为 `job.sql`）。
> 命名约定仅作兜底。

**重放序列**：备份原始 SQL → 改 ConfigMap 的 `job.sql`（timestamp 起点）→
patch CR `spec.job.upgradeMode=stateless` + 注入 `flinkui.sql-checksum` →
`Restart`（suspend→等 JM=0→resume，stateless 空状态从 timestamp 位点拉起）→ 观测验证。

**回退序列**：还原 `job.sql`（回 `group-offsets`）→ patch `upgradeMode=last-state`（+更新 checksum）→
`Restart`（从 stateless 期间产生的最新 checkpoint 恢复，安全接续，不再重放）。

## 4. 能力新增（分层）

### 4.1 cluster：ConfigMap 读写

新增可选接口，`KubeAccessor` 实现，形态与现有 `SecretAccessor` 对称：

```go
// ConfigMapAccessor reads/writes ConfigMaps in the managed namespace.
type ConfigMapAccessor interface {
    GetConfigMap(ctx context.Context, name string) (data map[string]string, found bool, err error)
    // PatchConfigMapKey updates a single data key (create-or-update), leaving
    // other keys intact — avoids clobbering multi-file ConfigMaps.
    PatchConfigMapKey(ctx context.Context, name, key, value string) error
}
```

实现直接用 `clientset.CoreV1().ConfigMaps(namespace)`，与 `GetSecret/ApplySecret` 同模式。
单键 patch（而非整体 Update）避免误删同一 ConfigMap 里的其它文件。

### 4.2 flink.Service：upgradeMode 与 SQL 原语

复用现有 `PatchFlinkDeployment` / `statePatch` / 每部署锁 / `Restart`。新增：

```go
// SetUpgradeMode patches spec.job.upgradeMode (stateless|last-state|savepoint).
func (s *Service) SetUpgradeMode(ctx context.Context, name, mode string) error

// GetJobSQL resolves the SQL ConfigMap from the CR's sql-scripts volume and
// returns the job.sql content (+ the resolved ConfigMap name for the caller).
func (s *Service) GetJobSQL(ctx context.Context, name string) (sql, cmName string, err error)

// PutJobSQL writes job.sql back and injects a fresh flinkui.sql-checksum into
// spec.flinkConfiguration so the operator will reconcile on the next restart.
func (s *Service) PutJobSQL(ctx context.Context, name, sql string) error
```

`SetUpgradeMode` 用 merge patch，与 `Rollback` 同机制；mode 取值白名单校验。

### 4.3 SQL 改写：结构化 vs 原始

- **结构化（向导默认）**：定位目标 `CREATE TABLE ... WITH ( ... )` 块，在其 `WITH` 选项里做
  **键级 upsert**（只改 `scan.startup.mode` / `scan.startup.timestamp-millis` /
  `properties.auto.offset.reset`），保留其余原样。多个 Kafka 源时由用户选定目标表。
  改写前后都产出 **diff 预览**，需确认。
- **原始编辑（逃生舱）**：高级用户直接编辑整段 `job.sql`，同样走 diff 预览 + 确认。

> 结构化改写只做「局部键替换」，不重排/格式化整份 SQL，把误改风险降到最低。解析失败时
> 自动降级为原始编辑并提示。

### 4.4 原始状态的保存（为可靠回退）

在改动前保存回退所需的原始态，存放两处（互为冗余，均随资源存活）：

1. SQL ConfigMap 里新增备份键 `job.sql.flinkui-prereplay-backup`（原始 `job.sql` 全文）。
2. FlinkDeployment 注解，记录重放会话元数据：
   - `flinkui.io/replay-active: "true"`
   - `flinkui.io/replay-started-at`, `-by`
   - `flinkui.io/replay-from-millis`
   - `flinkui.io/replay-prev-upgrade-mode`（回退目标，一般 `last-state`）

回退时优先用备份键还原 SQL、用 `replay-prev-upgrade-mode` 还原 upgradeMode，成功后清除备份键与注解。

## 5. 编排：重放/回退状态机

放在 `flink.Service`（或新 `internal/replay`）内，复用 `operationStore` 做异步进度（与
savepoint/restart 进度一致）。**全程持有该部署的锁**，串行化，避免与 suspend/resume/rollback 打架。

```
重放 Start:
  PREFLIGHT ── 校验:未在重放中? HA 归属 ok? 目标表可定位? ──► fail
      │
  BACKUP ── 备份 job.sql + 写重放注解
      │
  EDIT_SQL ── 结构化改写 job.sql (timestamp/earliest) + 注入 sql-checksum
      │
  SET_STATELESS ── patch upgradeMode=stateless
      │
  RESTART ── 复用 Restart(suspend→等 JM=0→resume)
      │
  OBSERVING ── 轮询/日志验证(见 §7),红绿灯
      │
  DONE / FAILED(可回退)

回退 Revert:
  RESTORE_SQL(备份键) → SET_LASTSTATE(upgradeMode=last-state) → RESTART → 清理备份/注解 → DONE
```

失败处理：任一步失败，状态机停在该步并标记 FAILED，暴露「回退」入口（尽最大努力还原已做的
改动）。不自动做破坏性清理。

## 6. API 设计（挂在现有 `/api` 保护组，自动获得 auth + audit）

```
GET  /api/jobs/:name/sql                 # 读取 job.sql(+ 解析出的 cm 名、检测到的 Kafka 源表)
GET  /api/jobs/:name/replay              # 当前重放会话状态(注解 + 状态机)
POST /api/jobs/:name/replay/preview      # 传 fromMillis/目标表,返回 SQL diff + 计划(不落盘)
POST /api/jobs/:name/replay              # 执行重放(异步,返回 operation id)
POST /api/jobs/:name/replay/revert       # 执行回退(异步,返回 operation id)
GET  /api/operations/:id                 # 复用现有异步进度查询
```

请求体（重放）：

```json
{
  "fromMillis": 1730000000000,        // 或 { "hoursAgo": 2 } 由后端换算
  "sourceTable": "orders",            // 多 Kafka 源时指定;单源可省
  "autoOffsetReset": "earliest",      // 默认 earliest,白名单校验(拒 latest)
  "confirmDrift": true                 // GitOps 环境要求显式确认
}
```

`preview` 必须先于 `replay` 调用并展示 diff；后端对「未预览直接执行」可要求带 diff 摘要哈希以确保用户看过。

## 7. 观测与验证（把 runbook §4 做成红绿灯）

OBSERVING 阶段复用现有 `Logs`，内置过滤与判定：

- 🟢 TM 日志出现 `Seeking to offset <非0、非末尾> for partition ...` → 起点解析正常。
- 🔴 JM 日志出现 `Restoring ... from Checkpoint` 或 `KUBERNETES_OPERATOR_LAST_STATE` →
  仍在 last-state（stateless 没生效，命中 runbook §3 坑），给出告警 + runbook 链接，**提示手工
  处理残留 HA 元数据**（不自动删）。
- 🔴 seek 到最小 offset → 时间戳越界走了 earliest 兜底；跳到末尾 → 误配 latest。
- 辅助：结合现有 metrics 的消费 lag，应有 ~N 小时积压并随重放收敛。

## 8. 安全 / 并发 / HA

- **默认关闭**：`FKO_REPLAY_ENABLED`（默认 false）。写作业逻辑，爆炸半径大。
- **RBAC 前置**：现状「登录即全权」。重放至少应受 `deployer` 级别控制；prod 建议加审批门
  （第二人 approve）。这依赖 roadmap 的细粒度 RBAC，先以实例级开关 + 审计兜底。
- **审计**：每步产出结构化审计记录（复用 `auditMiddleware` 的 `event=audit`，附
  user/operation=replay|revert/resource/fromMillis/status/duration）。
- **并发**：全程持部署锁，与 suspend/resume/restart/rollback 互斥。
- **HA 联锁**：若作业属某 HA group，复用 `failover.Service` 判定本侧是否 active/owner；非 owner
  拒绝重放（避免在备用侧或切换中误操作），与 secretsync 的 `SetRestartGuard` 思路一致。
- **漂移告警**：`FKO_REPLAY_GITOPS_MANAGED=true` 时 UI 醒目提示并要求 `confirmDrift`。

## 9. 前端

- 作业详情页新增「重放」入口（`app/job/[name]`）：
  - 时间选择器（N 小时前 / 绝对时间）→ 前端算 `fromMillis`；
  - 目标 Kafka 源表选择（后端解析给出候选）；
  - **SQL diff 预览**（一屏同时展示 ConfigMap 的 `job.sql` diff 与将设置的 `upgradeMode`，直击
    「缺一不可」易漏一半的问题）；
  - 执行 → 进度（复用现有 operation 进度组件）→ OBSERVING 红绿灯；
  - 「一键回退」按钮（会话 active 时出现）。
- 复用现有 `ConfirmDialog`；GitOps 环境展示漂移横幅。

## 10. 测试策略

- cluster：用 fake clientset 测 `GetConfigMap` / `PatchConfigMapKey`（单键 upsert 不误删其它键）。
- SQL 改写：表驱动测试——单源/多源、已有 timestamp、缺 `properties.` 前缀、注释/大小写、解析失败降级；断言只改目标三键、其余字节不变。
- Service：`SetUpgradeMode` 的 patch 内容；`GetJobSQL` 从 CR volume 正确解析 cm 名与 job.sql。
- 状态机：表驱动测各跃迁与失败回退；备份键/注解的写入与清理幂等。
- 观测判定：喂样例日志断言红绿灯。

## 11. 分阶段

1. **P1 只读 + 预览**：ConfigMap 读、SQL/源表解析、`/replay/preview` 返回 diff。零写入。
2. **P2 重放执行**：ConfigMap 写、`SetUpgradeMode`、备份/注解、状态机 + Restart + OBSERVING。
3. **P3 回退 + 红绿灯完善 + HA 联锁 + 审批门**。

## 12. 风险与取舍

- **GitOps 漂移**（最重要）：见 §2，用开关 + 强制确认 + 文档约束，不在受控同步环境作主路径。
- **SQL 解析脆弱**：结构化只做键级 upsert + 失败降级 + 强制 diff 预览，降低误改。
- **残留 HA 元数据**：检测告警，破坏性清理留手工，避免产品做高危删除。
- **契约变化**：flinkui 从「运维已部署作业」扩展到「在集群上改作业逻辑」，须配套 RBAC / 审计 /
  开关，避免能力被滥用。
- **与既有 lifecycle 的一致性**：重放本质是「改 SQL + 改 upgradeMode + 受控 restart」的组合，
  完全复用现有 patch/restart/锁/进度/审计原语，新增面小、风险可控。
