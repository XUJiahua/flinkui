# 主从切换平台化设计（Failover Platform, P1）

> 把 `scripts/failover.sh` 的主/备故障转移能力搬进 Flink Job Console。
> 对应 MVP 设计 §8 路线图的 P1「多集群 + 跨集群主备切换」。

## 1. 本期范围（已与需求方对齐）

**In scope**
- P1a **只读观测**：主备组拓扑、两侧状态、fencing token 指向、脑裂预警。
- P1b **手动切换**：failover（主→备）/ failback（备→主），复刻 `do_switch` 五步，人工二次确认。

**Out of scope（下一期）**
- 自动故障检测 + 自动切换（`failover.sh check` 的常驻控制循环、平台自身 HA / leader 选举）。
- 双活巡检面板、在线建作业。

**关键决策（已确认）**
- 跨集群访问：**方案 A —— 平台挂载对侧 kubeconfig（Secret），对应对侧最小权限 SA**。
- **双形态都支持**：真·跨集群（两份 kubeconfig）与单集群双 namespace（同一 accessor、不同 namespace，免跨集群凭证）。
- SwitchTask 状态：**暂时 in-memory**（平台重启丢失半途任务，可从集群实际状态 + token 重建视图；持久化留到自动切换期）。
- Fencing 机制：**沿用现状**（S3 token + 作业 Pod 的 fencing initContainer），平台只负责读写 token；平台用自己的 S3 client 直接写，省掉脚本的一次性 mc Pod。
- HA 组：**静态声明**（配置文件 / values）。

## 2. 复刻对象：failover.sh 的本质

- **HA 组** = 同一逻辑作业在两侧的映射：`primary(kubeconfig+ns+clusterId) / standby(...)`，共享同一份 S3（checkpoint / savepoint / fencing）。
- **Fencing token**：S3 上 `fencing/active-cluster` 记录当前活跃 `clusterId`；作业 Pod 的 fencing initContainer 校验 token==自身 clusterId 才启动；中性 token（`__switching__`）让两侧都起不来。
- **`do_switch(from,to)` 五步**（failover / failback 共用）：
  1. 写**中性** token（fence 两侧，杜绝过渡期并行）。
  2. **选恢复点**：源侧 `RUNNING/STABLE` → 经 JM REST 触发 savepoint（零丢失）；否则/超时 → S3 上全局最新 checkpoint。
  3. **停源侧**（`state=suspended`）并**等待 JM Pod 归零**。
  4. **token 指向目标侧** clusterId。
  5. **启动目标侧**：`state=running` + `initialSavepointPath` + `savepointRedeployNonce`（绕开 last-state）。
- **两道防并行保险**：中性 token + 等停稳。

## 3. 配置 schema（静态声明，向后兼容）

单集群配置保持不变；新增可选 `clusters` 与 `haGroups`：

```yaml
# 现有单集群仍有效
cluster: { name, namespace, kubeconfig, context, s3 }

# 命名集群池（每个 = 一份 kubeconfig 或 in-cluster）
clusters:
  cluster-a: { kubeconfig: /etc/flinkui/kube/a.yaml, s3: { endpoint, bucket, accessKey, secretKey, pathStyle, insecure } }
  cluster-b: { kubeconfig: /etc/flinkui/kube/b.yaml, s3: { ... } }
  local:     { kubeconfig: "", s3: { ... } }   # in-cluster

# 主从组静态声明
haGroups:
  - name: halykbank-codes
    s3Cluster: cluster-a         # 用哪份 S3 配置读写 fencing/恢复点（两侧共享）
    fencingKey: fencing/active-cluster
    neutralToken: __switching__
    primary: { cluster: cluster-a, namespace: flink-jobs, deployment: flink-sql-job-...-codes, clusterId: cluster-a }
    standby: { cluster: cluster-b, namespace: flink-jobs, deployment: flink-sql-job-...-codes, clusterId: cluster-b }
```

- 单集群双 namespace：`primary.cluster == standby.cluster`，仅 namespace 不同 → 免跨集群凭证。
- `clusterId` 即写入 fencing token 的值（对齐脚本）。
- 环境变量形态复杂（map/slice），跨集群/多组建议用 `-config config.yaml`。

## 4. 领域模型

```
SideRef      { Cluster, Namespace, Deployment, ClusterID }
HAGroup      { Name, S3Cluster, FencingKey, NeutralToken, Primary, Standby SideRef }
FencingState { Token string, PointsTo "primary"|"standby"|"neutral"|"unknown"|"unset" }
SideStatus   { flink.JobSummary + Reachable }      // 复用 summaryFrom
HAGroupView  { Group, Primary SideStatus, Standby SideStatus, Fencing FencingState,
               ActiveSide, SplitBrain bool, Warning string }
SwitchTask   { ID, Group, Direction(failover|failback), Status(running|succeeded|failed),
               Steps []StepState, RecoveryPoint {Path,Kind}, Error, StartedAt, FinishedAt }
StepState    { Name, Status(pending|running|done|failed), Message }
```

## 5. 后端组件（大量复用现有代码）

- **config**：加 `Clusters map[string]ClusterConfig` + `HAGroups []HAGroupConfig`；保持单集群兼容。
- **cluster.Registry**：`map[clusterName]ClusterAccessor`，启动时按 `clusters` 各建一个 `KubeAccessor`（已支持 kubeconfig / in-cluster）。`ClusterAccessor` 接口**不变**。
- **flink.Service（per side）**：List/Get/StatusText/Suspend/Resume/Savepoint(exec)/waitStoppedProgress **全部复用**，针对某 side 的 (accessor, namespace, deployment)。
- **store.FencingStore**（新，S3）：`ReadToken()`, `WriteToken(clusterId)`, `WriteNeutral()`；平台自己的 S3 client 直接写。恢复点列举复用现有 `store`。
- **failover.Service**（新）：
  - `GroupView(ctx, name)`：读两侧状态（复用 summaryFrom）+ token → 判定 ActiveSide / SplitBrain。
  - `Failover/Failback(name)`（P1b）：编排 `doSwitch` 五步，写 SwitchTask（复用 operation/进度框架，in-memory）。
- **脑裂判定**：两侧都 `RUNNING/STABLE`，或 token 指向侧与实际活跃侧不一致 → `SplitBrain=true` + `Warning`。

## 6. SwitchTask 状态机（P1b，复刻 do_switch）

```
PENDING → FENCE_NEUTRAL → PICK_RECOVERY_POINT → STOP_SOURCE(等 JM Pod 归零，报进度)
        → TOKEN_TO_TARGET → START_TARGET(initialSavepointPath+nonce) → VERIFY → DONE
                                          └──(任一步失败)──▶ FAILED（记录停在哪步）
```
- 每步写入 `Steps[]`，前端逐步展示；STOP_SOURCE 显示 Pod 数（复用 waitStoppedProgress）。
- 恢复点：源侧健康→ savepoint（复用 Savepoint）；否则→最新 checkpoint（复用 store 取最新）。
- in-memory：本期接受重启丢任务；视图可从集群实际状态 + token 重建。

## 7. API（P1a 只读 + P1b 切换）

```
GET  /api/ha-groups                        # 列表：两侧状态 + token + activeSide + splitBrain
GET  /api/ha-groups/:name                  # 详情（含两侧 pods/events）
GET  /api/ha-groups/:name/fencing          # 当前 token 状态
GET  /api/ha-groups/:name/recovery-points  # 该组 S3 恢复点（savepoint + checkpoint）
POST /api/ha-groups/:name/failover         # 启动 primary→standby，返回 SwitchTask   (P1b)
POST /api/ha-groups/:name/failback         # standby→primary                          (P1b)
GET  /api/switch-tasks/:id                 # 轮询进度（复用 operation 轮询模式）        (P1b)
```
鉴权沿用现有中间件；切换高危 → body 带确认 + 前端二次确认。

## 8. 前端

- **新页 `/ha`**：每个 HA 组一张卡 = 主/备两侧状态徽章 + 活跃侧高亮 + token 指向 + 脑裂红色横幅；header 加入口。
- **切换向导 Dialog**（P1b）：选方向 → 显示/选择恢复点（复用 rollback 选择器）→ 二次确认 → 五步进度（复用 `pollOperation`）。
- 复用现有 UI 组件（card/badge/dialog/table/toast/StatusBadge）。

## 9. Helm / RBAC

- 多 namespace 场景：chart 支持 `rbac.extraNamespaces` → 每个 namespace 建 Role/RoleBinding。
- 跨集群：新增 `clusterKubeconfigs`（Secret 挂到 `/etc/flinkui/kube/*.yaml`）+ `clusters`/`haGroups` 配置（经 `-config` 或 values 渲染的 config 文件）。对侧 kubeconfig 对应对侧最小权限 SA。

## 10. 分期与验收

**P1a（只读观测，零风险）**：config 扩展 → cluster.Registry → FencingStore 读 → failover.GroupView → 只读 API → `/ha` 页面。
验收：真实集群上（先用**单集群双 namespace** 模拟主从，免第二份 kubeconfig）看到两侧状态 + token；go vet/test、tsc/build、helm lint 全绿。

**P1b（手动切换）**：FencingStore 写（含中性）→ SwitchTask 状态机 + doSwitch → 切换 API → 前端向导。
验收：在单集群双 namespace 演练组上跑一次 failover→failback，验证零并行（中性 token + 等停稳）、恢复点、token 切换；清理。

**验证策略**：优先用单集群双 namespace 组（集群现成、无需第二份 kubeconfig、可安全演练）；真·跨集群作为配置项验证渲染。

## 11. 风险 / 备注

- 平台持有跨集群凭证的安全面（方案 A 已知取舍；用最小权限 SA 缓解）。
- SwitchTask in-memory：平台重启丢失半途任务（本期接受）。
- 切换高危：强制二次确认 + 全程 fencing 双保险，与脚本一致。
- 演进方向（记入路线图）：自动切换 + 平台 HA；peer/agent 模型（双侧各部署、平台间调用，免跨集群 kube 凭证）。
