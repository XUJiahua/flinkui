# Flink Job Console — 操作手册（Operations Manual）

> 适用版本：appVersion 0.1.3。本手册覆盖部署、配置、日常运维、恢复点回滚、
> 以及**去中心主从切换（failover）**的完整操作流程与故障处置。

---

## 目录

1. [平台概述](#1-平台概述)
2. [部署](#2-部署)
3. [配置参考](#3-配置参考)
4. [登录与访问](#4-登录与访问)
5. [日常运维（单作业）](#5-日常运维单作业)
6. [恢复点与回滚](#6-恢复点与回滚)
7. [去中心主从切换（failover）](#7-去中心主从切换failover)
8. [健康检查与可观测性](#8-健康检查与可观测性)
9. [故障排查](#9-故障排查)
10. [API 速查](#10-api-速查)
11. [安全与权限](#11-安全与权限)

---

## 1. 平台概述

Flink Job Console 是一个**单二进制**平台（前端静态导出 embed 进 Go 二进制），
对运行在 **Flink Kubernetes Operator**（`FlinkDeployment` CRD）上的**已部署**作业做
生命周期管理。直连 K8s API，无需经过脚本。

能力：
- 集中状态大盘（多作业，WebSocket 实时）+ 批量操作 + 作业详情（Pod/事件）。
- 一键生命周期：suspend / resume / restart / savepoint / rollback（savepoint、restart 异步带进度）。
- JM/TM 日志查看（多 TM 时可选单个 Pod）、Flink Web UI 深链。
- 回滚恢复点选择器（S3/MinIO 列举 savepoint/checkpoint）。
- **去中心主从切换**：连不上对端集群时，两侧各自本地 Release/Promote，靠共享 S3 fencing 协调。

两种部署形态：**集群内**（ServiceAccount，推荐）/ **集群外**（kubeconfig）。

---

## 2. 部署

### 2.1 Helm（推荐，集群内）

```bash
# 1) 鉴权 Secret（也可用 auth.existingSecret）
kubectl -n flink-jobs create secret generic flink-console-auth \
  --from-literal=username=admin \
  --from-literal=password='强密码' \
  --from-literal=session-secret="$(openssl rand -hex 16)"

# 2) 安装（RBAC/Deployment/Service 一并创建）
helm upgrade --install flinkui deploy/helm/flinkui \
  -n flink-jobs --create-namespace \
  --set image.tag=0.1.3 \
  --set config.clusterName=prod \
  --set config.targetNamespace=flink-jobs \
  --set auth.existingSecret=flink-console-auth \
  --set s3.enabled=true \
  --set s3.endpoint='https://minio.flink-operator:9000' \
  --set s3.insecure=true \
  --set s3.accessKey=minioadmin --set s3.secretKey=minioadmin

# 3) 访问
kubectl -n flink-jobs port-forward svc/flinkui 8080:80   # http://localhost:8080
```

完整可选项见 `deploy/helm/flinkui/values.yaml`；示例见
`deploy/helm/flinkui/values-example.yaml` 与 `values-example-full.yaml`。

> **命名空间解耦**：`config.targetNamespace` 是存放 FlinkDeployment 的 namespace，
> RBAC（Role/RoleBinding）建在这里，可与 release namespace 不同。
> **安全上下文**：distroless nonroot 需要数值 UID，chart 默认已设 `runAsUser: 65532`。

### 2.2 集群外（本地/运维机）

```bash
export FKO_ADDR=":8080"
export FKO_CLUSTER_NAME="dev" FKO_CLUSTER_NAMESPACE="flink-jobs"
export FKO_CLUSTER_KUBECONFIG="$HOME/.kube/config"   # 空 => 集群内
export FKO_AUTH_USERNAME="admin" FKO_AUTH_PASSWORD="强密码"
export FKO_AUTH_SESSION_SECRET="$(openssl rand -hex 16)"
# 可选 S3（回滚选择器）
export FKO_CLUSTER_S3_ENDPOINT="https://minio:9000" FKO_CLUSTER_S3_BUCKET="flink"
export FKO_CLUSTER_S3_ACCESS_KEY="..." FKO_CLUSTER_S3_SECRET_KEY="..."
export FKO_CLUSTER_S3_PATH_STYLE="true" FKO_CLUSTER_S3_INSECURE="true"
./bin/flinkui
```

多集群/HA 组等复杂配置用 `-config config.yaml`（map/slice 不适合环境变量）。

### 2.3 构建

```bash
make build      # 前端导出 -> embed -> Go 二进制 => ./bin/flinkui
make docker IMAGE=your-registry/flinkui:0.1.3
```

---

## 3. 配置参考

所有设置读 `FKO_*` 环境变量（嵌套键用 `_`），或 `-config path.yaml`。

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `FKO_ADDR` | `:8080` | 监听地址 |
| `FKO_DEPLOYMENT_PREFIX` | `flink-sql-job-` | FlinkDeployment 名 = 前缀 + 作业名（UI 显示短名） |
| `FKO_SAVEPOINT_TIMEOUT_SEC` | `120` | savepoint 轮询超时 |
| `FKO_STOP_TIMEOUT_SEC` | `120` | restart / 切换等 JM Pod 归零超时 |
| `FKO_LOG_TAIL_LINES` | `200` | 日志默认 tail 行数 |
| `FKO_STATUS_POLL_SEC` | `5` | WebSocket 状态推送间隔 |
| `FKO_CLUSTER_NAME` | `default` | 集群显示名 |
| `FKO_CLUSTER_NAMESPACE` | `flink-operator` | 存放 FlinkDeployment 的 namespace |
| `FKO_CLUSTER_KUBECONFIG` | _(空)_ | kubeconfig 路径；空 => 集群内 SA |
| `FKO_CLUSTER_CONTEXT` | _(空)_ | kubeconfig context |
| `FKO_CLUSTER_S3_ENDPOINT` | _(空)_ | S3/MinIO **API** 端点（非控制台端口） |
| `FKO_CLUSTER_S3_BUCKET` | _(空)_ | bucket |
| `FKO_CLUSTER_S3_ACCESS_KEY` / `_SECRET_KEY` | _(空)_ | S3 凭证 |
| `FKO_CLUSTER_S3_PATH_STYLE` | `true` | path-style（MinIO） |
| `FKO_CLUSTER_S3_INSECURE` | `false` | 跳过 TLS 校验（自签证书） |
| `FKO_AUTH_USERNAME` | `admin` | 登录用户名 |
| `FKO_AUTH_PASSWORD` | _(空)_ | **必设**登录密码 |
| `FKO_AUTH_SESSION_SECRET` | `change-me-please` | **必设**Cookie 签名密钥 |

去中心 HA 组配置（仅 `-config`，见 §7 与 `deploy/examples/failover-decentralized.example.yaml`）。

---

## 4. 登录与访问

1. 浏览器打开平台地址，用 `FKO_AUTH_USERNAME` / 密码登录。
2. 会话为 HMAC 签名 Cookie（`SameSite=Lax; HttpOnly`，有效期 12h）。
3. 顶部导航：**Jobs**（状态大盘）/ **HA**（主从切换）。

---

## 5. 日常运维（单作业）

### 5.1 状态大盘（Jobs）

- 表格列出当前 namespace 全部 FlinkDeployment：作业名、状态（`jobState/lifecycleState`）、
  期望状态、升级模式、并行度、Job ID。健康 = `RUNNING/STABLE`（绿）。
- WebSocket 实时刷新；断线自动退化为 5s 轮询。
- 支持多选做**批量操作**。

### 5.2 作业详情

点作业名进入：Pod 列表（JM/TM，就绪/重启/节点/存活时长）、K8s 事件、JobManager 日志、
Flink Web UI 深链。多 TaskManager 时可选择查看单个 Pod 的日志。

### 5.3 生命周期操作

| 操作 | 效果 | 备注 |
|---|---|---|
| **Suspend** | `spec.job.state=suspended` | 保留状态，释放算力；即时 |
| **Resume** | `spec.job.state=running` | 恢复运行；即时 |
| **Restart** | suspend→等 JM Pod 归零→resume（last-state） | **异步**，二次确认，进度显示"等待 JM Pod 归零" |
| **Savepoint** | JM REST 触发，轮询到 COMPLETED，返回 location | **异步**，进度/完成/超时 |
| **Rollback** | `state=running`+`initialSavepointPath`+`savepointRedeployNonce` | **高危**，见 §6 |

异步操作触发后返回操作 ID，UI 轮询 `GET /api/operations/:id` 展示进度。

---

## 6. 恢复点与回滚

1. 作业详情或大盘上点 **Rollback**。
2. 选择器从 S3 列举该作业的恢复点（`savepoints/…` 与 `checkpoints/…/chk-N/_metadata`），
   恢复点路径优先取作业自身 `state.savepoints.dir` / `state.checkpoints.dir`，兼容任意 bucket/前缀布局。
3. 选择一个 savepoint/checkpoint 或手填路径 → **二次确认** → 强制从该点重部署。

> 前提：配置了 S3（`s3.enabled` / `FKO_CLUSTER_S3_*`），且指向 MinIO 的 **S3 API 端口（通常 9000）**，不是 Web 控制台端口。

---

## 7. 去中心主从切换（failover）

面向**跨集群 k8s 互相连不通**的场景：两侧各部署一个 flinkui，**各自只操作本地集群**，
通过**共享 S3**（fencing token + 交接记录）协调。防"双跑"的硬保证是作业 Pod 的
fencing initContainer（启动期校验 token==自身 clusterId）。设计详见
`docs/failover-decentralized-design.md`。

### 7.1 配置（每侧镜像对称）

`-config`（示例 `deploy/examples/failover-decentralized.example.yaml`）：

```yaml
cluster:
  name: cluster-a
  namespace: flink-jobs
  kubeconfig: ""                 # 集群内 SA
  s3: { endpoint: "https://minio...:9000", bucket: flink, access_key: ..., secret_key: ..., path_style: true, insecure: true }  # 共享存储
ha:
  groups:
    - name: orders
      namespace: flink-jobs
      deployment: flink-sql-job-orders
      cluster_id: cluster-a       # 我这侧（token 值）
      peer_cluster_id: cluster-b  # 对端（仅展示/交接语义）
```
对端 flinkui 用镜像配置（`cluster_id: cluster-b`，local 指向它自己的 deployment）。

### 7.2 HA 页面（本地视角）

每组一张卡：本地侧状态、角色（active/standby/neutral/unknown）、共享 fencing token 指向、
交接记录（epoch/phase/recoveryPoint）、**对端标注"未观测（跨集群）"**、以及本地不一致告警
（如 token 指向对端但本地在跑 = 脑裂风险）。

### 7.3 计划切换（两侧都活，只是跨集群网断）——A → B

1. **在 A 侧 flinkui** 点某组的 **Release（让位）** → 勾选确认 → 执行：
   savepoint（本地健康）→ suspend 本地 → 等本地 JM Pod 归零 → token 置中性 → 写交接记录 `released`。
2. **在 B 侧 flinkui** 点同一组的 **Promote（接管）** → 勾选确认 → 执行：
   读交接记录（校验已 released）→ 选恢复点 → token 指向 B（epoch+1）→ 从恢复点启动本地 → 校验 RUNNING/STABLE。

两侧看的是同一份 S3 交接记录，据此协作。

### 7.4 灾难切换（A 已死/彻底不可达）——B 强制接管

1. 先尽力用其它手段确认 A 确实不可用。
2. **在 B 侧 flinkui** 点 **Promote** → 勾选 **Force（对端未 released）** → 勾选
   **数据丢失确认（ackDataLoss）** → 执行：从最新 checkpoint 恢复（拿不到 A 的 savepoint），
   token 指向 B（epoch+1），启动本地。

> ⚠️ **硬限制**：fencing 是**启动期**校验，能挡住 A 之后重启的 Pod，但**杀不掉 A 此刻仍在跑的 Pod**。
> 若 A 只是与 B/运维分区、其实还活着，force Promote 会导致**运行期双跑**。
> 因此 force 必须人工确认；根治需要给作业加"运行期自我隔离 sidecar"（路线图项）。

### 7.5 前提与注意

- **共享 S3 必须两侧可达且强一致**（它是唯一协调平面）。S3 也不可达时只能纯人工。
- 交接任务为 in-memory，平台重启会丢半途任务状态，但视图可从 S3 token+交接记录+本地状态重建。
- 单集群双 namespace（无分区）场景可直接用常规运维/回滚，不需要去中心切换。

---

## 8. 健康检查与可观测性

- `GET /healthz`（liveness）、`GET /readyz`（readiness）——公开、无需鉴权；Helm 探针已指向。
- WebSocket `GET /api/ws/status` 实时推送大盘。
- 日志：作业详情内查看 JM/TM 日志（tail 行数 + 关键字过滤 + 单 Pod 选择）。

---

## 9. 故障排查

| 现象 | 排查 |
|---|---|
| 作业显示 `UNREACHABLE` | 集群/对象不可达；检查 kubeconfig / 集群内 SA RBAC / namespace |
| 作业卡 `RECONCILING` | 看作业详情的 K8s 事件与 JM 日志 |
| Savepoint 失败 | 作业需 `RUNNING/STABLE`；savepoint 目标目录取自作业 `state.savepoints.dir`；看 JM 日志 |
| 恢复点列表为空/报错 | S3 端点须为 **API 端口(9000)** 非控制台；自签证书设 `insecure=true`；bucket 正确 |
| S3 报 `must be made to API port` | 用了 MinIO 控制台端口，改用 9000 |
| S3 报 400 `Bad Request`（http） | API 端口其实是 HTTPS，用 `https://` |
| HA 页 `no HA groups configured` | 未声明 `ha.groups`（需 `-config`） |
| HA `token unset while local job runs` | fencing token 未初始化；正常情况应由切换流程写入 |
| HA 脑裂告警（两侧都 RUNNING） | 立即介入：确认唯一活跃侧，停掉多余一侧，修正 token |

---

## 10. API 速查

鉴权：先 `POST /api/login {username,password}` 拿 Cookie；其余 `/api/*` 需带 Cookie。

**作业**
```
GET  /api/cluster
GET  /api/jobs
GET  /api/jobs/:name
GET  /api/jobs/:name/logs?tail=200[&component=jobmanager|taskmanager][&pod=<name>]
GET  /api/jobs/:name/recovery-points
GET  /api/jobs/:name/flink-ui         ANY /api/jobs/:name/ui/*path   （Web UI 反代）
POST /api/jobs/:name/suspend | resume | restart | savepoint
POST /api/jobs/:name/rollback         {path}
GET  /api/operations/:id              （异步 savepoint/restart 进度）
```

**去中心 HA**
```
GET  /api/ha
GET  /api/ha/:name
POST /api/ha/:name/release            {confirm:true}
POST /api/ha/:name/promote            {confirm:true, force?:bool, ackDataLoss?:bool}
GET  /api/ha-tasks/:id
```

**健康**：`GET /healthz`、`GET /readyz`（公开）。

---

## 11. 安全与权限

- 平台可变更集群作业，**必须**设置 `FKO_AUTH_PASSWORD` 与强 `FKO_AUTH_SESSION_SECRET`，不得裸奔。
- 高危操作（restart / rollback / release / promote）均需**二次确认**；force promote 额外需**数据丢失确认**。
- 最小 RBAC（Helm 自动创建）：`flinkdeployments` get/list/watch/patch、`pods`/`pods/log`、
  `pods/exec`、`pods/portforward`、`events`、`services` get/list——**全部 namespace 级，无集群级权限**。
- 跨集群（真·多集群）时对端 kubeconfig 应对应对端**最小权限 SA**，勿用 admin。
- S3 凭证经 Secret 注入，勿硬编码。

---

> 相关文档：`README.md`（架构/构建）、`docs/failover-decentralized-design.md`（去中心切换设计）、
> `docs/local-in-cluster.md`（本地模拟集群内）、`deploy/helm/flinkui/`（Helm chart 与示例）。
