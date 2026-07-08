# 去中心主从切换设计（Decentralized Failover）

> 面向「跨集群 k8s 互相连不通」的灾难/分区场景：两侧 flinkui 各自**只操作本地集群**，
> 通过**共享 S3**（fencing token + 交接记录）协调，运维在两边平台上各做一个本地半操作
> 完成切换。对应 MVP 设计 §8 P1 的演进（peer 模型）。

## 1. 为什么需要它

集中式 failover（一个平台持有两侧 kubeconfig、一个流程里既停源又起目标）在**对端 k8s
不可达**时必然做不完。去中心模式把一次切换拆成两个**本地半操作**，不依赖任何跨集群
k8s 调用：

- **Release（让位）**：源侧运维在**源侧 flinkui** 上执行——停本地作业、本地确认停稳、
  释放 token。
- **Promote（接管）**：目标侧运维在**目标侧 flinkui** 上执行——占用 token、从恢复点拉起
  本地作业。

## 2. 三条支柱

1. **本地只动自己**：每侧 flinkui 用本地 in-cluster SA，只操作本集群的 FlinkDeployment。
   天然分区容错。
2. **共享 S3 是唯一协调平面**：fencing token（谁可运行）+ 交接记录（handoff：epoch /
   恢复点 / 阶段 / 让位者）。前提：跨集群 k8s 断了，但两侧到共享对象存储仍可达。
3. **防"双跑"的硬保证是作业 Pod 的 fencing initContainer**：Pod 启动校验
   `token == 自身 clusterId`，不符拒启。**与平台连通性无关**。

## 3. 共享 S3 上的两个对象

- **Fencing token**（沿用现状，key 默认 `fencing/active-cluster`）：内容为当前活跃
  `clusterId`，或中性值 `__switching__`（两侧都被 fence）。
- **交接记录**（新，JSON，key 默认 `fencing/handoff/<group>`）：
  ```json
  {
    "group": "orders",
    "activeClusterId": "cluster-a",
    "epoch": 5,
    "phase": "stable|released|promoting",
    "recoveryPoint": { "path": "s3://...", "kind": "savepoint|checkpoint|none" },
    "releasedBy": "cluster-a",
    "updatedAt": "2026-07-07T09:00:00Z"
  }
  ```
  `epoch` 单调递增，用于防旧主 flapping 抢回、以及并发 Promote 竞态判定。

### 3.1 两者分工，以及为什么 `failover.sh` 用不到交接记录

| 对象 | 内容 | 作用 | 集中式 `failover.sh` |
|------|------|------|----------------------|
| **fencing token** | 一个 clusterId(或中性值) | 防脑裂的"谁能跑"——作业 Pod 的 fencing initContainer 也校验它 | ✅ 用到(唯一需要的) |
| **交接记录 handoff**(`handoff_key`) | epoch / phase / recoveryPoint / releasedBy | 去中心两侧之间的"交接留言" | ❌ 用不到 |

`failover.sh` 是**集中式、单进程**:一个脚本在一次运行里顺序完成"停源→选恢复点→起目标"。
所以恢复点路径是**内存变量**、"源侧已停"是**隐式**的、先后顺序**天然串行**——只需一个 fencing
token 防脑裂即可,别的都在进程内存里。

去中心模式把切换拆成**两个独立半操作**,跑在**互相连不通 k8s** 的两个 flinkui 上(A 做
Release、B 做 Promote),它俩**只能靠共享 S3 通信**。此时"进程内存里的东西"传不过去,bare
token(只是个 clusterId 字符串)也装不下,于是需要交接记录携带三样 token 装不下的信息:

1. **recoveryPoint(最关键)**:A 侧 savepoint 的 location 写入记录,B 侧读它做**零丢失**恢复;
   没有它 B 只能退回**最新 checkpoint**(丢失 ≤ checkpoint 间隔)。跨两侧传"零丢失恢复点"必须靠它。
2. **phase / releasedBy**:B 的**普通 Promote** 校验 `phase==released`(确认对端已干净让位)才接管;
   否则需 **force**(灾难,附数据丢失确认)。token 的中性值只能表达"切换中",表达不了"谁已让位"。
3. **epoch**:给多次 Promote 排序,防旧主 flapping 抢回 + 两侧并发 Promote 竞态(epoch 高者胜)。
   单进程脚本天然串行,不需要;两个独立执行者才需要这个排序令牌。

一句话:**token = "谁能跑"(两模式都用);handoff = 去中心两侧的"交接留言"(恢复点/是否已让位/epoch)**。
`handoff_key` 就是这条留言在共享 S3 上的对象键,默认 `fencing/handoff/<组名>`,一般不用改。

## 4. 两个本地半操作（状态机）

**Release（我在跑 → 让位），全在本地：**
```
SAVEPOINT(本地健康则触发, 写 handoff.recoveryPoint)
  → SUSPEND_LOCAL → WAIT_LOCAL_STOPPED(本地轮询 JM Pod 归零)
  → TOKEN_NEUTRAL(token=__switching__)
  → WRITE_HANDOFF(phase=released, releasedBy=me, epoch 不变)
```

**Promote（接管 → 我来跑），全在本地：**
```
READ_HANDOFF
  → PICK_RECOVERY_POINT(handoff.recoveryPoint 优先; 否则 S3 最新 checkpoint)
  → TOKEN_TO_SELF(token=myClusterId, epoch=handoff.epoch+1, phase=stable)
  → START_LOCAL(state=running + initialSavepointPath + savepointRedeployNonce)
  → VERIFY_LOCAL(本地轮询到 RUNNING/STABLE, 尽力而为)
```

计划切换：源侧先 Release → 目标侧再 Promote（看着同一份 S3 交接记录协作）。

## 5. 灾难 Promote 与硬限制（必须坦白）

若源侧真的挂了/彻底不可达：目标侧 **Promote(force)**，从**最新 checkpoint** 恢复，
运维必须**显式确认数据丢失**（`ackDataLoss`）。

**硬限制**：现有 fencing 是**启动期**校验，能挡住源侧之后重启的 Pod，但**杀不掉源侧此刻
仍在跑的 Pod**。当「源侧只是与目标/运维分区、其实还活着还在写」时，force Promote 会导致
**运行期双跑**。根治需把 fencing 升级为**运行期自我隔离**：给作业加 sidecar/watcher，
周期读 token，`token != 自身` 则自杀（软 STONITH）。本期不做该 sidecar，仅在文档标注为
强安全增强项；force Promote 以人工确认 + 单调 epoch 作为当前防线。

## 6. 竞态与一致性

- **handoff 写入用 S3 条件写（CAS）保证串行**：Promote 通过 `WriteHandoffCAS`
  （`If-Match: <etag>`，新建时 `If-None-Match: *`）作为**原子赢家**——两侧同时 Promote
  时只有一侧的条件写成功（epoch+1），另一侧收到 412（`ErrHandoffConflict`）后**中止**
  而非覆盖，其 Pod 也因 token 不符被 fence。Release 走带重试的 CAS 循环。后端不支持条件写
  时退化为 last-write-wins（不劣于改造前）。
- **⚠️ fencing token 的写入不在 handoff CAS 的原子范围内**：Promote 的顺序是
  「先 `WriteHandoffCAS`（原子占用交接记录）→ 再 `WriteToken`（无条件写 token=self）」。
  这**两步不是一个原子单元**。若进程在两步之间崩溃/超时，会留下一个中间态：**handoff 已是
  `self / epoch+1`，但 token 仍指向 peer 或 neutral**。
  - **安全性**：该中间态是 **fail-closed** 的——启动期 fencing 校验 `token == 自身 clusterId`，
    此时 token≠self，本地作业**会被拒启**，不会造成双跑。
  - **代价**：状态不一致且需人工介入；重跑 Promote 可恢复，但因 handoff 已被推进，新一轮会
    再 epoch+1，导致 **epoch 无谓自增**（不影响正确性，只是计数虚高）。
  - **观测**：`deriveRole` / `LocalView` 会把「handoff=self 但 token≠self」识别为本地不一致
    并给出告警，提示运维重试 Promote（或后续用 token 的条件写把两步收敛为幂等序列）。
- **共享 S3 必须两侧可达且强一致**（现代 S3/MinIO 的 Put 强一致）。S3 也不可达时不属于
  本模式能解决的范畴（纯人工）。

## 7. 配置（每侧实例声明自己的本地侧）

```yaml
cluster:            # 本地集群（in-cluster SA），其 s3 即共享存储
  name: cluster-a
  namespace: flink-jobs
  kubeconfig: ""
  s3: { endpoint: "https://minio...:9000", bucket: flink, access_key: ..., secret_key: ..., path_style: true, insecure: true }

ha:
  groups:
    - name: orders
      namespace: flink-jobs                # 本地 namespace
      deployment: flink-sql-job-orders      # 本地 deployment
      cluster_id: cluster-a                 # 我这侧的 clusterId（token 写这个值）
      peer_cluster_id: cluster-b            # 对端（仅展示 + 交接语义）
      fencing_key: fencing/active-cluster   # 可选，默认
      neutral_token: __switching__          # 可选，默认
      handoff_key: fencing/handoff/orders   # 可选，默认 fencing/handoff/<group>
```
对端那侧的 flinkui 用镜像对称的配置（cluster_id=cluster-b，local 指向它自己的 deployment）。

## 8. API（本地）

```
GET  /api/ha                     # 本地视角的所有组
GET  /api/ha/:name               # 单组本地视角
POST /api/ha/:name/release       # {confirm:true} 让位（本地）
POST /api/ha/:name/promote       # {confirm:true, force?:bool, ackDataLoss?:bool} 接管（本地）
GET  /api/ha-tasks/:id           # 轮询 release/promote 进度
```

LocalView：本地作业状态（复用 flink.Get）+ 共享 token（pointsTo self|peer|neutral|unset|
unknown）+ 交接记录 + 角色（active|standby|neutral|unknown）+ 本地不一致告警
（如 token=self 但本地没在跑 / token=peer 但本地在跑应停）。**对端状态标注为"未观测
（跨集群）"**。

## 9. 前端 `/ha`

每组一张卡：**本地侧**状态（StatusBadge）+ 共享 token 指向 + 交接记录（epoch/phase/
recoveryPoint）+ **对端(未观测)** 占位；按角色给 **Release**（本地在跑时）/ **Promote**
（force + 数据丢失确认）按钮 + 五步进度（复用 async task 轮询）。

## 10. 复用与新增

- **复用**（节选自集中式分支）：`store` 的 S3 client 构造、fencing token 读写思路、
  fencing/status 分类逻辑、async task/进度模式、`/ha` 页面骨架与 UI 组件。
- **新增**：交接记录（handoff）读写 + epoch、去中心 `LocalView`、`Release`/`Promote`
  本地状态机、本地 API、去中心 `/ha` 交互。
- **不做（本期）**：集中式 `do_switch`（连不上对端时用不了）、运行期自我隔离 sidecar、
  自动故障检测/自动切换。

## 11. 风险 / 备注

- 启动期 fencing 不杀在跑 Pod → force Promote 有运行期双跑风险（人工确认 + epoch 缓解；
  运行期 sidecar 为根治项）。
- Promote 的 handoff CAS 与 token 写入非原子（两步之间崩溃 → handoff=self 但 token≠self）：
  fail-closed（本地会被 fence 拒启，无双跑），可重跑恢复，代价是 epoch 虚增；详见 §6。
- 交接任务 in-memory（重启丢半途；视图可从 S3 token + handoff + 本地状态重建）。
- 共享 S3 是单点协调平面，必须两侧可达且强一致。
