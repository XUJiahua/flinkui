# 控制台自身敏感配置从 OpenBao/Vault 获取（Console Secrets From Vault）

> 让 flinkui **控制台自身**的敏感配置（会话签名密钥、本地登录口令、LDAP bind 口令、
> S3 凭据）可在启动时直接从 OpenBao/Vault 读取,作为 env / `existingSecret` / `--set`
> 之外的**第三种来源**。复用 flinkui 已内置的 Vault 客户端(Kubernetes auth + KV v2),
> 不引入 ESO 或 sidecar。区别于 [secret-sync](../README.md)(它同步的是**作业用** Secret)。

## 1. 背景与目标

`deploy/values-*.yaml` 里目前有若干控制台自身的敏感字段,通常以明文/`--set`/预建 Secret
提供:

| 字段 | 用途 | 现注入方式 |
|------|------|-----------|
| `auth.sessionSecret` | 会话 cookie 的 HMAC 签名密钥 | K8s Secret → `FKO_AUTH_SESSION_SECRET` |
| `auth.password` | 本地登录口令(break-glass) | K8s Secret → `FKO_AUTH_PASSWORD` |
| `auth.ldap.bindPassword` | LDAP 搜索绑定服务账号口令 | K8s Secret → `FKO_AUTH_LDAP_BIND_PASSWORD` |
| `cluster.s3.accessKey` / `secretKey` | 恢复点浏览用的 S3/MinIO 凭据 | K8s Secret → `FKO_CLUSTER_S3_ACCESS_KEY/SECRET_KEY` |

**目标**:新增一种来源——每个字段可声明一个 Vault「引用」(`_ref`),启动时由 flinkui
自己从 Vault 解析,渲染产物与 values 里**不再出现明文**。

**非目标**:
- 不引入 External Secrets Operator / Vault Agent Injector / CSI 驱动(保持本项目「无 ESO、
  app 原生 Vault」的取向;它们作为运维替代方案在 §7 列出)。
- 运行期热轮换分阶段:P1-P3 先做启动期解析(读一次);P4 加**热更新循环**在不重启的
  情况下就地刷新(见 §6)。
- 不改变 secret-sync 的职责(同步「作业用」Secret);本特性是**控制台自身**配置的来源。

## 2. 为什么不能复用 secret-sync(鸡生蛋)

secret-sync 是**启动后**运行的循环,把 Vault 值同步成**作业用**的 K8s Secret。而
`sessionSecret` 等是 flinkui 在**构造 `auth.Auth` / `store.Store`(启动即需)**时就要的。
若让 secret-sync 先同步出「控制台自己的」Secret、再用 `existingSecret` 指向它,就成了
「flinkui 必须先启动才能产出自身启动所需的 Secret」——死循环。

因此控制台自身 secret 的 Vault 化必须走**启动期解析**(在建 Auth/Store 之前完成),
而不是复用同步循环。

## 3. 复用现成能力

`internal/secretsync/openbao.go` 已有一个纯 stdlib 的 `baoClient`:

- `login()`:静态 token,或读 Pod SA token 走 Kubernetes auth(`/v1/auth/<mount>/login`);
- `readKV(token, path)`:读 KV v2 `<mount>/data/<path>` 返回 `map[string]string`;
- TLS:支持 CACert / https 自签跳过校验。

本设计把它**抽取到共享包 `internal/openbao`**,secret-sync 与启动期解析共用同一份契约与
连接配置(`config.OpenBaoConfig`),避免重复实现与漂移。

## 4. 配置模型

### 4.1 顶层 Vault 连接块

新增顶层 `vault`(与 `secret_sync.openbao` 同契约,可分开配置或将来共享):

```yaml
vault:
  addr: "http://vault.vault.svc.cluster.local:8200"
  kv_mount: "kv"
  auth_mount: "kubernetes"
  role: "flinkui-role"          # 绑定本 Pod SA 的 Vault role
  # namespace / token / cacert 同 OpenBaoConfig
```

对应 `FKO_VAULT_*` 环境变量(标量,可用 env 表达)。

### 4.2 每字段引用（`_ref`）

给每个敏感字段加一个可选的 `{path, key}` 引用。`SecretRef` 结构:

```go
type SecretRef struct {
    Path string `mapstructure:"path"` // KV v2 路径(mount 下)
    Key  string `mapstructure:"key"`  // 该路径下的字段名
}
```

挂载到既有配置结构:

```go
type AuthConfig struct {
    // ... 现有字段 ...
    PasswordRef      SecretRef `mapstructure:"password_ref"`
    SessionSecretRef SecretRef `mapstructure:"session_secret_ref"`
    LDAP LDAPConfig
}
type LDAPConfig struct {
    // ...
    BindPasswordRef SecretRef `mapstructure:"bind_password_ref"`
}
type S3Config struct {
    // ...
    AccessKeyRef SecretRef `mapstructure:"access_key_ref"`
    SecretKeyRef SecretRef `mapstructure:"secret_key_ref"`
}
```

配置示例(values 层由 chart 渲染进 `-config` 文件;`_ref` 是对象,同 secret_sync.items,
只能来自配置文件):

```yaml
auth:
  session_secret_ref: { path: config/cil/flink/console/auth, key: session-secret }
  ldap:
    bind_password_ref: { path: config/cil/flink/console/ldap, key: bind-password }
cluster:
  s3:
    access_key_ref: { path: config/cil/flink/console/s3, key: access-key }
    secret_key_ref: { path: config/cil/flink/console/s3, key: secret-key }
```

## 5. 解析流程与来源优先级

### 5.1 启动期解析

在 `config.Load` 之后、`main.go` 构造 `auth.New` / `store.New` **之前**,插入一个
`resolveVaultRefs(cfg)` 步骤:

```
Load(config) → resolveVaultRefs(cfg) → auth.New(cfg.Auth) / store.New(cfg.Cluster.S3) / ...
```

`resolveVaultRefs`:
1. 若没有任何 `_ref` 被设置 → 直接返回(零成本,现有部署不受影响)。
2. 否则用 `internal/openbao` 建客户端、`login()` 一次拿 token;
3. 对每个设置了 `_ref` **且字面量为空**的字段:`readKV(path)[key]` 填入 cfg;
4. 缓存同一 `path` 的读取结果(一个路径多 key 只读一次)。

### 5.2 来源优先级(明确定义,避免歧义)

对每个字段:

```
显式字面量 / env(非空) > vault _ref(设置了) > existingSecret（Helm 层）
```

即:字面量/env 一旦非空就**优先**(便于 break-glass 用 `--set` 覆盖);字面量为空且设了
`_ref` 才走 Vault。`existingSecret` 是 Helm 层机制(直接把 Secret 挂成 env),与 `_ref`
在部署时二选一(见 §8)。

### 5.3 失败处理

- Vault 不可达 / role 无权限 / path 不存在:**fail-fast**,启动即报清晰错误
  (`resolve vault ref auth.session_secret_ref (config/.../auth#session-secret): <err>`)。
  安全默认:宁可起不来,也不静默用空值(空 sessionSecret 会削弱 cookie 签名)。
- 提供可选 `vault.optional: true`(或 per-ref)在解析失败时回退到字面量/env,用于
  「Vault 抖动时靠预置 env 兜底」的部署;默认 `false`(严格)。

## 6. 运行期热轮换（Runtime rotation）

目标:Vault 里改了值,**不重启 Pod** 也能生效。做法是一个**周期性热更新循环**(复用同一个
Vault 客户端与连接;interval 同 `secret_sync` 风格,可配),定期重读所有 `_ref`,把新值放进
进程内**可原子替换的持有者**;各组件在**使用时**读取当前值:

| 字段 | 热更新方式 | 生效时机 |
|------|-----------|---------|
| `auth.password`(本地) | 登录比对时读持有者 | 立即(下次登录) |
| `auth.ldap.bindPassword` | 每次登录 bind 时读持有者 | 立即(下次登录) |
| `s3.accessKey/secretKey` | store 用可刷新 credentials provider,或原子替换 store 指针 | 立即(下次 S3 调用) |
| `auth.sessionSecret` | 原子替换签名密钥(**单密钥,不做 keyring**) | 立即;**已签发的会话 cookie 失效,用户需重新登录** |

关于 `sessionSecret`:它是低频轮换项,且强制重新登录的代价可接受,因此**不引入多密钥
keyring 优雅轮换**——直接单密钥就地替换,轮换时在线会话失效。文档提示即可。

分两级实现:
- **read-once(P1-P3)**:启动时解析一次(§5);值变更需重启才生效。
- **热更新(P4)**:上面的循环 + 原子持有者;`auth.Auth` / `store.Store` 增加
  `UpdateSecrets(...)` 类接口,由循环在检测到值变化时调用。read-once 仍是回退路径。

手动触发(WebUI/admin,均需已认证 + 审计):
- **「Reload secrets from Vault」**:立即重读一次(不等 interval),就地热换所有字段;
- **「Restart console」(可选逃生舱)**:受控 `exit(0)`,由 Deployment 重建 Pod 做全量干净
  重载。作为兜底(如热更新路径出问题时),带二次确认;单副本会有数秒不可用,建议
  `replicas≥2` + 滚动。**不是**日常轮换的主力——日常靠热更新循环无感完成。

## 7. 运维替代方案（不改代码,作对比)

| 方案 | 机制 | 取舍 |
|------|------|------|
| **本设计(A)** | app 启动期读 Vault | 无新组件,贴合 no-ESO;启动依赖 Vault |
| Vault Agent Injector | sidecar 渲染 secret 到文件,`-config` 读 | 无 app 改动;+sidecar,+模板 |
| Secrets Store CSI(Vault) | 卷挂载 / 同步成 Secret,走 env/existingSecret | 无 app 改动;+CSI 驱动 |
| ESO ExternalSecret | 拉 Vault→K8s Secret,走 existingSecret | 无 app 改动;项目刻意避开 ESO |

选 A 的理由:零新增集群组件、与既有 secret-sync 共用同一 Vault 契约与 role,和本项目
「in-process、无 ESO」的一贯取向一致。

## 8. 安全 / RBAC

- **Vault policy 隔离**:控制台自身 KV 路径(如 `config/.../console/*`)应与「作业用」路径
  分开;给 Pod SA 绑定的 Vault role 单独授权 read。
- **最小面**:`_ref` 只读取所需 key;渲染产物与 values **不含明文**。
- **break-glass 不丢**:env / `--set` / `existingSecret` 全部保留,Vault 故障时可临时用
  `--set auth.sessionSecret=... --set auth.password=...` 拉起(优先级见 §5.2)。
- **审计**:解析步只记录字段名与 path/key(不记值);失败日志不含 secret。

## 9. Helm 变更

- `values.yaml` 新增 `vault:` 连接块 + 各字段 `_ref`(默认空);
- `configmap.yaml`:当任一 `_ref` 或 `vault.addr` 设置时,渲染 `vault:` 与 `*_ref` 进
  `/etc/flinkui/config.yaml`(与 secret_sync 同一 config 文件);
- `secret-auth.yaml` / `secret-s3.yaml`:当某字段改用 `_ref` 时,**不再要求**该字段的明文
  (放宽现有 `required`);仍支持三源并存;
- 文档:一份 `values-*-vault.yaml` 示例(全 `_ref`,零明文)。

## 10. 分阶段

1. **P1**:抽 `internal/openbao` 共享客户端(纯重构,secret-sync 切过去,行为不变 + 测试)。
2. **P2**:config 加 `vault` 块 + `SecretRef` 字段 + `resolveVaultRefs`(带优先级与 fail-fast);
   `main.go` 接线;单测(httptest 假 Vault:解析成功、字段优先级、失败 fail-fast、optional 回退)。
3. **P3**:Helm 渲染 `_ref` + policy 示例 + 文档(含 sessionSecret 轮换与启动依赖警告);
   一个字段(建议 `s3` 或 `ldap.bindPassword`)端到端验证后铺开到全部字段。
4. **P4**:热更新循环 + 原子持有者 + `UpdateSecrets` 接口 + 手动「Reload from Vault」动作
   (可选「Restart console」逃生舱)。sessionSecret 单密钥就地替换(不做 keyring)。

## 11. 风险与取舍

- **启动期 Vault 强依赖**:默认 fail-fast → Vault 挂则 console 挂;用 `optional`+env 兜底缓解。
- **热更新(P4)前 read-once**:P4 之前值轮换要重启;`sessionSecret` 轮换(无论热换还是
  重启)都会使在线会话失效、需重新登录——已判定可接受,不做 keyring。
- **契约漂移**:抽共享包后 secret-sync 与本特性共用一套,降低漂移;role/policy 需运维正确授权。
- **范围克制**:不碰热轮换、不引入外部组件;三源并存不破坏现有部署。
