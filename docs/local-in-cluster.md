# 在本地开发环境模拟 in-cluster 使用场景

“In-cluster”（把控制台作为 Pod 运行）与本地直连相比，本质差异有两点：

1. **认证路径**：`rest.InClusterConfig()` 读取挂载的 ServiceAccount token
   （`/var/run/secrets/kubernetes.io/serviceaccount/{token,ca.crt}`）和
   `KUBERNETES_SERVICE_HOST/PORT`，而不是 kubeconfig。
2. **身份与权限（RBAC）**：使用绑定了最小 Role 的 ServiceAccount，而不是你本地
   kubeconfig 里的管理员身份。

据此有两种模拟方式，从“够用且快”到“最真实”。

---

## 方式 A（推荐日常用）：以 ServiceAccount 身份在本地运行

用 ServiceAccount 的 **身份 + 最小 RBAC** 运行本地二进制。代码仍走
out-of-cluster 的 kubeconfig 路径，但请求以 ServiceAccount token 发出，因此能
**验证 Helm chart 授予的最小 RBAC 是否恰好够用**（这是 in-cluster 最容易踩坑的地方）。

一条命令搞定（脚本会幂等创建 SA + Role + RoleBinding，签发 token，并生成一个以
该 SA 认证的 kubeconfig）：

```bash
ADMIN_KUBECONFIG=/path/to/admin.kubeconfig \
  scripts/dev-as-serviceaccount.sh flink-jobs
```

按脚本输出运行控制台（关键是 `FKO_CLUSTER_KUBECONFIG` 指向生成的 `.sa.kubeconfig`）：

```bash
export FKO_CLUSTER_KUBECONFIG="$PWD/.sa.kubeconfig"
export FKO_CLUSTER_NAME="in-cluster-sim"
export FKO_CLUSTER_NAMESPACE="flink-jobs"
export FKO_AUTH_PASSWORD="change-me"
export FKO_AUTH_SESSION_SECRET="$(openssl rand -hex 16)"
./bin/flinkui        # http://localhost:8080
```

验证授予的权限（应为 yes / yes / yes，越权项为 no）：

```bash
kubectl --kubeconfig .sa.kubeconfig auth can-i list  flinkdeployments      -n flink-jobs   # yes
kubectl --kubeconfig .sa.kubeconfig auth can-i patch flinkdeployments      -n flink-jobs   # yes
kubectl --kubeconfig .sa.kubeconfig auth can-i create pods --subresource=exec -n flink-jobs # yes (savepoint 需要)
kubectl --kubeconfig .sa.kubeconfig auth can-i delete flinkdeployments      -n flink-jobs   # no  (超出最小权限)
kubectl --kubeconfig .sa.kubeconfig auth can-i get   secrets               -n flink-jobs   # no
```

用完清理：

```bash
kubectl -n flink-jobs delete serviceaccount,role,rolebinding flink-console
rm -f .sa.kubeconfig
```

> 说明：MinIO/S3 是通过网络访问的，不占用 K8s RBAC；因此 SA 只需目标 namespace 内
> 的权限即可完成 savepoint/rollback 的恢复点列举。S3 的 endpoint 用集群内 Service
> 地址（如 `https://minio.flink-operator:9000`）时，本地需要 `kubectl port-forward`
> 或 NodePort/Ingress 才能连通（见 README 的 S3 说明）。

### 为什么不直接“伪造” InClusterConfig？

`client-go` 的 `rest.InClusterConfig()` 把 token/CA 路径写死为
`/var/run/secrets/kubernetes.io/serviceaccount/…`，且强制要求
`KUBERNETES_SERVICE_HOST/PORT` 环境变量。在 macOS 主机上很难无副作用地伪造这些
固定路径。方式 A 用 kubeconfig 承载同一个 SA token，等价地覆盖了“身份 + RBAC”这一
最重要的差异，成本低得多。若确实要跑 `InClusterConfig()` 这段代码本身，见方式 B。

---

## 方式 B（最真实）：作为 Pod 真正跑在集群里

这会真正走 `rest.InClusterConfig()` 代码路径，用挂载的 SA token 认证，并直连
JobManager REST / MinIO 的 ClusterIP Service。

用本地的 k3s/kind/minikube 迭代：

```bash
# 1) 构建镜像
make docker IMAGE=flinkui:dev

# 2) 载入镜像到本地集群（三选一）
k3d image import flinkui:dev -c <cluster>
#   kind load docker-image flinkui:dev
#   minikube image load flinkui:dev

# 3) 用 Helm 部署（SA + 最小 RBAC + Deployment + Service + Secret 一并创建）
helm upgrade --install flinkui deploy/helm/flinkui \
  -n flink-jobs --create-namespace \
  --set image.repository=flinkui --set image.tag=dev --set image.pullPolicy=Never \
  --set config.targetNamespace=flink-jobs \
  --set auth.password='change-me' \
  --set auth.sessionSecret="$(openssl rand -hex 16)"

# 4) 从本地访问集群内的控制台
kubectl -n flink-jobs port-forward svc/flinkui 8080:80
# 打开 http://localhost:8080
```

此形态下 `FKO_CLUSTER_KUBECONFIG` **留空**（用挂载的 ServiceAccount），且 S3 endpoint
可直接用集群内 Service 名，无需 port-forward。

> 说明：chart 用 `config.targetNamespace` 指定存放 FlinkDeployment 的 namespace，
> Role/RoleBinding 会创建到该 namespace（可与 release namespace 不同）。

---

## 两种方式对比

| 维度 | 方式 A（SA 身份本地跑） | 方式 B（Pod 内真跑） |
|------|--------------------------|------------------------|
| 认证代码路径 | out-of-cluster（kubeconfig 承载 SA token） | `rest.InClusterConfig()` 本体 |
| 身份 / RBAC | ✅ 真实 SA + 最小 Role | ✅ 真实 SA + 最小 Role |
| 迭代速度 | 快（本地二进制，改完即跑） | 慢（需重建镜像 + 重新部署） |
| 直连集群内 Service | 需 port-forward | 直连 ClusterIP |
| 适用场景 | 日常开发、验证 RBAC 是否够用 | 上线前的最终形态验证 |
