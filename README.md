# Flink Job Console (MVP)

A WebUI for lifecycle management of **already-deployed** Flink jobs running on the
[Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/)
(`FlinkDeployment` CRD). It replaces the manual `scripts/job.sh` workflow with a
centralized, real-time console that talks **directly to the Kubernetes API**.

Implements the MVP scope in [`docs/webui-mvp-design.md`](../fko-demo/docs/webui-mvp-design.md):
single-cluster status dashboard, one-click lifecycle operations
(suspend / resume / restart / savepoint / rollback), log viewing, a recovery-point
selector for rollback, and a Flink Web UI deep link.

> Out of scope for the MVP: cross-cluster failover/failback, active-active, job
> deployment/creation, fine-grained RBAC / audit / approvals (see design §8 roadmap).

## Architecture

```
Browser ── HTTP/WS ──► Go backend ── K8s API ──► FlinkDeployment CRD
 (static SPA,          (gin + client-go)         pods / logs / exec
  embedded)                 │
                            └── S3/MinIO (list savepoints & checkpoints)
```

- **Single binary**: the Next.js frontend is statically exported and embedded into
  the Go binary via `embed.FS`; gin serves `/api/*` and falls back to the SPA for
  all other routes.
- **ClusterAccessor abstraction** (`internal/cluster`): one client-go implementation
  works both **in-cluster** (ServiceAccount) and **out-of-cluster** (kubeconfig).
  Savepoints are triggered through the pod `exec` subresource (`curl localhost:8081`),
  matching the proven `job.sh` logic and working in both forms.
- **Per-deployment locking** serializes mutating operations to avoid patch races.

### Backend layout (`internal/`)

| Package   | Responsibility |
|-----------|----------------|
| `config`  | viper config, `FKO_*` env vars, deployment naming convention |
| `cluster` | `ClusterAccessor` interface + client-go impl (CRD, pods, logs, exec, events) |
| `flink`   | lifecycle service: list/get/status, suspend/resume/restart, savepoint, rollback, logs |
| `store`   | S3/MinIO recovery-point listing (savepoints + checkpoints) |
| `auth`    | login + HMAC-signed session cookie + middleware |
| `api`     | gin routes, WebSocket status pusher, Flink UI reverse proxy, static embed |

## Operation mapping (design §5)

| Operation | K8s action |
|-----------|------------|
| Status    | `GET flinkdeployment`, read `.status` / `.spec.job.state` (fallbacks per `job.sh`) |
| Suspend   | merge patch `spec.job.state=suspended` |
| Resume    | merge patch `spec.job.state=running` |
| Restart   | suspend → wait JM pod = 0 → resume (last-state) |
| Savepoint | JM REST `POST /jobs/{jobId}/savepoints` via pod exec, poll to COMPLETED |
| Rollback  | patch `state=running` + `initialSavepointPath` + `savepointRedeployNonce` |
| Logs      | `GET pods/log` with label selector `app=<dep>,component=<jobmanager\|taskmanager>` |

## Prerequisites

- Go 1.24+
- Node.js 22+ and pnpm 9+
- Access to a cluster running the Flink Kubernetes Operator (kubeconfig for local dev)

## Build

```bash
# Full single-binary build (frontend export -> embed -> Go binary)
make build          # produces ./bin/flinkui

# Or step by step
make frontend       # pnpm build + copy frontend/out -> web/dist
make backend        # go build -o bin/flinkui ./cmd/server
```

## Run (out-of-cluster / local)

```bash
export FKO_ADDR=":8080"
export FKO_CLUSTER_NAME="dev"
export FKO_CLUSTER_NAMESPACE="flink-operator"
export FKO_CLUSTER_KUBECONFIG="$HOME/.kube/config"   # empty => in-cluster
export FKO_AUTH_USERNAME="admin"
export FKO_AUTH_PASSWORD="change-me"
export FKO_AUTH_SESSION_SECRET="$(openssl rand -hex 16)"

# Optional: enable the rollback recovery-point selector
export FKO_CLUSTER_S3_ENDPOINT="http://minio:9000"
export FKO_CLUSTER_S3_BUCKET="halykbank-flink"
export FKO_CLUSTER_S3_ACCESS_KEY="..."
export FKO_CLUSTER_S3_SECRET_KEY="..."
export FKO_CLUSTER_S3_PATH_STYLE="true"

./bin/flinkui
# open http://localhost:8080
```

### Frontend dev server (hot reload)

```bash
cd frontend
# point the SPA at a running backend
NEXT_PUBLIC_API_BASE="http://localhost:8080" pnpm dev
```

## Configuration reference

All settings are read from `FKO_*` env vars (nested keys use `_`). A config file
can also be supplied with `-config path.yaml`.

| Env var | Default | Description |
|---------|---------|-------------|
| `FKO_ADDR` | `:8080` | HTTP listen address |
| `FKO_DEPLOYMENT_PREFIX` | `flink-sql-job-` | `FlinkDeployment` name = prefix + job name |
| `FKO_SAVEPOINT_TIMEOUT_SEC` | `120` | savepoint poll timeout |
| `FKO_STOP_TIMEOUT_SEC` | `120` | restart "wait JM pod = 0" timeout |
| `FKO_LOG_TAIL_LINES` | `200` | default log tail size |
| `FKO_STATUS_POLL_SEC` | `5` | WebSocket status push interval |
| `FKO_ALLOWED_ORIGINS` | _(empty)_ | comma-separated extra browser origins allowed to open the status WebSocket (same-origin is always allowed) |
| `FKO_CLUSTER_NAME` | `default` | cluster identifier (display) |
| `FKO_CLUSTER_NAMESPACE` | `flink-operator` | namespace holding FlinkDeployments |
| `FKO_CLUSTER_KUBECONFIG` | _(empty)_ | kubeconfig path; empty ⇒ in-cluster |
| `FKO_CLUSTER_CONTEXT` | _(empty)_ | kubeconfig context |
| `FKO_CLUSTER_S3_ENDPOINT` | _(empty)_ | S3/MinIO endpoint |
| `FKO_CLUSTER_S3_BUCKET` | _(empty)_ | bucket (savepoints/, checkpoints/); may include a path to isolate a shared bucket, e.g. `flink/tenant-a` |
| `FKO_CLUSTER_S3_PREFIX` | _(empty)_ | base key prefix inside the bucket for shared-bucket isolation (merged with any path in `_S3_BUCKET`) |
| `FKO_CLUSTER_S3_ACCESS_KEY` / `_SECRET_KEY` | _(empty)_ | S3 credentials |
| `FKO_CLUSTER_S3_PATH_STYLE` | `true` | path-style addressing (MinIO) |
| `FKO_AUTH_USERNAME` | `admin` | login username |
| `FKO_AUTH_PASSWORD` | _(empty)_ | login password — **set this** |
| `FKO_AUTH_SESSION_SECRET` | `change-me-please` | cookie signing secret — **set this** |
| `FKO_AUTH_COOKIE_SECURE` | `false` | set the `Secure` flag on the session cookie — **enable when served over TLS** |

> Security: this service can mutate cluster workloads, so it must not be exposed
> unauthenticated. Always set `FKO_AUTH_PASSWORD` and a strong
> `FKO_AUTH_SESSION_SECRET` (design §6). When served over HTTPS (e.g. behind a
> TLS-terminating ingress) set `FKO_AUTH_COOKIE_SECURE=true` so the session
> cookie is never sent over plain HTTP. The status WebSocket rejects cross-origin
> upgrades (same-origin plus `FKO_ALLOWED_ORIGINS`), and every mutating operation
> (suspend/resume/restart/savepoint/rollback/release/promote) emits a structured
> JSON audit record to stdout (`event=audit`, with user, operation, resource,
> status, duration). Fine-grained RBAC is on the roadmap.

## Container / in-cluster deployment

```bash
make docker IMAGE=your-registry/flinkui:0.1.0
docker push your-registry/flinkui:0.1.0
```

Deploy in-cluster with the **Helm chart** (see below), which creates the
ServiceAccount + minimal RBAC (Role granting `flinkdeployments`
get/list/watch/patch, `pods`/`pods/log`, `pods/exec`, `pods/portforward`,
`events`), the Deployment, Service, and the auth / S3 Secrets.

In-cluster the backend uses the mounted ServiceAccount (leave
`FKO_CLUSTER_KUBECONFIG` unset) and can reach the JobManager REST service and
S3 directly.

### Helm chart

A Helm chart is provided at [`deploy/helm/flinkui`](deploy/helm/flinkui). It
creates the ServiceAccount + minimal RBAC, Deployment, Service, and the auth /
S3 Secrets. The RBAC (Role/RoleBinding) is created in the namespace that holds
the FlinkDeployments (`config.targetNamespace`), which may differ from the
release namespace.

```bash
# Manage jobs in namespace "flink-jobs", enable the S3 rollback selector.
helm upgrade --install flinkui deploy/helm/flinkui \
  -n flink-jobs --create-namespace \
  --set image.tag=0.1.0 \
  --set config.clusterName=prod \
  --set config.targetNamespace=flink-jobs \
  --set auth.username=admin \
  --set auth.password='change-me' \
  --set auth.sessionSecret="$(openssl rand -hex 16)" \
  --set s3.enabled=true \
  --set s3.endpoint='https://minio.flink-operator:9000' \
  --set s3.insecure=true \
  --set s3.accessKey=minioadmin \
  --set s3.secretKey=minioadmin

# then reach the console:
kubectl -n flink-jobs port-forward svc/flinkui 8080:80   # http://localhost:8080
```

Key values (see [`values.yaml`](deploy/helm/flinkui/values.yaml) for all):

| Value | Default | Description |
|-------|---------|-------------|
| `image.repository` / `image.tag` | `docker.io/johnxu1989/flinkui` / _appVersion_ | image coordinates |
| `config.targetNamespace` | _(release ns)_ | namespace holding FlinkDeployments (RBAC target) |
| `config.clusterName` | `in-cluster` | display name |
| `auth.password` / `auth.sessionSecret` | _(empty)_ | **required** unless `auth.existingSecret` is set |
| `auth.existingSecret` | _(empty)_ | use a pre-created Secret (keys: username, password, session-secret) |
| `s3.enabled` | `false` | enable the rollback recovery-point selector |
| `s3.endpoint` / `s3.insecure` | _(empty)_ / `false` | MinIO S3 **API** endpoint; skip TLS for self-signed |
| `s3.existingSecret` | _(empty)_ | pre-created Secret (keys: access-key, secret-key) |
| `service.type` / `ingress.enabled` | `ClusterIP` / `false` | exposure |

An example values file for the sample cluster is at
[`deploy/helm/flinkui/values-example.yaml`](deploy/helm/flinkui/values-example.yaml);
a fully-annotated example covering ingress, existing secrets, resources and
scheduling is at
[`deploy/helm/flinkui/values-example-full.yaml`](deploy/helm/flinkui/values-example-full.yaml).

## Test

```bash
make test    # go test ./...
make vet     # go vet ./...
```

The API package includes wiring tests (embedded static serving, auth flow,
authenticated job listing, API 404 not falling through to the SPA) using an
in-memory fake `ClusterAccessor`.
