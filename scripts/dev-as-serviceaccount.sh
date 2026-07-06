#!/usr/bin/env bash
# =============================================================================
# dev-as-serviceaccount.sh
#
# Simulate the *in-cluster* usage scenario from your local machine by running
# the console with the ServiceAccount's identity and minimal RBAC — instead of
# your admin kubeconfig. This validates that deploy/rbac.yaml grants exactly the
# permissions the platform needs (get/list/watch/patch flinkdeployments, pods,
# pods/log, pods/exec, events).
#
# It provisions (idempotently) a ServiceAccount + Role + RoleBinding in the
# target namespace, mints a short-lived token, and writes a kubeconfig that
# authenticates as that ServiceAccount. Point the console at it via
# FKO_CLUSTER_KUBECONFIG.
#
# NOTE: this exercises the SA identity/RBAC, which is the meaningful difference
# in-cluster. To also exercise the rest.InClusterConfig() code path itself, run
# the binary as a Pod (see docs/local-in-cluster.md, option B).
#
# Usage:
#   scripts/dev-as-serviceaccount.sh [namespace]
#
# Env overrides:
#   ADMIN_KUBECONFIG  source (admin) kubeconfig            [default: $KUBECONFIG or ~/.kube/config]
#   NAMESPACE         namespace holding FlinkDeployments   [default: flink-jobs]
#   SA_NAME           ServiceAccount name                  [default: flink-console]
#   OUT               output kubeconfig path               [default: ./.sa.kubeconfig]
#   TOKEN_TTL         token duration                       [default: 8h]
# =============================================================================
set -euo pipefail

ADMIN_KUBECONFIG="${ADMIN_KUBECONFIG:-${KUBECONFIG:-$HOME/.kube/config}}"
NAMESPACE="${1:-${NAMESPACE:-flink-jobs}}"
SA_NAME="${SA_NAME:-flink-console}"
OUT="${OUT:-./.sa.kubeconfig}"
TOKEN_TTL="${TOKEN_TTL:-8h}"

K() { kubectl --kubeconfig "$ADMIN_KUBECONFIG" "$@"; }

echo "[1/4] Provisioning ServiceAccount + minimal RBAC in namespace '$NAMESPACE'..."
K apply -f - >/dev/null <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SA_NAME}
  namespace: ${NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ${SA_NAME}
  namespace: ${NAMESPACE}
rules:
  - apiGroups: ["flink.apache.org"]
    resources: ["flinkdeployments"]
    verbs: ["get", "list", "watch", "patch", "update"]
  - apiGroups: [""]
    resources: ["pods", "pods/log"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods/exec"]
    verbs: ["create", "get"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ${SA_NAME}
  namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: ${SA_NAME}
subjects:
  - kind: ServiceAccount
    name: ${SA_NAME}
    namespace: ${NAMESPACE}
YAML

echo "[2/4] Minting a ${TOKEN_TTL} token for ${NAMESPACE}/${SA_NAME}..."
TOKEN="$(K create token "$SA_NAME" -n "$NAMESPACE" --duration="$TOKEN_TTL")"

echo "[3/4] Extracting API server + CA and writing kubeconfig -> ${OUT}..."
SERVER="$(K config view --minify --raw -o jsonpath='{.clusters[0].cluster.server}')"
CA_TMP="$(mktemp)"
trap 'rm -f "$CA_TMP"' EXIT
if K config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | grep -q .; then
  K config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d > "$CA_TMP"
  CA_ARGS=(--certificate-authority="$CA_TMP" --embed-certs=true)
else
  CA_ARGS=(--insecure-skip-tls-verify=true)
fi

rm -f "$OUT"
kubectl --kubeconfig "$OUT" config set-cluster sa-cluster --server="$SERVER" "${CA_ARGS[@]}" >/dev/null
kubectl --kubeconfig "$OUT" config set-credentials "$SA_NAME" --token="$TOKEN" >/dev/null
kubectl --kubeconfig "$OUT" config set-context sa --cluster=sa-cluster --user="$SA_NAME" --namespace="$NAMESPACE" >/dev/null
kubectl --kubeconfig "$OUT" config use-context sa >/dev/null

echo "[4/4] Done."
cat <<EOF

ServiceAccount kubeconfig written to: ${OUT}

Run the console with the in-cluster identity:

  export FKO_CLUSTER_KUBECONFIG="$(cd "$(dirname "$OUT")" && pwd)/$(basename "$OUT")"
  export FKO_CLUSTER_NAME="in-cluster-sim"
  export FKO_CLUSTER_NAMESPACE="${NAMESPACE}"
  export FKO_AUTH_PASSWORD="change-me"
  export FKO_AUTH_SESSION_SECRET="\$(openssl rand -hex 16)"
  ./bin/flinkui

Sanity-check the granted RBAC:

  kubectl --kubeconfig "${OUT}" auth can-i patch flinkdeployments -n ${NAMESPACE}
  kubectl --kubeconfig "${OUT}" auth can-i create pods/exec -n ${NAMESPACE}

Clean up when finished:

  kubectl --kubeconfig "${ADMIN_KUBECONFIG}" -n ${NAMESPACE} delete serviceaccount,role,rolebinding ${SA_NAME}
  rm -f ${OUT}
EOF
