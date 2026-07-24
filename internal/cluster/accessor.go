// Package cluster abstracts access to a target Kubernetes cluster so that the
// business logic (FlinkDeployment lifecycle) does not care whether we run
// in-cluster (ServiceAccount) or out-of-cluster (kubeconfig). See design §3.4.
package cluster

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// FlinkDeploymentGVR is the GroupVersionResource for the Flink Operator CRD.
var FlinkDeploymentGVR = schema.GroupVersionResource{
	Group:    "flink.apache.org",
	Version:  "v1beta1",
	Resource: "flinkdeployments",
}

// PodInfo is a trimmed view of a Kubernetes Pod for the UI.
type PodInfo struct {
	Name      string `json:"name"`
	Phase     string `json:"phase"`
	Ready     string `json:"ready"` // e.g. "1/1"
	Restarts  int32  `json:"restarts"`
	Component string `json:"component"` // jobmanager / taskmanager (from label)
	NodeName  string `json:"nodeName"`
	Age       string `json:"age"`
}

// EventInfo is a trimmed view of a Kubernetes Event.
type EventInfo struct {
	Type      string `json:"type"` // Normal / Warning
	Reason    string `json:"reason"`
	Message   string `json:"message"`
	Count     int32  `json:"count"`
	LastSeen  string `json:"lastSeen"`
	Component string `json:"component"`
}

// ExecResult is the outcome of a pod exec call.
type ExecResult struct {
	Stdout string
	Stderr string
}

// ClusterAccessor is the single seam between business logic and the cluster.
// The in-cluster and out-of-cluster forms share this same implementation via
// client-go (rest.InClusterConfig vs kubeconfig); savepoint REST is issued
// through the pod exec subresource, which works in both forms (design §3.4).
type ClusterAccessor interface {
	// Name returns the cluster identifier.
	Name() string
	// Namespace returns the namespace being managed.
	Namespace() string

	// GetFlinkDeployment fetches a single FlinkDeployment as unstructured data.
	GetFlinkDeployment(ctx context.Context, name string) (*unstructured.Unstructured, error)
	// ListFlinkDeployments lists all FlinkDeployments in the namespace.
	ListFlinkDeployments(ctx context.Context) (*unstructured.UnstructuredList, error)
	// PatchFlinkDeployment applies a merge patch to a FlinkDeployment.
	PatchFlinkDeployment(ctx context.Context, name string, mergePatch []byte) error

	// ListPods lists pods matching the label selector.
	ListPods(ctx context.Context, labelSelector string) ([]PodInfo, error)
	// CountPods returns the number of pods matching the label selector.
	CountPods(ctx context.Context, labelSelector string) (int, error)
	// PodLogs returns tailed logs across pods matching the selector.
	PodLogs(ctx context.Context, labelSelector, container string, tailLines int64) (string, error)
	// PodLogsForPod returns tailed logs for a single named pod, which must match
	// the label selector (scoping the read to the deployment+component).
	PodLogsForPod(ctx context.Context, labelSelector, podName, container string, tailLines int64) (string, error)

	// Exec runs a command inside a container of a pod selected by labelSelector
	// (first matching pod). Used to issue the JM REST savepoint call.
	Exec(ctx context.Context, labelSelector, container string, cmd []string) (*ExecResult, error)

	// ListEvents returns recent events for the involved object name.
	ListEvents(ctx context.Context, involvedObjectName string) ([]EventInfo, error)
}

// SecretAccessor is an optional interface for accessors that can read/write
// Kubernetes Secrets in the managed namespace. Implemented by KubeAccessor.
// Used by the OpenBao/Vault secret-sync loop (internal/secretsync) so the
// business logic does not depend on client-go directly.
type SecretAccessor interface {
	// GetSecret returns the Secret's data (nil map if the Secret does not exist).
	GetSecret(ctx context.Context, name string) (map[string][]byte, bool, error)
	// ApplySecret creates or updates an Opaque Secret with the given data.
	ApplySecret(ctx context.Context, name string, data map[string][]byte) error
}

// Starter is an optional interface for accessors that run background machinery
// (e.g. informers) which must be started before serving.
type Starter interface {
	Start(ctx context.Context) error
}

// CachedLister is an optional interface for accessors backed by an informer
// cache. The bool is false until the cache has synced, so callers fall back to
// a live List (design §3.3).
type CachedLister interface {
	CachedListFlinkDeployments() ([]*unstructured.Unstructured, bool)
}
