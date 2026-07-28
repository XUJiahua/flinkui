package cluster

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestConfigMapReadPatchDelete(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "flink-sql-job1", Namespace: "flink-jobs"},
		Data:       map[string]string{"job.sql": "SELECT 1", "core-site.xml": "<x/>"},
	})
	k := &KubeAccessor{clientset: cs, namespace: "flink-jobs"}
	ctx := context.Background()

	data, found, err := k.GetConfigMap(ctx, "flink-sql-job1")
	if err != nil || !found {
		t.Fatalf("GetConfigMap: found=%v err=%v", found, err)
	}
	if data["job.sql"] != "SELECT 1" {
		t.Fatalf("job.sql = %q", data["job.sql"])
	}

	// Patch a single key; the co-resident core-site.xml must survive.
	if err := k.PatchConfigMapKey(ctx, "flink-sql-job1", "job.sql", "SELECT 2"); err != nil {
		t.Fatalf("PatchConfigMapKey: %v", err)
	}
	data, _, _ = k.GetConfigMap(ctx, "flink-sql-job1")
	if data["job.sql"] != "SELECT 2" {
		t.Errorf("job.sql not updated: %q", data["job.sql"])
	}
	if data["core-site.xml"] != "<x/>" {
		t.Errorf("co-resident key clobbered by single-key patch: %q", data["core-site.xml"])
	}

	// Add then delete a backup key.
	if err := k.PatchConfigMapKey(ctx, "flink-sql-job1", "job.sql.backup", "SELECT 1"); err != nil {
		t.Fatalf("add backup: %v", err)
	}
	if err := k.DeleteConfigMapKey(ctx, "flink-sql-job1", "job.sql.backup"); err != nil {
		t.Fatalf("DeleteConfigMapKey: %v", err)
	}
	data, _, _ = k.GetConfigMap(ctx, "flink-sql-job1")
	if _, ok := data["job.sql.backup"]; ok {
		t.Error("backup key was not deleted")
	}
	if data["job.sql"] != "SELECT 2" {
		t.Errorf("delete clobbered job.sql: %q", data["job.sql"])
	}

	// Missing ConfigMap => found=false, no error.
	if _, found, err := k.GetConfigMap(ctx, "nope"); found || err != nil {
		t.Errorf("missing cm: found=%v err=%v", found, err)
	}
	// Delete on missing cm is a no-op.
	if err := k.DeleteConfigMapKey(ctx, "nope", "x"); err != nil {
		t.Errorf("delete on missing cm should be no-op: %v", err)
	}
}
