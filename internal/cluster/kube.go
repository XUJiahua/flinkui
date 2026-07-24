package cluster

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

// KubeAccessor implements ClusterAccessor via client-go. It works both
// in-cluster (empty kubeconfig => rest.InClusterConfig) and out-of-cluster
// (kubeconfig path).
type KubeAccessor struct {
	name      string
	namespace string

	restCfg   *rest.Config
	clientset *kubernetes.Clientset
	dynClient dynamic.Interface

	// Informer-backed cache for FlinkDeployments (design §3.3: watch/informer
	// instead of polling). Optional — falls back to live List until synced.
	factory    dynamicinformer.DynamicSharedInformerFactory
	fdInformer informers.GenericInformer
	// stopCh keeps the informer's watch goroutines alive for the whole process
	// lifetime. It must be independent of the (bounded) context used to wait for
	// the initial cache sync — otherwise cancelling that context would stop the
	// watch and freeze the cache at its first snapshot.
	stopCh chan struct{}
}

// NewKubeAccessor builds an accessor. If kubeconfig is empty, in-cluster config
// is used; otherwise the kubeconfig file (optionally a named context) is loaded.
func NewKubeAccessor(name, namespace, kubeconfig, kubeContext string) (*KubeAccessor, error) {
	cfg, err := buildRestConfig(kubeconfig, kubeContext)
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build clientset: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build dynamic client: %w", err)
	}
	// Namespace-scoped informer factory for the FlinkDeployment CRD.
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, 30*time.Second, namespace, nil)
	fdInformer := factory.ForResource(FlinkDeploymentGVR)
	return &KubeAccessor{
		name:       name,
		namespace:  namespace,
		restCfg:    cfg,
		clientset:  cs,
		dynClient:  dyn,
		factory:    factory,
		fdInformer: fdInformer,
		stopCh:     make(chan struct{}),
	}, nil
}

// Start launches the informer(s) using a process-lifetime stop channel and
// blocks until their caches sync or the (bounded) ctx is cancelled. The watch
// goroutines keep running after Start returns — the ctx only bounds the initial
// sync wait, not the informer itself. Implements the optional cluster.Starter
// interface.
func (k *KubeAccessor) Start(ctx context.Context) error {
	k.factory.Start(k.stopCh)
	for gvr, ok := range k.factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return fmt.Errorf("informer cache for %s failed to sync", gvr.Resource)
		}
	}
	return nil
}

// Stop terminates the informer's watch goroutines. Safe to call once.
func (k *KubeAccessor) Stop() {
	if k.stopCh != nil {
		close(k.stopCh)
	}
}

// CachedListFlinkDeployments returns FlinkDeployments from the informer cache.
// The bool is false until the cache has synced, so callers can fall back to a
// live API list. Implements the optional cluster.CachedLister interface.
func (k *KubeAccessor) CachedListFlinkDeployments() ([]*unstructured.Unstructured, bool) {
	if k.fdInformer == nil || !k.fdInformer.Informer().HasSynced() {
		return nil, false
	}
	objs, err := k.fdInformer.Lister().List(labels.Everything())
	if err != nil {
		return nil, false
	}
	out := make([]*unstructured.Unstructured, 0, len(objs))
	for _, o := range objs {
		if u, ok := o.(*unstructured.Unstructured); ok {
			out = append(out, u)
		}
	}
	return out, true
}

func buildRestConfig(kubeconfig, kubeContext string) (*rest.Config, error) {
	if kubeconfig == "" {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("in-cluster config: %w", err)
		}
		return cfg, nil
	}
	loadingRules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig}
	overrides := &clientcmd.ConfigOverrides{}
	if kubeContext != "" {
		overrides.CurrentContext = kubeContext
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig %q: %w", kubeconfig, err)
	}
	return cfg, nil
}

func (k *KubeAccessor) Name() string      { return k.name }
func (k *KubeAccessor) Namespace() string { return k.namespace }

func (k *KubeAccessor) fd() dynamic.ResourceInterface {
	return k.dynClient.Resource(FlinkDeploymentGVR).Namespace(k.namespace)
}

func (k *KubeAccessor) GetFlinkDeployment(ctx context.Context, name string) (*unstructured.Unstructured, error) {
	return k.fd().Get(ctx, name, metav1.GetOptions{})
}

func (k *KubeAccessor) ListFlinkDeployments(ctx context.Context) (*unstructured.UnstructuredList, error) {
	return k.fd().List(ctx, metav1.ListOptions{})
}

func (k *KubeAccessor) PatchFlinkDeployment(ctx context.Context, name string, mergePatch []byte) error {
	_, err := k.fd().Patch(ctx, name, types.MergePatchType, mergePatch, metav1.PatchOptions{})
	return err
}

// GetSecret returns the Secret's data map, and false if the Secret is absent.
// Implements cluster.SecretAccessor.
func (k *KubeAccessor) GetSecret(ctx context.Context, name string) (map[string][]byte, bool, error) {
	sec, err := k.clientset.CoreV1().Secrets(k.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return sec.Data, true, nil
}

// ApplySecret creates or updates an Opaque Secret with the given data.
// Implements cluster.SecretAccessor.
func (k *KubeAccessor) ApplySecret(ctx context.Context, name string, data map[string][]byte) error {
	secrets := k.clientset.CoreV1().Secrets(k.namespace)
	existing, err := secrets.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = secrets.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k.namespace},
			Type:       corev1.SecretTypeOpaque,
			Data:       data,
		}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	existing.Data = data
	_, err = secrets.Update(ctx, existing, metav1.UpdateOptions{})
	return err
}

func (k *KubeAccessor) listPodObjects(ctx context.Context, labelSelector string) ([]corev1.Pod, error) {
	list, err := k.clientset.CoreV1().Pods(k.namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (k *KubeAccessor) ListPods(ctx context.Context, labelSelector string) ([]PodInfo, error) {
	pods, err := k.listPodObjects(ctx, labelSelector)
	if err != nil {
		return nil, err
	}
	out := make([]PodInfo, 0, len(pods))
	for i := range pods {
		p := &pods[i]
		ready, total, restarts := containerStats(p)
		out = append(out, PodInfo{
			Name:      p.Name,
			Phase:     string(p.Status.Phase),
			Ready:     fmt.Sprintf("%d/%d", ready, total),
			Restarts:  restarts,
			Component: p.Labels["component"],
			NodeName:  p.Spec.NodeName,
			Age:       age(p.CreationTimestamp.Time),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (k *KubeAccessor) CountPods(ctx context.Context, labelSelector string) (int, error) {
	pods, err := k.listPodObjects(ctx, labelSelector)
	if err != nil {
		return 0, err
	}
	return len(pods), nil
}

func containerStats(p *corev1.Pod) (ready, total int, restarts int32) {
	total = len(p.Status.ContainerStatuses)
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Ready {
			ready++
		}
		restarts += cs.RestartCount
	}
	return ready, total, restarts
}

func age(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

func (k *KubeAccessor) PodLogs(ctx context.Context, labelSelector, container string, tailLines int64) (string, error) {
	pods, err := k.listPodObjects(ctx, labelSelector)
	if err != nil {
		return "", err
	}
	if len(pods) == 0 {
		return "", nil
	}
	var buf bytes.Buffer
	for i := range pods {
		p := &pods[i]
		if err := k.streamPodLog(ctx, &buf, p.Name, container, tailLines); err != nil {
			fmt.Fprintf(&buf, "==== %s (error: %v) ====\n", p.Name, err)
		}
	}
	return buf.String(), nil
}

// PodLogsForPod returns logs for a single named pod. The pod must match the
// given label selector so a caller cannot read arbitrary pods in the namespace
// (the read stays scoped to the deployment + component).
func (k *KubeAccessor) PodLogsForPod(ctx context.Context, labelSelector, podName, container string, tailLines int64) (string, error) {
	pods, err := k.listPodObjects(ctx, labelSelector)
	if err != nil {
		return "", err
	}
	found := false
	for i := range pods {
		if pods[i].Name == podName {
			found = true
			break
		}
	}
	if !found {
		return "", fmt.Errorf("pod %q does not match selector %q", podName, labelSelector)
	}
	var buf bytes.Buffer
	if err := k.streamPodLog(ctx, &buf, podName, container, tailLines); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// streamPodLog writes a header and the tailed log stream of one pod into buf.
func (k *KubeAccessor) streamPodLog(ctx context.Context, buf *bytes.Buffer, podName, container string, tailLines int64) error {
	opts := &corev1.PodLogOptions{TailLines: &tailLines}
	if container != "" {
		opts.Container = container
	}
	req := k.clientset.CoreV1().Pods(k.namespace).GetLogs(podName, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()
	fmt.Fprintf(buf, "==== %s ====\n", podName)
	_, _ = buf.ReadFrom(stream)
	return nil
}

func (k *KubeAccessor) Exec(ctx context.Context, labelSelector, container string, cmd []string) (*ExecResult, error) {
	pods, err := k.listPodObjects(ctx, labelSelector)
	if err != nil {
		return nil, err
	}
	if len(pods) == 0 {
		return nil, fmt.Errorf("no pod matches selector %q", labelSelector)
	}
	podName := pods[0].Name

	req := k.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(k.namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   cmd,
			Stdout:    true,
			Stderr:    true,
			Stdin:     false,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(k.restCfg, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("init executor: %w", err)
	}
	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	res := &ExecResult{Stdout: stdout.String(), Stderr: stderr.String()}
	if err != nil {
		return res, fmt.Errorf("exec in %s: %w (stderr: %s)", podName, err, strings.TrimSpace(stderr.String()))
	}
	return res, nil
}

func (k *KubeAccessor) ListEvents(ctx context.Context, involvedObjectName string) ([]EventInfo, error) {
	fieldSel := fmt.Sprintf("involvedObject.name=%s", involvedObjectName)
	list, err := k.clientset.CoreV1().Events(k.namespace).List(ctx, metav1.ListOptions{FieldSelector: fieldSel})
	if err != nil {
		return nil, err
	}
	out := make([]EventInfo, 0, len(list.Items))
	for i := range list.Items {
		e := &list.Items[i]
		last := e.LastTimestamp.Time
		if last.IsZero() {
			last = e.EventTime.Time
		}
		out = append(out, EventInfo{
			Type:     e.Type,
			Reason:   e.Reason,
			Message:  e.Message,
			Count:    e.Count,
			LastSeen: age(last) + " ago",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastSeen > out[j].LastSeen })
	return out, nil
}
