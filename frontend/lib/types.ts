// Types mirror the Go backend JSON responses (internal/flink, internal/cluster,
// internal/store).

// Health mirrors the backend's explicit status classification (design §13).
export type Health =
  | "healthy"
  | "progressing"
  | "degraded"
  | "suspended"
  | "stopped"
  | "unreachable"
  | "notfound"
  | "unknown";

export interface JobSummary {
  namespace: string;
  deployment: string;
  jobName: string;
  jobState: string;
  lifecycleState: string;
  jobId: string;
  desiredState: string;
  upgradeMode: string;
  parallelism: number;
  statusText: string;
  healthy: boolean;
  health: Health;
  reachable: boolean;
}

export interface PodInfo {
  name: string;
  phase: string;
  ready: string;
  restarts: number;
  component: string;
  nodeName: string;
  age: string;
}

export interface EventInfo {
  type: string;
  reason: string;
  message: string;
  count: number;
  lastSeen: string;
  component: string;
}

export interface JobDetail extends JobSummary {
  pods: PodInfo[] | null;
  events: EventInfo[] | null;
}

export interface RecoveryPoint {
  type: "savepoint" | "checkpoint";
  path: string;
  name: string;
  modified: string;
}

export interface ClusterInfo {
  name: string;
  namespace: string;
  s3Configured: boolean;
  clusterReachable: boolean;
}

export interface SavepointResult {
  location: string;
}

export type OperationType = "savepoint" | "restart";
export type OperationStatus = "running" | "succeeded" | "failed";

/** LogComponent selects which pod role's logs to fetch. */
export type LogComponent = "jobmanager" | "taskmanager";

export interface Operation {
  id: string;
  type: OperationType;
  deployment: string;
  jobName: string;
  status: OperationStatus;
  progress: string;
  result?: string;
  error?: string;
  startedAt: string;
  finishedAt?: string;
}

// --- Decentralized HA (failover-decentralized) ---

export interface FencingState {
  token: string;
  pointsTo: "self" | "peer" | "neutral" | "unset" | "unknown";
  error?: string;
}

export interface HandoffRecord {
  group: string;
  activeClusterId: string;
  epoch: number;
  phase: "stable" | "released" | "promoting";
  recoveryPoint: { path: string; kind: string };
  releasedBy?: string;
  updatedAt: string;
}

export interface LocalView {
  name: string;
  clusterId: string;
  peerClusterId: string;
  namespace: string;
  deployment: string;
  local: JobDetail | null;
  fencing: FencingState;
  handoff: HandoffRecord | null;
  role: "active" | "standby" | "neutral" | "unknown";
  warning?: string;
}

export interface HAStepState {
  name: string;
  status: "pending" | "running" | "done" | "failed";
  message?: string;
}

export interface HATask {
  id: string;
  group: string;
  op: "release" | "promote";
  status: "running" | "succeeded" | "failed";
  steps: HAStepState[];
  recoveryPoint: { path: string; kind: string };
  epoch: number;
  error?: string;
  startedAt: string;
  finishedAt?: string;
}
