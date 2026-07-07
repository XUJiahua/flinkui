// Types mirror the Go backend JSON responses (internal/flink, internal/cluster,
// internal/store).

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
}

export interface SavepointResult {
  location: string;
}

export type OperationType = "savepoint" | "restart";
export type OperationStatus = "running" | "succeeded" | "failed";

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

// --- Failover / HA groups ---

export interface FencingState {
  token: string;
  pointsTo: "primary" | "standby" | "neutral" | "unset" | "unknown";
  error?: string;
}

export interface SideView {
  role: "primary" | "standby";
  cluster: string;
  namespace: string;
  deployment: string;
  clusterId: string;
  detail: JobDetail | null;
}

export interface GroupView {
  name: string;
  primary: SideView;
  standby: SideView;
  fencing: FencingState;
  activeSide: "primary" | "standby" | "none" | "unknown";
  splitBrain: boolean;
  warning?: string;
}
