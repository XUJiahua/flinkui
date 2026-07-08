// API client for the Go backend. In the single-binary deployment the frontend is
// served same-origin, so requests use relative /api paths. For `next dev` set
// NEXT_PUBLIC_API_BASE to point at a running backend.
import type {
  ClusterInfo,
  HATask,
  JobDetail,
  JobSummary,
  LocalView,
  LogComponent,
  Operation,
  RecoveryPoint,
} from "./types";

const BASE = process.env.NEXT_PUBLIC_API_BASE ?? "";

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    let msg = res.statusText;
    try {
      const body = await res.json();
      if (body?.error) msg = body.error;
    } catch {
      /* ignore */
    }
    throw new ApiError(res.status, msg);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export const api = {
  // Auth
  login: (username: string, password: string) =>
    request<{ username: string }>("/api/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),
  logout: () => request<{ ok: boolean }>("/api/logout", { method: "POST" }),
  me: () => request<{ username: string }>("/api/me"),

  // Cluster + jobs
  cluster: () => request<ClusterInfo>("/api/cluster"),
  listJobs: () => request<{ jobs: JobSummary[] }>("/api/jobs"),
  getJob: (name: string) => request<JobDetail>(`/api/jobs/${encodeURIComponent(name)}`),
  logs: (name: string, tail = 200, component: LogComponent = "jobmanager", pod = "") =>
    request<{ logs: string }>(
      `/api/jobs/${encodeURIComponent(name)}/logs?tail=${tail}&component=${component}` +
        (pod ? `&pod=${encodeURIComponent(pod)}` : ""),
    ),
  recoveryPoints: (name: string) =>
    request<{ recoveryPoints: RecoveryPoint[] }>(
      `/api/jobs/${encodeURIComponent(name)}/recovery-points`,
    ),
  flinkUiInfo: (name: string) =>
    request<{ proxyPath: string; target: string }>(
      `/api/jobs/${encodeURIComponent(name)}/flink-ui`,
    ),

  // Lifecycle operations
  suspend: (name: string) =>
    request<{ ok: boolean }>(`/api/jobs/${encodeURIComponent(name)}/suspend`, { method: "POST" }),
  resume: (name: string) =>
    request<{ ok: boolean }>(`/api/jobs/${encodeURIComponent(name)}/resume`, { method: "POST" }),
  restart: (name: string) =>
    request<Operation>(`/api/jobs/${encodeURIComponent(name)}/restart`, { method: "POST" }),
  savepoint: (name: string) =>
    request<Operation>(`/api/jobs/${encodeURIComponent(name)}/savepoint`, { method: "POST" }),
  rollback: (name: string, path: string) =>
    request<{ ok: boolean }>(`/api/jobs/${encodeURIComponent(name)}/rollback`, {
      method: "POST",
      body: JSON.stringify({ path }),
    }),

  // Async operation status (savepoint / restart progress).
  getOperation: (id: string) =>
    request<Operation>(`/api/operations/${encodeURIComponent(id)}`),

  // Decentralized HA.
  listHA: () => request<{ groups: LocalView[] }>("/api/ha"),
  getHA: (name: string) => request<LocalView>(`/api/ha/${encodeURIComponent(name)}`),
  claim: (name: string) =>
    request<{ ok: boolean }>(`/api/ha/${encodeURIComponent(name)}/claim`, {
      method: "POST",
      body: JSON.stringify({ confirm: true }),
    }),
  release: (name: string) =>
    request<HATask>(`/api/ha/${encodeURIComponent(name)}/release`, {
      method: "POST",
      body: JSON.stringify({ confirm: true }),
    }),
  promote: (name: string, force: boolean, ackDataLoss: boolean) =>
    request<HATask>(`/api/ha/${encodeURIComponent(name)}/promote`, {
      method: "POST",
      body: JSON.stringify({ confirm: true, force, ackDataLoss }),
    }),
  getHATask: (id: string) => request<HATask>(`/api/ha-tasks/${encodeURIComponent(id)}`),
};

/** pollHATask polls a release/promote task until it finishes (or times out). */
export async function pollHATask(
  id: string,
  onProgress?: (task: HATask) => void,
  opts: { intervalMs?: number; timeoutMs?: number } = {},
): Promise<HATask> {
  const intervalMs = opts.intervalMs ?? 2000;
  const timeoutMs = opts.timeoutMs ?? 10 * 60 * 1000;
  const deadline = Date.now() + timeoutMs;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const task = await api.getHATask(id);
    onProgress?.(task);
    if (task.status !== "running") return task;
    if (Date.now() > deadline) {
      return { ...task, status: "failed", error: "polling timed out" };
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

/** pollOperation polls an async operation until it finishes (or times out),
 *  invoking onProgress with each snapshot. Returns the terminal Operation. */
export async function pollOperation(
  id: string,
  onProgress?: (op: Operation) => void,
  opts: { intervalMs?: number; timeoutMs?: number } = {},
): Promise<Operation> {
  const intervalMs = opts.intervalMs ?? 2000;
  const timeoutMs = opts.timeoutMs ?? 5 * 60 * 1000;
  const deadline = Date.now() + timeoutMs;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const op = await api.getOperation(id);
    onProgress?.(op);
    if (op.status !== "running") return op;
    if (Date.now() > deadline) {
      return { ...op, status: "failed", error: "polling timed out" };
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

/** wsStatusUrl builds the WebSocket URL for the status stream. */
export function wsStatusUrl(): string {
  if (typeof window === "undefined") return "";
  const base = BASE || `${window.location.protocol}//${window.location.host}`;
  const url = new URL("/api/ws/status", base);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}
