// API client for the Go backend. In the single-binary deployment the frontend is
// served same-origin, so requests use relative /api paths. For `next dev` set
// NEXT_PUBLIC_API_BASE to point at a running backend.
import type {
  ClusterInfo,
  JobDetail,
  JobSummary,
  RecoveryPoint,
  SavepointResult,
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
  logs: (name: string, tail = 200) =>
    request<{ logs: string }>(`/api/jobs/${encodeURIComponent(name)}/logs?tail=${tail}`),
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
    request<{ ok: boolean }>(`/api/jobs/${encodeURIComponent(name)}/restart`, { method: "POST" }),
  savepoint: (name: string) =>
    request<SavepointResult>(`/api/jobs/${encodeURIComponent(name)}/savepoint`, { method: "POST" }),
  rollback: (name: string, path: string) =>
    request<{ ok: boolean }>(`/api/jobs/${encodeURIComponent(name)}/rollback`, {
      method: "POST",
      body: JSON.stringify({ path }),
    }),
};

/** wsStatusUrl builds the WebSocket URL for the status stream. */
export function wsStatusUrl(): string {
  if (typeof window === "undefined") return "";
  const base = BASE || `${window.location.protocol}//${window.location.host}`;
  const url = new URL("/api/ws/status", base);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}
