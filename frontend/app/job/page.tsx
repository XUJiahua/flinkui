"use client";

import * as React from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { ArrowLeft, ExternalLink } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { useAuthGuard } from "@/lib/use-auth";
import { Header } from "@/components/header";
import { StatusBadge } from "@/components/status-badge";
import { LifecycleActions } from "@/components/lifecycle-actions";
import { LogsViewer } from "@/components/logs-viewer";
import { Button, buttonVariants } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

function JobDetail({ name }: { name: string }) {
  const auth = useAuthGuard();
  const job = useQuery({
    queryKey: ["job", name],
    queryFn: () => api.getJob(name),
    refetchInterval: 5000,
    enabled: !!name,
  });

  const openFlinkUi = async () => {
    try {
      const info = await api.flinkUiInfo(name);
      window.open(info.proxyPath, "_blank", "noopener");
    } catch {
      /* ignore */
    }
  };

  if (auth.isLoading) return <p className="p-6 text-sm text-muted-foreground">Loading…</p>;
  if (auth.isError) return null;

  const d = job.data;

  return (
    <div>
      <Header />
      <main className="mx-auto max-w-7xl space-y-6 px-4 py-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Link
              href="/"
              aria-label="Back"
              className={buttonVariants({ variant: "ghost", size: "icon" })}
            >
              <ArrowLeft className="h-4 w-4" />
            </Link>
            <div>
              <h1 className="text-xl font-semibold">{d?.jobName || name}</h1>
              <p className="text-xs text-muted-foreground">
                {d?.namespace} / {d?.deployment}
              </p>
            </div>
            {d && <StatusBadge job={d} />}
          </div>
          <Button variant="outline" size="sm" onClick={openFlinkUi}>
            <ExternalLink className="h-4 w-4" />
            Flink Web UI
          </Button>
        </div>

        {job.isError && (
          <p className="text-sm text-destructive">
            {job.error instanceof ApiError ? job.error.message : "failed to load job"}
          </p>
        )}

        <Card>
          <CardHeader>
            <CardTitle>Lifecycle</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <LifecycleActions jobName={name} size="default" />
            <dl className="grid grid-cols-2 gap-4 text-sm sm:grid-cols-4">
              <Field label="Job State" value={d?.jobState} />
              <Field label="Lifecycle" value={d?.lifecycleState} />
              <Field label="Desired" value={d?.desiredState} />
              <Field label="Upgrade Mode" value={d?.upgradeMode} />
              <Field label="Parallelism" value={d?.parallelism ? String(d.parallelism) : undefined} />
              <Field label="Job ID" value={d?.jobId} mono />
            </dl>
          </CardContent>
        </Card>

        <MetricsCard name={name} enabled={d?.health === "healthy" || d?.jobState === "RUNNING"} />

        <Card>
          <CardHeader>
            <CardTitle>Pods</CardTitle>
          </CardHeader>
          <CardContent>
            {!d?.pods || d.pods.length === 0 ? (
              <p className="py-4 text-sm text-muted-foreground">No pods.</p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Component</TableHead>
                    <TableHead>Phase</TableHead>
                    <TableHead>Ready</TableHead>
                    <TableHead className="text-right">Restarts</TableHead>
                    <TableHead>Node</TableHead>
                    <TableHead>Age</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {d.pods.map((p) => (
                    <TableRow key={p.name}>
                      <TableCell className="font-mono text-xs">{p.name}</TableCell>
                      <TableCell>
                        <Badge variant="secondary">{p.component || "—"}</Badge>
                      </TableCell>
                      <TableCell>{p.phase}</TableCell>
                      <TableCell>{p.ready}</TableCell>
                      <TableCell className="text-right">{p.restarts}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">{p.nodeName}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">{p.age}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Events</CardTitle>
          </CardHeader>
          <CardContent>
            {!d?.events || d.events.length === 0 ? (
              <p className="py-4 text-sm text-muted-foreground">No recent events.</p>
            ) : (
              <div className="space-y-2">
                {d.events.map((e, i) => (
                  <div key={i} className="flex items-start gap-2 text-sm">
                    <Badge variant={e.type === "Warning" ? "warning" : "secondary"}>{e.reason}</Badge>
                    <span className="flex-1">{e.message}</span>
                    <span className="shrink-0 text-xs text-muted-foreground">
                      ×{e.count} · {e.lastSeen}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Logs</CardTitle>
          </CardHeader>
          <CardContent>
            <LogsViewer jobName={name} />
          </CardContent>
        </Card>
      </main>
    </div>
  );
}

function Field({ label, value, mono }: { label: string; value?: string; mono?: boolean }) {
  return (
    <div>
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className={mono ? "break-all font-mono text-xs" : "font-medium"}>{value || "—"}</dd>
    </div>
  );
}

/** formatBytes renders a byte count in human units (base 1024). */
function formatBytes(n: number): string {
  if (!n) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(Math.floor(Math.log(n) / Math.log(1024)), units.length - 1);
  return `${(n / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

/** formatDuration renders a millisecond duration compactly (e.g. "1h 3m"). */
function formatDuration(ms: number): string {
  if (ms <= 0) return "—";
  const s = Math.floor(ms / 1000);
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${sec}s`;
  return `${sec}s`;
}

function formatNumber(n: number): string {
  return n.toLocaleString();
}

/** MetricsCard shows a compact job-internal metrics snapshot (state/uptime,
 *  aggregate throughput counters, checkpoint health) from the JobManager REST
 *  API. It only queries when the job is plausibly running to avoid noisy
 *  "job not running" errors (design backlog P2-1). */
function MetricsCard({ name, enabled }: { name: string; enabled?: boolean }) {
  const q = useQuery({
    queryKey: ["metrics", name],
    queryFn: () => api.metrics(name),
    refetchInterval: 5000,
    enabled: !!name && !!enabled,
    retry: false,
  });

  const m = q.data;
  const cp = m?.checkpoints;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Metrics</CardTitle>
      </CardHeader>
      <CardContent>
        {!enabled ? (
          <p className="py-4 text-sm text-muted-foreground">Job is not running; no live metrics.</p>
        ) : q.isError ? (
          <p className="py-4 text-sm text-muted-foreground">
            {q.error instanceof ApiError ? q.error.message : "metrics unavailable"}
          </p>
        ) : !m ? (
          <p className="py-4 text-sm text-muted-foreground">Loading metrics…</p>
        ) : (
          <div className="space-y-4">
            <dl className="grid grid-cols-2 gap-4 text-sm sm:grid-cols-4">
              <Field label="State" value={m.state} />
              <Field label="Uptime" value={formatDuration(m.durationMs)} />
              <Field label="Operators" value={String(m.vertices)} />
              <Field label="Total Parallelism" value={String(m.parallelism)} />
              <Field label="Records In" value={formatNumber(m.readRecords)} />
              <Field label="Records Out" value={formatNumber(m.writeRecords)} />
              <Field label="Bytes In" value={formatBytes(m.readBytes)} />
              <Field label="Bytes Out" value={formatBytes(m.writeBytes)} />
            </dl>
            <div>
              <p className="mb-2 text-xs font-medium text-muted-foreground">Checkpoints</p>
              {!cp ? (
                <p className="text-sm text-muted-foreground">Checkpointing not available.</p>
              ) : (
                <dl className="grid grid-cols-2 gap-4 text-sm sm:grid-cols-4">
                  <Field label="Completed" value={formatNumber(cp.completed)} />
                  <Field label="Failed" value={formatNumber(cp.failed)} />
                  <Field label="In Progress" value={formatNumber(cp.inProgress)} />
                  <Field label="Restored" value={formatNumber(cp.restored)} />
                  <Field label="Last Size" value={cp.lastSizeBytes ? formatBytes(cp.lastSizeBytes) : "—"} />
                  <Field
                    label="Last Duration"
                    value={cp.lastDurationMs ? formatDuration(cp.lastDurationMs) : "—"}
                  />
                  <Field
                    label="Last Checkpoint"
                    value={cp.lastTimestampMs ? new Date(cp.lastTimestampMs).toLocaleString() : "—"}
                  />
                </dl>
              )}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function JobDetailWithParams() {
  const params = useSearchParams();
  const name = params.get("name") ?? "";
  if (!name) {
    return (
      <div className="p-6 text-sm text-muted-foreground">
        No job selected. <Link href="/" className="underline">Back to dashboard</Link>
      </div>
    );
  }
  return <JobDetail name={name} />;
}

export default function JobPage() {
  return (
    <React.Suspense fallback={<div className="p-6 text-sm text-muted-foreground">Loading…</div>}>
      <JobDetailWithParams />
    </React.Suspense>
  );
}
