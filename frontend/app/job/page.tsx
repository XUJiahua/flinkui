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
            <CardTitle>JobManager Logs</CardTitle>
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
