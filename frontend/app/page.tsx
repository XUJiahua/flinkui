"use client";

import * as React from "react";
import Link from "next/link";
import { Circle, Wifi, WifiOff } from "lucide-react";
import { useAuthGuard } from "@/lib/use-auth";
import { useJobsStream } from "@/lib/use-jobs-stream";
import { Header } from "@/components/header";
import { StatusBadge } from "@/components/status-badge";
import { LifecycleActions } from "@/components/lifecycle-actions";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

export default function DashboardPage() {
  const auth = useAuthGuard();
  const { jobs, live, error, loading } = useJobsStream();

  if (auth.isLoading) {
    return <CenteredMessage>Loading…</CenteredMessage>;
  }
  if (auth.isError) {
    return null; // redirecting to /login
  }

  return (
    <div>
      <Header />
      <main className="mx-auto max-w-7xl px-4 py-6">
        <Card>
          <CardHeader className="flex-row items-center justify-between space-y-0">
            <CardTitle>FlinkDeployments</CardTitle>
            <span className="flex items-center gap-1 text-xs text-muted-foreground">
              {live ? (
                <>
                  <Wifi className="h-3.5 w-3.5 text-green-600" /> live
                </>
              ) : (
                <>
                  <WifiOff className="h-3.5 w-3.5" /> polling
                </>
              )}
            </span>
          </CardHeader>
          <CardContent>
            {error && <p className="mb-4 text-sm text-destructive">{error}</p>}
            {loading ? (
              <p className="py-8 text-center text-sm text-muted-foreground">Loading jobs…</p>
            ) : jobs.length === 0 ? (
              <p className="py-8 text-center text-sm text-muted-foreground">
                No FlinkDeployments found in this namespace.
              </p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Job</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Desired</TableHead>
                    <TableHead>Upgrade</TableHead>
                    <TableHead className="text-right">Parallelism</TableHead>
                    <TableHead>Job ID</TableHead>
                    <TableHead className="text-right">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {jobs.map((job) => (
                    <TableRow key={job.deployment}>
                      <TableCell>
                        <Link
                          href={`/job?name=${encodeURIComponent(job.deployment)}`}
                          className="font-medium hover:underline"
                        >
                          {job.jobName || job.deployment}
                        </Link>
                        <div className="text-xs text-muted-foreground">{job.namespace}</div>
                      </TableCell>
                      <TableCell>
                        <StatusBadge job={job} />
                      </TableCell>
                      <TableCell>
                        <span className="flex items-center gap-1 text-sm">
                          <Circle
                            className={
                              job.desiredState === "running"
                                ? "h-2 w-2 fill-green-600 text-green-600"
                                : "h-2 w-2 fill-muted-foreground text-muted-foreground"
                            }
                          />
                          {job.desiredState || "—"}
                        </span>
                      </TableCell>
                      <TableCell className="text-sm">{job.upgradeMode || "—"}</TableCell>
                      <TableCell className="text-right text-sm">{job.parallelism || "—"}</TableCell>
                      <TableCell className="max-w-[10rem] truncate font-mono text-xs text-muted-foreground">
                        {job.jobId || "—"}
                      </TableCell>
                      <TableCell>
                        <div className="flex justify-end">
                          <LifecycleActions jobName={job.deployment} compact />
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </main>
    </div>
  );
}

function CenteredMessage({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex min-h-screen items-center justify-center text-sm text-muted-foreground">
      {children}
    </div>
  );
}
