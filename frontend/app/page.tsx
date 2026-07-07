"use client";

import * as React from "react";
import Link from "next/link";
import { ArrowDown, ArrowUp, ChevronsUpDown, Circle, Wifi, WifiOff } from "lucide-react";
import { useAuthGuard } from "@/lib/use-auth";
import { useJobsStream } from "@/lib/use-jobs-stream";
import { Header } from "@/components/header";
import { StatusBadge } from "@/components/status-badge";
import { LifecycleActions } from "@/components/lifecycle-actions";
import { BatchActions } from "@/components/batch-actions";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { JobSummary } from "@/lib/types";

// Sortable columns and how to extract their comparable value from a job.
type SortKey = "jobName" | "statusText" | "desiredState" | "upgradeMode" | "parallelism" | "jobId";
type SortDir = "asc" | "desc";

const SORT_ACCESSORS: Record<SortKey, (job: JobSummary) => string | number> = {
  jobName: (j) => j.jobName || j.deployment,
  statusText: (j) => j.statusText,
  desiredState: (j) => j.desiredState,
  upgradeMode: (j) => j.upgradeMode,
  parallelism: (j) => j.parallelism,
  jobId: (j) => j.jobId,
};

/** compareJobs orders two jobs by the active sort key, using natural
 *  (locale + numeric-aware) comparison for strings so e.g. "job2" sorts before
 *  "job10". Ties fall back to the deployment name for a stable order. */
function compareJobs(a: JobSummary, b: JobSummary, key: SortKey, dir: SortDir): number {
  const av = SORT_ACCESSORS[key](a);
  const bv = SORT_ACCESSORS[key](b);
  let cmp: number;
  if (typeof av === "number" && typeof bv === "number") {
    cmp = av - bv;
  } else {
    cmp = String(av).localeCompare(String(bv), undefined, { numeric: true, sensitivity: "base" });
  }
  if (cmp === 0) {
    // Stable tiebreaker so equal rows never reshuffle between refreshes.
    cmp = a.deployment.localeCompare(b.deployment, undefined, { numeric: true });
  }
  return dir === "asc" ? cmp : -cmp;
}

export default function DashboardPage() {
  const auth = useAuthGuard();
  const { jobs, live, error, loading } = useJobsStream();

  // Default sort: Job name ascending (design: deterministic default order).
  const [sortKey, setSortKey] = React.useState<SortKey>("jobName");
  const [sortDir, setSortDir] = React.useState<SortDir>("asc");

  const toggleSort = React.useCallback((key: SortKey) => {
    setSortKey((prevKey) => {
      if (prevKey === key) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
        return prevKey;
      }
      setSortDir("asc");
      return key;
    });
  }, []);

  // Sort a copy so the source array (from the WS/poll) is never mutated. The
  // sort is reapplied on every render, so live updates keep the chosen order.
  const sortedJobs = React.useMemo(
    () => [...jobs].sort((a, b) => compareJobs(a, b, sortKey, sortDir)),
    [jobs, sortKey, sortDir],
  );

  // Multi-select for batch operations, keyed by deployment name.
  const [selected, setSelected] = React.useState<Set<string>>(new Set());

  const visibleNames = React.useMemo(() => sortedJobs.map((j) => j.deployment), [sortedJobs]);

  // Keep the selection in sync with what's actually listed (prune vanished jobs).
  React.useEffect(() => {
    setSelected((prev) => {
      const visible = new Set(visibleNames);
      let changed = false;
      const next = new Set<string>();
      prev.forEach((n) => {
        if (visible.has(n)) next.add(n);
        else changed = true;
      });
      return changed ? next : prev;
    });
  }, [visibleNames]);

  const selectedList = React.useMemo(() => [...selected], [selected]);
  const allSelected = visibleNames.length > 0 && visibleNames.every((n) => selected.has(n));
  const someSelected = visibleNames.some((n) => selected.has(n));

  const toggleAll = React.useCallback(() => {
    setSelected((prev) => {
      if (visibleNames.every((n) => prev.has(n))) return new Set();
      return new Set(visibleNames);
    });
  }, [visibleNames]);

  const toggleOne = React.useCallback((name: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  }, []);

  const clearSelection = React.useCallback(() => setSelected(new Set()), []);

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
              <>
                {someSelected && (
                  <div className="mb-3">
                    <BatchActions selected={selectedList} onClear={clearSelection} />
                  </div>
                )}
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-8">
                        <SelectAllCheckbox
                          checked={allSelected}
                          indeterminate={someSelected && !allSelected}
                          onChange={toggleAll}
                        />
                      </TableHead>
                      <SortableHead label="Job" sortKey="jobName" activeKey={sortKey} dir={sortDir} onSort={toggleSort} />
                      <SortableHead label="Status" sortKey="statusText" activeKey={sortKey} dir={sortDir} onSort={toggleSort} />
                      <SortableHead label="Desired" sortKey="desiredState" activeKey={sortKey} dir={sortDir} onSort={toggleSort} />
                      <SortableHead label="Upgrade" sortKey="upgradeMode" activeKey={sortKey} dir={sortDir} onSort={toggleSort} />
                      <SortableHead label="Parallelism" sortKey="parallelism" activeKey={sortKey} dir={sortDir} onSort={toggleSort} align="right" />
                      <SortableHead label="Job ID" sortKey="jobId" activeKey={sortKey} dir={sortDir} onSort={toggleSort} />
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {sortedJobs.map((job) => (
                      <TableRow key={job.deployment} data-state={selected.has(job.deployment) ? "selected" : undefined}>
                        <TableCell>
                          <input
                            type="checkbox"
                            className="h-4 w-4 rounded border-input align-middle accent-primary"
                            checked={selected.has(job.deployment)}
                            onChange={() => toggleOne(job.deployment)}
                            aria-label={`Select ${job.jobName || job.deployment}`}
                          />
                        </TableCell>
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
              </>
            )}
          </CardContent>
        </Card>
      </main>
    </div>
  );
}

/** SortableHead is a clickable column header that toggles sort on the column and
 *  shows the active sort direction. */
function SortableHead({
  label,
  sortKey,
  activeKey,
  dir,
  onSort,
  align = "left",
}: {
  label: string;
  sortKey: SortKey;
  activeKey: SortKey;
  dir: SortDir;
  onSort: (key: SortKey) => void;
  align?: "left" | "right";
}) {
  const active = activeKey === sortKey;
  return (
    <TableHead className={align === "right" ? "text-right" : undefined}>
      <button
        type="button"
        onClick={() => onSort(sortKey)}
        aria-sort={active ? (dir === "asc" ? "ascending" : "descending") : "none"}
        className={`inline-flex items-center gap-1 hover:text-foreground ${
          align === "right" ? "flex-row-reverse" : ""
        } ${active ? "text-foreground" : ""}`}
      >
        {label}
        {active ? (
          dir === "asc" ? (
            <ArrowUp className="h-3.5 w-3.5" />
          ) : (
            <ArrowDown className="h-3.5 w-3.5" />
          )
        ) : (
          <ChevronsUpDown className="h-3.5 w-3.5 opacity-40" />
        )}
      </button>
    </TableHead>
  );
}

function CenteredMessage({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex min-h-screen items-center justify-center text-sm text-muted-foreground">
      {children}
    </div>
  );
}

/** SelectAllCheckbox is a header checkbox supporting the indeterminate state
 *  (some-but-not-all rows selected), which HTML only exposes via a ref. */
function SelectAllCheckbox({
  checked,
  indeterminate,
  onChange,
}: {
  checked: boolean;
  indeterminate: boolean;
  onChange: () => void;
}) {
  const ref = React.useRef<HTMLInputElement>(null);
  React.useEffect(() => {
    if (ref.current) ref.current.indeterminate = indeterminate;
  }, [indeterminate]);
  return (
    <input
      ref={ref}
      type="checkbox"
      className="h-4 w-4 rounded border-input align-middle accent-primary"
      checked={checked}
      onChange={onChange}
      aria-label="Select all jobs"
    />
  );
}
