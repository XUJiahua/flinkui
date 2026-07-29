"use client";

import * as React from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle } from "lucide-react";
import { api, ApiError, pollOperation } from "@/lib/api";
import type { ClusterInfo, ReplayPlan } from "@/lib/types";
import { Dialog } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useToast } from "@/components/ui/toast";
import { PlanSteps } from "@/components/plan-steps";

interface ReplayDialogProps {
  open: boolean;
  jobName: string;
  cluster?: ClusterInfo;
  onClose: () => void;
}

/** ReplayDialog drives the "replay from timestamp" flow: pick a start time and
 *  Kafka source, preview the SQL diff (scan.startup.mode=timestamp) plus the
 *  forced upgradeMode=stateless, then execute. It is an OUT-OF-BAND change; on
 *  GitOps-managed targets it requires explicit drift confirmation. */
export function ReplayDialog({ open, jobName, cluster, onClose }: ReplayDialogProps) {
  const { toast } = useToast();
  const qc = useQueryClient();

  const [hoursAgo, setHoursAgo] = React.useState("2");
  const [sourceTable, setSourceTable] = React.useState("");
  const [confirmDrift, setConfirmDrift] = React.useState(false);
  const [plan, setPlan] = React.useState<ReplayPlan | null>(null);
  const [previewing, setPreviewing] = React.useState(false);
  const [submitting, setSubmitting] = React.useState(false);
  const [progress, setProgress] = React.useState("");

  // Load the SQL so we can offer the detected Kafka source tables.
  const sql = useQuery({
    queryKey: ["job-sql", jobName],
    queryFn: () => api.getSQL(jobName),
    enabled: open,
  });
  const sources = sql.data?.kafkaSources ?? [];

  React.useEffect(() => {
    if (open) {
      setPlan(null);
      setProgress("");
      setConfirmDrift(false);
    }
  }, [open]);

  // Auto-pick the sole Kafka source.
  React.useEffect(() => {
    if (sources.length === 1 && !sourceTable) setSourceTable(sources[0]);
  }, [sources, sourceTable]);

  const hours = Number(hoursAgo);
  const validHours = Number.isFinite(hours) && hours > 0;
  const gitops = cluster?.replayGitopsManaged ?? false;

  const doPreview = async () => {
    if (!validHours) return;
    setPreviewing(true);
    setPlan(null);
    try {
      const p = await api.replayPreview(jobName, { hoursAgo: hours, sourceTable: sourceTable || undefined });
      setPlan(p);
    } catch (err) {
      toast({
        title: "Preview failed",
        description: err instanceof ApiError ? err.message : String(err),
        variant: "error",
      });
    } finally {
      setPreviewing(false);
    }
  };

  const doReplay = async () => {
    setSubmitting(true);
    setProgress("starting…");
    try {
      const op = await api.startReplay(jobName, {
        hoursAgo: hours,
        sourceTable: sourceTable || undefined,
        confirmDrift: gitops ? true : undefined,
      });
      const final = await pollOperation(op.id, (o) => setProgress(o.progress));
      if (final.status === "succeeded") {
        toast({ title: "Replay started", description: final.progress, variant: "success" });
        await qc.invalidateQueries({ queryKey: ["job", jobName] });
        await qc.invalidateQueries({ queryKey: ["replay-session", jobName] });
        onClose();
      } else {
        toast({ title: "Replay failed", description: final.error || final.progress, variant: "error" });
      }
    } catch (err) {
      toast({
        title: "Replay failed",
        description: err instanceof ApiError ? err.message : String(err),
        variant: "error",
      });
    } finally {
      setSubmitting(false);
      setProgress("");
    }
  };

  const canReplay = !!plan && !submitting && (!gitops || confirmDrift);

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={`Replay ${jobName} from a timestamp`}
      description="Rewrites the Kafka source to start at a timestamp and forces a stateless restart. Revert when the replay has caught up."
      className="max-w-3xl"
    >
      <div className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-1">
            <label className="text-sm font-medium">Start from (hours ago)</label>
            <Input
              type="number"
              min="0"
              step="0.5"
              value={hoursAgo}
              onChange={(e) => {
                setHoursAgo(e.target.value);
                setPlan(null);
              }}
            />
          </div>
          <div className="space-y-1">
            <label className="text-sm font-medium">Kafka source table</label>
            {sources.length > 1 ? (
              <select
                className="h-9 w-full rounded-md border bg-background px-3 text-sm"
                value={sourceTable}
                onChange={(e) => {
                  setSourceTable(e.target.value);
                  setPlan(null);
                }}
              >
                <option value="">Select a table…</option>
                {sources.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            ) : (
              <Input value={sourceTable || (sql.isLoading ? "loading…" : "—")} readOnly />
            )}
          </div>
        </div>

        <Button variant="outline" onClick={doPreview} disabled={!validHours || previewing}>
          {previewing ? "Previewing…" : "Preview changes"}
        </Button>

        {plan && (
          <div className="space-y-3">
            <p className="text-sm">
              Rewrites table <span className="font-mono">{plan.table}</span>, sets{" "}
              <span className="font-mono">upgradeMode={plan.upgradeMode}</span>, replay start{" "}
              <span className="font-mono">{new Date(plan.fromMillis).toLocaleString()}</span>.
            </p>
            <SqlDiff original={plan.originalSql} next={plan.newSql} />
            <PlanSteps steps={plan.steps} title="What the replay will do" />
          </div>
        )}

        {gitops && (
          <label className="flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-3 text-sm">
            <input
              type="checkbox"
              className="mt-0.5"
              checked={confirmDrift}
              onChange={(e) => setConfirmDrift(e.target.checked)}
            />
            <span className="flex items-start gap-1.5">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />
              This target is GitOps-managed. A live SQL edit will be reverted by the next sync
              (Argo/Flux). I understand and want to proceed anyway.
            </span>
          </label>
        )}

        {submitting && progress && (
          <p className="text-xs text-muted-foreground">Replay: {progress}</p>
        )}

        <div className="sticky bottom-0 flex justify-end gap-2 border-t bg-card pt-3">
          <Button variant="outline" onClick={onClose} disabled={submitting}>
            Cancel
          </Button>
          <Button variant="destructive" onClick={doReplay} disabled={!canReplay}>
            {submitting ? "Starting…" : "Start replay"}
          </Button>
        </div>
      </div>
    </Dialog>
  );
}

type DiffKind = "equal" | "del" | "add" | "mod";
interface DiffRow {
  left?: string;
  right?: string;
  kind: DiffKind;
}

// diffRows computes a line-level, LCS-aligned diff so unchanged lines stay on
// the same row on both sides and edits line up as replacements.
function diffRows(a: string[], b: string[]): DiffRow[] {
  const n = a.length;
  const m = b.length;
  // LCS length table.
  const dp: number[][] = Array.from({ length: n + 1 }, () => new Array<number>(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = a[i] === b[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }
  const ops: { t: "eq" | "del" | "add"; l?: string; r?: string }[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) {
      ops.push({ t: "eq", l: a[i], r: b[j] });
      i++;
      j++;
    } else if (dp[i + 1][j] >= dp[i][j + 1]) {
      ops.push({ t: "del", l: a[i] });
      i++;
    } else {
      ops.push({ t: "add", r: b[j] });
      j++;
    }
  }
  while (i < n) ops.push({ t: "del", l: a[i++] });
  while (j < m) ops.push({ t: "add", r: b[j++] });

  // Coalesce runs of del/add so replacements pair up side by side.
  const rows: DiffRow[] = [];
  let k = 0;
  while (k < ops.length) {
    if (ops[k].t === "eq") {
      rows.push({ left: ops[k].l, right: ops[k].r, kind: "equal" });
      k++;
      continue;
    }
    const dels: string[] = [];
    const adds: string[] = [];
    while (k < ops.length && ops[k].t !== "eq") {
      if (ops[k].t === "del") dels.push(ops[k].l as string);
      else adds.push(ops[k].r as string);
      k++;
    }
    const max = Math.max(dels.length, adds.length);
    for (let x = 0; x < max; x++) {
      const l = dels[x];
      const r = adds[x];
      if (l !== undefined && r !== undefined) rows.push({ left: l, right: r, kind: "mod" });
      else if (l !== undefined) rows.push({ left: l, kind: "del" });
      else rows.push({ right: r, kind: "add" });
    }
  }
  return rows;
}

/** SqlDiff renders a side-by-side (before | after) line diff. Removed/changed
 *  lines are highlighted on the left, added/changed on the right, aligned by an
 *  LCS so unchanged context stays level. */
function SqlDiff({ original, next }: { original: string; next: string }) {
  const rows = React.useMemo(() => diffRows(original.split("\n"), next.split("\n")), [original, next]);
  const cell = "whitespace-pre-wrap break-all px-2 py-0.5";
  return (
    <div className="overflow-hidden rounded-md border font-mono text-xs">
      <div className="grid grid-cols-2 border-b bg-muted/50 font-sans text-[11px] font-medium text-muted-foreground">
        <div className="border-r px-2 py-1">Before</div>
        <div className="px-2 py-1">After</div>
      </div>
      <div className="grid max-h-80 grid-cols-2 overflow-auto leading-relaxed">
        {rows.map((row, idx) => {
          const leftBg = row.kind === "del" || row.kind === "mod" ? "bg-red-500/15" : "";
          const rightBg = row.kind === "add" || row.kind === "mod" ? "bg-green-500/15" : "";
          const leftEmpty = row.left === undefined;
          const rightEmpty = row.right === undefined;
          return (
            <React.Fragment key={idx}>
              <div className={`border-r ${cell} ${leftBg} ${leftEmpty ? "bg-muted/20" : ""}`}>
                {!leftEmpty && (
                  <>
                    <span className="select-none text-muted-foreground">
                      {row.kind === "del" || row.kind === "mod" ? "- " : "  "}
                    </span>
                    {row.left}
                  </>
                )}
              </div>
              <div className={`${cell} ${rightBg} ${rightEmpty ? "bg-muted/20" : ""}`}>
                {!rightEmpty && (
                  <>
                    <span className="select-none text-muted-foreground">
                      {row.kind === "add" || row.kind === "mod" ? "+ " : "  "}
                    </span>
                    {row.right}
                  </>
                )}
              </div>
            </React.Fragment>
          );
        })}
      </div>
    </div>
  );
}
