"use client";

import * as React from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, XCircle, Loader2, Circle } from "lucide-react";
import { api, ApiError, pollSwitchTask } from "@/lib/api";
import type { GroupView, SwitchTask } from "@/lib/types";
import { Dialog } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

interface SwitchWizardProps {
  open: boolean;
  group: GroupView;
  direction: "failover" | "failback";
  onClose: () => void;
}

/** SwitchWizard drives a failover/failback: shows the plan + auto-picked
 *  recovery points, requires explicit confirmation, then streams the five-step
 *  progress by polling the switch task (design failover §6). */
export function SwitchWizard({ open, group, direction, onClose }: SwitchWizardProps) {
  const qc = useQueryClient();
  const [confirmed, setConfirmed] = React.useState(false);
  const [task, setTask] = React.useState<SwitchTask | null>(null);
  const [running, setRunning] = React.useState(false);
  const [error, setError] = React.useState("");

  const from = direction === "failover" ? "primary" : "standby";
  const to = direction === "failover" ? "standby" : "primary";
  const fromSide = group[from];
  const toSide = group[to];

  // Recovery points (read-only, informational — the server auto-picks one).
  const rp = useQuery({
    queryKey: ["ha-recovery-points", group.name],
    queryFn: () => api.haRecoveryPoints(group.name),
    enabled: open,
  });

  const reset = () => {
    setConfirmed(false);
    setTask(null);
    setRunning(false);
    setError("");
  };

  const close = () => {
    if (running) return; // don't close mid-switch
    reset();
    onClose();
  };

  const start = async () => {
    setRunning(true);
    setError("");
    try {
      const started = direction === "failover" ? await api.failover(group.name) : await api.failback(group.name);
      setTask(started);
      const final = await pollSwitchTask(started.id, (t) => setTask(t));
      setTask(final);
      qc.invalidateQueries({ queryKey: ["ha-groups"] });
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setRunning(false);
    }
  };

  const points = rp.data?.recoveryPoints ?? [];

  return (
    <Dialog
      open={open}
      onClose={close}
      title={`${direction === "failover" ? "Failover" : "Failback"} — ${group.name}`}
      description={`Switch ${from} (${fromSide.clusterId}) → ${to} (${toSide.clusterId}). The active side will change.`}
      className="max-w-2xl"
    >
      {!task ? (
        <div className="space-y-4">
          <div className="rounded-md border p-3 text-sm">
            <div className="mb-2 font-medium">Plan</div>
            <ol className="list-decimal space-y-1 pl-5 text-muted-foreground">
              <li>Write neutral fencing token (fence both sides)</li>
              <li>Pick recovery point (savepoint if source healthy, else latest checkpoint)</li>
              <li>Stop {from} and wait for its JobManager pod to terminate</li>
              <li>Point fencing token → {toSide.clusterId}</li>
              <li>Start {to} from the recovery point</li>
            </ol>
          </div>

          <div className="rounded-md border p-3 text-sm">
            <div className="mb-1 font-medium">Latest recovery points (auto-selected server-side)</div>
            {rp.isLoading ? (
              <p className="text-muted-foreground">Loading…</p>
            ) : points.length === 0 ? (
              <p className="text-muted-foreground">None found — target will use last-state.</p>
            ) : (
              <ul className="space-y-1">
                {points.slice(0, 3).map((p) => (
                  <li key={p.path} className="flex items-center gap-2">
                    <Badge variant={p.type === "savepoint" ? "default" : "secondary"}>{p.type}</Badge>
                    <span className="truncate font-mono text-xs text-muted-foreground">{p.path}</span>
                  </li>
                ))}
              </ul>
            )}
          </div>

          <label className="flex items-center gap-2 text-sm">
            <input type="checkbox" checked={confirmed} onChange={(e) => setConfirmed(e.target.checked)} />
            I understand this stops {from} and starts {to} (brief unavailability).
          </label>

          {error && <p className="text-sm text-destructive">{error}</p>}

          <div className="flex justify-end gap-2">
            <Button variant="outline" onClick={close}>
              Cancel
            </Button>
            <Button variant="destructive" disabled={!confirmed} onClick={start}>
              Start {direction}
            </Button>
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          <div className="space-y-2">
            {task.steps.map((s) => (
              <div key={s.name} className="flex items-start gap-2 text-sm">
                <StepIcon status={s.status} />
                <div className="min-w-0">
                  <div className="font-medium">{s.name}</div>
                  {s.message && <div className="text-xs text-muted-foreground">{s.message}</div>}
                </div>
              </div>
            ))}
          </div>

          <div className="rounded-md bg-muted p-2 text-xs">
            recovery point: <span className="font-mono">{task.recoveryPoint.kind}</span>
            {task.recoveryPoint.path && <> — <span className="break-all font-mono">{task.recoveryPoint.path}</span></>}
          </div>

          {task.status === "failed" && (
            <p className="text-sm text-destructive">Failed: {task.error}</p>
          )}
          {task.status === "succeeded" && (
            <p className="text-sm text-green-700">Switch complete.</p>
          )}

          <div className="flex justify-end">
            <Button variant="outline" onClick={close} disabled={running}>
              {running ? "Working…" : "Close"}
            </Button>
          </div>
        </div>
      )}
    </Dialog>
  );
}

function StepIcon({ status }: { status: string }) {
  switch (status) {
    case "done":
      return <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-green-600" />;
    case "failed":
      return <XCircle className="mt-0.5 h-4 w-4 shrink-0 text-destructive" />;
    case "running":
      return <Loader2 className="mt-0.5 h-4 w-4 shrink-0 animate-spin text-blue-600" />;
    default:
      return <Circle className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />;
  }
}
