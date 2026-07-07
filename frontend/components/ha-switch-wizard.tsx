"use client";

import * as React from "react";
import { useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, XCircle, Loader2, Circle } from "lucide-react";
import { api, ApiError, pollHATask } from "@/lib/api";
import type { HATask, LocalView } from "@/lib/types";
import { Dialog } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";

interface HASwitchWizardProps {
  open: boolean;
  view: LocalView;
  op: "release" | "promote";
  onClose: () => void;
}

/** HASwitchWizard drives a LOCAL Release (step down) or Promote (take over),
 *  requiring confirmation (and, for a forced promote, a data-loss ack), then
 *  streams the step progress (design failover-decentralized §4/§8). */
export function HASwitchWizard({ open, view, op, onClose }: HASwitchWizardProps) {
  const qc = useQueryClient();
  const [confirmed, setConfirmed] = React.useState(false);
  const [force, setForce] = React.useState(false);
  const [ackDataLoss, setAckDataLoss] = React.useState(false);
  const [task, setTask] = React.useState<HATask | null>(null);
  const [running, setRunning] = React.useState(false);
  const [error, setError] = React.useState("");

  const isPromote = op === "promote";
  const canStart = confirmed && (!isPromote || !force || ackDataLoss);

  const close = () => {
    if (running) return;
    setConfirmed(false);
    setForce(false);
    setAckDataLoss(false);
    setTask(null);
    setError("");
    onClose();
  };

  const start = async () => {
    setRunning(true);
    setError("");
    try {
      const started = isPromote ? await api.promote(view.name, force, ackDataLoss) : await api.release(view.name);
      setTask(started);
      const final = await pollHATask(started.id, (t) => setTask(t));
      setTask(final);
      qc.invalidateQueries({ queryKey: ["ha"] });
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setRunning(false);
    }
  };

  return (
    <Dialog
      open={open}
      onClose={close}
      title={`${isPromote ? "Promote (take over)" : "Release (step down)"} — ${view.name}`}
      description={
        isPromote
          ? `Claim the fencing token for ${view.clusterId} and start the local job from a recovery point.`
          : `Stop the local job on ${view.clusterId}, then release the fencing token so the peer can take over.`
      }
      className="max-w-2xl"
    >
      {!task ? (
        <div className="space-y-4">
          <div className="rounded-md border p-3 text-sm">
            <div className="mb-2 font-medium">This side ({view.clusterId})</div>
            <ol className="list-decimal space-y-1 pl-5 text-muted-foreground">
              {isPromote ? (
                <>
                  <li>Read the shared handoff record</li>
                  <li>Pick recovery point (handoff savepoint, else latest checkpoint)</li>
                  <li>Claim fencing token → {view.clusterId} (epoch+1)</li>
                  <li>Start the local job from the recovery point</li>
                  <li>Verify local RUNNING/STABLE</li>
                </>
              ) : (
                <>
                  <li>Savepoint the local job (if healthy)</li>
                  <li>Suspend the local job & wait for its JobManager to stop</li>
                  <li>Set the fencing token to neutral (fence both sides)</li>
                  <li>Publish the handoff record (released)</li>
                </>
              )}
            </ol>
          </div>

          {isPromote && (
            <div className="space-y-2 rounded-md border border-amber-500 bg-amber-50 p-3 text-sm text-amber-900">
              <label className="flex items-center gap-2">
                <input type="checkbox" checked={force} onChange={(e) => setForce(e.target.checked)} />
                Force take-over (peer has <b>not</b> released — disaster/partition)
              </label>
              {force && (
                <label className="flex items-center gap-2">
                  <input type="checkbox" checked={ackDataLoss} onChange={(e) => setAckDataLoss(e.target.checked)} />
                  I acknowledge possible data loss and split-brain risk (peer may still be running)
                </label>
              )}
            </div>
          )}

          <label className="flex items-center gap-2 text-sm">
            <input type="checkbox" checked={confirmed} onChange={(e) => setConfirmed(e.target.checked)} />
            I understand this acts on the local cluster only ({view.clusterId}).
          </label>

          {error && <p className="text-sm text-destructive">{error}</p>}

          <div className="flex justify-end gap-2">
            <Button variant="outline" onClick={close}>
              Cancel
            </Button>
            <Button variant="destructive" disabled={!canStart} onClick={start}>
              {isPromote ? "Promote" : "Release"}
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
                  {s.message && <div className="break-all text-xs text-muted-foreground">{s.message}</div>}
                </div>
              </div>
            ))}
          </div>
          {task.recoveryPoint?.kind && (
            <div className="rounded-md bg-muted p-2 text-xs">
              recovery point: <span className="font-mono">{task.recoveryPoint.kind}</span>
              {task.recoveryPoint.path && <> — <span className="break-all font-mono">{task.recoveryPoint.path}</span></>}
              {task.epoch ? <> · epoch {task.epoch}</> : null}
            </div>
          )}
          {task.status === "failed" && <p className="text-sm text-destructive">Failed: {task.error}</p>}
          {task.status === "succeeded" && (
            <p className="text-sm text-green-700">{isPromote ? "Promoted." : "Released."} Coordinate with the peer side.</p>
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
