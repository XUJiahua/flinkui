"use client";

import * as React from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Clock, CheckCircle2, AlertTriangle, Loader2, Undo2 } from "lucide-react";
import { api, ApiError, pollOperation } from "@/lib/api";
import type { ClusterInfo } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/components/ui/toast";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { ReplayDialog } from "@/components/replay-dialog";

interface ReplayPanelProps {
  jobName: string;
  cluster?: ClusterInfo;
}

/** ReplayPanel exposes the replay-from-timestamp entry point and, while a replay
 *  is active, a status banner (log-based verdict) plus a one-click revert. It
 *  renders nothing unless the feature is enabled on the backend. */
export function ReplayPanel({ jobName, cluster }: ReplayPanelProps) {
  const { toast } = useToast();
  const qc = useQueryClient();
  const [open, setOpen] = React.useState(false);
  const [confirmRevert, setConfirmRevert] = React.useState(false);
  const [reverting, setReverting] = React.useState(false);
  const [progress, setProgress] = React.useState("");

  const session = useQuery({
    queryKey: ["replay-session", jobName],
    queryFn: () => api.replaySession(jobName),
    enabled: !!cluster?.replayEnabled,
    refetchInterval: 5000,
  });

  if (!cluster?.replayEnabled) return null;

  const s = session.data;
  const active = s?.active;

  const doRevert = async () => {
    setConfirmRevert(false);
    setReverting(true);
    setProgress("starting…");
    try {
      const op = await api.revertReplay(jobName);
      const final = await pollOperation(op.id, (o) => setProgress(o.progress));
      if (final.status === "succeeded") {
        toast({ title: "Reverted", description: final.progress, variant: "success" });
      } else {
        toast({ title: "Revert failed", description: final.error || final.progress, variant: "error" });
      }
      await qc.invalidateQueries({ queryKey: ["job", jobName] });
      await qc.invalidateQueries({ queryKey: ["replay-session", jobName] });
    } catch (err) {
      toast({
        title: "Revert failed",
        description: err instanceof ApiError ? err.message : String(err),
        variant: "error",
      });
    } finally {
      setReverting(false);
      setProgress("");
    }
  };

  return (
    <div className="space-y-3">
      {active ? (
        <div className="rounded-md border border-amber-500/40 bg-amber-500/10 p-3 text-sm">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="warning">REPLAY ACTIVE</Badge>
            {s?.fromMillis && (
              <span className="text-muted-foreground">
                from {new Date(s.fromMillis).toLocaleString()}
              </span>
            )}
            {s?.sourceTable && <span className="font-mono text-xs">{s.sourceTable}</span>}
            <Verdict verdict={s?.verdict} />
            <div className="ml-auto flex items-center gap-2">
              {reverting && progress && (
                <span className="text-xs text-muted-foreground">{progress}</span>
              )}
              <Button size="sm" variant="outline" disabled={reverting} onClick={() => setConfirmRevert(true)}>
                {reverting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Undo2 className="h-4 w-4" />}
                Revert to normal
              </Button>
            </div>
          </div>
          {s?.verdictDetail && <p className="mt-2 text-xs text-muted-foreground">{s.verdictDetail}</p>}
        </div>
      ) : (
        <Button variant="outline" size="sm" onClick={() => setOpen(true)}>
          <Clock className="h-4 w-4" />
          Replay from timestamp…
        </Button>
      )}

      <ReplayDialog open={open} jobName={jobName} cluster={cluster} onClose={() => setOpen(false)} />
      <ConfirmDialog
        open={confirmRevert}
        title={`Revert replay on ${jobName}?`}
        description="Restores the pre-replay SQL and upgradeMode, then restarts so the job resumes from the latest checkpoint (no more replaying from the start)."
        confirmLabel="Revert"
        loading={reverting}
        onClose={() => setConfirmRevert(false)}
        onConfirm={doRevert}
      />
    </div>
  );
}

function Verdict({ verdict }: { verdict?: string }) {
  if (!verdict) return null;
  switch (verdict) {
    case "ok":
      return (
        <span className="inline-flex items-center gap-1 text-xs text-green-600">
          <CheckCircle2 className="h-3.5 w-3.5" /> seeking to timestamp offsets
        </span>
      );
    case "still-last-state":
      return (
        <span className="inline-flex items-center gap-1 text-xs text-destructive">
          <AlertTriangle className="h-3.5 w-3.5" /> still restoring from checkpoint
        </span>
      );
    default:
      return <span className="text-xs text-muted-foreground">verifying…</span>;
  }
}
