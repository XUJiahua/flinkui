"use client";

import * as React from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Pause, Play, RotateCw, Save, History, Loader2 } from "lucide-react";
import { api, ApiError, pollOperation } from "@/lib/api";
import type { Operation } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { useToast } from "@/components/ui/toast";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { RollbackDialog } from "@/components/rollback-dialog";

interface LifecycleActionsProps {
  jobName: string;
  size?: "sm" | "default";
  /** compact hides labels (used in dense table rows). */
  compact?: boolean;
}

/** LifecycleActions renders suspend/resume/restart/savepoint/rollback controls.
 *  Restart and savepoint are asynchronous: after triggering, their progress is
 *  polled and shown inline until they succeed/fail/time out (design §4.2). */
export function LifecycleActions({ jobName, size = "sm", compact }: LifecycleActionsProps) {
  const { toast } = useToast();
  const qc = useQueryClient();
  const [busy, setBusy] = React.useState<string | null>(null);
  const [progress, setProgress] = React.useState<string>("");
  const [confirmRestart, setConfirmRestart] = React.useState(false);
  const [rollbackOpen, setRollbackOpen] = React.useState(false);

  const refresh = () => {
    qc.invalidateQueries({ queryKey: ["jobs"] });
    qc.invalidateQueries({ queryKey: ["job", jobName] });
  };

  // Immediate operations (suspend/resume): single request, then refresh.
  const runImmediate = async (label: string, fn: () => Promise<unknown>) => {
    setBusy(label);
    try {
      await fn();
      toast({ title: `${label} requested`, variant: "success" });
      refresh();
    } catch (err) {
      toast({
        title: `${label} failed`,
        description: err instanceof ApiError ? err.message : String(err),
        variant: "error",
      });
    } finally {
      setBusy(null);
    }
  };

  // Async operations (savepoint/restart): trigger -> poll progress -> result.
  const runAsync = async (label: string, start: () => Promise<Operation>) => {
    setBusy(label);
    setProgress("starting…");
    try {
      const op = await start();
      const final = await pollOperation(op.id, (o) => setProgress(o.progress));
      if (final.status === "succeeded") {
        toast({
          title: `${label} completed`,
          description: final.result || undefined,
          variant: "success",
        });
      } else {
        toast({
          title: `${label} failed`,
          description: final.error || final.progress,
          variant: "error",
        });
      }
      refresh();
    } catch (err) {
      toast({
        title: `${label} failed`,
        description: err instanceof ApiError ? err.message : String(err),
        variant: "error",
      });
    } finally {
      setBusy(null);
      setProgress("");
    }
  };

  const label = (text: string) => (compact ? null : <span>{text}</span>);
  const spin = <Loader2 className="h-4 w-4 animate-spin" />;

  return (
    <div className="flex flex-wrap items-center gap-2">
      <Button
        variant="outline"
        size={size}
        disabled={!!busy}
        onClick={() => runImmediate("Suspend", () => api.suspend(jobName))}
      >
        <Pause className="h-4 w-4" />
        {label("Suspend")}
      </Button>

      <Button
        variant="outline"
        size={size}
        disabled={!!busy}
        onClick={() => runImmediate("Resume", () => api.resume(jobName))}
      >
        <Play className="h-4 w-4" />
        {label("Resume")}
      </Button>

      <Button
        variant="outline"
        size={size}
        disabled={!!busy}
        onClick={() => setConfirmRestart(true)}
      >
        {busy === "Restart" ? spin : <RotateCw className="h-4 w-4" />}
        {label("Restart")}
      </Button>

      <Button
        variant="outline"
        size={size}
        disabled={!!busy}
        onClick={() => runAsync("Savepoint", () => api.savepoint(jobName))}
      >
        {busy === "Savepoint" ? spin : <Save className="h-4 w-4" />}
        {label("Savepoint")}
      </Button>

      <Button
        variant="destructive"
        size={size}
        disabled={!!busy}
        onClick={() => setRollbackOpen(true)}
      >
        <History className="h-4 w-4" />
        {label("Rollback")}
      </Button>

      {busy && progress && (
        <span className="text-xs text-muted-foreground">
          {busy}: {progress}
        </span>
      )}

      <ConfirmDialog
        open={confirmRestart}
        title={`Restart ${jobName}?`}
        description="This suspends the job, waits for the JobManager pod to terminate, then resumes from last-state. The job will be briefly unavailable."
        confirmLabel="Restart"
        destructive
        loading={busy === "Restart"}
        onClose={() => setConfirmRestart(false)}
        onConfirm={async () => {
          setConfirmRestart(false);
          await runAsync("Restart", () => api.restart(jobName));
        }}
      />

      <RollbackDialog open={rollbackOpen} jobName={jobName} onClose={() => setRollbackOpen(false)} />
    </div>
  );
}
