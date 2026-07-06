"use client";

import * as React from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Pause, Play, RotateCw, Save, History } from "lucide-react";
import { api, ApiError } from "@/lib/api";
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

/** LifecycleActions renders suspend/resume/restart/savepoint/rollback controls
 *  with confirmations for high-risk ops (design §4.2). */
export function LifecycleActions({ jobName, size = "sm", compact }: LifecycleActionsProps) {
  const { toast } = useToast();
  const qc = useQueryClient();
  const [busy, setBusy] = React.useState<string | null>(null);
  const [confirmRestart, setConfirmRestart] = React.useState(false);
  const [rollbackOpen, setRollbackOpen] = React.useState(false);

  const refresh = () => {
    qc.invalidateQueries({ queryKey: ["jobs"] });
    qc.invalidateQueries({ queryKey: ["job", jobName] });
  };

  const run = async (label: string, fn: () => Promise<unknown>, successMsg?: string) => {
    setBusy(label);
    try {
      const res = await fn();
      const desc =
        successMsg ?? (res && typeof res === "object" && "location" in res
          ? String((res as { location: string }).location)
          : undefined);
      toast({ title: `${label} requested`, description: desc, variant: "success" });
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

  const label = (text: string) => (compact ? null : <span>{text}</span>);

  return (
    <div className="flex flex-wrap items-center gap-2">
      <Button
        variant="outline"
        size={size}
        disabled={!!busy}
        onClick={() => run("Suspend", () => api.suspend(jobName))}
      >
        <Pause className="h-4 w-4" />
        {label("Suspend")}
      </Button>

      <Button
        variant="outline"
        size={size}
        disabled={!!busy}
        onClick={() => run("Resume", () => api.resume(jobName))}
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
        <RotateCw className="h-4 w-4" />
        {label("Restart")}
      </Button>

      <Button
        variant="outline"
        size={size}
        disabled={!!busy}
        onClick={() => run("Savepoint", () => api.savepoint(jobName))}
      >
        <Save className="h-4 w-4" />
        {label(busy === "Savepoint" ? "Saving…" : "Savepoint")}
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

      <ConfirmDialog
        open={confirmRestart}
        title={`Restart ${jobName}?`}
        description="This suspends the job, waits for the JobManager pod to terminate, then resumes from last-state. The job will be briefly unavailable."
        confirmLabel="Restart"
        destructive
        loading={busy === "Restart"}
        onClose={() => setConfirmRestart(false)}
        onConfirm={async () => {
          await run("Restart", () => api.restart(jobName));
          setConfirmRestart(false);
        }}
      />

      <RollbackDialog open={rollbackOpen} jobName={jobName} onClose={() => setRollbackOpen(false)} />
    </div>
  );
}
