"use client";

import * as React from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Pause, Play, RotateCw, Save, Loader2, X } from "lucide-react";
import { api, ApiError, pollOperation } from "@/lib/api";
import type { Operation } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { useToast } from "@/components/ui/toast";
import { ConfirmDialog } from "@/components/confirm-dialog";

interface BatchActionsProps {
  /** Selected FlinkDeployment names (deployment, not display job name). */
  selected: string[];
  /** Clears the selection after a batch completes. */
  onClear: () => void;
}

/** BatchActions runs a lifecycle operation across every selected deployment.
 *  Operations fan out client-side over the existing per-job endpoints; async
 *  ops (restart/savepoint) are polled to completion. Results are aggregated
 *  into a single success/failure summary toast. Rollback is intentionally
 *  excluded because it needs a per-job recovery-point path. */
export function BatchActions({ selected, onClear }: BatchActionsProps) {
  const { toast } = useToast();
  const qc = useQueryClient();
  const [busy, setBusy] = React.useState<string | null>(null);
  const [progress, setProgress] = React.useState("");
  const [confirmRestart, setConfirmRestart] = React.useState(false);

  const count = selected.length;

  const refresh = () => qc.invalidateQueries({ queryKey: ["jobs"] });

  // runBatch applies perJob to every selection in parallel, tracks completion
  // progress, and reports an aggregated result. perJob must throw on failure.
  const runBatch = async (label: string, perJob: (name: string) => Promise<void>) => {
    setBusy(label);
    setProgress(`0/${count}`);
    let done = 0;
    const results = await Promise.allSettled(
      selected.map(async (name) => {
        try {
          await perJob(name);
        } catch (err) {
          const msg = err instanceof ApiError ? err.message : String(err);
          throw new Error(`${name}: ${msg}`);
        } finally {
          done += 1;
          setProgress(`${done}/${count}`);
        }
      }),
    );

    const failures = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
    if (failures.length === 0) {
      toast({ title: `${label}: ${count} job(s) succeeded`, variant: "success" });
    } else {
      toast({
        title: `${label}: ${count - failures.length}/${count} succeeded`,
        description: failures.map((f) => String(f.reason?.message ?? f.reason)).join("; "),
        variant: "error",
      });
    }

    setBusy(null);
    setProgress("");
    refresh();
    onClear();
  };

  const runImmediate = (label: string, fn: (name: string) => Promise<unknown>) =>
    runBatch(label, async (name) => {
      await fn(name);
    });

  const runAsync = (label: string, start: (name: string) => Promise<Operation>) =>
    runBatch(label, async (name) => {
      const op = await start(name);
      const final = await pollOperation(op.id);
      if (final.status !== "succeeded") {
        throw new Error(final.error || final.progress || "operation failed");
      }
    });

  const disabled = count === 0 || !!busy;

  return (
    <div className="flex flex-wrap items-center gap-2 rounded-md border bg-muted/40 p-2">
      <span className="px-1 text-sm font-medium">{count} selected</span>

      <Button
        variant="outline"
        size="sm"
        disabled={disabled}
        onClick={() => runImmediate("Suspend", (n) => api.suspend(n))}
      >
        <Pause className="h-4 w-4" />
        Suspend
      </Button>

      <Button
        variant="outline"
        size="sm"
        disabled={disabled}
        onClick={() => runImmediate("Resume", (n) => api.resume(n))}
      >
        <Play className="h-4 w-4" />
        Resume
      </Button>

      <Button
        variant="outline"
        size="sm"
        disabled={disabled}
        onClick={() => setConfirmRestart(true)}
      >
        {busy === "Restart" ? <Loader2 className="h-4 w-4 animate-spin" /> : <RotateCw className="h-4 w-4" />}
        Restart
      </Button>

      <Button
        variant="outline"
        size="sm"
        disabled={disabled}
        onClick={() => runAsync("Savepoint", (n) => api.savepoint(n))}
      >
        {busy === "Savepoint" ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
        Savepoint
      </Button>

      {busy && progress && (
        <span className="text-xs text-muted-foreground">
          {busy}: {progress}
        </span>
      )}

      <Button
        variant="ghost"
        size="sm"
        className="ml-auto"
        disabled={!!busy}
        onClick={onClear}
      >
        <X className="h-4 w-4" />
        Clear
      </Button>

      <ConfirmDialog
        open={confirmRestart}
        title={`Restart ${count} job(s)?`}
        description="Each job is suspended, waits for its JobManager pod to terminate, then resumes from last-state. The jobs will be briefly unavailable."
        confirmLabel="Restart"
        destructive
        loading={busy === "Restart"}
        onClose={() => setConfirmRestart(false)}
        onConfirm={async () => {
          setConfirmRestart(false);
          await runAsync("Restart", (n) => api.restart(n));
        }}
      />
    </div>
  );
}
