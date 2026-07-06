"use client";

import * as React from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, ApiError } from "@/lib/api";
import { Dialog } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/components/ui/toast";
import { cn } from "@/lib/utils";

interface RollbackDialogProps {
  open: boolean;
  jobName: string;
  onClose: () => void;
}

/** RollbackDialog lists S3 recovery points and force-redeploys from the chosen
 *  savepoint/checkpoint. High-risk: requires explicit selection + confirm. */
export function RollbackDialog({ open, jobName, onClose }: RollbackDialogProps) {
  const { toast } = useToast();
  const qc = useQueryClient();
  const [selected, setSelected] = React.useState("");
  const [manual, setManual] = React.useState("");
  const [submitting, setSubmitting] = React.useState(false);

  const rp = useQuery({
    queryKey: ["recovery-points", jobName],
    queryFn: () => api.recoveryPoints(jobName),
    enabled: open,
  });

  const path = manual.trim() || selected;

  const onConfirm = async () => {
    if (!path) return;
    setSubmitting(true);
    try {
      await api.rollback(jobName, path);
      toast({ title: "Rollback requested", description: path, variant: "success" });
      await qc.invalidateQueries({ queryKey: ["job", jobName] });
      onClose();
    } catch (err) {
      toast({
        title: "Rollback failed",
        description: err instanceof ApiError ? err.message : String(err),
        variant: "error",
      });
    } finally {
      setSubmitting(false);
    }
  };

  const points = rp.data?.recoveryPoints ?? [];

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={`Rollback ${jobName}`}
      description="Force redeploy from a savepoint/checkpoint. This restarts the job."
      className="max-w-2xl"
    >
      <div className="space-y-4">
        <div className="max-h-72 overflow-auto rounded-md border">
          {rp.isLoading && <p className="p-4 text-sm text-muted-foreground">Loading recovery points…</p>}
          {rp.isError && (
            <p className="p-4 text-sm text-destructive">
              {rp.error instanceof ApiError ? rp.error.message : "failed to load recovery points"}
            </p>
          )}
          {!rp.isLoading && !rp.isError && points.length === 0 && (
            <p className="p-4 text-sm text-muted-foreground">No recovery points found.</p>
          )}
          {points.map((p) => (
            <button
              key={p.path}
              type="button"
              onClick={() => {
                setSelected(p.path);
                setManual("");
              }}
              className={cn(
                "flex w-full items-center justify-between gap-2 border-b px-3 py-2 text-left text-sm last:border-0 hover:bg-muted/50",
                selected === p.path && !manual && "bg-muted",
              )}
            >
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <Badge variant={p.type === "savepoint" ? "default" : "secondary"}>{p.type}</Badge>
                  <span className="font-medium">{p.name}</span>
                </div>
                <div className="truncate text-xs text-muted-foreground">{p.path}</div>
              </div>
              <span className="shrink-0 text-xs text-muted-foreground">
                {p.modified ? new Date(p.modified).toLocaleString() : ""}
              </span>
            </button>
          ))}
        </div>

        <div className="space-y-1">
          <label className="text-sm font-medium">Or enter a path manually</label>
          <Input
            placeholder="s3://bucket/savepoints/<job>/savepoint-xxxx"
            value={manual}
            onChange={(e) => {
              setManual(e.target.value);
              setSelected("");
            }}
          />
        </div>

        {path && (
          <p className="break-all rounded-md bg-muted p-2 text-xs">
            Selected: <span className="font-mono">{path}</span>
          </p>
        )}

        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={onClose} disabled={submitting}>
            Cancel
          </Button>
          <Button variant="destructive" onClick={onConfirm} disabled={!path || submitting}>
            {submitting ? "Rolling back…" : "Rollback"}
          </Button>
        </div>
      </div>
    </Dialog>
  );
}
