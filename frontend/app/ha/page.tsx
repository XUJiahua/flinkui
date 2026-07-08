"use client";

import * as React from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, ShieldCheck, ShieldAlert, HelpCircle } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { useAuthGuard } from "@/lib/use-auth";
import { Header } from "@/components/header";
import { StatusBadge } from "@/components/status-badge";
import { HASwitchWizard } from "@/components/ha-switch-wizard";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import type { LocalView } from "@/lib/types";

export default function HAPage() {
  const auth = useAuthGuard();
  const groups = useQuery({ queryKey: ["ha"], queryFn: api.listHA, refetchInterval: 5000 });

  if (auth.isLoading) return <div className="p-6 text-sm text-muted-foreground">Loading…</div>;
  if (auth.isError) return null;

  const list = groups.data?.groups ?? [];
  return (
    <div>
      <Header />
      <main className="mx-auto max-w-7xl space-y-6 px-4 py-6">
        <div>
          <h1 className="text-xl font-semibold">HA Groups — local view</h1>
          <p className="text-sm text-muted-foreground">
            This console acts on its <b>local</b> cluster only; the peer side is coordinated through
            the shared S3 fencing token. Switch = Release here, then Promote on the peer console.
          </p>
        </div>

        {groups.isError && (
          <p className="text-sm text-destructive">
            {groups.error instanceof ApiError ? groups.error.message : "failed to load HA groups"}
          </p>
        )}
        {!groups.isLoading && list.length === 0 && (
          <p className="py-8 text-center text-sm text-muted-foreground">No HA groups configured.</p>
        )}

        <div className="space-y-6">
          {list.map((g) => (
            <GroupCard key={g.name} view={g} />
          ))}
        </div>
      </main>
    </div>
  );
}

function GroupCard({ view }: { view: LocalView }) {
  const qc = useQueryClient();
  const [wizard, setWizard] = React.useState<null | "release" | "promote">(null);
  const [claiming, setClaiming] = React.useState(false);
  const [claimErr, setClaimErr] = React.useState("");
  const [resuming, setResuming] = React.useState(false);
  const localRunning = !!view.local?.healthy;
  const tokenUnset = view.fencing.pointsTo === "unset";
  const isActive = view.role === "active";
  // This side owns the group but its local job is stopped (typically a plain
  // Jobs-page Suspend). The fix is Resume, not Promote.
  const ownedButStopped = isActive && !localRunning;

  const onClaim = async () => {
    if (!window.confirm(`Initialize the fencing token to ${view.clusterId} (mark this side active)? This does not restart the job.`)) {
      return;
    }
    setClaiming(true);
    setClaimErr("");
    try {
      await api.claim(view.name);
      qc.invalidateQueries({ queryKey: ["ha"] });
    } catch (e) {
      setClaimErr(e instanceof ApiError ? e.message : String(e));
    } finally {
      setClaiming(false);
    }
  };

  const onResume = async () => {
    setResuming(true);
    setClaimErr("");
    try {
      await api.resume(view.deployment);
      qc.invalidateQueries({ queryKey: ["ha"] });
      qc.invalidateQueries({ queryKey: ["jobs"] });
    } catch (e) {
      setClaimErr(e instanceof ApiError ? e.message : String(e));
    } finally {
      setResuming(false);
    }
  };

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between space-y-0">
        <CardTitle className="flex items-center gap-2">
          {view.name}
          <RoleBadge role={view.role} />
          <Badge variant="outline">token → {view.fencing.token || view.fencing.pointsTo}</Badge>
        </CardTitle>
        <div className="flex items-center gap-2">
          {tokenUnset && (
            <Button size="sm" variant="secondary" disabled={claiming} onClick={onClaim}>
              {claiming ? "Initializing…" : "Initialize (claim active)"}
            </Button>
          )}
          {ownedButStopped && (
            <Button size="sm" variant="default" disabled={resuming} onClick={onResume}>
              {resuming ? "Resuming…" : "Resume local job"}
            </Button>
          )}
          <Button size="sm" variant="outline" disabled={!localRunning} onClick={() => setWizard("release")}>
            Release (step down)
          </Button>
          <Button
            size="sm"
            variant="destructive"
            disabled={isActive}
            title={
              isActive
                ? "This side already owns the group; Promote is for taking over a peer that has Released. To restart a suspended local job, use Resume."
                : undefined
            }
            onClick={() => setWizard("promote")}
          >
            Promote (take over)
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {claimErr && <p className="text-sm text-destructive">{claimErr}</p>}
        {view.warning && (
          <div className="flex items-center gap-2 rounded-md border border-amber-500 bg-amber-50 p-3 text-sm text-amber-900">
            <AlertTriangle className="h-4 w-4 shrink-0" />
            <span>{view.warning}</span>
          </div>
        )}

        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          {/* Local side */}
          <div className={cn("rounded-lg border p-4", view.role === "active" ? "border-green-600 ring-1 ring-green-600" : "border-border")}>
            <div className="mb-2 flex items-center justify-between">
              <div className="flex items-center gap-2 font-medium">
                {view.role === "active" ? (
                  <ShieldCheck className="h-4 w-4 text-green-600" />
                ) : (
                  <ShieldAlert className="h-4 w-4 text-muted-foreground" />
                )}
                Local ({view.clusterId})
              </div>
              {view.local && <StatusBadge job={view.local} />}
            </div>
            <dl className="space-y-1 text-xs text-muted-foreground">
              <Row label="namespace" value={view.namespace} />
              <Row label="deployment" value={view.deployment} mono />
              <Row label="pods" value={view.local?.pods ? String(view.local.pods.length) : "0"} />
            </dl>
          </div>

          {/* Peer side — not observed cross-cluster */}
          <div className="rounded-lg border border-dashed p-4 text-muted-foreground">
            <div className="mb-2 flex items-center gap-2 font-medium">
              <HelpCircle className="h-4 w-4" />
              Peer ({view.peerClusterId})
            </div>
            <p className="text-xs">Not observed (cross-cluster). State inferred only from the shared fencing token / handoff record below.</p>
          </div>
        </div>

        {/* Shared coordination */}
        <div className="rounded-md bg-muted p-3 text-xs">
          <div className="mb-1 font-medium text-foreground">Shared S3 coordination</div>
          <div>
            fencing token: <span className="font-mono">{view.fencing.token || "(unset)"}</span> — points to{" "}
            <b>{view.fencing.pointsTo}</b>
            {view.fencing.error ? <span className="text-destructive"> ({view.fencing.error})</span> : null}
          </div>
          {view.handoff ? (
            <div className="mt-1 space-y-0.5">
              <div>
                handoff: active=<span className="font-mono">{view.handoff.activeClusterId || "—"}</span> · epoch{" "}
                {view.handoff.epoch} · phase <b>{view.handoff.phase}</b>
                {view.handoff.releasedBy && <> · releasedBy {view.handoff.releasedBy}</>}
              </div>
              {view.handoff.recoveryPoint?.path ? (
                <div>
                  recovery point (<span className="font-mono">{view.handoff.recoveryPoint.kind}</span>):{" "}
                  <span className="break-all font-mono">{view.handoff.recoveryPoint.path}</span>
                </div>
              ) : (
                <div>recovery point: (none — target will use last-state)</div>
              )}
              {view.handoff.updatedAt && (
                <div>updated: {new Date(view.handoff.updatedAt).toLocaleString()}</div>
              )}
            </div>
          ) : (
            <div className="mt-1">
              handoff: (none) — written on <b>Initialize / Release / Promote</b>. If you set the token
              manually, click <b>Initialize (claim active)</b> to create it.
            </div>
          )}
        </div>
      </CardContent>

      {wizard && <HASwitchWizard open view={view} op={wizard} onClose={() => setWizard(null)} />}
    </Card>
  );
}

function RoleBadge({ role }: { role: string }) {
  const variant = role === "active" ? "success" : role === "standby" ? "secondary" : role === "neutral" ? "warning" : "outline";
  return <Badge variant={variant as never}>{role}</Badge>;
}

function Row({ label, value, mono }: { label: string; value?: string; mono?: boolean }) {
  return (
    <div className="flex justify-between gap-2">
      <dt>{label}</dt>
      <dd className={cn("text-foreground", mono && "break-all font-mono")}>{value || "—"}</dd>
    </div>
  );
}
