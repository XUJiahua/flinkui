"use client";

import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, ArrowRight, ShieldCheck, ShieldAlert } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { useAuthGuard } from "@/lib/use-auth";
import { Header } from "@/components/header";
import { StatusBadge } from "@/components/status-badge";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import type { GroupView, SideView } from "@/lib/types";

export default function HAPage() {
  const auth = useAuthGuard();
  const groups = useQuery({
    queryKey: ["ha-groups"],
    queryFn: api.listHAGroups,
    refetchInterval: 5000,
  });

  if (auth.isLoading) {
    return <div className="p-6 text-sm text-muted-foreground">Loading…</div>;
  }
  if (auth.isError) return null;

  const list = groups.data?.groups ?? [];

  return (
    <div>
      <Header />
      <main className="mx-auto max-w-7xl space-y-6 px-4 py-6">
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-semibold">HA Groups (Primary / Standby)</h1>
        </div>

        {groups.isError && (
          <p className="text-sm text-destructive">
            {groups.error instanceof ApiError ? groups.error.message : "failed to load HA groups"}
          </p>
        )}

        {!groups.isLoading && list.length === 0 && (
          <p className="py-8 text-center text-sm text-muted-foreground">
            No HA groups configured.
          </p>
        )}

        <div className="space-y-6">
          {list.map((g) => (
            <GroupCard key={g.name} group={g} />
          ))}
        </div>
      </main>
    </div>
  );
}

function GroupCard({ group }: { group: GroupView }) {
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between space-y-0">
        <CardTitle className="flex items-center gap-2">
          {group.name}
          <FencingBadge group={group} />
        </CardTitle>
        <span className="text-sm text-muted-foreground">
          active: <span className="font-medium text-foreground">{group.activeSide}</span>
        </span>
      </CardHeader>
      <CardContent className="space-y-4">
        {group.splitBrain && (
          <div className="flex items-center gap-2 rounded-md border border-destructive bg-red-50 p-3 text-sm text-red-900">
            <AlertTriangle className="h-4 w-4 shrink-0" />
            <span>Split-brain: {group.warning}</span>
          </div>
        )}
        {!group.splitBrain && group.warning && (
          <div className="flex items-center gap-2 rounded-md border border-amber-500 bg-amber-50 p-3 text-sm text-amber-900">
            <AlertTriangle className="h-4 w-4 shrink-0" />
            <span>{group.warning}</span>
          </div>
        )}

        <div className="grid grid-cols-1 items-stretch gap-4 md:grid-cols-[1fr_auto_1fr]">
          <SideCard side={group.primary} active={group.activeSide === "primary"} />
          <div className="flex items-center justify-center">
            <ArrowRight className="h-5 w-5 text-muted-foreground" />
          </div>
          <SideCard side={group.standby} active={group.activeSide === "standby"} />
        </div>
      </CardContent>
    </Card>
  );
}

function FencingBadge({ group }: { group: GroupView }) {
  const p = group.fencing.pointsTo;
  const variant =
    p === "neutral" ? "warning" : p === "unknown" || p === "unset" ? "secondary" : "default";
  return (
    <Badge variant={variant as never}>
      token → {group.fencing.token || p}
    </Badge>
  );
}

function SideCard({ side, active }: { side: SideView; active: boolean }) {
  const d = side.detail;
  return (
    <div
      className={cn(
        "rounded-lg border p-4",
        active ? "border-green-600 ring-1 ring-green-600" : "border-border",
      )}
    >
      <div className="mb-2 flex items-center justify-between">
        <div className="flex items-center gap-2">
          {active ? (
            <ShieldCheck className="h-4 w-4 text-green-600" />
          ) : (
            <ShieldAlert className="h-4 w-4 text-muted-foreground" />
          )}
          <span className="font-medium capitalize">{side.role}</span>
          {active && <Badge variant="success">active</Badge>}
        </div>
        {d && <StatusBadge job={d} />}
      </div>
      <dl className="space-y-1 text-xs text-muted-foreground">
        <Row label="clusterId" value={side.clusterId} />
        <Row label="cluster" value={side.cluster} />
        <Row label="namespace" value={side.namespace} />
        <Row label="deployment" value={side.deployment} mono />
        <Row label="pods" value={d?.pods ? String(d.pods.length) : "0"} />
      </dl>
    </div>
  );
}

function Row({ label, value, mono }: { label: string; value?: string; mono?: boolean }) {
  return (
    <div className="flex justify-between gap-2">
      <dt>{label}</dt>
      <dd className={cn("text-foreground", mono && "break-all font-mono")}>{value || "—"}</dd>
    </div>
  );
}
