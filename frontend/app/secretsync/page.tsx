"use client";

import * as React from "react";
import { useQueryClient } from "@tanstack/react-query";
import { RefreshCw, ShieldCheck, AlertTriangle, CheckCircle2, XCircle, Radio } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { useAuthGuard } from "@/lib/use-auth";
import { useSecretSyncStream } from "@/lib/use-secretsync-stream";
import { Header } from "@/components/header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useToast } from "@/components/ui/toast";
import type { SecretSyncStatus } from "@/lib/types";

export default function SecretSyncPage() {
  const auth = useAuthGuard();
  const qc = useQueryClient();
  const { toast } = useToast();
  const [syncing, setSyncing] = React.useState(false);

  const stream = useSecretSyncStream();

  if (auth.isLoading) return <div className="p-6 text-sm text-muted-foreground">Loading…</div>;
  if (auth.isError) return null;

  const s: SecretSyncStatus | undefined = stream.status;

  async function syncNow() {
    setSyncing(true);
    try {
      await api.secretSyncNow();
      await qc.invalidateQueries({ queryKey: ["secretsync"] });
      toast({ title: "Sync triggered", variant: "success" });
    } catch (e) {
      toast({
        title: "Sync failed",
        description: e instanceof ApiError ? e.message : String(e),
        variant: "error",
      });
    } finally {
      setSyncing(false);
    }
  }

  return (
    <div>
      <Header />
      <main className="mx-auto max-w-7xl space-y-6 px-4 py-6">
        <div className="flex items-start justify-between">
          <div>
            <h1 className="flex items-center gap-2 text-xl font-semibold">
              Secret Sync — OpenBao / Vault
              {stream.live && (
                <span className="flex items-center gap-1 text-xs font-normal text-emerald-600">
                  <Radio className="h-3.5 w-3.5" /> live
                </span>
              )}
            </h1>
            <p className="text-sm text-muted-foreground">
              Keeps the flink-* Secrets in sync from OpenBao/Vault (no ESO). When a value changes and
              auto-restart is on, the affected FlinkDeployment is restarted (last-state) to load it.
            </p>
          </div>
          {s?.enabled && (
            <Button onClick={syncNow} disabled={syncing || s.running}>
              <RefreshCw className={"mr-2 h-4 w-4" + (syncing || s.running ? " animate-spin" : "")} />
              {syncing || s.running ? "Syncing…" : "Sync now"}
            </Button>
          )}
        </div>

        {stream.error && (
          <p className="text-sm text-destructive">{stream.error}</p>
        )}

        {s && !s.enabled && (
          <Card>
            <CardContent className="py-8 text-center text-sm text-muted-foreground">
              Secret sync is <b>disabled</b>. Enable it via <code>secret_sync.enabled</code> and set
              <code> secretSync.enabled=true</code> on the Helm chart. See docs/vault-integration.md.
            </CardContent>
          </Card>
        )}

        {s?.enabled && (
          <>
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <ShieldCheck className="h-4 w-4" /> Status
                </CardTitle>
              </CardHeader>
              <CardContent className="grid grid-cols-2 gap-4 text-sm sm:grid-cols-4">
                <Field label="Auto-restart">
                  {s.autoRestart ? <Badge>on</Badge> : <Badge variant="secondary">off</Badge>}
                </Field>
                <Field label="Interval">{s.intervalSec ? `${s.intervalSec}s` : "—"}</Field>
                <Field label="Last sync">
                  {s.lastSyncUnix ? new Date(s.lastSyncUnix * 1000).toLocaleString() : "never"}
                </Field>
                <Field label="Running">{s.running ? "yes" : "no"}</Field>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="text-base">Items</CardTitle>
              </CardHeader>
              <CardContent>
                <table className="w-full text-sm">
                  <thead className="text-left text-muted-foreground">
                    <tr className="border-b">
                      <th className="py-2 font-medium">Secret</th>
                      <th className="py-2 font-medium">KV path</th>
                      <th className="py-2 font-medium">Restart target</th>
                      <th className="py-2 font-medium">Keys</th>
                      <th className="py-2 font-medium">Last result</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(s.items ?? []).map((it) => (
                      <tr key={it.secretName} className="border-b last:border-0">
                        <td className="py-2 font-medium">{it.secretName}</td>
                        <td className="py-2 font-mono text-xs text-muted-foreground">{it.kvPath}</td>
                        <td className="py-2 text-xs">{it.restartDeployment || "—"}</td>
                        <td className="py-2">{it.keys}</td>
                        <td className="py-2">
                          {it.ok ? (
                            <span className="flex items-center gap-1 text-emerald-600">
                              <CheckCircle2 className="h-4 w-4" /> ok
                            </span>
                          ) : (
                            <span
                              className="flex items-center gap-1 text-destructive"
                              title={it.error}
                            >
                              <XCircle className="h-4 w-4" /> {it.error || "error"}
                            </span>
                          )}
                        </td>
                      </tr>
                    ))}
                    {(s.items ?? []).length === 0 && (
                      <tr>
                        <td colSpan={5} className="py-6 text-center text-muted-foreground">
                          No items configured, or no sync has run yet.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </CardContent>
            </Card>

            {s.skipped && Object.keys(s.skipped).length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2 text-base">
                    <AlertTriangle className="h-4 w-4" /> Restart skipped (HA interlock)
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-1 text-sm">
                  {Object.entries(s.skipped).map(([dep, reason]) => (
                    <div key={dep} className="flex justify-between gap-4">
                      <span className="font-mono text-xs">{dep}</span>
                      <span className="text-muted-foreground">{reason}</span>
                    </div>
                  ))}
                </CardContent>
              </Card>
            )}

            {s.restarts && Object.keys(s.restarts).length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2 text-base">
                    <AlertTriangle className="h-4 w-4" /> Restarts triggered (this process)
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-1 text-sm">
                  {Object.entries(s.restarts).map(([dep, n]) => (
                    <div key={dep} className="flex justify-between">
                      <span className="font-mono text-xs">{dep}</span>
                      <span>{n}</span>
                    </div>
                  ))}
                </CardContent>
              </Card>
            )}
          </>
        )}
      </main>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="text-xs uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className="mt-1">{children}</div>
    </div>
  );
}
