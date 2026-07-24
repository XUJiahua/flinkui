"use client";

import * as React from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Activity, LogOut, AlertTriangle } from "lucide-react";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";

/** Header shows cluster identity and a logout control. */
export function Header() {
  const router = useRouter();
  const qc = useQueryClient();
  const cluster = useQuery({ queryKey: ["cluster"], queryFn: api.cluster, refetchInterval: 15000 });

  const onLogout = async () => {
    await api.logout();
    qc.clear();
    router.replace("/login");
  };

  const degraded = cluster.data && cluster.data.clusterReachable === false;

  return (
    <header className="sticky top-0 z-40 border-b bg-background/95 backdrop-blur">
      <div className="mx-auto flex h-14 max-w-7xl items-center justify-between px-4">
        <Link href="/" className="flex items-center gap-2 font-semibold">
          <Activity className="h-5 w-5" />
          Flink Job Console
        </Link>
        <nav className="flex items-center gap-4 text-sm">
          <Link href="/" className="text-muted-foreground hover:text-foreground">
            Jobs
          </Link>
          <Link href="/ha" className="text-muted-foreground hover:text-foreground">
            HA
          </Link>
          <Link href="/secretsync" className="text-muted-foreground hover:text-foreground">
            Secrets
          </Link>
        </nav>
        <div className="flex items-center gap-4 text-sm text-muted-foreground">
          {degraded && (
            <span
              className="flex items-center gap-1 rounded-md border border-destructive/40 bg-destructive/10 px-2 py-1 text-xs font-medium text-destructive"
              title="The Kubernetes API for this cluster is currently unreachable; data may be stale."
            >
              <AlertTriangle className="h-3.5 w-3.5" /> cluster unreachable
            </span>
          )}
          {cluster.data && (
            <span>
              cluster <span className="font-medium text-foreground">{cluster.data.name}</span> · ns{" "}
              <span className="font-medium text-foreground">{cluster.data.namespace}</span>
            </span>
          )}
          <Button variant="ghost" size="sm" onClick={onLogout}>
            <LogOut className="h-4 w-4" />
            Logout
          </Button>
        </div>
      </div>
    </header>
  );
}
