"use client";

import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import { api, wsStatusUrl } from "@/lib/api";
import type { SecretSyncStatus } from "@/lib/types";

interface SecretSyncStream {
  status: SecretSyncStatus | undefined;
  live: boolean;
  error: string | null;
  loading: boolean;
}

/** useSecretSyncStream reads the secret-sync status from the shared WebSocket
 *  status stream (same frame as jobs), falling back to REST polling when the
 *  socket is unavailable. This mirrors useJobsStream to avoid extra polling. */
export function useSecretSyncStream(): SecretSyncStream {
  const [status, setStatus] = React.useState<SecretSyncStatus | undefined>(undefined);
  const [live, setLive] = React.useState(false);

  // REST fallback: only polls while the socket is not live.
  const poll = useQuery({
    queryKey: ["secretsync"],
    queryFn: api.secretSyncStatus,
    refetchInterval: live ? false : 5000,
  });

  React.useEffect(() => {
    let ws: WebSocket | null = null;
    let closed = false;
    try {
      ws = new WebSocket(wsStatusUrl());
      ws.onopen = () => setLive(true);
      ws.onmessage = (ev) => {
        try {
          const data = JSON.parse(ev.data);
          if (data.secretSync) setStatus(data.secretSync as SecretSyncStatus);
        } catch {
          /* ignore malformed frames */
        }
      };
      ws.onerror = () => setLive(false);
      ws.onclose = () => {
        if (!closed) setLive(false);
      };
    } catch {
      setLive(false);
    }
    return () => {
      closed = true;
      ws?.close();
    };
  }, []);

  const effective = live && status ? status : poll.data ?? status;
  return {
    status: effective,
    live,
    error: poll.error ? String(poll.error) : null,
    loading: !status && poll.isLoading,
  };
}
