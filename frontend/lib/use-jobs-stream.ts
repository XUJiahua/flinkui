"use client";

import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import { api, wsStatusUrl } from "@/lib/api";
import type { JobSummary } from "@/lib/types";

interface JobsStream {
  jobs: JobSummary[];
  live: boolean;
  error: string | null;
  loading: boolean;
}

/** useJobsStream subscribes to the WebSocket status stream (design §3.3) and
 *  transparently falls back to REST polling if the socket cannot be used. */
export function useJobsStream(): JobsStream {
  const [jobs, setJobs] = React.useState<JobSummary[] | null>(null);
  const [live, setLive] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  // REST fallback: only polls while the socket is not live.
  const poll = useQuery({
    queryKey: ["jobs"],
    queryFn: api.listJobs,
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
          if (Array.isArray(data.jobs)) {
            setJobs(data.jobs);
            setError(null);
          } else if (data.error) {
            setError(data.error);
          }
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

  const effectiveJobs = live && jobs ? jobs : poll.data?.jobs ?? jobs ?? [];

  return {
    jobs: effectiveJobs,
    live,
    error: error ?? (poll.error ? String(poll.error) : null),
    loading: !jobs && poll.isLoading,
  };
}
