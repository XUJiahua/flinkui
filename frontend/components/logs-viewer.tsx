"use client";

import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import { RefreshCw } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

/** LogsViewer tails JobManager logs with adjustable tail size and keyword filter
 *  (design §4.3). */
export function LogsViewer({ jobName }: { jobName: string }) {
  const [tail, setTail] = React.useState(200);
  const [filter, setFilter] = React.useState("");

  const logs = useQuery({
    queryKey: ["logs", jobName, tail],
    queryFn: () => api.logs(jobName, tail),
  });

  const text = logs.data?.logs ?? "";
  const lines = React.useMemo(() => {
    const all = text.split("\n");
    if (!filter) return all;
    const f = filter.toLowerCase();
    return all.filter((l) => l.toLowerCase().includes(f));
  }, [text, filter]);

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <label className="text-sm text-muted-foreground">Tail</label>
        <select
          className="h-9 rounded-md border border-input bg-background px-2 text-sm"
          value={tail}
          onChange={(e) => setTail(Number(e.target.value))}
        >
          {[100, 200, 500, 1000, 2000].map((n) => (
            <option key={n} value={n}>
              {n}
            </option>
          ))}
        </select>
        <Input
          placeholder="Filter (keyword)…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="max-w-xs"
        />
        <Button variant="outline" size="sm" onClick={() => logs.refetch()} disabled={logs.isFetching}>
          <RefreshCw className={logs.isFetching ? "h-4 w-4 animate-spin" : "h-4 w-4"} />
          Refresh
        </Button>
      </div>

      {logs.isError && (
        <p className="text-sm text-destructive">
          {logs.error instanceof ApiError ? logs.error.message : "failed to load logs"}
        </p>
      )}

      <pre className="max-h-[28rem] overflow-auto rounded-md bg-zinc-950 p-4 text-xs leading-relaxed text-zinc-100">
        {logs.isLoading ? "Loading logs…" : lines.join("\n") || "No log output."}
      </pre>
    </div>
  );
}
