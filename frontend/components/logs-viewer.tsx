"use client";

import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import { RefreshCw } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import type { LogComponent } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

const COMPONENTS: { value: LogComponent; label: string }[] = [
  { value: "jobmanager", label: "JobManager" },
  { value: "taskmanager", label: "TaskManager" },
];

/** LogsViewer tails JobManager or TaskManager logs with adjustable tail size,
 *  a component switch, an optional single-pod selector (useful when there are
 *  multiple TaskManagers), and a keyword filter (design §4.3). */
export function LogsViewer({ jobName }: { jobName: string }) {
  const [component, setComponent] = React.useState<LogComponent>("jobmanager");
  const [pod, setPod] = React.useState<string>(""); // "" = all pods of the role
  const [tail, setTail] = React.useState(200);
  const [filter, setFilter] = React.useState("");

  // Pod list for the current role, so the user can isolate a single instance.
  // Reuses the cached job query the detail page already polls.
  const job = useQuery({
    queryKey: ["job", jobName],
    queryFn: () => api.getJob(jobName),
    enabled: !!jobName,
  });
  const rolePods = React.useMemo(
    () => (job.data?.pods ?? []).filter((p) => p.component === component),
    [job.data, component],
  );

  // If the selected pod is no longer present for the role, fall back to "all".
  React.useEffect(() => {
    if (pod && !rolePods.some((p) => p.name === pod)) setPod("");
  }, [rolePods, pod]);

  const logs = useQuery({
    queryKey: ["logs", jobName, component, pod, tail],
    queryFn: () => api.logs(jobName, tail, component, pod),
  });

  const text = logs.data?.logs ?? "";
  const lines = React.useMemo(() => {
    const all = text.split("\n");
    if (!filter) return all;
    const f = filter.toLowerCase();
    return all.filter((l) => l.toLowerCase().includes(f));
  }, [text, filter]);

  const roleLabel = component === "taskmanager" ? "TaskManager" : "JobManager";

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <div className="inline-flex rounded-md border border-input p-0.5">
          {COMPONENTS.map((c) => (
            <button
              key={c.value}
              type="button"
              onClick={() => {
                setComponent(c.value);
                setPod(""); // reset instance selection when switching role
              }}
              className={
                "rounded px-3 py-1 text-sm transition-colors " +
                (component === c.value
                  ? "bg-secondary text-secondary-foreground"
                  : "text-muted-foreground hover:text-foreground")
              }
              aria-pressed={component === c.value}
            >
              {c.label}
            </button>
          ))}
        </div>
        <select
          className="h-9 max-w-[18rem] rounded-md border border-input bg-background px-2 text-sm"
          value={pod}
          onChange={(e) => setPod(e.target.value)}
          aria-label="Pod instance"
        >
          <option value="">
            All {roleLabel}s{rolePods.length ? ` (${rolePods.length})` : ""}
          </option>
          {rolePods.map((p) => (
            <option key={p.name} value={p.name}>
              {p.name}
            </option>
          ))}
        </select>
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
        {logs.isLoading ? "Loading logs…" : lines.join("\n") || `No ${roleLabel} log output.`}
      </pre>
    </div>
  );
}
