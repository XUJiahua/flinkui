"use client";

import * as React from "react";
import { Terminal } from "lucide-react";
import type { PlanStep } from "@/lib/types";

/** PlanSteps renders a collapsible list of the operations the console will run,
 *  each with its equivalent kubectl command for transparency / manual recovery.
 *  The console performs these via the Kubernetes API — the commands are a
 *  reference, not literal shell execution. */
export function PlanSteps({
  steps,
  title = "What the console will do",
  defaultOpen = false,
}: {
  steps?: PlanStep[] | null;
  title?: string;
  defaultOpen?: boolean;
}) {
  if (!steps || steps.length === 0) return null;
  return (
    <details open={defaultOpen} className="rounded-md border">
      <summary className="flex cursor-pointer select-none items-center gap-2 px-3 py-2 text-sm font-medium">
        <Terminal className="h-4 w-4" />
        {title}
        <span className="text-xs font-normal text-muted-foreground">(kubectl equivalent)</span>
      </summary>
      <div className="space-y-2 border-t px-3 py-2">
        <p className="text-xs text-muted-foreground">
          The console performs these steps via the Kubernetes API (client-go). The commands below are
          equivalents for review and manual recovery.
        </p>
        <ol className="space-y-2">
          {steps.map((s, i) => (
            <li key={i} className="text-sm">
              <div className="flex items-baseline gap-2">
                <span className="text-xs text-muted-foreground">{i + 1}.</span>
                <span className="font-medium">{s.title}</span>
              </div>
              {s.detail && <p className="ml-5 text-xs text-muted-foreground">{s.detail}</p>}
              {s.kubectl && (
                <pre className="ml-5 mt-1 overflow-x-auto rounded bg-muted/60 p-2 font-mono text-[11px] leading-relaxed">
                  {s.kubectl}
                </pre>
              )}
            </li>
          ))}
        </ol>
      </div>
    </details>
  );
}
