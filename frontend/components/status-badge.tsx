import { Badge } from "@/components/ui/badge";
import type { Health, JobSummary } from "@/lib/types";

const HEALTH_VARIANT: Record<Health, "success" | "warning" | "destructive" | "secondary"> = {
  healthy: "success",
  progressing: "warning",
  degraded: "destructive",
  suspended: "secondary",
  stopped: "secondary",
  unreachable: "destructive",
  notfound: "destructive",
  unknown: "secondary",
};

/** StatusBadge renders the combined jobStatus/lifecycleState, colored by the
 *  backend's explicit health classification (design §13). Falls back to the
 *  legacy substring heuristic if health is absent (older payloads). */
export function StatusBadge({
  job,
}: {
  job: Pick<JobSummary, "statusText" | "healthy" | "reachable"> & Partial<Pick<JobSummary, "health">>;
}) {
  let variant: "success" | "warning" | "destructive" | "secondary";
  if (job.health) {
    variant = HEALTH_VARIANT[job.health] ?? "secondary";
  } else {
    variant = legacyVariant(job);
  }
  return <Badge variant={variant}>{job.statusText}</Badge>;
}

function legacyVariant(
  job: Pick<JobSummary, "statusText" | "healthy" | "reachable">,
): "success" | "warning" | "destructive" | "secondary" {
  if (!job.reachable) return "destructive";
  if (job.healthy) return "success";
  if (
    job.statusText.includes("RECONCILING") ||
    job.statusText.includes("STARTING") ||
    job.statusText.includes("SUSPENDED")
  ) {
    return "warning";
  }
  if (
    job.statusText.includes("FAILED") ||
    job.statusText.includes("UNREACHABLE") ||
    job.statusText.includes("NOTFOUND")
  ) {
    return "destructive";
  }
  return "secondary";
}
