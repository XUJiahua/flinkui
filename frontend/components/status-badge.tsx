import { Badge } from "@/components/ui/badge";
import type { JobSummary } from "@/lib/types";

/** StatusBadge renders the combined jobStatus/lifecycleState with health color. */
export function StatusBadge({ job }: { job: Pick<JobSummary, "statusText" | "healthy" | "reachable"> }) {
  let variant: "success" | "warning" | "destructive" | "secondary" = "secondary";
  if (!job.reachable) {
    variant = "destructive";
  } else if (job.healthy) {
    variant = "success";
  } else if (
    job.statusText.includes("RECONCILING") ||
    job.statusText.includes("STARTING") ||
    job.statusText.includes("SUSPENDED")
  ) {
    variant = "warning";
  } else if (
    job.statusText.includes("FAILED") ||
    job.statusText.includes("UNREACHABLE") ||
    job.statusText.includes("NOTFOUND")
  ) {
    variant = "destructive";
  }
  return <Badge variant={variant}>{job.statusText}</Badge>;
}
