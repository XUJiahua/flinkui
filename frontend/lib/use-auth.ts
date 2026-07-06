"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";

/** useAuthGuard redirects to /login when the session is invalid. */
export function useAuthGuard() {
  const router = useRouter();
  const query = useQuery({
    queryKey: ["me"],
    queryFn: api.me,
    retry: false,
  });

  React.useEffect(() => {
    if (query.isError) {
      router.replace("/login");
    }
  }, [query.isError, router]);

  return query;
}
