"use client";

import * as React from "react";
import { cn } from "@/lib/utils";

type ToastVariant = "default" | "success" | "error";

interface Toast {
  id: number;
  title: string;
  description?: string;
  variant: ToastVariant;
}

interface ToastContextValue {
  toast: (t: Omit<Toast, "id">) => void;
}

const ToastContext = React.createContext<ToastContextValue | null>(null);

/** useToast returns a function to push transient notifications. */
export function useToast() {
  const ctx = React.useContext(ToastContext);
  if (!ctx) throw new Error("useToast must be used within ToastProvider");
  return ctx;
}

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = React.useState<Toast[]>([]);

  const toast = React.useCallback((t: Omit<Toast, "id">) => {
    const id = Date.now() + Math.random();
    setToasts((prev) => [...prev, { ...t, id }]);
    setTimeout(() => {
      setToasts((prev) => prev.filter((x) => x.id !== id));
    }, 5000);
  }, []);

  return (
    <ToastContext.Provider value={{ toast }}>
      {children}
      <div className="fixed bottom-4 right-4 z-[100] flex w-full max-w-sm flex-col gap-2">
        {toasts.map((t) => (
          <div
            key={t.id}
            className={cn(
              "rounded-md border p-4 shadow-lg",
              t.variant === "success" && "border-green-600 bg-green-50 text-green-900",
              t.variant === "error" && "border-destructive bg-red-50 text-red-900",
              t.variant === "default" && "bg-card",
            )}
          >
            <div className="text-sm font-semibold">{t.title}</div>
            {t.description && (
              <div className="mt-1 break-all text-xs text-muted-foreground">{t.description}</div>
            )}
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}
