import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

/** cn merges conditional class names, de-duplicating Tailwind classes. */
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}
