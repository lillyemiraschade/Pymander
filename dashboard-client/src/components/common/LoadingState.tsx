/* ── Loading spinner ── */

interface LoadingStateProps {
  message?: string;
}

export function LoadingState({ message = "Loading..." }: LoadingStateProps) {
  return (
    <div className="flex items-center justify-center py-12 gap-3">
      <div className="w-5 h-5 border-2 border-[var(--muted)] border-t-[var(--primary)] rounded-full animate-spin" />
      <span className="text-sm text-[var(--muted)]">{message}</span>
    </div>
  );
}
