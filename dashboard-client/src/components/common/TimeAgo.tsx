/* ── Relative time display ── */

import { useMemo } from "react";

interface TimeAgoProps {
  timestamp: string;
  className?: string;
}

function formatTimeAgo(ts: string): string {
  const now = Date.now();
  const then = new Date(ts).getTime();
  const diffMs = now - then;

  if (isNaN(then)) return ts;
  if (diffMs < 0) return "just now";

  const seconds = Math.floor(diffMs / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months}mo ago`;
  return `${Math.floor(months / 12)}y ago`;
}

export function TimeAgo({ timestamp, className }: TimeAgoProps) {
  const text = useMemo(() => formatTimeAgo(timestamp), [timestamp]);

  return (
    <time
      dateTime={timestamp}
      title={new Date(timestamp).toLocaleString()}
      className={className ?? "text-[10px] text-[var(--muted)]"}
    >
      {text}
    </time>
  );
}
