/* ── Narrative status indicator ── */

import type { NarrativeStatus } from "../../lib/types";
import { statusColors, statusLabels } from "../../lib/constants";

interface StatusBadgeProps {
  status: NarrativeStatus;
}

export function StatusBadge({ status }: StatusBadgeProps) {
  const color = statusColors[status] ?? "#B5B5AD";
  const label = statusLabels[status] ?? status;

  return (
    <span className="inline-flex items-center gap-1.5 text-[11px] font-medium">
      <span
        className="w-2 h-2 rounded-full animate-pulse"
        style={{ background: color }}
      />
      <span style={{ color }}>{label}</span>
    </span>
  );
}
