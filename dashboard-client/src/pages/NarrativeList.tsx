/* ── Narrative feed with filters ── */

import { useState } from "react";
import { useApi } from "../hooks/useApi";
import { listNarratives } from "../lib/api";
import { NarrativeFeed } from "../components/narrative/NarrativeFeed";
import type { NarrativeStatus } from "../lib/types";
import { statusColors, statusLabels } from "../lib/constants";

const STATUSES: NarrativeStatus[] = [
  "emerging", "growing", "viral", "peaking", "declining", "dormant", "resurgent",
];

export function NarrativeList() {
  const [statusFilter, setStatusFilter] = useState<string | undefined>(undefined);

  const { data, isLoading } = useApi(
    ["narratives", statusFilter ?? "all"],
    () => listNarratives({ status: statusFilter, limit: 100 }),
    { refetchInterval: 30_000 },
  );

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-bold text-[var(--dark)]">Narratives</h1>
        <span className="text-xs text-[var(--muted)]">{data?.count ?? 0} total</span>
      </div>

      {/* Status filters */}
      <div className="flex gap-1.5 flex-wrap">
        <button
          onClick={() => setStatusFilter(undefined)}
          className={`text-[11px] px-2.5 py-1 rounded-full font-medium transition-colors ${
            !statusFilter
              ? "bg-[var(--dark)] text-white"
              : "bg-white text-[var(--muted)] border border-[#E8E6E3] hover:border-[var(--primary)]"
          }`}
        >
          All
        </button>
        {STATUSES.map((s) => (
          <button
            key={s}
            onClick={() => setStatusFilter(s === statusFilter ? undefined : s)}
            className={`text-[11px] px-2.5 py-1 rounded-full font-medium transition-colors flex items-center gap-1.5 ${
              statusFilter === s
                ? "text-white"
                : "bg-white text-[var(--muted)] border border-[#E8E6E3] hover:border-[var(--primary)]"
            }`}
            style={statusFilter === s ? { background: statusColors[s] } : undefined}
          >
            <span
              className="w-1.5 h-1.5 rounded-full"
              style={{ background: statusColors[s] }}
            />
            {statusLabels[s]}
          </button>
        ))}
      </div>

      <NarrativeFeed
        narratives={data?.narratives ?? []}
        loading={isLoading}
        maxHeight="calc(100vh - 160px)"
      />
    </div>
  );
}
