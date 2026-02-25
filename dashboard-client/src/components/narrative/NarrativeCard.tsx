/* ── Compact narrative card with sparkline ── */

import { Link } from "react-router-dom";
import type { Narrative } from "../../lib/types";
import { StatusBadge } from "../common/StatusBadge";
import { PlatformBadge } from "../common/PlatformBadge";
import { TimeAgo } from "../common/TimeAgo";
import { VelocityChart } from "../charts/VelocityChart";

interface NarrativeCardProps {
  narrative: Narrative;
}

export function NarrativeCard({ narrative }: NarrativeCardProps) {
  const id = narrative.narrative_id ?? narrative.id;
  const latestSnapshot = narrative.snapshots?.[narrative.snapshots.length - 1];

  return (
    <Link
      to={`/narratives/${id}`}
      className="block bg-white rounded-lg border border-[#E8E6E3] hover:border-[var(--primary)] transition-all px-3 py-2.5 group"
    >
      <div className="flex items-start justify-between gap-2 mb-1.5">
        <div className="flex-1 min-w-0">
          <h3 className="text-sm font-semibold text-[var(--dark)] truncate group-hover:text-[var(--primary)] transition-colors">
            {narrative.title}
          </h3>
          {narrative.description && (
            <p className="text-xs text-[var(--muted)] mt-0.5 line-clamp-1">{narrative.description}</p>
          )}
        </div>
        <StatusBadge status={narrative.status} />
      </div>

      <div className="flex items-center gap-3 mb-2">
        {/* Platforms */}
        <div className="flex gap-0.5">
          {(narrative.platforms ?? []).slice(0, 5).map((p) => (
            <PlatformBadge key={p} platform={p} />
          ))}
        </div>

        {/* Coordination indicator */}
        {narrative.coordination_detected && (
          <span className="text-[10px] font-semibold text-[var(--alert-high)] bg-[#C2553A12] px-1.5 py-0.5 rounded">
            COORD
          </span>
        )}

        <div className="flex-1" />

        {/* Stats */}
        {latestSnapshot && (
          <div className="flex gap-3 text-[10px] text-[var(--muted)]">
            <span>{latestSnapshot.content_count} posts</span>
            <span>{latestSnapshot.actor_count} actors</span>
          </div>
        )}

        {narrative.velocity !== undefined && (
          <span className="text-[10px] font-bold text-[var(--primary)]">
            v{narrative.velocity.toFixed(1)}
          </span>
        )}
      </div>

      {/* Sparkline */}
      {narrative.snapshots && narrative.snapshots.length > 1 && (
        <div className="h-8 -mx-1">
          <VelocityChart
            data={narrative.snapshots.map((s) => ({
              timestamp: s.timestamp,
              velocity: s.velocity,
            }))}
            height={32}
            compact
          />
        </div>
      )}

      <div className="flex items-center justify-between mt-1.5">
        <div className="flex gap-1 flex-wrap">
          {narrative.keywords?.slice(0, 3).map((kw) => (
            <span key={kw} className="text-[9px] bg-[var(--bg)] text-[var(--muted)] px-1.5 py-0.5 rounded">
              {kw}
            </span>
          ))}
        </div>
        <TimeAgo timestamp={narrative.updated_at} />
      </div>
    </Link>
  );
}
