/* ── Scrollable narrative card list ── */

import type { Narrative } from "../../lib/types";
import { NarrativeCard } from "./NarrativeCard";
import { LoadingState } from "../common/LoadingState";

interface NarrativeFeedProps {
  narratives: Narrative[];
  loading?: boolean;
  title?: string;
  maxHeight?: string;
}

export function NarrativeFeed({
  narratives,
  loading = false,
  title,
  maxHeight = "calc(100vh - 280px)",
}: NarrativeFeedProps) {
  if (loading) return <LoadingState message="Loading narratives..." />;

  return (
    <div>
      {title && (
        <div className="flex items-center justify-between mb-2">
          <h2 className="text-sm font-bold text-[var(--dark)] uppercase tracking-wide">{title}</h2>
          <span className="text-[10px] text-[var(--muted)]">{narratives.length} total</span>
        </div>
      )}
      <div className="space-y-2 overflow-y-auto pr-1" style={{ maxHeight }}>
        {narratives.length === 0 ? (
          <div className="text-sm text-[var(--muted)] text-center py-8">No narratives found</div>
        ) : (
          narratives.map((n) => (
            <NarrativeCard key={n.narrative_id ?? n.id} narrative={n} />
          ))
        )}
      </div>
    </div>
  );
}
