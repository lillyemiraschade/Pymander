/* ── Coordination cluster summary card ── */

import { Link } from "react-router-dom";
import type { CoordinationCluster } from "../../lib/types";
import { severityColors, severityLabels } from "../../lib/constants";
import { TimeAgo } from "../common/TimeAgo";

interface ClusterCardProps {
  cluster: CoordinationCluster;
}

export function ClusterCard({ cluster }: ClusterCardProps) {
  const borderColor = severityColors[cluster.severity];

  return (
    <Link
      to={`/coordination`}
      className="block bg-white rounded-lg border border-[#E8E6E3] hover:border-[var(--primary)] transition-all px-3 py-2.5 border-l-3"
      style={{ borderLeftColor: borderColor }}
    >
      <div className="flex items-center justify-between mb-1.5">
        <div className="flex items-center gap-2">
          <span
            className="text-[10px] font-bold uppercase px-1.5 py-0.5 rounded"
            style={{ background: `${borderColor}15`, color: borderColor }}
          >
            {severityLabels[cluster.severity]}
          </span>
          <span className="text-xs font-semibold text-[var(--dark)]">
            {cluster.account_count} accounts
          </span>
        </div>
        <span className="text-xs font-bold text-[var(--dark)]">
          {(cluster.confidence * 100).toFixed(0)}% confidence
        </span>
      </div>

      <div className="flex flex-wrap gap-1 mb-1.5">
        {cluster.signal_types.map((st) => (
          <span
            key={st}
            className="text-[9px] bg-[var(--bg)] text-[var(--muted)] px-1.5 py-0.5 rounded font-medium"
          >
            {st.replace(/_/g, " ")}
          </span>
        ))}
      </div>

      <div className="flex items-center justify-between text-[10px] text-[var(--muted)]">
        <span>{cluster.signal_count} signals</span>
        <span>Reach: {cluster.estimated_reach.toLocaleString()}</span>
        {cluster.last_signal && <TimeAgo timestamp={cluster.last_signal} />}
      </div>

      {cluster.associated_narratives.length > 0 && (
        <div className="mt-1.5 text-[10px] text-[var(--muted)]">
          Linked narratives: {cluster.associated_narratives.length}
        </div>
      )}
    </Link>
  );
}
