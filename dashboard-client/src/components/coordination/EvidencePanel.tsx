/* ── Expandable evidence detail panel ── */

import { useState } from "react";
import type { CoordinationCluster } from "../../lib/types";
import { severityColors } from "../../lib/constants";
import { TimeAgo } from "../common/TimeAgo";

interface EvidencePanelProps {
  cluster: CoordinationCluster;
}

export function EvidencePanel({ cluster }: EvidencePanelProps) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="bg-white rounded-lg border border-[#E8E6E3] overflow-hidden">
      {/* Header */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between px-4 py-3 hover:bg-[var(--bg)] transition-colors text-left"
      >
        <div className="flex items-center gap-3">
          <div
            className="w-3 h-3 rounded-full"
            style={{ background: severityColors[cluster.severity] }}
          />
          <div>
            <div className="text-sm font-semibold text-[var(--dark)]">
              Cluster: {cluster.account_count} coordinated accounts
            </div>
            <div className="text-xs text-[var(--muted)]">
              {cluster.signal_count} signals | {(cluster.confidence * 100).toFixed(0)}% confidence
            </div>
          </div>
        </div>
        <span className="text-[var(--muted)] text-lg">{expanded ? "-" : "+"}</span>
      </button>

      {/* Body */}
      {expanded && (
        <div className="border-t border-[#E8E6E3] px-4 py-3 space-y-3">
          {/* Signal types */}
          <div>
            <h4 className="text-[10px] font-bold uppercase text-[var(--muted)] tracking-wide mb-1">
              Signal Types
            </h4>
            <div className="flex flex-wrap gap-1">
              {cluster.signal_types.map((st) => (
                <span key={st} className="text-[10px] bg-[var(--bg)] text-[var(--dark)] px-2 py-0.5 rounded font-medium">
                  {st.replace(/_/g, " ")}
                </span>
              ))}
            </div>
          </div>

          {/* Accounts */}
          <div>
            <h4 className="text-[10px] font-bold uppercase text-[var(--muted)] tracking-wide mb-1">
              Accounts ({cluster.accounts.length})
            </h4>
            <div className="flex flex-wrap gap-1 max-h-24 overflow-y-auto">
              {cluster.accounts.slice(0, 20).map((acct) => (
                <span key={acct} className="text-[10px] bg-[#E8E6E3] text-[var(--dark)] px-1.5 py-0.5 rounded font-mono">
                  {acct.slice(0, 12)}...
                </span>
              ))}
              {cluster.accounts.length > 20 && (
                <span className="text-[10px] text-[var(--muted)]">
                  +{cluster.accounts.length - 20} more
                </span>
              )}
            </div>
          </div>

          {/* Signals detail */}
          {cluster.signals.length > 0 && (
            <div>
              <h4 className="text-[10px] font-bold uppercase text-[var(--muted)] tracking-wide mb-1">
                Evidence ({cluster.signals.length})
              </h4>
              <div className="space-y-1 max-h-48 overflow-y-auto">
                {cluster.signals.map((signal, i) => (
                  <div key={i} className="text-[10px] bg-[var(--bg)] rounded px-2 py-1.5">
                    <div className="font-medium text-[var(--dark)]">
                      {String(signal["type"] ?? "signal")}
                    </div>
                    {typeof signal["detected_at"] === "string" && (
                      <TimeAgo timestamp={signal["detected_at"]} />
                    )}
                    <pre className="text-[9px] text-[var(--muted)] mt-0.5 whitespace-pre-wrap">
                      {JSON.stringify(signal, null, 1).slice(0, 200)}
                    </pre>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Analyst notes */}
          {cluster.analyst_notes && (
            <div>
              <h4 className="text-[10px] font-bold uppercase text-[var(--muted)] tracking-wide mb-1">
                Analyst Notes
              </h4>
              <p className="text-xs text-[var(--dark)]">{cluster.analyst_notes}</p>
            </div>
          )}

          {/* Metadata */}
          <div className="flex items-center gap-4 text-[10px] text-[var(--muted)] pt-1 border-t border-[#E8E6E3]">
            <span>Reach: {cluster.estimated_reach.toLocaleString()}</span>
            <span>Status: {cluster.status}</span>
            {cluster.first_detected && (
              <span>First seen: <TimeAgo timestamp={cluster.first_detected} /></span>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
