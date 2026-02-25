/* ── Coordination cluster dashboard ── */

import { useState } from "react";
import { useApi } from "../hooks/useApi";
import { listClusters, listSignals } from "../lib/api";
import { EvidencePanel } from "../components/coordination/EvidencePanel";
import { MetricCard } from "../components/common/MetricCard";
import { LoadingState } from "../components/common/LoadingState";
import { severityColors } from "../lib/constants";
import type { AlertSeverity } from "../lib/types";

export function CoordinationDashboard() {
  const [minConfidence, setMinConfidence] = useState(0);
  const clusters = useApi(
    ["clusters", minConfidence],
    () => listClusters(minConfidence),
    { refetchInterval: 60_000 },
  );
  const signals = useApi(["signals", "24h"], () => listSignals({ hours: 24 }), {
    refetchInterval: 30_000,
  });

  const allClusters = clusters.data?.clusters ?? [];
  const highSeverity = allClusters.filter((c) => c.severity === "high" || c.severity === "critical");
  const totalAccounts = allClusters.reduce((sum, c) => sum + c.account_count, 0);
  const totalReach = allClusters.reduce((sum, c) => sum + c.estimated_reach, 0);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-bold text-[var(--dark)]">Coordination Detection</h1>
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-[var(--muted)]">Min confidence:</span>
          <input
            type="range"
            min={0}
            max={100}
            value={minConfidence * 100}
            onChange={(e) => setMinConfidence(Number(e.target.value) / 100)}
            className="w-24 h-1 accent-[var(--primary)]"
          />
          <span className="text-[10px] text-[var(--dark)] w-8">
            {(minConfidence * 100).toFixed(0)}%
          </span>
        </div>
      </div>

      {/* Metrics */}
      <div className="grid grid-cols-4 gap-3">
        <MetricCard label="Active Clusters" value={allClusters.length} icon="K" />
        <MetricCard
          label="High Severity"
          value={highSeverity.length}
          icon="!"
        />
        <MetricCard label="Coordinated Accounts" value={totalAccounts} icon="A" />
        <MetricCard label="Estimated Reach" value={totalReach.toLocaleString()} icon="R" />
      </div>

      {/* Signals summary */}
      <div className="bg-white rounded-lg border border-[#E8E6E3] px-4 py-3">
        <h3 className="text-xs font-bold text-[var(--dark)] uppercase tracking-wide mb-2">
          Signals (24h): {signals.data?.count ?? 0}
        </h3>
        <div className="flex gap-4 flex-wrap">
          {(signals.data?.signals ?? []).slice(0, 10).map((sig) => (
            <div
              key={sig.id}
              className="text-[10px] bg-[var(--bg)] rounded px-2 py-1"
            >
              <span className="font-semibold text-[var(--dark)]">
                {sig.type.replace(/_/g, " ")}
              </span>
              <span className="text-[var(--muted)] ml-1">
                {(sig.confidence * 100).toFixed(0)}%
              </span>
            </div>
          ))}
        </div>
      </div>

      {/* Clusters list */}
      {clusters.isLoading ? (
        <LoadingState message="Loading clusters..." />
      ) : allClusters.length === 0 ? (
        <div className="text-sm text-[var(--muted)] text-center py-12">
          No coordination clusters detected
        </div>
      ) : (
        <div className="space-y-2">
          {/* Severity legend */}
          <div className="flex gap-3">
            {(["critical", "high", "medium", "low"] as AlertSeverity[]).map((sev) => {
              const count = allClusters.filter((c) => c.severity === sev).length;
              return (
                <span key={sev} className="flex items-center gap-1 text-[10px]">
                  <span
                    className="w-2 h-2 rounded-full"
                    style={{ background: severityColors[sev] }}
                  />
                  <span className="text-[var(--muted)] capitalize">{sev}</span>
                  <span className="font-bold text-[var(--dark)]">{count}</span>
                </span>
              );
            })}
          </div>

          {allClusters.map((cluster) => (
            <EvidencePanel key={cluster.cluster_id} cluster={cluster} />
          ))}
        </div>
      )}
    </div>
  );
}
