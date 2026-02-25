/* ── Command Center: main overview screen ── */

import { useApi } from "../hooks/useApi";
import { listNarratives, listAlerts, listClusters } from "../lib/api";
import { MetricCard } from "../components/common/MetricCard";
import { NarrativeFeed } from "../components/narrative/NarrativeFeed";
import { AlertBanner } from "../components/common/AlertBanner";
import { ClusterCard } from "../components/coordination/ClusterCard";
import { LoadingState } from "../components/common/LoadingState";

export function CommandCenter() {
  const narratives = useApi(["narratives", "all"], () => listNarratives({ limit: 50 }), {
    refetchInterval: 30_000,
  });
  const alerts = useApi(["alerts", "24h"], () => listAlerts({ hours: 24, limit: 20 }), {
    refetchInterval: 15_000,
  });
  const clusters = useApi(["clusters"], () => listClusters(), {
    refetchInterval: 60_000,
  });

  const allNarratives = narratives.data?.narratives ?? [];
  const newToday = allNarratives.filter((n) => {
    const created = new Date(n.created_at).getTime();
    const dayAgo = Date.now() - 86_400_000;
    return created > dayAgo;
  });

  const platforms = new Set<string>();
  allNarratives.forEach((n) => n.platforms?.forEach((p) => platforms.add(p)));

  if (narratives.isLoading && alerts.isLoading) return <LoadingState />;

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-bold text-[var(--dark)]">Command Center</h1>
        <span className="text-[10px] text-[var(--muted)]">
          Last updated: {new Date().toLocaleTimeString()}
        </span>
      </div>

      {/* Metric cards row */}
      <div className="grid grid-cols-4 gap-3">
        <MetricCard
          label="Active Narratives"
          value={allNarratives.length}
          icon="N"
        />
        <MetricCard
          label="New Today"
          value={newToday.length}
          trend={newToday.length > 0 ? ((newToday.length / Math.max(allNarratives.length, 1)) * 100) : 0}
          trendLabel="of total"
          icon="+"
        />
        <MetricCard
          label="Platforms Tracked"
          value={platforms.size}
          icon="P"
        />
        <MetricCard
          label="Coordination Clusters"
          value={clusters.data?.count ?? 0}
          icon="K"
        />
      </div>

      {/* Main content grid */}
      <div className="grid grid-cols-12 gap-4">
        {/* Narrative feed - center column */}
        <div className="col-span-7">
          <NarrativeFeed
            narratives={allNarratives}
            loading={narratives.isLoading}
            title="Active Narratives"
            maxHeight="calc(100vh - 240px)"
          />
        </div>

        {/* Right sidebar */}
        <div className="col-span-5 space-y-4">
          {/* Active alerts */}
          <div>
            <h2 className="text-sm font-bold text-[var(--dark)] uppercase tracking-wide mb-2">
              Alerts (24h)
            </h2>
            <div className="space-y-1.5 max-h-64 overflow-y-auto">
              {alerts.isLoading ? (
                <LoadingState message="Loading alerts..." />
              ) : (alerts.data?.alerts ?? []).length === 0 ? (
                <div className="text-xs text-[var(--muted)] text-center py-4">No active alerts</div>
              ) : (
                (alerts.data?.alerts ?? []).slice(0, 8).map((alert) => (
                  <AlertBanner
                    key={alert.id}
                    severity={alert.severity}
                    title={alert.type.replace(/_/g, " ")}
                    message={alert.alert_category}
                    timestamp={alert.detected_at}
                  />
                ))
              )}
            </div>
          </div>

          {/* Coordination clusters */}
          <div>
            <h2 className="text-sm font-bold text-[var(--dark)] uppercase tracking-wide mb-2">
              Coordination Clusters
            </h2>
            <div className="space-y-1.5 max-h-64 overflow-y-auto">
              {clusters.isLoading ? (
                <LoadingState message="Loading clusters..." />
              ) : (clusters.data?.clusters ?? []).length === 0 ? (
                <div className="text-xs text-[var(--muted)] text-center py-4">No active clusters</div>
              ) : (
                (clusters.data?.clusters ?? []).slice(0, 5).map((cluster) => (
                  <ClusterCard key={cluster.cluster_id} cluster={cluster} />
                ))
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
