/* ── Actor behavioral profile ── */

import { useParams, Link } from "react-router-dom";
import { useApi } from "../hooks/useApi";
import { getActor, getActorHistory } from "../lib/api";
import { PlatformBadge } from "../components/common/PlatformBadge";
import { TimeAgo } from "../components/common/TimeAgo";
import { LoadingState } from "../components/common/LoadingState";
import { MetricCard } from "../components/common/MetricCard";

export function ActorProfile() {
  const { id } = useParams<{ id: string }>();

  const { data, isLoading } = useApi(["actor", id], () => getActor(id!), {
    enabled: !!id,
  });
  const history = useApi(["actor-history", id], () => getActorHistory(id!), {
    enabled: !!id,
  });

  if (isLoading) return <LoadingState />;
  if (!data) return <div className="text-sm text-[var(--muted)] py-8">Actor not found</div>;

  const actor = data.actor;

  return (
    <div className="space-y-4 max-w-5xl">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-[var(--muted)]">
        <Link to="/network" className="hover:text-[var(--primary)]">Network</Link>
        <span>/</span>
        <span className="text-[var(--dark)]">{actor.username}</span>
      </div>

      {/* Header */}
      <div className="bg-white rounded-lg border border-[#E8E6E3] px-5 py-4">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h1 className="text-xl font-bold text-[var(--dark)]">{actor.username}</h1>
            {actor.display_name && (
              <p className="text-sm text-[var(--muted)]">{actor.display_name}</p>
            )}
          </div>
          <PlatformBadge platform={actor.primary_platform} showName size="md" />
        </div>

        {actor.coordination_cluster_id && (
          <div className="mt-2 bg-[#C2553A12] rounded px-3 py-1.5 inline-block">
            <span className="text-[10px] font-bold text-[var(--alert-high)]">
              COORDINATION CLUSTER: {actor.coordination_cluster_id.slice(0, 12)}...
            </span>
          </div>
        )}
      </div>

      {/* Metrics */}
      <div className="grid grid-cols-4 gap-3">
        <MetricCard
          label="Influence Score"
          value={actor.influence_score.toFixed(3)}
          icon="I"
        />
        <MetricCard
          label="Outgoing Connections"
          value={data.outgoing_connections}
          icon=">"
        />
        <MetricCard
          label="Incoming Connections"
          value={data.incoming_connections}
          icon="<"
        />
        <MetricCard
          label="Total Content"
          value={actor.total_content_count ?? 0}
          icon="C"
        />
      </div>

      <div className="grid grid-cols-2 gap-4">
        {/* Linked accounts */}
        <div className="bg-white rounded-lg border border-[#E8E6E3] px-4 py-3">
          <h3 className="text-xs font-bold text-[var(--dark)] uppercase tracking-wide mb-2">
            Linked Accounts ({data.linked_accounts.length})
          </h3>
          {data.linked_accounts.length === 0 ? (
            <div className="text-xs text-[var(--muted)] py-2">No linked accounts</div>
          ) : (
            <div className="space-y-1.5">
              {data.linked_accounts.map((linked) => (
                <Link
                  key={linked.internal_uuid}
                  to={`/actors/${linked.internal_uuid}`}
                  className="flex items-center gap-2 bg-[var(--bg)] rounded px-2 py-1.5 hover:bg-[#E8E6E3] transition-colors"
                >
                  <PlatformBadge platform={linked.primary_platform} />
                  <span className="text-xs font-medium text-[var(--dark)]">{linked.username}</span>
                </Link>
              ))}
            </div>
          )}
        </div>

        {/* Properties */}
        <div className="bg-white rounded-lg border border-[#E8E6E3] px-4 py-3">
          <h3 className="text-xs font-bold text-[var(--dark)] uppercase tracking-wide mb-2">
            Properties
          </h3>
          <div className="space-y-1">
            {Object.entries(actor)
              .filter(([key]) => !["internal_uuid", "username", "display_name", "primary_platform"].includes(key))
              .slice(0, 12)
              .map(([key, val]) => (
                <div key={key} className="flex justify-between text-[11px]">
                  <span className="text-[var(--muted)]">{key.replace(/_/g, " ")}</span>
                  <span className="text-[var(--dark)] font-medium truncate max-w-48">
                    {typeof val === "number" ? val.toFixed(3) : String(val ?? "-")}
                  </span>
                </div>
              ))}
          </div>
        </div>
      </div>

      {/* Activity history */}
      <div className="bg-white rounded-lg border border-[#E8E6E3] px-4 py-3">
        <h3 className="text-xs font-bold text-[var(--dark)] uppercase tracking-wide mb-2">
          Recent Activity ({history.data?.history.length ?? 0})
        </h3>
        <div className="space-y-1 max-h-80 overflow-y-auto">
          {history.isLoading ? (
            <LoadingState message="Loading history..." />
          ) : (history.data?.history ?? []).length === 0 ? (
            <div className="text-xs text-[var(--muted)] py-2">No recent activity</div>
          ) : (
            history.data!.history.slice(0, 50).map((item, i) => (
              <div key={i} className="flex items-start gap-2 bg-[var(--bg)] rounded px-2 py-1.5">
                <PlatformBadge platform={item.platform} />
                <div className="flex-1 min-w-0">
                  {item.text && (
                    <p className="text-[11px] text-[var(--dark)] line-clamp-2">{item.text}</p>
                  )}
                  <div className="flex gap-2 text-[9px] text-[var(--muted)]">
                    {item.content_type && <span>{item.content_type}</span>}
                    <TimeAgo timestamp={item.timestamp} />
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
