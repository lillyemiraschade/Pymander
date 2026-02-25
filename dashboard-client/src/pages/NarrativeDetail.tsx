/* ── Full narrative biography ── */

import { useParams, Link } from "react-router-dom";
import { useApi } from "../hooks/useApi";
import {
  getNarrative,
  getNarrativeVelocity,
  getNarrativePredictions,
  getNarrativeMigrations,
} from "../lib/api";
import { StatusBadge } from "../components/common/StatusBadge";
import { PlatformBadge } from "../components/common/PlatformBadge";
import { TimeAgo } from "../components/common/TimeAgo";
import { LoadingState } from "../components/common/LoadingState";
import { VelocityChart } from "../components/charts/VelocityChart";
import { EngagementCurve } from "../components/charts/EngagementCurve";
import { PlatformTimeline } from "../components/charts/PlatformTimeline";

export function NarrativeDetail() {
  const { id } = useParams<{ id: string }>();

  const { data, isLoading } = useApi(["narrative", id], () => getNarrative(id!), {
    enabled: !!id,
  });
  const velocity = useApi(["velocity", id], () => getNarrativeVelocity(id!), {
    enabled: !!id,
  });
  const predictions = useApi(["predictions", id], () => getNarrativePredictions(id!), {
    enabled: !!id,
  });
  const migrations = useApi(["migrations", id], () => getNarrativeMigrations(id!), {
    enabled: !!id,
  });

  if (isLoading) return <LoadingState />;
  if (!data) return <div className="text-sm text-[var(--muted)] py-8">Narrative not found</div>;

  const n = data.narrative;
  const latestSnapshot = n.snapshots?.[n.snapshots.length - 1];

  return (
    <div className="space-y-4 max-w-6xl">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-[var(--muted)]">
        <Link to="/narratives" className="hover:text-[var(--primary)]">Narratives</Link>
        <span>/</span>
        <span className="text-[var(--dark)]">{n.title}</span>
      </div>

      {/* Header */}
      <div className="bg-white rounded-lg border border-[#E8E6E3] px-5 py-4">
        <div className="flex items-start justify-between gap-4">
          <div className="flex-1">
            <h1 className="text-xl font-bold text-[var(--dark)]">{n.title}</h1>
            {n.description && (
              <p className="text-sm text-[var(--muted)] mt-1">{n.description}</p>
            )}
          </div>
          <StatusBadge status={n.status} />
        </div>

        <div className="flex items-center gap-4 mt-3 flex-wrap">
          {(n.platforms ?? []).map((p) => (
            <PlatformBadge key={p} platform={p} showName size="md" />
          ))}
          <div className="flex-1" />
          <div className="flex gap-4 text-xs text-[var(--muted)]">
            <span>Created: <TimeAgo timestamp={n.created_at} /></span>
            <span>Updated: <TimeAgo timestamp={n.updated_at} /></span>
          </div>
        </div>

        {/* Quick stats */}
        {latestSnapshot && (
          <div className="flex gap-6 mt-3 pt-3 border-t border-[#E8E6E3]">
            <div>
              <span className="text-[10px] text-[var(--muted)] uppercase">Content</span>
              <div className="text-lg font-bold text-[var(--dark)]">{latestSnapshot.content_count}</div>
            </div>
            <div>
              <span className="text-[10px] text-[var(--muted)] uppercase">Actors</span>
              <div className="text-lg font-bold text-[var(--dark)]">{latestSnapshot.actor_count}</div>
            </div>
            <div>
              <span className="text-[10px] text-[var(--muted)] uppercase">Velocity</span>
              <div className="text-lg font-bold text-[var(--primary)]">{latestSnapshot.velocity.toFixed(2)}</div>
            </div>
            {latestSnapshot.sentiment_avg !== null && (
              <div>
                <span className="text-[10px] text-[var(--muted)] uppercase">Sentiment</span>
                <div className="text-lg font-bold text-[var(--dark)]">{latestSnapshot.sentiment_avg.toFixed(2)}</div>
              </div>
            )}
          </div>
        )}

        {/* Keywords */}
        {n.keywords.length > 0 && (
          <div className="flex gap-1.5 flex-wrap mt-3">
            {n.keywords.map((kw) => (
              <span key={kw} className="text-[10px] bg-[var(--bg)] text-[var(--muted)] px-2 py-0.5 rounded font-medium">
                {kw}
              </span>
            ))}
          </div>
        )}
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-2 gap-4">
        {/* Velocity */}
        <div className="bg-white rounded-lg border border-[#E8E6E3] px-4 py-3">
          <h3 className="text-xs font-bold text-[var(--dark)] uppercase tracking-wide mb-2">
            Velocity
          </h3>
          <VelocityChart data={velocity.data?.velocity ?? data.velocity ?? []} height={220} />
        </div>

        {/* Engagement */}
        <div className="bg-white rounded-lg border border-[#E8E6E3] px-4 py-3">
          <h3 className="text-xs font-bold text-[var(--dark)] uppercase tracking-wide mb-2">
            Engagement
          </h3>
          <EngagementCurve data={n.snapshots ?? []} height={220} />
        </div>
      </div>

      {/* Platform timeline */}
      <div className="bg-white rounded-lg border border-[#E8E6E3] px-4 py-3">
        <h3 className="text-xs font-bold text-[var(--dark)] uppercase tracking-wide mb-3">
          Platform Migration Timeline
        </h3>
        <PlatformTimeline
          migrations={migrations.data?.migrations ?? []}
          platforms={n.platforms ?? []}
        />
      </div>

      {/* Predictions */}
      {(predictions.data?.predictions ?? []).length > 0 && (
        <div className="bg-white rounded-lg border border-[#E8E6E3] px-4 py-3">
          <h3 className="text-xs font-bold text-[var(--dark)] uppercase tracking-wide mb-2">
            Predictions
          </h3>
          <div className="space-y-2">
            {predictions.data!.predictions.map((pred) => (
              <div key={pred.id} className="bg-[var(--bg)] rounded px-3 py-2">
                <div className="flex items-center justify-between mb-1">
                  <span className="text-xs font-semibold text-[var(--dark)]">
                    {pred.prediction_type.replace(/_/g, " ")}
                  </span>
                  <span className="text-[10px] font-bold text-[var(--primary)]">
                    {(pred.confidence * 100).toFixed(0)}% confidence
                  </span>
                </div>
                <p className="text-xs text-[var(--muted)]">{pred.description}</p>
                {pred.caveats.length > 0 && (
                  <div className="mt-1.5">
                    {pred.caveats.map((c, i) => (
                      <p key={i} className="text-[10px] text-[var(--alert-medium)]">* {c}</p>
                    ))}
                  </div>
                )}
                {pred.predicted_timeframe_hours && (
                  <span className="text-[10px] text-[var(--muted)]">
                    Timeframe: {pred.predicted_timeframe_hours}h
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Related narratives */}
      {n.related_narrative_ids.length > 0 && (
        <div className="bg-white rounded-lg border border-[#E8E6E3] px-4 py-3">
          <h3 className="text-xs font-bold text-[var(--dark)] uppercase tracking-wide mb-2">
            Related Narratives
          </h3>
          <div className="flex gap-2 flex-wrap">
            {n.related_narrative_ids.map((rid) => (
              <Link
                key={rid}
                to={`/narratives/${rid}`}
                className="text-xs text-[var(--primary)] hover:underline bg-[var(--bg)] px-2 py-1 rounded"
              >
                {rid.slice(0, 8)}...
              </Link>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
