/* ── Full briefing content ── */

import { useParams, Link } from "react-router-dom";
import { useApi } from "../hooks/useApi";
import { getBriefing } from "../lib/api";
import { LoadingState } from "../components/common/LoadingState";
import { TimeAgo } from "../components/common/TimeAgo";

export function BriefingDetail() {
  const { id } = useParams<{ id: string }>();
  const { data, isLoading } = useApi(["briefing", id], () => getBriefing(id!), {
    enabled: !!id,
  });

  if (isLoading) return <LoadingState />;
  if (!data) return <div className="text-sm text-[var(--muted)] py-8">Briefing not found</div>;

  return (
    <div className="space-y-4 max-w-4xl">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-[var(--muted)]">
        <Link to="/briefings" className="hover:text-[var(--primary)]">Briefings</Link>
        <span>/</span>
        <span className="text-[var(--dark)]">{data.type} briefing</span>
      </div>

      {/* Header */}
      <div className="bg-white rounded-lg border border-[#E8E6E3] px-5 py-4">
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center gap-2">
            <span className="text-[10px] font-bold uppercase px-2 py-0.5 rounded bg-[var(--primary)] text-white">
              {data.type}
            </span>
            <h1 className="text-lg font-bold text-[var(--dark)]">
              {new Date(data.generated_at).toLocaleDateString(undefined, {
                weekday: "long",
                year: "numeric",
                month: "long",
                day: "numeric",
              })}
            </h1>
          </div>
          <TimeAgo timestamp={data.generated_at} />
        </div>
        <div className="flex gap-4 text-[10px] text-[var(--muted)]">
          {data.model_used && <span>Model: {data.model_used}</span>}
          {data.token_cost > 0 && <span>Tokens: {data.token_cost.toLocaleString()}</span>}
          <span>Status: {data.status}</span>
        </div>
      </div>

      {/* Content */}
      <div className="bg-white rounded-lg border border-[#E8E6E3] px-5 py-4">
        <div className="prose prose-sm max-w-none text-[var(--dark)]">
          {data.content.split("\n").map((line, i) => (
            <p key={i} className={`text-sm leading-relaxed ${line.startsWith("#") ? "font-bold text-base mt-4" : "mt-1"}`}>
              {line.replace(/^#+\s*/, "")}
            </p>
          ))}
        </div>
      </div>

      {/* Sections (structured data) */}
      {Object.keys(data.sections).length > 0 && (
        <div className="bg-white rounded-lg border border-[#E8E6E3] px-5 py-4">
          <h3 className="text-xs font-bold text-[var(--dark)] uppercase tracking-wide mb-3">
            Structured Sections
          </h3>
          <div className="space-y-3">
            {Object.entries(data.sections).map(([key, val]) => (
              <div key={key}>
                <h4 className="text-xs font-semibold text-[var(--dark)] mb-1">
                  {key.replace(/_/g, " ")}
                </h4>
                <pre className="text-[10px] text-[var(--muted)] bg-[var(--bg)] rounded p-2 whitespace-pre-wrap overflow-x-auto">
                  {JSON.stringify(val, null, 2)}
                </pre>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Data snapshot */}
      {Object.keys(data.data_snapshot).length > 0 && (
        <div className="bg-white rounded-lg border border-[#E8E6E3] px-5 py-4">
          <h3 className="text-xs font-bold text-[var(--dark)] uppercase tracking-wide mb-3">
            Data Snapshot
          </h3>
          <pre className="text-[10px] text-[var(--muted)] bg-[var(--bg)] rounded p-3 whitespace-pre-wrap overflow-x-auto max-h-64 overflow-y-auto">
            {JSON.stringify(data.data_snapshot, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}
