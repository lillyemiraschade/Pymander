/* ── Briefing list ── */

import { Link } from "react-router-dom";
import { useApi } from "../hooks/useApi";
import { listBriefings, generateBriefing } from "../lib/api";
import { LoadingState } from "../components/common/LoadingState";
import { TimeAgo } from "../components/common/TimeAgo";
import { useMutation, useQueryClient } from "@tanstack/react-query";

export function BriefingList() {
  const queryClient = useQueryClient();
  const { data, isLoading } = useApi(["briefings"], () => listBriefings(50));

  const generate = useMutation({
    mutationFn: () => generateBriefing(24),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["briefings"] });
    },
  });

  if (isLoading) return <LoadingState />;

  return (
    <div className="space-y-4 max-w-4xl">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-bold text-[var(--dark)]">Briefings</h1>
        <button
          onClick={() => generate.mutate()}
          disabled={generate.isPending}
          className="text-xs bg-[var(--primary)] text-white px-3 py-1.5 rounded hover:bg-[#4A6346] transition-colors disabled:opacity-50"
        >
          {generate.isPending ? "Generating..." : "Generate Briefing"}
        </button>
      </div>

      {(data?.briefings ?? []).length === 0 ? (
        <div className="text-sm text-[var(--muted)] text-center py-12">No briefings generated yet</div>
      ) : (
        <div className="space-y-2">
          {data!.briefings.map((briefing) => (
            <Link
              key={briefing.id}
              to={`/briefings/${briefing.id}`}
              className="block bg-white rounded-lg border border-[#E8E6E3] hover:border-[var(--primary)] transition-all px-4 py-3"
            >
              <div className="flex items-center justify-between mb-1">
                <div className="flex items-center gap-2">
                  <span className="text-[10px] font-bold uppercase px-1.5 py-0.5 rounded bg-[var(--bg)] text-[var(--primary)]">
                    {briefing.type}
                  </span>
                  <span className="text-sm font-semibold text-[var(--dark)]">
                    {new Date(briefing.generated_at).toLocaleDateString(undefined, {
                      weekday: "short",
                      month: "short",
                      day: "numeric",
                    })}
                  </span>
                </div>
                <TimeAgo timestamp={briefing.generated_at} />
              </div>
              <div className="flex gap-4 text-[10px] text-[var(--muted)]">
                {briefing.period_start && (
                  <span>
                    Period: {new Date(briefing.period_start).toLocaleDateString()} -{" "}
                    {briefing.period_end ? new Date(briefing.period_end).toLocaleDateString() : "now"}
                  </span>
                )}
                {briefing.model_used && <span>Model: {briefing.model_used}</span>}
                {briefing.token_cost > 0 && <span>Tokens: {briefing.token_cost.toLocaleString()}</span>}
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}
