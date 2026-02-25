/* ── Global search with tabbed results ── */

import { useState, useCallback } from "react";
import { Link } from "react-router-dom";
import { useApi } from "../hooks/useApi";
import { globalSearch } from "../lib/api";
import { PlatformBadge } from "../components/common/PlatformBadge";
import { StatusBadge } from "../components/common/StatusBadge";
import { LoadingState } from "../components/common/LoadingState";
import type { NarrativeStatus } from "../lib/types";

type TabKey = "narratives" | "actors" | "alerts";

export function SearchPage() {
  const [query, setQuery] = useState("");
  const [submittedQuery, setSubmittedQuery] = useState("");
  const [activeTab, setActiveTab] = useState<TabKey>("narratives");

  const { data, isLoading } = useApi(
    ["search", submittedQuery],
    () => globalSearch({ q: submittedQuery }),
    { enabled: submittedQuery.length >= 2 },
  );

  const handleSubmit = useCallback(
    (e: React.FormEvent) => {
      e.preventDefault();
      if (query.length >= 2) setSubmittedQuery(query);
    },
    [query],
  );

  const tabs: { key: TabKey; label: string; count: number }[] = [
    { key: "narratives", label: "Narratives", count: data?.results.narratives.length ?? 0 },
    { key: "actors", label: "Actors", count: data?.results.actors.length ?? 0 },
    { key: "alerts", label: "Alerts", count: data?.results.alerts.length ?? 0 },
  ];

  return (
    <div className="space-y-4 max-w-4xl">
      <h1 className="text-lg font-bold text-[var(--dark)]">Search</h1>

      {/* Search input */}
      <form onSubmit={handleSubmit} className="flex gap-2">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search narratives, actors, alerts..."
          className="flex-1 text-sm bg-white border border-[#E8E6E3] rounded-lg px-4 py-2.5 focus:outline-none focus:border-[var(--primary)] transition-colors"
        />
        <button
          type="submit"
          className="text-sm bg-[var(--primary)] text-white px-5 py-2.5 rounded-lg hover:bg-[#4A6346] transition-colors"
        >
          Search
        </button>
      </form>

      {/* Results */}
      {submittedQuery && (
        <>
          {/* Tabs */}
          <div className="flex gap-0.5 border-b border-[#E8E6E3]">
            {tabs.map((tab) => (
              <button
                key={tab.key}
                onClick={() => setActiveTab(tab.key)}
                className={`text-xs font-medium px-3 py-2 border-b-2 transition-colors ${
                  activeTab === tab.key
                    ? "border-[var(--primary)] text-[var(--primary)]"
                    : "border-transparent text-[var(--muted)] hover:text-[var(--dark)]"
                }`}
              >
                {tab.label} ({tab.count})
              </button>
            ))}
            <div className="flex-1" />
            {data && (
              <span className="text-[10px] text-[var(--muted)] self-center">
                {data.total} results for "{data.query}"
              </span>
            )}
          </div>

          {isLoading ? (
            <LoadingState message="Searching..." />
          ) : (
            <div className="space-y-1.5">
              {/* Narratives tab */}
              {activeTab === "narratives" &&
                (data?.results.narratives ?? []).map((r) => (
                  <Link
                    key={r.id}
                    to={`/narratives/${r.id}`}
                    className="flex items-center justify-between bg-white rounded-lg border border-[#E8E6E3] hover:border-[var(--primary)] px-3 py-2 transition-all"
                  >
                    <div className="flex-1 min-w-0">
                      <div className="text-sm font-medium text-[var(--dark)] truncate">
                        {r.summary ?? r.id}
                      </div>
                    </div>
                    {r.status && <StatusBadge status={r.status as NarrativeStatus} />}
                  </Link>
                ))}

              {/* Actors tab */}
              {activeTab === "actors" &&
                (data?.results.actors ?? []).map((r) => (
                  <Link
                    key={r.id}
                    to={`/actors/${r.id}`}
                    className="flex items-center gap-3 bg-white rounded-lg border border-[#E8E6E3] hover:border-[var(--primary)] px-3 py-2 transition-all"
                  >
                    {r.platform && <PlatformBadge platform={r.platform} />}
                    <span className="text-sm font-medium text-[var(--dark)]">{r.username ?? r.id}</span>
                    {r.influence !== undefined && (
                      <span className="text-[10px] text-[var(--muted)] ml-auto">
                        influence: {r.influence.toFixed(3)}
                      </span>
                    )}
                  </Link>
                ))}

              {/* Alerts tab */}
              {activeTab === "alerts" &&
                (data?.results.alerts ?? []).map((r) => (
                  <Link
                    key={r.id}
                    to="/alerts"
                    className="flex items-center gap-3 bg-white rounded-lg border border-[#E8E6E3] hover:border-[var(--primary)] px-3 py-2 transition-all"
                  >
                    <span className="text-sm font-medium text-[var(--dark)]">{r.summary ?? r.id}</span>
                  </Link>
                ))}

              {/* Empty state */}
              {data &&
                (data.results[activeTab] ?? []).length === 0 && (
                  <div className="text-sm text-[var(--muted)] text-center py-8">
                    No {activeTab} found for "{submittedQuery}"
                  </div>
                )}
            </div>
          )}
        </>
      )}
    </div>
  );
}
