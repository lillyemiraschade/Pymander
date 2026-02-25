/* ── Full-screen graph explorer with side panel ── */

import { useState, useCallback, useRef, useEffect } from "react";
import { Link } from "react-router-dom";
import { useApi } from "../hooks/useApi";
import { getGraph, listCommunities } from "../lib/api";
import { GraphExplorer } from "../components/network/GraphExplorer";
import { PlatformBadge } from "../components/common/PlatformBadge";
import { LoadingState } from "../components/common/LoadingState";
import type { NetworkNode } from "../lib/types";

export function NetworkExplorerPage() {
  const [communityFilter, setCommunityFilter] = useState<string | null>(null);
  const [platformFilter, setPlatformFilter] = useState<string | null>(null);
  const [influenceThreshold, setInfluenceThreshold] = useState(0);
  const [selectedNode, setSelectedNode] = useState<NetworkNode | null>(null);
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 });
  const containerRef = useRef<HTMLDivElement>(null);

  const graph = useApi(
    ["graph", communityFilter, platformFilter, influenceThreshold],
    () =>
      getGraph({
        community_id: communityFilter ?? undefined,
        platform: platformFilter ?? undefined,
        min_influence: influenceThreshold,
        limit: 2000,
      }),
  );

  const communities = useApi(["communities"], listCommunities);

  const handleNodeClick = useCallback((node: NetworkNode) => {
    setSelectedNode(node);
  }, []);

  // Responsive canvas sizing
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const obs = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (entry) {
        setDimensions({
          width: entry.contentRect.width,
          height: entry.contentRect.height,
        });
      }
    });
    obs.observe(el);
    return () => obs.disconnect();
  }, []);

  return (
    <div className="flex h-full">
      {/* Graph area */}
      <div className="flex-1 relative" ref={containerRef}>
        {graph.isLoading ? (
          <LoadingState message="Loading graph..." />
        ) : (
          <GraphExplorer
            nodes={graph.data?.nodes ?? []}
            edges={graph.data?.edges ?? []}
            width={dimensions.width}
            height={dimensions.height}
            onNodeClick={handleNodeClick}
            communityFilter={communityFilter}
            platformFilter={platformFilter}
            influenceThreshold={influenceThreshold}
          />
        )}

        {/* Filter bar */}
        <div className="absolute bottom-3 left-3 right-3 flex gap-2 bg-white/95 rounded-lg border border-[#E8E6E3] px-3 py-2 shadow-sm">
          {/* Community filter */}
          <select
            value={communityFilter ?? ""}
            onChange={(e) => setCommunityFilter(e.target.value || null)}
            className="text-[11px] bg-[var(--bg)] rounded px-2 py-1 border border-[#E8E6E3]"
          >
            <option value="">All communities</option>
            {(communities.data?.communities ?? []).map((c) => (
              <option key={c.id} value={c.id}>
                {c.name ?? c.id} ({c.node_count})
              </option>
            ))}
          </select>

          {/* Platform filter */}
          <select
            value={platformFilter ?? ""}
            onChange={(e) => setPlatformFilter(e.target.value || null)}
            className="text-[11px] bg-[var(--bg)] rounded px-2 py-1 border border-[#E8E6E3]"
          >
            <option value="">All platforms</option>
            {["twitter", "reddit", "telegram", "youtube", "4chan", "bluesky"].map((p) => (
              <option key={p} value={p}>{p}</option>
            ))}
          </select>

          {/* Influence slider */}
          <div className="flex items-center gap-2 flex-1">
            <span className="text-[10px] text-[var(--muted)] shrink-0">Influence:</span>
            <input
              type="range"
              min={0}
              max={100}
              value={influenceThreshold * 100}
              onChange={(e) => setInfluenceThreshold(Number(e.target.value) / 100)}
              className="flex-1 h-1 accent-[var(--primary)]"
            />
            <span className="text-[10px] text-[var(--dark)] w-8">
              {(influenceThreshold * 100).toFixed(0)}%
            </span>
          </div>
        </div>
      </div>

      {/* Side panel */}
      {selectedNode && (
        <div className="w-72 bg-white border-l border-[#E8E6E3] p-4 overflow-y-auto shrink-0">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-bold text-[var(--dark)]">Node Detail</h3>
            <button
              onClick={() => setSelectedNode(null)}
              className="text-[var(--muted)] hover:text-[var(--dark)] text-sm"
            >
              x
            </button>
          </div>

          <div className="space-y-3">
            <div>
              <span className="text-[10px] text-[var(--muted)] uppercase">Username</span>
              <div className="text-sm font-semibold text-[var(--dark)]">{selectedNode.label}</div>
            </div>

            {selectedNode.platform && (
              <div>
                <span className="text-[10px] text-[var(--muted)] uppercase">Platform</span>
                <div className="mt-1">
                  <PlatformBadge platform={selectedNode.platform} showName size="md" />
                </div>
              </div>
            )}

            <div className="grid grid-cols-2 gap-2">
              <div>
                <span className="text-[10px] text-[var(--muted)] uppercase">Influence</span>
                <div className="text-sm font-bold text-[var(--primary)]">
                  {selectedNode.influence.toFixed(3)}
                </div>
              </div>
              {selectedNode.bridge_score !== undefined && (
                <div>
                  <span className="text-[10px] text-[var(--muted)] uppercase">Bridge Score</span>
                  <div className="text-sm font-bold text-[var(--dark)]">
                    {selectedNode.bridge_score.toFixed(3)}
                  </div>
                </div>
              )}
              {selectedNode.content_count !== undefined && (
                <div>
                  <span className="text-[10px] text-[var(--muted)] uppercase">Content</span>
                  <div className="text-sm font-bold text-[var(--dark)]">{selectedNode.content_count}</div>
                </div>
              )}
              {selectedNode.community && (
                <div>
                  <span className="text-[10px] text-[var(--muted)] uppercase">Community</span>
                  <div className="text-xs font-mono text-[var(--dark)]">
                    {selectedNode.community.slice(0, 8)}
                  </div>
                </div>
              )}
            </div>

            {selectedNode.coordination_cluster && (
              <div className="bg-[#C2553A12] rounded px-2 py-1.5">
                <span className="text-[10px] font-bold text-[var(--alert-high)]">
                  COORDINATION CLUSTER
                </span>
                <div className="text-xs font-mono text-[var(--dark)]">
                  {selectedNode.coordination_cluster.slice(0, 12)}...
                </div>
              </div>
            )}

            <Link
              to={`/actors/${selectedNode.id}`}
              className="block text-xs text-center bg-[var(--primary)] text-white rounded py-1.5 hover:bg-[#4A6346] transition-colors"
            >
              View Full Profile
            </Link>
          </div>
        </div>
      )}
    </div>
  );
}
