/* ── D3 force-directed graph (Canvas renderer) ── */

import { useRef, useEffect, useCallback, useState, useMemo } from "react";
import * as d3 from "d3";
import type { NetworkNode, NetworkEdge } from "../../lib/types";
import { communityColor, colors } from "../../lib/constants";
import { PlatformBadge } from "../common/PlatformBadge";

// ── Internal simulation types ──────────────────────

interface SimNode extends d3.SimulationNodeDatum {
  id: string;
  label: string;
  platform: string | null;
  influence: number;
  bridge_score?: number;
  community: string | null;
  content_count?: number;
  coordination_cluster?: string | null;
}

interface SimEdge extends d3.SimulationLinkDatum<SimNode> {
  edge_type: string;
  weight: number;
  interactions?: number;
}

// ── Constants ──────────────────────────────────────

const MAX_VISIBLE = 5000;
const NODE_MIN_R = 3;
const NODE_MAX_R = 18;

interface GraphExplorerProps {
  nodes: NetworkNode[];
  edges: NetworkEdge[];
  width?: number;
  height?: number;
  onNodeClick?: (node: NetworkNode) => void;
  communityFilter?: string | null;
  platformFilter?: string | null;
  influenceThreshold?: number;
}

export function GraphExplorer({
  nodes,
  edges,
  width = 800,
  height = 600,
  onNodeClick,
  communityFilter,
  platformFilter,
  influenceThreshold = 0,
}: GraphExplorerProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const simRef = useRef<d3.Simulation<SimNode, SimEdge> | null>(null);
  const [hoveredNode, setHoveredNode] = useState<SimNode | null>(null);
  const transformRef = useRef(d3.zoomIdentity);

  // ── Filter and limit nodes ─────────────────────

  const { filteredNodes, filteredEdges } = useMemo(() => {
    let fNodes = nodes.slice();
    if (communityFilter) fNodes = fNodes.filter((n) => n.community === communityFilter);
    if (platformFilter) fNodes = fNodes.filter((n) => n.platform === platformFilter);
    if (influenceThreshold > 0) fNodes = fNodes.filter((n) => n.influence >= influenceThreshold);
    fNodes = fNodes.slice(0, MAX_VISIBLE);

    const nodeIds = new Set(fNodes.map((n) => n.id));
    const fEdges = edges.filter((e) => nodeIds.has(e.source) && nodeIds.has(e.target));

    return { filteredNodes: fNodes, filteredEdges: fEdges };
  }, [nodes, edges, communityFilter, platformFilter, influenceThreshold]);

  // ── Radius scale ───────────────────────────────

  const radiusScale = useMemo(() => {
    const maxInfluence = Math.max(...filteredNodes.map((n) => n.influence), 0.01);
    return d3.scaleSqrt().domain([0, maxInfluence]).range([NODE_MIN_R, NODE_MAX_R]);
  }, [filteredNodes]);

  // ── Build simulation data ──────────────────────

  const simNodes = useMemo<SimNode[]>(
    () => filteredNodes.map((n) => ({ ...n })),
    [filteredNodes],
  );

  const simEdges = useMemo<SimEdge[]>(
    () =>
      filteredEdges.map((e) => ({
        source: e.source,
        target: e.target,
        edge_type: e.edge_type,
        weight: e.weight,
        interactions: e.interactions,
      })),
    [filteredEdges],
  );

  // ── Draw function ──────────────────────────────

  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const t = transformRef.current;
    ctx.save();
    ctx.clearRect(0, 0, width, height);
    ctx.translate(t.x, t.y);
    ctx.scale(t.k, t.k);

    // Edges
    ctx.globalAlpha = 0.15;
    simEdges.forEach((e) => {
      const s = e.source as SimNode;
      const tgt = e.target as SimNode;
      if (s.x == null || s.y == null || tgt.x == null || tgt.y == null) return;
      ctx.beginPath();
      ctx.moveTo(s.x, s.y);
      ctx.lineTo(tgt.x, tgt.y);
      ctx.strokeStyle = colors.muted;
      ctx.lineWidth = Math.min(e.weight * 0.5, 3);
      ctx.stroke();
    });

    // Nodes
    ctx.globalAlpha = 1;
    simNodes.forEach((n) => {
      if (n.x == null || n.y == null) return;
      const r = radiusScale(n.influence);
      const color = communityColor(n.community);

      ctx.beginPath();
      ctx.arc(n.x, n.y, r, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();

      // Coordination cluster ring
      if (n.coordination_cluster) {
        ctx.beginPath();
        ctx.arc(n.x, n.y, r + 1.5, 0, Math.PI * 2);
        ctx.strokeStyle = colors.alertHigh;
        ctx.lineWidth = 1.5;
        ctx.stroke();
      }
    });

    // Hovered node label
    if (hoveredNode && hoveredNode.x != null && hoveredNode.y != null) {
      const r = radiusScale(hoveredNode.influence);
      ctx.font = "bold 10px 'DM Sans'";
      ctx.fillStyle = colors.dark;
      ctx.textAlign = "center";
      ctx.fillText(hoveredNode.label, hoveredNode.x, hoveredNode.y - r - 4);
    }

    ctx.restore();
  }, [simNodes, simEdges, width, height, radiusScale, hoveredNode]);

  // ── Simulation ─────────────────────────────────

  useEffect(() => {
    if (simRef.current) simRef.current.stop();

    const sim = d3
      .forceSimulation<SimNode>(simNodes)
      .force(
        "link",
        d3
          .forceLink<SimNode, SimEdge>(simEdges)
          .id((d) => d.id)
          .distance(60)
          .strength((d) => Math.min(d.weight * 0.1, 0.5)),
      )
      .force("charge", d3.forceManyBody().strength(-40).distanceMax(300))
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("collision", d3.forceCollide<SimNode>().radius((d) => radiusScale(d.influence) + 2))
      .alphaDecay(0.02)
      .on("tick", draw);

    simRef.current = sim;

    return () => {
      sim.stop();
    };
  }, [simNodes, simEdges, width, height, radiusScale, draw]);

  // ── Zoom + interaction ─────────────────────────

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const selection = d3.select(canvas);

    const zoom = d3
      .zoom<HTMLCanvasElement, unknown>()
      .scaleExtent([0.1, 8])
      .on("zoom", (event: d3.D3ZoomEvent<HTMLCanvasElement, unknown>) => {
        transformRef.current = event.transform;
        draw();
      });

    selection.call(zoom);

    // Hit detection
    const findNode = (mx: number, my: number): SimNode | null => {
      const t = transformRef.current;
      const x = (mx - t.x) / t.k;
      const y = (my - t.y) / t.k;
      for (let i = simNodes.length - 1; i >= 0; i--) {
        const n = simNodes[i]!;
        if (n.x == null || n.y == null) continue;
        const r = radiusScale(n.influence);
        const dx = x - n.x;
        const dy = y - n.y;
        if (dx * dx + dy * dy < (r + 2) * (r + 2)) return n;
      }
      return null;
    };

    const onMove = (e: MouseEvent) => {
      const rect = canvas.getBoundingClientRect();
      const node = findNode(e.clientX - rect.left, e.clientY - rect.top);
      setHoveredNode(node);
      canvas.style.cursor = node ? "pointer" : "grab";
    };

    const onClick = (e: MouseEvent) => {
      const rect = canvas.getBoundingClientRect();
      const node = findNode(e.clientX - rect.left, e.clientY - rect.top);
      if (node && onNodeClick) {
        onNodeClick(node as unknown as NetworkNode);
      }
    };

    canvas.addEventListener("mousemove", onMove);
    canvas.addEventListener("click", onClick);

    return () => {
      canvas.removeEventListener("mousemove", onMove);
      canvas.removeEventListener("click", onClick);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      selection.on(".zoom", null) as any;
    };
  }, [simNodes, radiusScale, draw, onNodeClick]);

  return (
    <div className="relative">
      <canvas
        ref={canvasRef}
        width={width}
        height={height}
        className="rounded-lg bg-white border border-[#E8E6E3]"
      />

      {/* Stats overlay */}
      <div className="absolute top-2 left-2 bg-white/90 rounded px-2 py-1 text-[10px] text-[var(--muted)] border border-[#E8E6E3]">
        {filteredNodes.length} nodes | {filteredEdges.length} edges
      </div>

      {/* Hovered node detail */}
      {hoveredNode && (
        <div className="absolute top-2 right-2 bg-white rounded-lg border border-[#E8E6E3] px-3 py-2 shadow-sm max-w-52">
          <div className="text-xs font-bold text-[var(--dark)]">{hoveredNode.label}</div>
          <div className="flex items-center gap-2 mt-1">
            {hoveredNode.platform && <PlatformBadge platform={hoveredNode.platform} showName />}
          </div>
          <div className="flex gap-3 mt-1 text-[10px] text-[var(--muted)]">
            <span>Influence: {hoveredNode.influence.toFixed(3)}</span>
            {hoveredNode.content_count != null && <span>{hoveredNode.content_count} posts</span>}
          </div>
        </div>
      )}
    </div>
  );
}
