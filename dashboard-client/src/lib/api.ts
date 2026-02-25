/* ── Pymander v2 API client ── */

import type {
  NarrativeListResponse,
  NarrativeDetailResponse,
  VelocityResponse,
  PredictionsResponse,
  MigrationsResponse,
  AlertsResponse,
  BriefingsResponse,
  Briefing,
  ClustersResponse,
  CoordinationCluster,
  SignalsResponse,
  GraphResponse,
  CommunitiesResponse,
  ActorDetailResponse,
  ActorHistoryResponse,
  SearchResponse,
} from "./types";

const BASE = "/api/v2";

function apiKey(): string {
  return localStorage.getItem("pymander_api_key") ?? "";
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": apiKey(),
      ...init?.headers,
    },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${body}`);
  }
  return res.json() as Promise<T>;
}

// ── Narratives ────────────────────────────────────

export function listNarratives(params?: {
  status?: string;
  limit?: number;
  cursor?: string;
}): Promise<NarrativeListResponse> {
  const sp = new URLSearchParams();
  if (params?.status) sp.set("status", params.status);
  if (params?.limit) sp.set("limit", String(params.limit));
  if (params?.cursor) sp.set("cursor", params.cursor);
  const qs = sp.toString();
  return request(`/narratives${qs ? `?${qs}` : ""}`);
}

export function getNarrative(id: string): Promise<NarrativeDetailResponse> {
  return request(`/narratives/${id}`);
}

export function getNarrativeVelocity(id: string): Promise<VelocityResponse> {
  return request(`/narratives/${id}/velocity`);
}

export function getNarrativePredictions(id: string): Promise<PredictionsResponse> {
  return request(`/narratives/${id}/predictions`);
}

export function getNarrativeMigrations(id: string): Promise<MigrationsResponse> {
  return request(`/narratives/${id}/migrations`);
}

// ── Alerts ────────────────────────────────────────

export function listAlerts(params?: {
  hours?: number;
  severity?: string;
  limit?: number;
}): Promise<AlertsResponse> {
  const sp = new URLSearchParams();
  if (params?.hours) sp.set("hours", String(params.hours));
  if (params?.severity) sp.set("severity", params.severity);
  if (params?.limit) sp.set("limit", String(params.limit));
  const qs = sp.toString();
  return request(`/alerts${qs ? `?${qs}` : ""}`);
}

export function acknowledgeAlert(id: string): Promise<{ status: string; alert_id: string }> {
  return request(`/alerts/${id}/acknowledge`, { method: "PUT" });
}

// ── Briefings ─────────────────────────────────────

export function listBriefings(limit = 20): Promise<BriefingsResponse> {
  return request(`/briefings?limit=${limit}`);
}

export function getLatestBriefing(type = "daily"): Promise<Briefing> {
  return request(`/briefings/latest?briefing_type=${type}`);
}

export function getBriefing(id: string): Promise<Briefing> {
  return request(`/briefings/${id}`);
}

export function generateBriefing(hours = 24): Promise<Briefing> {
  return request(`/briefings/generate?hours=${hours}`, { method: "POST" });
}

// ── Coordination ──────────────────────────────────

export function listClusters(minConfidence = 0): Promise<ClustersResponse> {
  const sp = new URLSearchParams();
  if (minConfidence > 0) sp.set("min_confidence", String(minConfidence));
  const qs = sp.toString();
  return request(`/coordination/clusters${qs ? `?${qs}` : ""}`);
}

export function getCluster(id: string): Promise<CoordinationCluster> {
  return request(`/coordination/clusters/${id}`);
}

export function listSignals(params?: {
  hours?: number;
  signal_type?: string;
  limit?: number;
}): Promise<SignalsResponse> {
  const sp = new URLSearchParams();
  if (params?.hours) sp.set("hours", String(params.hours));
  if (params?.signal_type) sp.set("signal_type", params.signal_type);
  if (params?.limit) sp.set("limit", String(params.limit));
  const qs = sp.toString();
  return request(`/coordination/signals${qs ? `?${qs}` : ""}`);
}

// ── Network ───────────────────────────────────────

export function getGraph(params?: {
  community_id?: string;
  min_influence?: number;
  platform?: string;
  limit?: number;
}): Promise<GraphResponse> {
  const sp = new URLSearchParams();
  if (params?.community_id) sp.set("community_id", params.community_id);
  if (params?.min_influence) sp.set("min_influence", String(params.min_influence));
  if (params?.platform) sp.set("platform", params.platform);
  if (params?.limit) sp.set("limit", String(params.limit));
  const qs = sp.toString();
  return request(`/network/graph${qs ? `?${qs}` : ""}`);
}

export function listCommunities(): Promise<CommunitiesResponse> {
  return request(`/network/communities`);
}

export function listBridges(limit = 50): Promise<{ count: number; bridges: unknown[] }> {
  return request(`/network/bridges?limit=${limit}`);
}

export function getActor(id: string): Promise<ActorDetailResponse> {
  return request(`/network/actors/${id}`);
}

export function getActorHistory(id: string): Promise<ActorHistoryResponse> {
  return request(`/network/actors/${id}/history`);
}

export function getShortestPath(
  fromId: string,
  toId: string,
): Promise<{ path_found: boolean; path_length?: number; nodes?: unknown[]; edges?: unknown[] }> {
  return request(`/network/path/${fromId}/${toId}`);
}

// ── Search ────────────────────────────────────────

export function globalSearch(params: {
  q: string;
  platform?: string;
  content_type?: string;
  limit?: number;
}): Promise<SearchResponse> {
  const sp = new URLSearchParams({ q: params.q });
  if (params.platform) sp.set("platform", params.platform);
  if (params.content_type) sp.set("content_type", params.content_type);
  if (params.limit) sp.set("limit", String(params.limit));
  return request(`/search?${sp}`);
}
