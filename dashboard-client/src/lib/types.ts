/* ── Pymander client-facing TypeScript types ── */

// ── Enums ──────────────────────────────────────────────

export type NarrativeStatus =
  | "emerging"
  | "growing"
  | "viral"
  | "peaking"
  | "declining"
  | "dormant"
  | "dead"
  | "resurgent";

export type Platform =
  | "twitter"
  | "reddit"
  | "telegram"
  | "youtube"
  | "tiktok"
  | "facebook"
  | "instagram"
  | "4chan"
  | "gab"
  | "truth_social"
  | "rumble"
  | "rss"
  | "web"
  | "bluesky"
  | "substack"
  | "wikipedia"
  | "other";

export type AlertSeverity = "low" | "medium" | "high" | "critical";

export type CoordinationSignalType =
  | "temporal_burst"
  | "synchronized_activation"
  | "time_zone_anomaly"
  | "semantic_clone"
  | "template_language"
  | "amplification_chain"
  | "star_topology"
  | "fresh_account_swarm"
  | "posting_regularity"
  | "content_diversity_anomaly"
  | "engagement_asymmetry";

export type BriefingType = "daily" | "weekly" | "on_demand" | "alert";

// ── Narrative ──────────────────────────────────────────

export interface NarrativeSnapshot {
  timestamp: string;
  content_count: number;
  actor_count: number;
  velocity: number;
  sentiment_avg: number | null;
}

export interface Narrative {
  id: string;
  narrative_id?: string;
  title: string;
  description: string | null;
  summary?: string;
  status: NarrativeStatus;
  created_at: string;
  updated_at: string;
  keywords: string[];
  representative_content_ids: string[];
  snapshots: NarrativeSnapshot[];
  parent_narrative_id: string | null;
  related_narrative_ids: string[];
  platforms?: Platform[];
  coordination_detected?: boolean;
  content_count?: number;
  actor_count?: number;
  velocity?: number;
}

export interface NarrativeListResponse {
  count: number;
  cursor: string | null;
  narratives: Narrative[];
}

export interface NarrativeDetailResponse {
  narrative: Narrative;
  velocity: VelocityPoint[] | null;
}

// ── Velocity ───────────────────────────────────────────

export interface VelocityPoint {
  timestamp: string;
  velocity: number;
  content_count?: number;
  actor_count?: number;
}

export interface VelocityResponse {
  narrative_id: string;
  velocity: VelocityPoint[];
}

// ── Predictions ────────────────────────────────────────

export interface PatternMatch {
  matched_narrative_id: string;
  matched_narrative_summary: string;
  similarity_score: number;
  outcome_summary: string;
  matched_lifecycle: Record<string, unknown>;
  similarity_dimensions: Record<string, unknown>;
}

export interface Prediction {
  id: string;
  narrative_id: string;
  prediction_type: string;
  description: string;
  confidence: number;
  basis: PatternMatch[];
  caveats: string[];
  predicted_at: string;
  predicted_timeframe_hours: number | null;
  outcome: string | null;
}

export interface PredictionsResponse {
  narrative_id: string;
  predictions: Prediction[];
}

// ── Coordination ───────────────────────────────────────

export interface CoordinationSignal {
  id: string;
  type: CoordinationSignalType;
  narrative_id: string | null;
  accounts: string[];
  confidence: number;
  evidence: Record<string, unknown>;
  detected_at: string;
  platform: string | null;
  alert_category?: string;
  acknowledged?: boolean;
}

export interface CoordinationCluster {
  cluster_id: string;
  accounts: string[];
  account_count: number;
  confidence: number;
  signal_types: CoordinationSignalType[];
  signal_count: number;
  signals: Record<string, unknown>[];
  associated_narratives: string[];
  first_detected: string | null;
  last_signal: string | null;
  estimated_reach: number;
  status: string;
  analyst_notes: string;
  severity: AlertSeverity;
}

export interface ClustersResponse {
  count: number;
  clusters: CoordinationCluster[];
}

export interface SignalsResponse {
  count: number;
  signals: CoordinationSignal[];
}

// ── Migration ──────────────────────────────────────────

export interface MigrationEvent {
  id: string;
  narrative_id: string;
  from_platform: string;
  to_platform: string;
  detected_at: string;
  migration_time_seconds: number;
  bridge_content_ids: string[];
  bridge_account_ids: string[];
}

export interface MigrationsResponse {
  narrative_id: string;
  migrations: MigrationEvent[];
}

// ── Alerts ─────────────────────────────────────────────

export interface Alert {
  id: string;
  type: string;
  severity: AlertSeverity;
  alert_category: string;
  detected_at: string;
  timestamp?: string;
  narrative_id?: string | null;
  community_id?: string | null;
  details?: Record<string, unknown>;
  accounts?: string[];
  confidence?: number;
  acknowledged?: boolean;
  acknowledged_by?: string;
}

export interface AlertsResponse {
  count: number;
  alerts: Alert[];
}

// ── Briefing ───────────────────────────────────────────

export interface BriefingSummary {
  id: string;
  type: BriefingType;
  generated_at: string;
  period_start: string | null;
  period_end: string | null;
  model_used: string;
  token_cost: number;
}

export interface Briefing extends BriefingSummary {
  content: string;
  sections: Record<string, unknown>;
  data_snapshot: Record<string, unknown>;
  status: string;
}

export interface BriefingsResponse {
  count: number;
  briefings: BriefingSummary[];
}

// ── Network ────────────────────────────────────────────

export interface NetworkNode {
  id: string;
  label: string;
  platform: string | null;
  influence: number;
  bridge_score?: number;
  community: string | null;
  content_count?: number;
  coordination_cluster?: string | null;
}

export interface NetworkEdge {
  source: string;
  target: string;
  edge_type: string;
  weight: number;
  interactions?: number;
}

export interface GraphResponse {
  nodes: NetworkNode[];
  edges: NetworkEdge[];
}

export interface Community {
  id: string;
  name?: string;
  node_count: number;
  avg_influence?: number;
  platforms?: string[];
}

export interface CommunitiesResponse {
  count: number;
  communities: Community[];
}

// ── Actor ──────────────────────────────────────────────

export interface Actor {
  internal_uuid: string;
  username: string;
  display_name?: string;
  primary_platform: Platform;
  influence_score: number;
  bridge_score?: number;
  community_id?: string;
  total_content_count?: number;
  coordination_cluster_id?: string | null;
  first_seen?: string;
  last_seen?: string;
  [key: string]: unknown;
}

export interface ActorDetailResponse {
  actor: Actor;
  outgoing_connections: number;
  incoming_connections: number;
  linked_accounts: { internal_uuid: string; username: string; primary_platform: string }[];
}

export interface ActorHistoryItem {
  timestamp: string;
  platform: string;
  content_id?: string;
  content_type?: string;
  text?: string;
  [key: string]: unknown;
}

export interface ActorHistoryResponse {
  actor_id: string;
  history: ActorHistoryItem[];
}

// ── Search ─────────────────────────────────────────────

export interface SearchResult {
  id: string;
  type: "narrative" | "actor";
  summary?: string;
  status?: string;
  username?: string;
  platform?: string;
  influence?: number;
}

export interface SearchResponse {
  query: string;
  total: number;
  results: {
    narratives: SearchResult[];
    actors: SearchResult[];
    alerts: SearchResult[];
  };
}

// ── Paginated wrapper ──────────────────────────────────

export interface PaginatedResponse<T> {
  count: number;
  cursor?: string | null;
  items: T[];
}
