export interface SourceHealth {
  platform: string;
  status: 'green' | 'yellow' | 'red' | 'gray';
  records_per_hour: number;
  records_today: number;
  errors_today: number;
  rate_limit_pauses: number;
}

export interface OverviewMetrics {
  total_records: number;
  records_today: number;
  records_per_minute: number;
  active_sources: number;
  total_sources: number;
  source_health: SourceHealth[];
  active_narratives: number;
  pipeline: {
    embedding_throughput: number;
    last_clustering: string | null;
    clusters_found: number | null;
  };
}

export interface PipelineStatus {
  embedding: {
    throughput_per_minute: number;
    queue_depth: number;
    errors_today: number;
  };
  clustering: {
    last_run: string | null;
    clusters_found: number | null;
    noise_ratio: number | null;
    errors_today: number;
  };
  narrative_validation: {
    api_calls_today: number;
    api_cost_cents_today: number;
    validated_today: number;
    rejected_today: number;
  };
  engagement_poller: {
    queue_depth: number;
    snapshots_today: number;
    errors_today: number;
    anomalies_today: number;
  };
  image_hasher: {
    hashed_today: number;
    errors_today: number;
    cross_platform_today: number;
  };
}

export interface WsMetrics {
  records_per_minute: number;
  sources: Record<string, { status: string; rpm: number; errors: number }>;
  embedding_rpm: number;
  active_narratives: number;
  anomalies_today: number;
}

export interface SearchResult {
  id: string;
  platform: string;
  content_type: string;
  title: string | null;
  text: string | null;
  url: string | null;
  created_at: string | null;
  actor: Record<string, unknown>;
  engagement: Record<string, unknown>;
}

export interface HourlyCount {
  hour_offset: number;
  timestamp: number;
  count: number;
}

export interface IngestionRate {
  hours: number;
  series: Record<string, HourlyCount[]>;
}
