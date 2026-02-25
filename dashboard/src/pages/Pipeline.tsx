import { useApi } from '../hooks/useApi';
import type { PipelineStatus } from '../types/api';

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
      <div className="text-xs text-gray-500 uppercase tracking-wider mb-3">{title}</div>
      <div className="space-y-2">{children}</div>
    </div>
  );
}

function Row({ label, value, warn }: { label: string; value: string | number; warn?: boolean }) {
  return (
    <div className="flex justify-between text-sm">
      <span className="text-gray-400">{label}</span>
      <span className={warn ? 'text-yellow-400' : 'text-white'}>{value}</span>
    </div>
  );
}

export function Pipeline() {
  const { data, loading } = useApi<PipelineStatus>('/pipeline/status', 10000);

  if (loading && !data) return <div className="p-8 text-gray-500">Loading pipeline status...</div>;
  if (!data) return <div className="p-8 text-red-400">Failed to load pipeline status</div>;

  return (
    <div className="p-6 grid grid-cols-2 gap-4">
      <Section title="Embedding Pipeline">
        <Row label="Throughput" value={`${data.embedding.throughput_per_minute}/min`} />
        <Row label="Queue Depth" value={data.embedding.queue_depth} warn={data.embedding.queue_depth > 100} />
        <Row label="Errors Today" value={data.embedding.errors_today} warn={data.embedding.errors_today > 0} />
      </Section>

      <Section title="Clustering">
        <Row label="Last Run" value={data.clustering.last_run ? new Date(data.clustering.last_run).toLocaleTimeString() : 'Never'} />
        <Row label="Clusters Found" value={data.clustering.clusters_found ?? 'N/A'} />
        <Row label="Noise Ratio" value={data.clustering.noise_ratio !== null ? `${(data.clustering.noise_ratio * 100).toFixed(1)}%` : 'N/A'} />
        <Row label="Errors Today" value={data.clustering.errors_today} warn={data.clustering.errors_today > 0} />
      </Section>

      <Section title="Narrative Validation">
        <Row label="API Calls Today" value={data.narrative_validation.api_calls_today} />
        <Row label="API Cost Today" value={`$${(data.narrative_validation.api_cost_cents_today / 100).toFixed(2)}`} />
        <Row label="Validated" value={data.narrative_validation.validated_today} />
        <Row label="Rejected" value={data.narrative_validation.rejected_today} />
      </Section>

      <Section title="Engagement Poller">
        <Row label="Queue Depth" value={data.engagement_poller.queue_depth} />
        <Row label="Snapshots Today" value={data.engagement_poller.snapshots_today} />
        <Row label="Errors Today" value={data.engagement_poller.errors_today} warn={data.engagement_poller.errors_today > 0} />
        <Row label="Anomalies Today" value={data.engagement_poller.anomalies_today} warn={data.engagement_poller.anomalies_today > 0} />
      </Section>

      <Section title="Image Hasher">
        <Row label="Hashed Today" value={data.image_hasher.hashed_today} />
        <Row label="Errors Today" value={data.image_hasher.errors_today} warn={data.image_hasher.errors_today > 0} />
        <Row label="Cross-Platform Detected" value={data.image_hasher.cross_platform_today} />
      </Section>
    </div>
  );
}
