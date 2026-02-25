import { useApi } from '../hooks/useApi';
import { MetricCard } from '../components/MetricCard';
import { SourceCard } from '../components/SourceCard';
import { PipelineFlow } from '../components/PipelineFlow';
import type { OverviewMetrics, IngestionRate, WsMetrics } from '../types/api';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';

const COLORS = ['#6366f1', '#22d3ee', '#f97316', '#a855f7', '#ef4444', '#10b981', '#eab308'];

interface Props {
  ws: WsMetrics | null;
}

export function Overview({ ws }: Props) {
  const { data: overview } = useApi<OverviewMetrics>('/metrics/overview', 10000);
  const { data: rates } = useApi<IngestionRate>('/metrics/ingestion_rate?hours=24', 30000);

  if (!overview) {
    return <div className="p-8 text-gray-500">Loading metrics...</div>;
  }

  // Build chart data from ingestion rates
  const chartData: Record<string, number | string>[] = [];
  if (rates) {
    const platforms = Object.keys(rates.series);
    const maxLen = Math.max(...platforms.map((p) => rates.series[p]?.length ?? 0));
    for (let i = maxLen - 1; i >= 0; i--) {
      const row: Record<string, number | string> = { hour: `${i}h ago` };
      for (const p of platforms) {
        const arr = rates.series[p];
        row[p] = arr && arr[i] ? arr[i].count : 0;
      }
      chartData.push(row);
    }
  }

  const rpm = ws?.records_per_minute ?? overview.records_per_minute;

  return (
    <div className="p-6 space-y-6">
      {/* Top row: key metrics */}
      <div className="grid grid-cols-4 gap-4">
        <MetricCard label="Total Records" value={overview.total_records} sub="since launch" />
        <MetricCard label="Records Today" value={overview.records_today} sub="since midnight" />
        <MetricCard
          label="Records / Min"
          value={rpm}
          sub="current rate"
          color={rpm > 0 ? 'text-green-400' : 'text-red-400'}
        />
        <MetricCard
          label="Active Sources"
          value={`${overview.active_sources}/${overview.total_sources}`}
          sub="online"
        />
      </div>

      {/* Source health */}
      <div>
        <div className="text-xs text-gray-500 uppercase tracking-wider mb-2">Source Health</div>
        <div className="flex gap-3 flex-wrap">
          {overview.source_health.map((s) => (
            <SourceCard key={s.platform} source={s} />
          ))}
        </div>
      </div>

      {/* Ingestion rate chart */}
      {chartData.length > 0 && (
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
          <div className="text-xs text-gray-500 uppercase tracking-wider mb-3">
            Ingestion Rate (Last 24h)
          </div>
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={chartData}>
              <XAxis dataKey="hour" stroke="#4b5563" tick={{ fontSize: 10 }} />
              <YAxis stroke="#4b5563" tick={{ fontSize: 10 }} />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', fontSize: 12 }}
              />
              {Object.keys(rates!.series).map((p, i) => (
                <Area
                  key={p}
                  type="monotone"
                  dataKey={p}
                  stackId="1"
                  stroke={COLORS[i % COLORS.length]}
                  fill={COLORS[i % COLORS.length]}
                  fillOpacity={0.3}
                />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Pipeline flow */}
      <PipelineFlow
        rpm={rpm}
        embeddingRpm={overview.pipeline.embedding_throughput}
        lastClustering={overview.pipeline.last_clustering}
        narratives={overview.active_narratives}
      />
    </div>
  );
}
