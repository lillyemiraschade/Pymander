interface StageProps {
  label: string;
  metric: string;
  status: 'ok' | 'warn' | 'error';
  sub?: string;
}

function Stage({ label, metric, status, sub }: StageProps) {
  const colors = { ok: 'text-green-400', warn: 'text-yellow-400', error: 'text-red-400' };
  const dots = { ok: 'bg-green-500', warn: 'bg-yellow-500', error: 'bg-red-500' };
  return (
    <div className="flex flex-col items-center">
      <div className="text-xs text-gray-500 uppercase">{label}</div>
      <div className={`text-sm font-bold ${colors[status]}`}>{metric}</div>
      <div className={`w-2 h-2 rounded-full mt-1 ${dots[status]}`} />
      {sub && <div className="text-xs text-gray-600 mt-0.5">{sub}</div>}
    </div>
  );
}

function Arrow() {
  return <div className="text-gray-600 text-lg px-2">→</div>;
}

interface PipelineFlowProps {
  rpm: number;
  embeddingRpm: number;
  lastClustering: string | null;
  narratives: number;
}

export function PipelineFlow({ rpm, embeddingRpm, lastClustering, narratives }: PipelineFlowProps) {
  const clusterAge = lastClustering
    ? Math.round((Date.now() - new Date(lastClustering).getTime()) / 60000)
    : null;

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
      <div className="text-xs text-gray-500 uppercase tracking-wider mb-3">Pipeline Health</div>
      <div className="flex items-center justify-center gap-1 flex-wrap">
        <Stage
          label="Ingestion"
          metric={`${rpm}/min`}
          status={rpm > 0 ? 'ok' : 'error'}
        />
        <Arrow />
        <Stage
          label="Embedding"
          metric={`${embeddingRpm}/min`}
          status={embeddingRpm > 0 ? 'ok' : rpm > 0 ? 'warn' : 'error'}
        />
        <Arrow />
        <Stage
          label="Clustering"
          metric={clusterAge !== null ? `${clusterAge}m ago` : 'N/A'}
          status={clusterAge !== null && clusterAge < 60 ? 'ok' : 'warn'}
        />
        <Arrow />
        <Stage
          label="Narratives"
          metric={`${narratives} active`}
          status={narratives > 0 ? 'ok' : 'warn'}
        />
        <Arrow />
        <Stage label="Storage" metric="OK" status="ok" />
      </div>
    </div>
  );
}
