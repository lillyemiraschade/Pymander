import type { SourceHealth } from '../types/api';

const STATUS_COLORS: Record<string, string> = {
  green: 'bg-green-500',
  yellow: 'bg-yellow-500',
  red: 'bg-red-500',
  gray: 'bg-gray-600',
};

const STATUS_LABELS: Record<string, string> = {
  green: 'Active',
  yellow: 'Degraded',
  red: 'Error',
  gray: 'Not Started',
};

export function SourceCard({ source }: { source: SourceHealth }) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-lg p-3 min-w-[140px]">
      <div className="flex items-center gap-2 mb-2">
        <div className={`w-2.5 h-2.5 rounded-full ${STATUS_COLORS[source.status]}`} />
        <span className="text-sm font-semibold uppercase">{source.platform}</span>
      </div>
      <div className="text-lg font-bold">
        {Math.round(source.records_per_hour)}<span className="text-xs text-gray-500">/hr</span>
      </div>
      <div className="text-xs text-gray-500">{STATUS_LABELS[source.status]}</div>
      {source.errors_today > 0 && (
        <div className="text-xs text-red-400 mt-1">Errors: {source.errors_today}</div>
      )}
    </div>
  );
}
