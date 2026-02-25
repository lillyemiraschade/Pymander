import { useState } from 'react';
import { useApi } from '../hooks/useApi';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';

const PLATFORMS = ['reddit', 'rss', 'twitter', 'telegram', 'youtube', '4chan', 'web'];

interface SourceMetrics {
  platform: string;
  total_posts: number;
  total_comments: number;
  total_articles: number;
  errors_today: number;
  records_per_minute: number;
  hourly_counts: { hour_offset: number; timestamp: number; count: number }[];
}

export function Ingestion() {
  const [selected, setSelected] = useState('reddit');
  const { data, loading } = useApi<SourceMetrics>(`/metrics/source/${selected}`, 15000);

  const chartData = data
    ? [...data.hourly_counts]
        .reverse()
        .map((h) => ({ hour: `${-h.hour_offset}h`, count: h.count }))
    : [];

  return (
    <div className="p-6 space-y-6">
      <div className="flex gap-2">
        {PLATFORMS.map((p) => (
          <button
            key={p}
            onClick={() => setSelected(p)}
            className={`text-xs px-3 py-1.5 rounded uppercase ${
              selected === p
                ? 'bg-indigo-600 text-white'
                : 'bg-gray-800 text-gray-400 hover:text-white'
            }`}
          >
            {p}
          </button>
        ))}
      </div>

      {loading && !data ? (
        <div className="text-gray-500">Loading...</div>
      ) : data ? (
        <>
          <div className="grid grid-cols-4 gap-4">
            <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
              <div className="text-xs text-gray-500 uppercase">Posts</div>
              <div className="text-xl font-bold">{data.total_posts.toLocaleString()}</div>
            </div>
            <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
              <div className="text-xs text-gray-500 uppercase">Comments</div>
              <div className="text-xl font-bold">{data.total_comments.toLocaleString()}</div>
            </div>
            <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
              <div className="text-xs text-gray-500 uppercase">Articles</div>
              <div className="text-xl font-bold">{data.total_articles.toLocaleString()}</div>
            </div>
            <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
              <div className="text-xs text-gray-500 uppercase">Records/Min</div>
              <div className={`text-xl font-bold ${data.records_per_minute > 0 ? 'text-green-400' : 'text-gray-500'}`}>
                {data.records_per_minute}
              </div>
            </div>
          </div>

          {data.errors_today > 0 && (
            <div className="bg-red-950 border border-red-800 rounded-lg p-3 text-red-300 text-sm">
              {data.errors_today} errors today for {selected}
            </div>
          )}

          <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-500 uppercase tracking-wider mb-3">
              Hourly Ingestion ({selected})
            </div>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={chartData}>
                <XAxis dataKey="hour" stroke="#4b5563" tick={{ fontSize: 10 }} />
                <YAxis stroke="#4b5563" tick={{ fontSize: 10 }} />
                <Tooltip
                  contentStyle={{ background: '#1f2937', border: '1px solid #374151', fontSize: 12 }}
                />
                <Bar dataKey="count" fill="#6366f1" radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </>
      ) : null}
    </div>
  );
}
