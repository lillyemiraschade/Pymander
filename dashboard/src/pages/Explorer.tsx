import { useState } from 'react';
import type { SearchResult } from '../types/api';

interface SearchResponse {
  total: number;
  offset: number;
  limit: number;
  results: SearchResult[];
}

export function Explorer() {
  const [query, setQuery] = useState('');
  const [platform, setPlatform] = useState('');
  const [data, setData] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);

  const search = async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      if (query) params.set('q', query);
      if (platform) params.set('platform', platform);
      params.set('limit', '50');
      const res = await fetch(`/api/v1/search?${params}`);
      setData(await res.json());
    } catch {
      setData(null);
    }
    setLoading(false);
  };

  return (
    <div className="p-6 space-y-4">
      <div className="flex gap-3">
        <input
          type="text"
          placeholder="Search content..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && search()}
          className="flex-1 bg-gray-900 border border-gray-700 rounded-lg px-4 py-2 text-sm text-white placeholder-gray-500 focus:outline-none focus:border-indigo-500"
        />
        <select
          value={platform}
          onChange={(e) => setPlatform(e.target.value)}
          className="bg-gray-900 border border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-300"
        >
          <option value="">All Platforms</option>
          <option value="reddit">Reddit</option>
          <option value="rss">RSS/News</option>
          <option value="twitter">Twitter</option>
          <option value="telegram">Telegram</option>
          <option value="youtube">YouTube</option>
        </select>
        <button
          onClick={search}
          className="bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-2 rounded-lg text-sm"
        >
          Search
        </button>
      </div>

      {loading && <div className="text-gray-500 text-sm">Searching...</div>}

      {data && (
        <div className="text-xs text-gray-500 mb-2">
          {data.total} results
        </div>
      )}

      <div className="space-y-2">
        {data?.results.map((r) => (
          <div key={r.id} className="bg-gray-900 border border-gray-800 rounded-lg p-3">
            <div className="flex items-start justify-between gap-4">
              <div className="flex-1 min-w-0">
                {r.title && (
                  <div className="text-sm font-medium text-white truncate mb-1">{r.title}</div>
                )}
                <div className="text-xs text-gray-400 line-clamp-2">{r.text}</div>
                <div className="flex gap-3 mt-2 text-xs text-gray-500">
                  <span className="uppercase font-medium text-indigo-400">{r.platform}</span>
                  <span>{r.content_type}</span>
                  <span>{r.created_at ? new Date(r.created_at).toLocaleString() : ''}</span>
                  {r.url && (
                    <a href={r.url} target="_blank" rel="noopener noreferrer" className="text-indigo-400 hover:underline">
                      source
                    </a>
                  )}
                </div>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
