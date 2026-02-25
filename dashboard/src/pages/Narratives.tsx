import { useApi } from '../hooks/useApi';

interface NarrativeItem {
  narrative_id: string;
  summary: string;
  category: string;
  confidence: number;
  content_count: number;
  status: string;
  created_at: string;
}

interface NarrativeListResponse {
  count: number;
  narratives: NarrativeItem[];
}

const CATEGORY_COLORS: Record<string, string> = {
  breaking_news: 'bg-red-900 text-red-300',
  political_claim: 'bg-blue-900 text-blue-300',
  corporate_crisis: 'bg-orange-900 text-orange-300',
  cultural_trend: 'bg-purple-900 text-purple-300',
  conspiracy_theory: 'bg-yellow-900 text-yellow-300',
  coordinated_campaign: 'bg-red-900 text-red-300',
  organic_movement: 'bg-green-900 text-green-300',
  tech_discourse: 'bg-cyan-900 text-cyan-300',
  financial_narrative: 'bg-emerald-900 text-emerald-300',
  health_narrative: 'bg-pink-900 text-pink-300',
};

export function Narratives() {
  const { data, loading } = useApi<NarrativeListResponse>('/narratives', 15000);

  if (loading && !data) return <div className="p-8 text-gray-500">Loading narratives...</div>;

  return (
    <div className="p-6 space-y-4">
      <div className="text-xs text-gray-500 uppercase tracking-wider">
        Active Narratives ({data?.count ?? 0})
      </div>

      {data?.narratives.length === 0 && (
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-6 text-center text-gray-500">
          No narratives detected yet. The clustering pipeline needs data to identify narrative clusters.
        </div>
      )}

      <div className="space-y-2">
        {data?.narratives.map((n) => (
          <div key={n.narrative_id} className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="flex items-start justify-between">
              <div className="flex-1">
                <div className="text-sm font-medium text-white mb-1">{n.summary}</div>
                <div className="flex gap-2 items-center">
                  <span className={`text-xs px-2 py-0.5 rounded ${CATEGORY_COLORS[n.category] || 'bg-gray-800 text-gray-400'}`}>
                    {n.category?.replace(/_/g, ' ')}
                  </span>
                  <span className="text-xs text-gray-500">
                    Confidence: {(n.confidence * 100).toFixed(0)}%
                  </span>
                  <span className="text-xs text-gray-500">
                    {n.content_count} items
                  </span>
                </div>
              </div>
              <div className="text-xs text-gray-600">
                {n.created_at ? new Date(n.created_at).toLocaleString() : ''}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
