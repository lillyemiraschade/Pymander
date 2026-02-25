interface NavProps {
  current: string;
  onNavigate: (page: string) => void;
  connected: boolean;
}

const PAGES = [
  { id: 'overview', label: 'Overview' },
  { id: 'ingestion', label: 'Ingestion' },
  { id: 'pipeline', label: 'Pipeline' },
  { id: 'narratives', label: 'Narratives' },
  { id: 'explorer', label: 'Data Explorer' },
];

export function Nav({ current, onNavigate, connected }: NavProps) {
  return (
    <nav className="bg-gray-900 border-b border-gray-800 px-4 py-2 flex items-center gap-6">
      <div className="text-lg font-bold text-indigo-400 mr-4">PYMANDER</div>
      {PAGES.map((p) => (
        <button
          key={p.id}
          onClick={() => onNavigate(p.id)}
          className={`text-sm px-2 py-1 rounded transition-colors ${
            current === p.id
              ? 'bg-gray-800 text-white'
              : 'text-gray-400 hover:text-white'
          }`}
        >
          {p.label}
        </button>
      ))}
      <div className="ml-auto flex items-center gap-2">
        <div className={`w-2 h-2 rounded-full ${connected ? 'bg-green-500' : 'bg-red-500'}`} />
        <span className="text-xs text-gray-500">{connected ? 'Live' : 'Disconnected'}</span>
      </div>
    </nav>
  );
}
