import { useState } from 'react';
import { Nav } from './components/Nav';
import { Overview } from './pages/Overview';
import { Ingestion } from './pages/Ingestion';
import { Pipeline } from './pages/Pipeline';
import { Narratives } from './pages/Narratives';
import { Explorer } from './pages/Explorer';
import { useWebSocket } from './hooks/useWebSocket';

function App() {
  const [page, setPage] = useState('overview');
  const { metrics: wsMetrics, connected } = useWebSocket();

  return (
    <div className="min-h-screen bg-[#0a0a0f]">
      <Nav current={page} onNavigate={setPage} connected={connected} />
      <main>
        {page === 'overview' && <Overview ws={wsMetrics} />}
        {page === 'ingestion' && <Ingestion />}
        {page === 'pipeline' && <Pipeline />}
        {page === 'narratives' && <Narratives />}
        {page === 'explorer' && <Explorer />}
      </main>
    </div>
  );
}

export default App;
