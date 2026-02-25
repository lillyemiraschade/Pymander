/* ── Main app shell with sidebar navigation ── */

import { Routes, Route, NavLink, useLocation } from "react-router-dom";
import { CommandCenter } from "./pages/CommandCenter";
import { NarrativeList } from "./pages/NarrativeList";
import { NarrativeDetail } from "./pages/NarrativeDetail";
import { NetworkExplorerPage } from "./pages/NetworkExplorer";
import { CoordinationDashboard } from "./pages/CoordinationDashboard";
import { ActorProfile } from "./pages/ActorProfile";
import { BriefingList } from "./pages/BriefingList";
import { BriefingDetail } from "./pages/BriefingDetail";
import { SearchPage } from "./pages/SearchPage";
import { AlertFeed } from "./pages/AlertFeed";

const navItems = [
  { path: "/", label: "Command Center", icon: "C" },
  { path: "/narratives", label: "Narratives", icon: "N" },
  { path: "/network", label: "Network", icon: "G" },
  { path: "/coordination", label: "Coordination", icon: "K" },
  { path: "/briefings", label: "Briefings", icon: "B" },
  { path: "/alerts", label: "Alerts", icon: "!" },
  { path: "/search", label: "Search", icon: "S" },
];

export default function App() {
  const location = useLocation();
  const isFullscreen = location.pathname === "/network";

  return (
    <div className="flex h-screen overflow-hidden">
      {/* Sidebar */}
      <aside className="w-48 bg-[var(--dark)] text-white flex flex-col shrink-0">
        <div className="px-4 py-4 border-b border-white/10">
          <h1 className="text-base font-bold tracking-tight">PYMANDER</h1>
          <p className="text-[10px] text-[var(--muted)] mt-0.5">Narrative Intelligence</p>
        </div>

        <nav className="flex-1 py-2">
          {navItems.map((item) => (
            <NavLink
              key={item.path}
              to={item.path}
              end={item.path === "/"}
              className={({ isActive }) =>
                `flex items-center gap-2.5 px-4 py-2 text-xs font-medium transition-colors ${
                  isActive
                    ? "bg-[var(--primary)] text-white"
                    : "text-[#999] hover:text-white hover:bg-white/5"
                }`
              }
            >
              <span className="w-5 h-5 rounded bg-white/10 flex items-center justify-center text-[10px] font-bold shrink-0">
                {item.icon}
              </span>
              {item.label}
            </NavLink>
          ))}
        </nav>

        <div className="px-4 py-3 border-t border-white/10 text-[10px] text-[#666]">
          v2.0 Client API
        </div>
      </aside>

      {/* Main content */}
      <main
        className={`flex-1 overflow-y-auto ${isFullscreen ? "p-0" : "p-4"}`}
        style={{ background: "var(--bg)" }}
      >
        <Routes>
          <Route path="/" element={<CommandCenter />} />
          <Route path="/narratives" element={<NarrativeList />} />
          <Route path="/narratives/:id" element={<NarrativeDetail />} />
          <Route path="/network" element={<NetworkExplorerPage />} />
          <Route path="/coordination" element={<CoordinationDashboard />} />
          <Route path="/actors/:id" element={<ActorProfile />} />
          <Route path="/briefings" element={<BriefingList />} />
          <Route path="/briefings/:id" element={<BriefingDetail />} />
          <Route path="/search" element={<SearchPage />} />
          <Route path="/alerts" element={<AlertFeed />} />
        </Routes>
      </main>
    </div>
  );
}
