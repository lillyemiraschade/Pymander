/* ── Horizontal platform migration timeline ── */

import { platformConfigs } from "../../lib/constants";
import type { MigrationEvent, Platform } from "../../lib/types";

interface PlatformTimelineProps {
  migrations: MigrationEvent[];
  platforms?: string[];
}

export function PlatformTimeline({ migrations, platforms = [] }: PlatformTimelineProps) {
  if (!migrations.length && !platforms.length) {
    return (
      <div className="text-xs text-[var(--muted)] py-4 text-center">
        No migration data available
      </div>
    );
  }

  // Derive unique platforms from migrations
  const allPlatforms = new Set<string>(platforms);
  migrations.forEach((m) => {
    allPlatforms.add(m.from_platform);
    allPlatforms.add(m.to_platform);
  });

  // Build timeline: find earliest detected_at per platform
  const platformFirst = new Map<string, number>();
  migrations.forEach((m) => {
    const t = new Date(m.detected_at).getTime();
    const migrationMs = m.migration_time_seconds * 1000;
    const fromTime = t - migrationMs;
    if (!platformFirst.has(m.from_platform) || fromTime < platformFirst.get(m.from_platform)!) {
      platformFirst.set(m.from_platform, fromTime);
    }
    if (!platformFirst.has(m.to_platform) || t < platformFirst.get(m.to_platform)!) {
      platformFirst.set(m.to_platform, t);
    }
  });

  const sorted = [...allPlatforms].sort((a, b) => {
    const aT = platformFirst.get(a) ?? Infinity;
    const bT = platformFirst.get(b) ?? Infinity;
    return aT - bT;
  });

  const minTime = Math.min(...platformFirst.values());
  const maxTime = Math.max(...platformFirst.values());
  const range = maxTime - minTime || 1;

  return (
    <div className="space-y-1.5">
      {sorted.map((plat) => {
        const config = platformConfigs[plat as Platform] ?? { name: plat, color: "#B5B5AD", emoji: "?" };
        const t = platformFirst.get(plat);
        const pct = t !== undefined ? ((t - minTime) / range) * 80 : 0;

        return (
          <div key={plat} className="flex items-center gap-2 h-6">
            <span
              className="text-[10px] font-bold w-8 text-right shrink-0"
              style={{ color: config.color }}
            >
              {config.emoji}
            </span>
            <div className="flex-1 relative h-3 bg-[#E8E6E3] rounded-full overflow-hidden">
              <div
                className="absolute top-0 h-full rounded-full transition-all"
                style={{
                  left: `${pct}%`,
                  right: 0,
                  background: `${config.color}40`,
                  borderLeft: `3px solid ${config.color}`,
                }}
              />
            </div>
            {t !== undefined && (
              <span className="text-[9px] text-[var(--muted)] w-12 shrink-0">
                {new Date(t).toLocaleDateString(undefined, { month: "short", day: "numeric" })}
              </span>
            )}
          </div>
        );
      })}
    </div>
  );
}
