/* ── Engagement over time line chart ── */

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import type { NarrativeSnapshot } from "../../lib/types";
import { colors } from "../../lib/constants";

interface EngagementCurveProps {
  data: NarrativeSnapshot[];
  height?: number;
}

function formatTime(ts: string): string {
  const d = new Date(ts);
  return `${d.getMonth() + 1}/${d.getDate()}`;
}

export function EngagementCurve({ data, height = 180 }: EngagementCurveProps) {
  if (!data.length) {
    return (
      <div className="flex items-center justify-center text-xs text-[var(--muted)]" style={{ height }}>
        No engagement data
      </div>
    );
  }

  const chartData = data.map((s) => ({
    time: formatTime(s.timestamp),
    content: s.content_count,
    actors: s.actor_count,
    velocity: s.velocity,
  }));

  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={chartData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#E8E6E3" />
        <XAxis
          dataKey="time"
          tick={{ fontSize: 10, fill: colors.muted }}
          tickLine={false}
          axisLine={false}
        />
        <YAxis
          tick={{ fontSize: 10, fill: colors.muted }}
          tickLine={false}
          axisLine={false}
          width={40}
        />
        <Tooltip
          contentStyle={{
            background: colors.white,
            border: "1px solid #E8E6E3",
            borderRadius: 6,
            fontSize: 12,
            fontFamily: "DM Sans",
          }}
        />
        <Line
          type="monotone"
          dataKey="content"
          stroke={colors.primary}
          strokeWidth={2}
          dot={false}
          name="Content"
        />
        <Line
          type="monotone"
          dataKey="actors"
          stroke={colors.alertMedium}
          strokeWidth={1.5}
          dot={false}
          name="Actors"
          strokeDasharray="4 2"
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
