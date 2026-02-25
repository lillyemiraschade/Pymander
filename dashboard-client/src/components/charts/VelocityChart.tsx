/* ── Narrative velocity area chart ── */

import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import type { VelocityPoint } from "../../lib/types";
import { colors } from "../../lib/constants";

interface VelocityChartProps {
  data: VelocityPoint[];
  height?: number;
  compact?: boolean;
}

function formatTime(ts: string): string {
  const d = new Date(ts);
  return `${d.getMonth() + 1}/${d.getDate()} ${d.getHours()}:${String(d.getMinutes()).padStart(2, "0")}`;
}

export function VelocityChart({ data, height = 200, compact = false }: VelocityChartProps) {
  if (!data.length) {
    return (
      <div className="flex items-center justify-center text-xs text-[var(--muted)]" style={{ height }}>
        No velocity data
      </div>
    );
  }

  const chartData = data.map((p) => ({
    ...p,
    time: formatTime(p.timestamp),
  }));

  if (compact) {
    return (
      <ResponsiveContainer width="100%" height={height}>
        <AreaChart data={chartData}>
          <defs>
            <linearGradient id="velGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={colors.primary} stopOpacity={0.3} />
              <stop offset="95%" stopColor={colors.primary} stopOpacity={0} />
            </linearGradient>
          </defs>
          <Area
            type="monotone"
            dataKey="velocity"
            stroke={colors.primary}
            fill="url(#velGrad)"
            strokeWidth={1.5}
            dot={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={chartData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id="velGradFull" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor={colors.primary} stopOpacity={0.25} />
            <stop offset="95%" stopColor={colors.primary} stopOpacity={0} />
          </linearGradient>
        </defs>
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
        <Area
          type="monotone"
          dataKey="velocity"
          stroke={colors.primary}
          fill="url(#velGradFull)"
          strokeWidth={2}
          dot={false}
          activeDot={{ r: 3, fill: colors.primary }}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
