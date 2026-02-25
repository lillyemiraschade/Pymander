/* ── Big number metric display card ── */

interface MetricCardProps {
  label: string;
  value: string | number;
  trend?: number | null;
  trendLabel?: string;
  icon?: string;
}

export function MetricCard({ label, value, trend, trendLabel, icon }: MetricCardProps) {
  const trendColor =
    trend === undefined || trend === null
      ? ""
      : trend > 0
        ? "text-[var(--alert-high)]"
        : trend < 0
          ? "text-[var(--primary)]"
          : "text-[var(--muted)]";

  const trendArrow =
    trend === undefined || trend === null ? "" : trend > 0 ? "+" : trend < 0 ? "" : "";

  return (
    <div className="bg-white rounded-lg px-4 py-3 border border-[#E8E6E3] hover:border-[var(--primary)] transition-colors">
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs font-medium text-[var(--muted)] uppercase tracking-wide">
          {label}
        </span>
        {icon && <span className="text-sm">{icon}</span>}
      </div>
      <div className="text-2xl font-bold text-[var(--dark)] leading-tight">{value}</div>
      {trend !== undefined && trend !== null && (
        <div className={`text-xs mt-1 font-medium ${trendColor}`}>
          {trendArrow}
          {trend > 0 ? "+" : ""}
          {typeof trend === "number" ? trend.toFixed(1) : trend}%{" "}
          {trendLabel && <span className="text-[var(--muted)]">{trendLabel}</span>}
        </div>
      )}
    </div>
  );
}
