/* ── Alert feed with severity filter ── */

import { useState } from "react";
import { useApi } from "../hooks/useApi";
import { listAlerts, acknowledgeAlert } from "../lib/api";
import { AlertBanner } from "../components/common/AlertBanner";
import { LoadingState } from "../components/common/LoadingState";
import { MetricCard } from "../components/common/MetricCard";
import { TimeAgo } from "../components/common/TimeAgo";
import { severityColors } from "../lib/constants";
import type { AlertSeverity } from "../lib/types";
import { useMutation, useQueryClient } from "@tanstack/react-query";

const SEVERITIES: (AlertSeverity | "all")[] = ["all", "critical", "high", "medium", "low"];

export function AlertFeed() {
  const [severityFilter, setSeverityFilter] = useState<string | undefined>(undefined);
  const [hours, setHours] = useState(24);
  const queryClient = useQueryClient();

  const { data, isLoading } = useApi(
    ["alerts", hours, severityFilter ?? "all"],
    () => listAlerts({ hours, severity: severityFilter, limit: 200 }),
    { refetchInterval: 15_000 },
  );

  const ack = useMutation({
    mutationFn: (id: string) => acknowledgeAlert(id),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["alerts"] });
    },
  });

  const alerts = data?.alerts ?? [];
  const criticalCount = alerts.filter((a) => a.severity === "critical").length;
  const highCount = alerts.filter((a) => a.severity === "high").length;
  const unacknowledged = alerts.filter((a) => !a.acknowledged).length;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-bold text-[var(--dark)]">Alerts</h1>
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-[var(--muted)]">Time range:</span>
          {[6, 24, 48, 168].map((h) => (
            <button
              key={h}
              onClick={() => setHours(h)}
              className={`text-[10px] px-2 py-0.5 rounded ${
                hours === h
                  ? "bg-[var(--dark)] text-white"
                  : "bg-white text-[var(--muted)] border border-[#E8E6E3]"
              }`}
            >
              {h}h
            </button>
          ))}
        </div>
      </div>

      {/* Metrics */}
      <div className="grid grid-cols-4 gap-3">
        <MetricCard label="Total Alerts" value={alerts.length} icon="!" />
        <MetricCard label="Critical" value={criticalCount} icon="!!" />
        <MetricCard label="High" value={highCount} icon="!" />
        <MetricCard label="Unacknowledged" value={unacknowledged} icon="?" />
      </div>

      {/* Severity filters */}
      <div className="flex gap-1.5">
        {SEVERITIES.map((sev) => (
          <button
            key={sev}
            onClick={() => setSeverityFilter(sev === "all" ? undefined : sev)}
            className={`text-[11px] px-2.5 py-1 rounded-full font-medium transition-colors flex items-center gap-1.5 ${
              (sev === "all" && !severityFilter) || severityFilter === sev
                ? "bg-[var(--dark)] text-white"
                : "bg-white text-[var(--muted)] border border-[#E8E6E3] hover:border-[var(--primary)]"
            }`}
          >
            {sev !== "all" && (
              <span
                className="w-1.5 h-1.5 rounded-full"
                style={{ background: severityColors[sev] }}
              />
            )}
            {sev === "all" ? "All" : sev.charAt(0).toUpperCase() + sev.slice(1)}
          </button>
        ))}
      </div>

      {/* Alert list */}
      {isLoading ? (
        <LoadingState message="Loading alerts..." />
      ) : alerts.length === 0 ? (
        <div className="text-sm text-[var(--muted)] text-center py-12">
          No alerts in the last {hours} hours
        </div>
      ) : (
        <div className="space-y-1.5">
          {alerts.map((alert) => (
            <div key={alert.id} className="flex items-start gap-2">
              <div className="flex-1">
                <AlertBanner
                  severity={alert.severity}
                  title={alert.type.replace(/_/g, " ")}
                  message={
                    [
                      alert.alert_category,
                      alert.narrative_id ? `Narrative: ${alert.narrative_id.slice(0, 8)}...` : null,
                      alert.community_id ? `Community: ${alert.community_id.slice(0, 8)}...` : null,
                    ]
                      .filter(Boolean)
                      .join(" | ")
                  }
                  timestamp={alert.detected_at}
                  onAcknowledge={
                    !alert.acknowledged
                      ? () => ack.mutate(alert.id)
                      : undefined
                  }
                />
              </div>
              <div className="shrink-0 text-right">
                <TimeAgo timestamp={alert.detected_at} />
                {alert.acknowledged && (
                  <div className="text-[9px] text-[var(--primary)]">ACK</div>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
