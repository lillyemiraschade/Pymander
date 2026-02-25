/* ── Alert notification banner ── */

import type { AlertSeverity } from "../../lib/types";
import { severityColors } from "../../lib/constants";

interface AlertBannerProps {
  severity: AlertSeverity;
  title: string;
  message?: string;
  timestamp?: string;
  onDismiss?: () => void;
  onAcknowledge?: () => void;
}

export function AlertBanner({
  severity,
  title,
  message,
  timestamp,
  onDismiss,
  onAcknowledge,
}: AlertBannerProps) {
  const borderColor = severityColors[severity];

  return (
    <div
      className="bg-white rounded-md px-3 py-2 border-l-3 flex items-start gap-2"
      style={{ borderLeftColor: borderColor }}
    >
      <div
        className="w-2 h-2 rounded-full mt-1.5 shrink-0"
        style={{ background: borderColor }}
      />
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-xs font-semibold uppercase" style={{ color: borderColor }}>
            {severity}
          </span>
          <span className="text-sm font-medium text-[var(--dark)] truncate">{title}</span>
        </div>
        {message && (
          <p className="text-xs text-[var(--muted)] mt-0.5 line-clamp-2">{message}</p>
        )}
        {timestamp && (
          <span className="text-[10px] text-[var(--muted)]">{timestamp}</span>
        )}
      </div>
      <div className="flex gap-1 shrink-0">
        {onAcknowledge && (
          <button
            onClick={onAcknowledge}
            className="text-[10px] px-2 py-0.5 rounded bg-[var(--bg)] text-[var(--dark)] hover:bg-[var(--primary)] hover:text-white transition-colors"
          >
            ACK
          </button>
        )}
        {onDismiss && (
          <button
            onClick={onDismiss}
            className="text-[10px] px-1 text-[var(--muted)] hover:text-[var(--dark)]"
          >
            x
          </button>
        )}
      </div>
    </div>
  );
}
