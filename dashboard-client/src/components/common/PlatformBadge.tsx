/* ── Platform icon + name badge ── */

import type { Platform } from "../../lib/types";
import { platformConfigs } from "../../lib/constants";

interface PlatformBadgeProps {
  platform: Platform | string;
  showName?: boolean;
  size?: "sm" | "md";
}

export function PlatformBadge({ platform, showName = false, size = "sm" }: PlatformBadgeProps) {
  const config = platformConfigs[platform as Platform] ?? {
    name: platform,
    color: "#B5B5AD",
    emoji: "?",
  };

  const sizeClasses = size === "sm" ? "text-[10px] px-1.5 py-0.5" : "text-xs px-2 py-1";

  return (
    <span
      className={`inline-flex items-center gap-1 rounded font-semibold ${sizeClasses}`}
      style={{
        background: `${config.color}18`,
        color: config.color,
        border: `1px solid ${config.color}30`,
      }}
    >
      <span className="font-bold">{config.emoji}</span>
      {showName && <span>{config.name}</span>}
    </span>
  );
}
