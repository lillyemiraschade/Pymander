/* ── Design system constants ── */

import type { NarrativeStatus, AlertSeverity, Platform } from "./types";

// ── Colors ─────────────────────────────────────────

export const colors = {
  bg: "#F6F4F1",
  primary: "#5E7D58",
  primaryLight: "#7A9B74",
  primaryDark: "#4A6346",
  dark: "#1A1A1A",
  muted: "#B5B5AD",
  mutedLight: "#D4D4CC",
  alertHigh: "#C2553A",
  alertMedium: "#D4A843",
  alertLow: "#5E7D58",
  alertCritical: "#8B1E3F",
  white: "#FFFFFF",
  surface: "#FFFFFF",
  surfaceHover: "#EDEBE8",
} as const;

// ── Narrative status colors ────────────────────────

export const statusColors: Record<NarrativeStatus, string> = {
  emerging: "#6B8EDB",
  growing: "#5E7D58",
  viral: "#C2553A",
  peaking: "#D4A843",
  declining: "#B5B5AD",
  dormant: "#8A8A84",
  dead: "#D4D4CC",
  resurgent: "#9B59B6",
};

export const statusLabels: Record<NarrativeStatus, string> = {
  emerging: "Emerging",
  growing: "Growing",
  viral: "Viral",
  peaking: "Peaking",
  declining: "Declining",
  dormant: "Dormant",
  dead: "Dead",
  resurgent: "Resurgent",
};

// ── Alert severity ─────────────────────────────────

export const severityColors: Record<AlertSeverity, string> = {
  low: "#5E7D58",
  medium: "#D4A843",
  high: "#C2553A",
  critical: "#8B1E3F",
};

export const severityLabels: Record<AlertSeverity, string> = {
  low: "Low",
  medium: "Medium",
  high: "High",
  critical: "Critical",
};

// ── Platform config ────────────────────────────────

export interface PlatformConfig {
  name: string;
  color: string;
  emoji: string;
}

export const platformConfigs: Record<Platform, PlatformConfig> = {
  twitter: { name: "Twitter/X", color: "#1DA1F2", emoji: "X" },
  reddit: { name: "Reddit", color: "#FF4500", emoji: "R" },
  telegram: { name: "Telegram", color: "#0088CC", emoji: "T" },
  youtube: { name: "YouTube", color: "#FF0000", emoji: "Y" },
  tiktok: { name: "TikTok", color: "#000000", emoji: "TK" },
  facebook: { name: "Facebook", color: "#1877F2", emoji: "F" },
  instagram: { name: "Instagram", color: "#E4405F", emoji: "IG" },
  "4chan": { name: "4chan", color: "#789922", emoji: "4" },
  gab: { name: "Gab", color: "#21CF7A", emoji: "G" },
  truth_social: { name: "Truth Social", color: "#5448EE", emoji: "TS" },
  rumble: { name: "Rumble", color: "#85C742", emoji: "RU" },
  rss: { name: "RSS", color: "#EE802F", emoji: "RS" },
  web: { name: "Web", color: "#B5B5AD", emoji: "W" },
  bluesky: { name: "Bluesky", color: "#0085FF", emoji: "BS" },
  substack: { name: "Substack", color: "#FF6719", emoji: "SS" },
  wikipedia: { name: "Wikipedia", color: "#636466", emoji: "WK" },
  other: { name: "Other", color: "#B5B5AD", emoji: "?" },
};

// ── Community colors (D3 graph) ────────────────────

export const communityPalette = [
  "#5E7D58", "#C2553A", "#D4A843", "#6B8EDB", "#9B59B6",
  "#1ABC9C", "#E67E22", "#2980B9", "#E74C3C", "#8E44AD",
  "#16A085", "#F39C12", "#2C3E50", "#D35400", "#7F8C8D",
  "#27AE60", "#C0392B", "#3498DB", "#8B1E3F", "#1A1A1A",
];

export function communityColor(communityId: string | null): string {
  if (!communityId) return colors.muted;
  let hash = 0;
  for (let i = 0; i < communityId.length; i++) {
    hash = communityId.charCodeAt(i) + ((hash << 5) - hash);
  }
  return communityPalette[Math.abs(hash) % communityPalette.length]!;
}
