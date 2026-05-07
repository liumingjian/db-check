import type { NextConfig } from "next";

function hostnameFromValue(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) return "";
  if (trimmed === "*" || trimmed === "**") return "";
  try {
    const withScheme = /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(trimmed) ? trimmed : `http://${trimmed}`;
    return new URL(withScheme).hostname;
  } catch {
    return trimmed;
  }
}

function splitHostnames(value: string | undefined): string[] {
  const seen = new Set<string>();
  for (const part of (value ?? "").split(",")) {
    const hostname = hostnameFromValue(part);
    if (hostname) seen.add(hostname);
  }
  return Array.from(seen);
}

const allowedDevOrigins = splitHostnames(process.env.NEXT_ALLOWED_DEV_ORIGINS);

const nextConfig: NextConfig = {
  ...(allowedDevOrigins.length > 0 ? { allowedDevOrigins } : {}),
};

export default nextConfig;
