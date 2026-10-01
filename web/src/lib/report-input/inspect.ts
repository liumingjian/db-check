/**
 * Report input inspection (spec #19, seam 2): pure browser-side reading of a
 * dropped collector ZIP, before anything crosses the API.
 */
import { strFromU8, unzipSync } from "fflate";
import type { ReportItemInput, ReportTaskInput } from "@/lib/api/reports/contract";
import { DB_TYPES, type DbType } from "@/lib/types";

export type ZipProblem =
  | "corrupt"
  | "no_manifest"
  | "multiple_manifests"
  | "unreadable_manifest"
  | "unsupported_db_type";

export type ZipInspection =
  | { ok: true; dbType: DbType; collectorVersion: string | null }
  | { ok: false; problem: ZipProblem; reason: string };

/** Mirrors the backend's entry-name normalisation (`zip_extract.go`). */
function normalize(name: string): string {
  return name.replaceAll("\\", "/").replace(/^\.\//, "");
}

/** Entries the backend skips when looking for the run directory (`DetectRunDirByManifest`). */
function isArtifact(path: string): boolean {
  const parts = path.split("/");
  const base = parts.at(-1) ?? "";
  return parts.includes("__MACOSX") || parts.includes(".git") || base === ".DS_Store" || base.startsWith("._");
}

function dirOf(path: string): string {
  const cut = path.lastIndexOf("/");
  return cut < 0 ? "" : path.slice(0, cut + 1);
}

/** Decompresses only the entries whose normalised name passes `want`. Throws on a corrupt ZIP. */
function readEntries(data: Uint8Array, want: (path: string) => boolean): Map<string, Uint8Array> {
  const entries = unzipSync(data, { filter: (file) => want(normalize(file.name)) });
  return new Map(Object.entries(entries).map(([name, bytes]) => [normalize(name), bytes]));
}

type JsonObject = Record<string, unknown>;

function asObject(value: unknown): JsonObject | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? (value as JsonObject) : null;
}

/** The entry parsed as a JSON object, or `null` when absent, not JSON, or not an object. */
function parseObject(bytes: Uint8Array | undefined): JsonObject | null {
  if (!bytes) return null;
  try {
    return asObject(JSON.parse(strFromU8(bytes)));
  } catch {
    return null;
  }
}

function isDbType(value: unknown): value is DbType {
  return DB_TYPES.includes(value as DbType);
}

const fail = (problem: ZipProblem, reason: string): ZipInspection => ({ ok: false, problem, reason });

/**
 * Finds the ZIP's run directory (the unique directory holding `manifest.json`,
 * as the backend does) and reads its `db_type`, plus `collector_version` from
 * the `meta` block of the result JSON the manifest names.
 */
export async function inspectReportZip(zip: Blob): Promise<ZipInspection> {
  const data = new Uint8Array(await zip.arrayBuffer());
  let manifests: Map<string, Uint8Array>;
  try {
    manifests = readEntries(data, (path) => !isArtifact(path) && path.split("/").at(-1) === "manifest.json");
  } catch {
    return fail("corrupt", "不是有效的 ZIP 文件");
  }
  if (manifests.size === 0) return fail("no_manifest", "ZIP 中没有 manifest.json");
  if (manifests.size > 1) return fail("multiple_manifests", `ZIP 中有 ${manifests.size} 个 manifest.json`);

  const [[manifestPath, manifestBytes]] = manifests;
  const manifest = parseObject(manifestBytes);
  if (!manifest) return fail("unreadable_manifest", "manifest.json 无法解析");
  const dbType = manifest.db_type;
  if (!isDbType(dbType)) {
    return fail(
      "unsupported_db_type",
      dbType === undefined ? "manifest.json 缺少 db_type" : `不支持的数据库类型：${String(dbType)}`,
    );
  }

  return { ok: true, dbType, collectorVersion: readCollectorVersion(data, dirOf(manifestPath), manifest) };
}

/** One dropped ZIP, as a row of the generate section shows it. */
export interface InspectedZip {
  file: File;
  inspection: ZipInspection;
}

const isZip = (file: File) => file.name.toLowerCase().endsWith(".zip");

/**
 * Inspects the ZIPs of a drop, in drop order. Other files are skipped, and so
 * is a ZIP whose name is already among `existing` (the rows already shown).
 */
export async function inspectDrop(files: File[], existing: InspectedZip[] = []): Promise<InspectedZip[]> {
  const seen = new Set(existing.map((item) => item.file.name));
  const fresh = files.filter((file) => isZip(file) && !seen.has(file.name) && seen.add(file.name));
  return Promise.all(fresh.map(async (file) => ({ file, inspection: await inspectReportZip(file) })));
}

/** The report task input for these rows, or `null` while submission is blocked: no rows, or any red row. */
export function toTaskInput(items: InspectedZip[]): ReportTaskInput | null {
  const inputs: ReportItemInput[] = [];
  for (const { file, inspection } of items) {
    if (!inspection.ok) return null;
    inputs.push({ zip: file, dbType: inspection.dbType, collectorVersion: inspection.collectorVersion, diagnostics: [] });
  }
  return inputs.length > 0 ? { items: inputs } : null;
}

/** `meta.collector_version` of the result JSON; `null` (unknown) when anything along the way is missing. */
function readCollectorVersion(data: Uint8Array, runDir: string, manifest: JsonObject): string | null {
  const resultName = asObject(manifest.artifacts)?.result;
  if (typeof resultName !== "string" || !resultName) return null;
  const resultPath = normalize(runDir + resultName);
  const result = parseObject(readEntries(data, (path) => path === resultPath).get(resultPath));
  const version = asObject(result?.meta)?.collector_version;
  return typeof version === "string" && version.trim() ? version.trim() : null;
}
