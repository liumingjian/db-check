import { strToU8, zipSync } from "fflate";
import { describe, expect, it } from "vitest";
import { inspectDrop, inspectReportZip, toTaskInput } from "@/lib/report-input/inspect";

/** Builds a ZIP `File` from path → content; objects are written as JSON. */
function zipOf(name: string, entries: Record<string, string | object>): File {
  const files = Object.fromEntries(
    Object.entries(entries).map(([path, content]) => [
      path,
      strToU8(typeof content === "string" ? content : JSON.stringify(content)),
    ]),
  );
  return new File([zipSync(files)], name, { type: "application/zip" });
}

function manifest(dbType: unknown, result = "result.json") {
  return { schema_version: "1.0", db_type: dbType, artifacts: { result } };
}

function result(collectorVersion: unknown) {
  return { meta: { schema_version: "2.0", collector_version: collectorVersion } };
}

describe("inspectReportZip: detection", () => {
  it.each([
    {
      case: "mysql manifest at the root",
      zip: zipOf("a.zip", { "manifest.json": manifest("mysql"), "result.json": result("1.2.0") }),
      expected: { ok: true, dbType: "mysql", collectorVersion: "1.2.0" },
    },
    {
      case: "oracle run directory inside a subdirectory",
      zip: zipOf("b.zip", {
        "oracle-10.0.0.8-20260312/manifest.json": manifest("oracle", "result_oracle.json"),
        "oracle-10.0.0.8-20260312/result_oracle.json": result("1.1.0"),
      }),
      expected: { ok: true, dbType: "oracle", collectorVersion: "1.1.0" },
    },
    {
      case: "gaussdb with macOS archive artifacts beside the run directory",
      zip: zipOf("c.zip", {
        "run/manifest.json": manifest("gaussdb"),
        "run/result.json": result("1.2.0"),
        "__MACOSX/run/manifest.json": "binary resource fork",
        "run/._manifest.json": "binary resource fork",
      }),
      expected: { ok: true, dbType: "gaussdb", collectorVersion: "1.2.0" },
    },
    {
      case: "manifest names no result file: version unknown",
      zip: zipOf("l.zip", { "manifest.json": { db_type: "mysql" } }),
      expected: { ok: true, dbType: "mysql", collectorVersion: null },
    },
    {
      case: "named result file is missing: version unknown",
      zip: zipOf("m.zip", { "manifest.json": manifest("oracle", "missing.json") }),
      expected: { ok: true, dbType: "oracle", collectorVersion: null },
    },
    {
      case: "result file is not JSON: version unknown",
      zip: zipOf("n.zip", { "manifest.json": manifest("mysql"), "result.json": "{oops" }),
      expected: { ok: true, dbType: "mysql", collectorVersion: null },
    },
    {
      case: "result meta has no collector_version: version unknown",
      zip: zipOf("o.zip", { "manifest.json": manifest("mysql"), "result.json": { meta: {} } }),
      expected: { ok: true, dbType: "mysql", collectorVersion: null },
    },
    {
      case: "blank collector_version: version unknown",
      zip: zipOf("p.zip", { "manifest.json": manifest("mysql"), "result.json": result("  ") }),
      expected: { ok: true, dbType: "mysql", collectorVersion: null },
    },
  ])("$case", async ({ zip, expected }) => {
    expect(await inspectReportZip(zip)).toEqual(expected);
  });
});

describe("inspectReportZip: rejection", () => {
  it.each([
    {
      case: "corrupt ZIP",
      zip: new File(["PK this is not really a zip"], "broken.zip"),
      problem: "corrupt",
    },
    {
      case: "empty file",
      zip: new File([], "empty.zip"),
      problem: "corrupt",
    },
    {
      case: "no manifest.json",
      zip: zipOf("d.zip", { "result.json": result("1.2.0") }),
      problem: "no_manifest",
    },
    {
      case: "manifest.json only under __MACOSX",
      zip: zipOf("e.zip", { "__MACOSX/manifest.json": manifest("mysql") }),
      problem: "no_manifest",
    },
    {
      case: "two run directories",
      zip: zipOf("f.zip", { "a/manifest.json": manifest("mysql"), "b/manifest.json": manifest("oracle") }),
      problem: "multiple_manifests",
    },
    {
      case: "manifest.json is not JSON",
      zip: zipOf("g.zip", { "manifest.json": "{ db_type: mysql" }),
      problem: "unreadable_manifest",
    },
    {
      case: "manifest.json is a JSON array",
      zip: zipOf("h.zip", { "manifest.json": "[]" }),
      problem: "unreadable_manifest",
    },
    {
      case: "manifest.json without db_type",
      zip: zipOf("i.zip", { "manifest.json": { schema_version: "1.0" } }),
      problem: "unsupported_db_type",
    },
    {
      case: "postgresql is not a supported type",
      zip: zipOf("j.zip", { "manifest.json": manifest("postgresql"), "result.json": result("1.2.0") }),
      problem: "unsupported_db_type",
    },
    {
      case: "db_type matching is exact",
      zip: zipOf("k.zip", { "manifest.json": manifest("MySQL"), "result.json": result("1.2.0") }),
      problem: "unsupported_db_type",
    },
  ])("$case", async ({ zip, problem }) => {
    const inspection = await inspectReportZip(zip);
    expect(inspection).toMatchObject({ ok: false, problem });
    expect(inspection.ok === false && inspection.reason).toBeTruthy();
  });

  it("names the unsupported type in the reason", async () => {
    const inspection = await inspectReportZip(zipOf("j.zip", { "manifest.json": manifest("dameng") }));
    expect(inspection).toMatchObject({ ok: false, reason: expect.stringContaining("dameng") });
  });
});

describe("inspectDrop and toTaskInput: a drop becomes report items", () => {
  const oracle = zipOf("ora-01.zip", { "run/manifest.json": manifest("oracle"), "run/result.json": result("1.1.0") });
  const mysql = zipOf("MALL.ZIP", { "manifest.json": manifest("mysql"), "result.json": result("1.2.0") });
  const broken = new File(["garbage"], "broken.zip");
  const notes = new File(["hello"], "notes.txt");

  it("turns each ZIP of a mixed drop into a report item, in drop order, skipping other files", async () => {
    const items = await inspectDrop([oracle, notes, mysql, broken]);
    expect(items.map((i) => [i.file.name, i.inspection.ok && i.inspection.dbType])).toEqual([
      ["ora-01.zip", "oracle"],
      ["MALL.ZIP", "mysql"],
      ["broken.zip", false],
    ]);
  });

  it("skips a ZIP whose name is already among the items", async () => {
    const first = await inspectDrop([oracle]);
    const again = await inspectDrop([oracle, mysql], first);
    expect(again.map((i) => i.file.name)).toEqual(["MALL.ZIP"]);
  });

  it.each([
    { case: "nothing dropped", files: [], submittable: false },
    { case: "one red item blocks the rest", files: [oracle, broken], submittable: false },
    { case: "only valid items", files: [oracle, mysql], submittable: true },
  ])("submission gate: $case", async ({ files, submittable }) => {
    expect(toTaskInput(await inspectDrop(files)) !== null).toBe(submittable);
  });

  it("carries each item's ZIP, database type and collector version into the task input", async () => {
    expect(toTaskInput(await inspectDrop([oracle, mysql]))).toEqual({
      items: [
        { zip: oracle, dbType: "oracle", collectorVersion: "1.1.0", diagnostics: [] },
        { zip: mysql, dbType: "mysql", collectorVersion: "1.2.0", diagnostics: [] },
      ],
    });
  });
});
