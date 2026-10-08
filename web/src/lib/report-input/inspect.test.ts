import { strToU8, zipSync } from "fflate";
import { describe, expect, it } from "vitest";
import {
  inspectDrop,
  inspectReportZip,
  addDiagnostics,
  toTaskInput,
  type InspectedZip,
} from "@/lib/report-input/inspect";

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

describe("addDiagnostics: direct selection on a report item", () => {
  const awr = new File(["<html>awr</html>"], "awrrpt_1_100_101.html");
  const awr2 = new File(["<html>second awr</html>"], "awrrpt_1_101_102.html");
  const wdr = new File(["<html>wdr</html>"], "wdr_1.html");
  const wdr2 = new File(["<html>wdr 2</html>"], "wdr_2.html");

  async function rowOf(dbType: string | null, diagnostics: File[] = []): Promise<InspectedZip> {
    const zip =
      dbType === null
        ? new File(["garbage"], "broken.zip")
        : zipOf(`${dbType}.zip`, { "manifest.json": manifest(dbType) });
    const [row] = await inspectDrop([zip]);
    return { ...row, diagnostics };
  }

  it.each([
    { case: "an AWR onto an empty oracle row", row: () => rowOf("oracle"), file: awr, paired: [awr] },
    { case: "a WDR onto an empty gaussdb row", row: () => rowOf("gaussdb"), file: wdr, paired: [wdr] },
    { case: "more WDRs onto a gaussdb row", row: () => rowOf("gaussdb", [wdr]), file: wdr2, paired: [wdr, wdr2] },
  ])("accepts $case, leaving the original row untouched", async ({ row, file, paired }) => {
    const before = await row();
    const outcome = addDiagnostics(before, [file]);
    expect(outcome).toMatchObject({ ok: true });
    expect(outcome.ok && outcome.item.diagnostics).toEqual(paired);
    expect(before.diagnostics).not.toContain(file);
  });

  it.each([
    { case: "a second AWR on an oracle row", row: () => rowOf("oracle", [awr]), file: awr2, problem: "slot_full" },
    { case: "an AWR on a mysql row", row: () => rowOf("mysql"), file: awr, problem: "no_slot" },
    { case: "a WDR on a mysql row", row: () => rowOf("mysql"), file: wdr, problem: "no_slot" },
    { case: "any HTML on a red row", row: () => rowOf(null), file: awr, problem: "unreadable_item" },
    { case: "the same WDR twice on a gaussdb row", row: () => rowOf("gaussdb", [wdr]), file: wdr, problem: "already_added" },
  ])("refuses $case, with a reason", async ({ row, file, problem }) => {
    const outcome = addDiagnostics(await row(), [file]);
    expect(outcome).toMatchObject({ ok: false, problem });
    expect(outcome.ok === false && outcome.reason).toBeTruthy();
  });
});

describe("toTaskInput: paired inputs reach the report task", () => {
  const oracle = zipOf("ora.zip", { "manifest.json": manifest("oracle"), "result.json": result("1.2.0") });
  const gauss = zipOf("gs.zip", { "manifest.json": manifest("gaussdb"), "result.json": result("1.2.0") });
  const mysql = zipOf("my.zip", { "manifest.json": manifest("mysql"), "result.json": result("1.2.0") });
  const awr = new File(["<html>awr</html>"], "awr.html");
  const wdr1 = new File(["<html>wdr 1</html>"], "wdr_1.html");
  const wdr2 = new File(["<html>wdr 2</html>"], "wdr_2.html");

  async function pairedRows(): Promise<InspectedZip[]> {
    const [ora, gs, my] = await inspectDrop([oracle, gauss, mysql, awr, wdr1, wdr2]);
    let rows = [ora, gs, my];
    for (const [index, file] of [[0, awr], [1, wdr1], [1, wdr2]] as const) {
      const outcome = addDiagnostics(rows[index], [file]);
      if (!outcome.ok) throw new Error(outcome.reason);
      rows = rows.map((row, i) => (i === index ? outcome.item : row));
    }
    return rows;
  }

  it("carries each item's paired files with that item", async () => {
    expect(toTaskInput(await pairedRows())?.items.map((item) => [item.zip.name, item.diagnostics])).toEqual([
      ["ora.zip", [awr]],
      ["gs.zip", [wdr1, wdr2]],
      ["my.zip", []],
    ]);
  });

  it("rejects excess Oracle selections without adding either file", async () => {
    const [row] = await inspectDrop([oracle]);
    const second = new File(["second"], "second.htm");
    expect(addDiagnostics(row, [awr, second])).toMatchObject({ ok: false, problem: "slot_full" });
    expect(row.diagnostics).toEqual([]);
  });

  it("rejects non-HTML selections", async () => {
    const [row] = await inspectDrop([gauss]);
    expect(addDiagnostics(row, [new File(["text"], "notes.txt")])).toMatchObject({ ok: false, problem: "unsupported_file" });
  });

  it("keeps associations after another item is removed", async () => {
    const rows = await pairedRows();
    expect(toTaskInput(rows.filter((row) => row.file.name !== "ora.zip"))?.items.map((item) => [item.zip.name, item.diagnostics])).toEqual([
      ["gs.zip", [wdr1, wdr2]], ["my.zip", []],
    ]);
  });
});
