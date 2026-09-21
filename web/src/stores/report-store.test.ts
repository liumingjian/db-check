import { beforeEach, describe, expect, it } from "vitest";
import { useReportStore } from "@/stores/report-store";

function collectorFile(name = "collector.zip", contents = "report input"): File {
  return new File([contents], name, { type: "application/zip", lastModified: 1 });
}

describe("report submission keys", () => {
  beforeEach(() => {
    useReportStore.getState().reset();
    useReportStore.setState({ dbType: "mysql" });
  });

  it("reuses an uncertain key and rotates it after an input mutation", () => {
    useReportStore.getState().addZipFiles([collectorFile()]);

    const first = useReportStore.getState().ensureSubmissionKey();
    expect(useReportStore.getState().ensureSubmissionKey()).toBe(first);

    useReportStore.getState().setAwrFile("missing", [collectorFile("input.html", "awr")]);
    const second = useReportStore.getState().ensureSubmissionKey();
    expect(second).not.toBe(first);
  });

  it("rehydrates the retained key and clears it after acceptance", () => {
    useReportStore.getState().addZipFiles([collectorFile()]);
    const key = useReportStore.getState().ensureSubmissionKey();

    useReportStore.setState({ submissionKey: null, submissionFingerprint: null });
    expect(useReportStore.getState().ensureSubmissionKey()).toBe(key);

    useReportStore.getState().setTaskId("task-1");
    expect(useReportStore.getState().submissionKey).toBeNull();
    expect(useReportStore.getState().submissionFingerprint).toBeNull();
  });

  it("restores an accepted task without changing selected files", () => {
    const file = collectorFile();
    useReportStore.getState().addZipFiles([file]);
    const selected = useReportStore.getState().zipFiles;

    useReportStore.getState().restoreTask("task-1");

    expect(useReportStore.getState()).toMatchObject({ currentStep: 3, taskId: "task-1" });
    expect(useReportStore.getState().zipFiles).toEqual(selected);
  });

  it("starts a deliberate retry with no accepted task or retained submission key", () => {
    useReportStore.getState().addZipFiles([collectorFile("first.zip")]);
    const originalKey = useReportStore.getState().ensureSubmissionKey();
    useReportStore.getState().setTaskId("task-1");

    useReportStore.getState().beginNewTask();

    expect(useReportStore.getState()).toMatchObject({
      currentStep: 2,
      dbType: "mysql",
      taskId: null,
      submissionKey: null,
      submissionFingerprint: null,
      zipFiles: [],
      awrFiles: {},
    });

    useReportStore.getState().addZipFiles([collectorFile("retry.zip")]);
    expect(useReportStore.getState().ensureSubmissionKey()).not.toBe(originalKey);
  });
});
