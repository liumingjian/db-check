"use client";

import { useRef, useState } from "react";
import { ConsoleSection } from "@/components/console/console-shell";
import { DoneBand, Intro, RunProgress } from "@/components/console/home/generate/headline";
import { ItemsPanel } from "@/components/console/home/generate/items-panel";
import { useReportInputs } from "@/components/console/home/generate/use-report-inputs";
import { useReportRun } from "@/components/console/home/generate/use-report-run";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/stores/auth-store";

/** 生成报告: the drop zone, one row per report item, overall progress, then the «报告好了。» band. */
export function GenerateSection() {
  const inputs = useReportInputs();
  const { run, submitting, downloading, submit, download, reset } = useReportRun();
  const token = useAuthStore((state) => state.token);
  const busy = run?.outcome === "running" || submitting;
  const { dragging, dropProps } = useFileDrop(!busy, (files) => void inputs.add(files));
  const picker = useRef<HTMLInputElement>(null);

  function restart() {
    reset();
    if (run?.outcome !== "error") inputs.clear();
  }

  async function generate() {
    if (!token || submitting || inputs.validating) return;
    const input = await inputs.validate(token);
    if (input) {
      reset();
      await submit(input);
    }
  }

  if (run?.outcome === "done") {
    return (
      <ConsoleSection id="new-report">
        <DoneBand run={run} downloading={downloading} onDownload={download} onRestart={restart} />
      </ConsoleSection>
    );
  }

  return (
    <ConsoleSection id="new-report">
      <div
        {...dropProps}
        className={cn(
          "flex min-h-[min(100vh,860px)] flex-col transition-[background-color,color] duration-200",
          dragging && "bg-primary text-primary-foreground",
        )}
      >
        <FilePicker inputRef={picker} onFiles={(files) => void inputs.add(files)} />
        <div className="mx-auto grid w-full max-w-[1240px] flex-1 grid-cols-[1.3fr_1fr] items-center gap-16 px-8 py-16">
          <div>
            {dragging ? (
              <h1 className="text-[120px] leading-[1] font-bold tracking-[-4px]">松手。</h1>
            ) : run ? (
              <RunProgress run={run} onRestart={restart} />
            ) : (
              <Intro onPick={() => picker.current?.click()} />
            )}
          </div>
          <div className={cn(dragging && "invisible")}>
            <ItemsPanel
              inputs={inputs}
              run={run}
              submitting={submitting || inputs.validating}
              onSubmit={() => void generate()}
            />
          </div>
        </div>
      </div>
    </ConsoleSection>
  );
}

/** The hidden input behind 选择采集包: primary ZIPs. */
function FilePicker({
  inputRef,
  onFiles,
}: {
  inputRef: React.RefObject<HTMLInputElement | null>;
  onFiles: (files: File[]) => void;
}) {
  return (
    <input
      ref={inputRef}
      type="file"
      multiple
      accept=".zip"
      aria-label="选择 ZIP 采集包"
      className="hidden"
      onChange={(e) => {
        onFiles(Array.from(e.target.files ?? []));
        e.target.value = "";
      }}
    />
  );
}

/**
 * Makes the whole section a drop target for files from the desktop while
 * `enabled`. Nested `dragenter`/`dragleave` pairs are counted, and in-page
 * drags without files are ignored.
 */
function useFileDrop(enabled: boolean, onFiles: (files: File[]) => void) {
  const [dragging, setDragging] = useState(false);
  const depth = useRef(0);
  const carriesFiles = (e: React.DragEvent) => e.dataTransfer.types.includes("Files");
  const dropProps = enabled
    ? {
        onDragEnter: (e: React.DragEvent) => {
          if (!carriesFiles(e)) return;
          depth.current += 1;
          setDragging(true);
        },
        onDragOver: (e: React.DragEvent) => {
          if (carriesFiles(e)) e.preventDefault();
        },
        onDragLeave: () => {
          depth.current = Math.max(0, depth.current - 1);
          if (depth.current === 0) setDragging(false);
        },
        onDrop: (e: React.DragEvent) => {
          e.preventDefault();
          depth.current = 0;
          setDragging(false);
          onFiles(Array.from(e.dataTransfer.files));
        },
      }
    : {};
  return { dragging, dropProps };
}
