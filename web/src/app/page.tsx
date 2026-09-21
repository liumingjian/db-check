"use client";

import { useEffect } from "react";
import { useReportStore } from "@/stores/report-store";
import { StepIndicator } from "@/components/step-indicator";
import { DbTypeSelector } from "@/components/db-type-selector";
import { FileUploadStep } from "@/components/file-upload-step";
import { GenerationStep } from "@/components/generation-step";

export default function Home() {
  const currentStep = useReportStore((s) => s.currentStep);
  const dbType = useReportStore((s) => s.dbType);
  const setDbType = useReportStore((s) => s.setDbType);
  const nextStep = useReportStore((s) => s.nextStep);
  const restoreTask = useReportStore((s) => s.restoreTask);

  useEffect(() => {
    const taskID = sessionStorage.getItem("dbcheck_task_id");
    if (taskID) {
      restoreTask(taskID);
    }
  }, [restoreTask]);

  return (
    <div className="flex flex-col gap-10">
      {/* Step indicator */}
      <StepIndicator current={currentStep} />

      {/* Step content */}
      {currentStep === 1 && (
        <DbTypeSelector
          selected={dbType}
          onSelect={setDbType}
          onNext={nextStep}
        />
      )}

      {currentStep === 2 && <FileUploadStep />}

      {currentStep === 3 && <GenerationStep />}
    </div>
  );
}
