"use client";

import { useReportStore } from "@/stores/report-store";
import { StepIndicator } from "@/components/step-indicator";
import { FileUploadStep } from "@/components/file-upload-step";
import { GenerationStep } from "@/components/generation-step";

export function ReportPage() {
  const currentStep = useReportStore((s) => s.currentStep);

  return (
    <div className="flex flex-col gap-8">
      {/* Step indicator */}
      <StepIndicator current={currentStep} />

      {/* Step content */}
      {currentStep === 1 && <FileUploadStep />}
      {currentStep === 2 && <GenerationStep />}
    </div>
  );
}
