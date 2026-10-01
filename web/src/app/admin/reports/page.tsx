"use client";

import { Suspense } from "react";
import { AllReports } from "@/components/console/admin/all-reports";

/** 管理 → 全部报告. The submitter filter lives in the URL query. */
export default function AdminReportsPage() {
  // useSearchParams needs a Suspense boundary, or the static build bails out.
  return (
    <Suspense>
      <AllReports />
    </Suspense>
  );
}
