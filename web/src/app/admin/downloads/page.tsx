"use client";

import { Suspense } from "react";
import { DownloadRecords } from "@/components/console/admin/download-records";

/** 管理 → 下载记录. Filters live in the URL query, so the release `···` menu can deep-link here. */
export default function AdminDownloadsPage() {
  // useSearchParams needs a Suspense boundary, or the static build bails out.
  return (
    <Suspense>
      <DownloadRecords />
    </Suspense>
  );
}
