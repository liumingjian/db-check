"use client";

import { useState } from "react";
import { useDialogs } from "@/components/console/dialog-host";
import { ApiError, errorMessage } from "@/lib/api";
import { saveBlob } from "@/lib/files";

/**
 * Fetches a blob and saves it as `fileName`. A failure shows as a toast; the
 * resolved boolean says whether the file was saved. `downloading` is true
 * while a fetch runs, for disabling the button.
 */
export function useBlobDownload() {
  const { toast } = useDialogs();
  const [downloading, setDownloading] = useState(false);

  async function download(fetchBlob: () => Promise<Blob>, fileName: string): Promise<boolean> {
    setDownloading(true);
    try {
      saveBlob(await fetchBlob(), fileName);
      return true;
    } catch (e) {
      toast(e instanceof ApiError ? e.message : `下载失败：${errorMessage(e)}`);
      return false;
    } finally {
      setDownloading(false);
    }
  }

  return { download, downloading };
}
