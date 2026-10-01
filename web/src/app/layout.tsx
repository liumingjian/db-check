import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "DB-Check — 数据库巡检平台",
  description: "数据库巡检平台：下载采集器，上传采集包，生成巡检报告。支持 MySQL、Oracle、GaussDB。",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="zh-CN">
      <body className="bg-background text-foreground antialiased">{children}</body>
    </html>
  );
}
