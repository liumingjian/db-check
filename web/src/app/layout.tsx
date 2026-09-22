import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "DB-Check — 数据库巡检平台",
  description:
    "数据库巡检工具下载、采集包上传与报告自动生成平台。支持 MySQL、Oracle、GaussDB、PostgreSQL、达梦。",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="zh-CN">
      <body className="antialiased">
        {children}
      </body>
    </html>
  );
}
