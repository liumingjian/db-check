import type { Metadata } from "next";
import { Plus_Jakarta_Sans } from "next/font/google";
import { Database } from "lucide-react";
import "./globals.css";

const jakarta = Plus_Jakarta_Sans({
  variable: "--font-sans",
  subsets: ["latin"],
  weight: ["300", "400", "500", "600", "700"],
});

export const metadata: Metadata = {
  title: "DB-Check Reporter — 数据库巡检报告生成工具",
  description:
    "上传数据库采集产物，自动生成专业 Word 巡检报告。支持 MySQL、Oracle、GaussDB。",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="zh-CN">
      <body className={`${jakarta.variable} antialiased`}>
        {/* Floating navbar */}
        <nav className="fixed top-4 left-4 right-4 z-50 flex items-center gap-3 rounded-xl border border-border bg-card/80 px-5 py-3 backdrop-blur-md">
          <Database className="h-5 w-5 text-primary" />
          <span className="text-sm font-semibold tracking-tight">
            DB-Check Reporter
          </span>
        </nav>

        {/* Main content with navbar offset */}
        <main className="mx-auto max-w-4xl px-4 pt-24 pb-16">
          {children}
        </main>
      </body>
    </html>
  );
}
