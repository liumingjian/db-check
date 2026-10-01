// PROTOTYPE — throwaway. Console layout variants for the role-aware platform
// (generate report, collectors, my reports, admin, pending/rejected),
// switchable via `?variant=` on this route. Lives only on the prototype branch.
import { Suspense } from "react";
import { notFound } from "next/navigation";
import { Geist, Geist_Mono, Inter, JetBrains_Mono } from "next/font/google";
import { ConsolePrototype } from "@/components/prototype/console/console-prototype";

const inter = Inter({ subsets: ["latin"], variable: "--font-inter" });
const geist = Geist({ subsets: ["latin"], variable: "--font-geist" });
const geistMono = Geist_Mono({ subsets: ["latin"], variable: "--font-geist-mono" });
const jetbrains = JetBrains_Mono({ subsets: ["latin"], variable: "--font-jetbrains" });

export default function ConsolePrototypePage() {
  if (process.env.NODE_ENV === "production") notFound();
  return (
    <div className={`${inter.variable} ${geist.variable} ${geistMono.variable} ${jetbrains.variable}`}>
      <Suspense>
        <ConsolePrototype />
      </Suspense>
    </div>
  );
}
