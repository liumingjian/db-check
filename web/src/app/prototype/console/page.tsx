// PROTOTYPE — throwaway. Three layouts for the role-aware console
// (collectors, users, report tasks, pending/rejected), switchable via
// `?variant=A|B|C` on this route. Lives only on the prototype branch.
import { Suspense } from "react";
import { notFound } from "next/navigation";
import { ConsolePrototype } from "@/components/prototype/console/console-prototype";

export default function ConsolePrototypePage() {
  if (process.env.NODE_ENV === "production") notFound();
  return (
    <Suspense>
      <ConsolePrototype />
    </Suspense>
  );
}
