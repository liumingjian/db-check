/**
 * Persistence for the mock implementation. Each domain keeps its own
 * collection under its own storage key, seeded on first read, so domains
 * evolve their seed and shape independently.
 */
const KEY_PREFIX = "dbcheck_mock_";

export interface MockContext {
  storage: Storage;
  /** Pause between simulated long-running steps; tests use 0. */
  stepDelayMs: number;
}

export interface MockCollection<T> {
  read(): T;
  write(value: T): void;
}

export function mockCollection<T>(storage: Storage, name: string, seed: () => T): MockCollection<T> {
  const key = KEY_PREFIX + name;
  return {
    read() {
      const raw = storage.getItem(key);
      return raw === null ? seed() : (JSON.parse(raw) as T);
    },
    write(value) {
      storage.setItem(key, JSON.stringify(value));
    },
  };
}

/** Drops every mock collection, so the next read returns the seed. */
export function clearMockData(storage: Storage): void {
  const keys: string[] = [];
  for (let i = 0; i < storage.length; i++) {
    const key = storage.key(i);
    if (key?.startsWith(KEY_PREFIX)) keys.push(key);
  }
  keys.forEach((key) => storage.removeItem(key));
}

export function mockId(prefix: string): string {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
