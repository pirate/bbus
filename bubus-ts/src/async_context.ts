type AsyncLocalStorageLike = {
  getStore(): unknown;
  run<T>(store: unknown, callback: () => T): T;
  enterWith?(store: unknown): void;
};

export let async_local_storage: AsyncLocalStorageLike | null = null;

const is_node =
  typeof process !== "undefined" &&
  typeof process.versions !== "undefined" &&
  typeof process.versions.node === "string";

if (is_node) {
  try {
    const importer = new Function(
      "specifier",
      "return import(specifier)"
    ) as (specifier: string) => Promise<{ AsyncLocalStorage?: new () => AsyncLocalStorageLike }>;
    const mod = await importer("node:async_hooks");
    if (mod?.AsyncLocalStorage) {
      async_local_storage = new mod.AsyncLocalStorage();
    }
  } catch {
    async_local_storage = null;
  }
}

export const capture_async_context = (): unknown | null => {
  if (!async_local_storage) {
    return null;
  }
  return async_local_storage.getStore() ?? null;
};

export const run_with_async_context = <T>(context: unknown | null, fn: () => T): T => {
  if (!async_local_storage) {
    return fn();
  }
  return async_local_storage.run(context ?? undefined, fn);
};

export const has_async_local_storage = (): boolean => async_local_storage !== null;
