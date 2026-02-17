declare const process: { versions?: { node?: string } } | undefined

type AsyncLocalStorageLike = {
  getStore(): unknown
  run<T>(store: unknown, callback: () => T): T
  enterWith?(store: unknown): void
}

export type { AsyncLocalStorageLike }

// Cache the AsyncLocalStorage constructor so multiple modules can create separate instances.
let _AsyncLocalStorageClass: (new () => AsyncLocalStorageLike) | null = null

const is_node = typeof process !== 'undefined' && typeof process.versions !== 'undefined' && typeof process.versions.node === 'string'

if (is_node) {
  try {
    const importer = new Function('specifier', 'return import(specifier)') as (
      specifier: string
    ) => Promise<{ AsyncLocalStorage?: new () => AsyncLocalStorageLike }>
    const mod = await importer('node:async_hooks')
    if (mod?.AsyncLocalStorage) {
      _AsyncLocalStorageClass = mod.AsyncLocalStorage
    }
  } catch {
    _AsyncLocalStorageClass = null
  }
}

/** Create a new AsyncLocalStorage instance, or null if unavailable (e.g. in browsers). */
export const createAsyncLocalStorage = (): AsyncLocalStorageLike | null => {
  if (!_AsyncLocalStorageClass) return null
  return new _AsyncLocalStorageClass()
}

// The primary AsyncLocalStorage instance used for event dispatch context propagation.
export let async_local_storage: AsyncLocalStorageLike | null = _AsyncLocalStorageClass ? new _AsyncLocalStorageClass() : null

export const captureAsyncContext = (): unknown | null => {
  if (!async_local_storage) {
    return null
  }
  return async_local_storage.getStore() ?? null
}

export const _runWithAsyncContext = <T>(context: unknown | null, fn: () => T): T => {
  if (!async_local_storage) {
    return fn()
  }
  return async_local_storage.run(context ?? undefined, fn)
}

export const hasAsyncLocalStorage = (): boolean => async_local_storage !== null
