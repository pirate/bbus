export type Deferred<T> = {
  promise: Promise<T>
  resolve: (value: T | PromiseLike<T>) => void
  reject: (reason?: unknown) => void
}

export const withResolvers = <T>(): Deferred<T> => {
  if (typeof Promise.withResolvers === 'function') {
    return Promise.withResolvers<T>()
  }
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((resolve_fn, reject_fn) => {
    resolve = resolve_fn
    reject = reject_fn
  })
  return { promise, resolve, reject }
}

export const CONCURRENCY_MODES = ['global-serial', 'bus-serial', 'parallel', 'auto'] as const
export type ConcurrencyMode = (typeof CONCURRENCY_MODES)[number]

export class AsyncLimiter {
  size: number
  in_use: number
  waiters: Array<() => void>

  constructor(size: number) {
    this.size = size
    this.in_use = 0
    this.waiters = []
  }

  async acquire(): Promise<void> {
    if (this.size === Infinity) {
      return
    }
    if (this.in_use < this.size) {
      this.in_use += 1
      return
    }
    await new Promise<void>((resolve) => {
      this.waiters.push(resolve)
    })
    this.in_use += 1
  }

  release(): void {
    if (this.size === Infinity) {
      return
    }
    this.in_use = Math.max(0, this.in_use - 1)
    const next = this.waiters.shift()
    if (next) {
      next()
    }
  }
}

export const resolveConcurrencyMode = (mode: ConcurrencyMode | undefined, fallback: ConcurrencyMode): ConcurrencyMode => {
  const normalized_fallback = fallback === 'auto' ? 'bus-serial' : fallback
  if (!mode || mode === 'auto') {
    return normalized_fallback
  }
  return mode
}

export const limiterForMode = (mode: ConcurrencyMode, global_limiter: AsyncLimiter, bus_limiter: AsyncLimiter): AsyncLimiter | null => {
  if (mode === 'parallel') {
    return null
  }
  if (mode === 'global-serial') {
    return global_limiter
  }
  if (mode === 'bus-serial') {
    return bus_limiter
  }
  return bus_limiter
}

export const runWithLimiter = async <T>(limiter: AsyncLimiter | null, fn: () => Promise<T>): Promise<T> => {
  if (!limiter) {
    return await fn()
  }
  await limiter.acquire()
  try {
    return await fn()
  } finally {
    limiter.release()
  }
}
