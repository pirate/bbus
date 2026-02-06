import type { BaseEvent } from './base_event.js'
import type { EventResult } from './event_result.js'
import type { HandlerOptions } from './types.js'

// ─── Deferred / withResolvers ────────────────────────────────────────────────

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

// ─── Concurrency modes ──────────────────────────────────────────────────────

export const CONCURRENCY_MODES = ['global-serial', 'bus-serial', 'parallel', 'auto'] as const
export type ConcurrencyMode = (typeof CONCURRENCY_MODES)[number]

export const resolveConcurrencyMode = (mode: ConcurrencyMode | undefined, fallback: ConcurrencyMode): ConcurrencyMode => {
  const normalized_fallback = fallback === 'auto' ? 'bus-serial' : fallback
  if (!mode || mode === 'auto') {
    return normalized_fallback
  }
  return mode
}

// ─── AsyncSemaphore ──────────────────────────────────────────────────────────

export class AsyncSemaphore {
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

export const semaphoreForMode = (mode: ConcurrencyMode, global_semaphore: AsyncSemaphore, bus_semaphore: AsyncSemaphore): AsyncSemaphore | null => {
  if (mode === 'parallel') {
    return null
  }
  if (mode === 'global-serial') {
    return global_semaphore
  }
  if (mode === 'bus-serial') {
    return bus_semaphore
  }
  return bus_semaphore
}

export const runWithSemaphore = async <T>(semaphore: AsyncSemaphore | null, fn: () => Promise<T>): Promise<T> => {
  if (!semaphore) {
    return await fn()
  }
  await semaphore.acquire()
  try {
    return await fn()
  } finally {
    semaphore.release()
  }
}

// ─── HandlerLock ─────────────────────────────────────────────────────────────

export type HandlerExecutionState = 'held' | 'yielded' | 'closed'

// Tracks a single handler execution's ownership of a semaphore lock.
// Reacquire is race-safe: if the handler exits while waiting to reclaim,
// the reclaimed lock is immediately released to avoid leaks.
export class HandlerLock {
  private semaphore: AsyncSemaphore | null
  private state: HandlerExecutionState

  constructor(semaphore: AsyncSemaphore | null) {
    this.semaphore = semaphore
    this.state = 'held'
  }

  getExecutionState(): HandlerExecutionState {
    return this.state
  }

  yieldHandlerLockForChildRun(): boolean {
    if (!this.semaphore || this.state !== 'held') {
      return false
    }
    this.state = 'yielded'
    this.semaphore.release()
    return true
  }

  async reclaimHandlerLockIfRunning(): Promise<boolean> {
    if (!this.semaphore || this.state !== 'yielded') {
      return false
    }
    await this.semaphore.acquire()
    if (this.state !== 'yielded') {
      // Handler exited while this reacquire was pending.
      this.semaphore.release()
      return false
    }
    this.state = 'held'
    return true
  }

  exitHandlerRun(): void {
    if (this.state === 'closed') {
      return
    }
    const should_release = !!this.semaphore && this.state === 'held'
    this.state = 'closed'
    if (should_release) {
      this.semaphore!.release()
    }
  }

  async runQueueJump<T>(fn: () => Promise<T>): Promise<T> {
    const yielded = this.yieldHandlerLockForChildRun()
    try {
      return await fn()
    } finally {
      if (yielded) {
        await this.reclaimHandlerLockIfRunning()
      }
    }
  }
}

// ─── LockManager ─────────────────────────────────────────────────────────────

type LockManagerOptions = {
  get_idle_snapshot: () => boolean
  get_event_concurrency_default: () => ConcurrencyMode
  get_handler_concurrency_default: () => ConcurrencyMode
  get_bus_event_semaphore: () => AsyncSemaphore
  get_bus_handler_semaphore: () => AsyncSemaphore
  get_global_event_semaphore: () => AsyncSemaphore
  get_global_handler_semaphore: () => AsyncSemaphore
}

export class LockManager {
  private get_idle_snapshot: () => boolean
  private get_event_concurrency_default: () => ConcurrencyMode
  private get_handler_concurrency_default: () => ConcurrencyMode
  private get_bus_event_semaphore: () => AsyncSemaphore
  private get_bus_handler_semaphore: () => AsyncSemaphore
  private get_global_event_semaphore: () => AsyncSemaphore
  private get_global_handler_semaphore: () => AsyncSemaphore

  private pause_depth: number
  private pause_waiters: Array<() => void>
  private queue_jump_pause_releases: WeakMap<EventResult, () => void>
  private active_handler_results: EventResult[]

  private idle_waiters: Array<() => void>
  private idle_check_pending: boolean
  private idle_check_streak: number

  constructor(options: LockManagerOptions) {
    this.get_idle_snapshot = options.get_idle_snapshot
    this.get_event_concurrency_default = options.get_event_concurrency_default
    this.get_handler_concurrency_default = options.get_handler_concurrency_default
    this.get_bus_event_semaphore = options.get_bus_event_semaphore
    this.get_bus_handler_semaphore = options.get_bus_handler_semaphore
    this.get_global_event_semaphore = options.get_global_event_semaphore
    this.get_global_handler_semaphore = options.get_global_handler_semaphore

    this.pause_depth = 0
    this.pause_waiters = []
    this.queue_jump_pause_releases = new WeakMap()
    this.active_handler_results = []

    this.idle_waiters = []
    this.idle_check_pending = false
    this.idle_check_streak = 0
  }

  requestPause(): () => void {
    this.pause_depth += 1
    let released = false
    return () => {
      if (released) {
        return
      }
      released = true
      this.pause_depth = Math.max(0, this.pause_depth - 1)
      if (this.pause_depth !== 0) {
        return
      }
      const waiters = this.pause_waiters
      this.pause_waiters = []
      for (const resolve of waiters) {
        resolve()
      }
    }
  }

  waitUntilResumed(): Promise<void> {
    if (this.pause_depth === 0) {
      return Promise.resolve()
    }
    return new Promise((resolve) => {
      this.pause_waiters.push(resolve)
    })
  }

  isPaused(): boolean {
    return this.pause_depth > 0
  }

  enterHandlerContext(result: EventResult): void {
    this.active_handler_results.push(result)
  }

  exitHandlerContext(result: EventResult): void {
    const idx = this.active_handler_results.indexOf(result)
    if (idx >= 0) {
      this.active_handler_results.splice(idx, 1)
    }
  }

  getCurrentHandlerResult(): EventResult | undefined {
    return this.active_handler_results[this.active_handler_results.length - 1]
  }

  isInsideHandlerContext(): boolean {
    return this.active_handler_results.length > 0
  }

  ensureQueueJumpPauseForResult(result: EventResult): void {
    if (this.queue_jump_pause_releases.has(result)) {
      return
    }
    this.queue_jump_pause_releases.set(result, this.requestPause())
  }

  releaseQueueJumpPauseForResult(result: EventResult): void {
    const release_pause = this.queue_jump_pause_releases.get(result)
    if (!release_pause) {
      return
    }
    this.queue_jump_pause_releases.delete(result)
    release_pause()
  }

  waitForIdle(): Promise<void> {
    if (this.get_idle_snapshot()) {
      return Promise.resolve()
    }
    return new Promise((resolve) => {
      this.idle_waiters.push(resolve)
      this.scheduleIdleCheck()
    })
  }

  notifyIdleListeners(): void {
    if (!this.get_idle_snapshot()) {
      this.idle_check_streak = 0
      if (this.idle_waiters.length > 0) {
        this.scheduleIdleCheck()
      }
      return
    }

    this.idle_check_streak += 1
    if (this.idle_check_streak < 2) {
      if (this.idle_waiters.length > 0) {
        this.scheduleIdleCheck()
      }
      return
    }

    this.idle_check_streak = 0
    const waiters = this.idle_waiters
    this.idle_waiters = []
    for (const resolve of waiters) {
      resolve()
    }
  }

  getSemaphoreForEvent(event: BaseEvent): AsyncSemaphore | null {
    const resolved = resolveConcurrencyMode(event.event_concurrency, this.get_event_concurrency_default())
    return semaphoreForMode(resolved, this.get_global_event_semaphore(), this.get_bus_event_semaphore())
  }

  getSemaphoreForHandler(event: BaseEvent, options?: HandlerOptions): AsyncSemaphore | null {
    const event_override = event.handler_concurrency && event.handler_concurrency !== 'auto' ? event.handler_concurrency : undefined
    const handler_override =
      options?.handler_concurrency && options.handler_concurrency !== 'auto' ? options.handler_concurrency : undefined
    const fallback = this.get_handler_concurrency_default()
    const resolved = resolveConcurrencyMode(event_override ?? handler_override ?? fallback, fallback)
    return semaphoreForMode(resolved, this.get_global_handler_semaphore(), this.get_bus_handler_semaphore())
  }

  clear(): void {
    this.pause_depth = 0
    this.pause_waiters = []
    this.queue_jump_pause_releases = new WeakMap()
    this.active_handler_results = []
    this.idle_waiters = []
    this.idle_check_pending = false
    this.idle_check_streak = 0
  }

  private scheduleIdleCheck(): void {
    if (this.idle_check_pending) {
      return
    }
    this.idle_check_pending = true
    setTimeout(() => {
      this.idle_check_pending = false
      this.notifyIdleListeners()
    }, 0)
  }
}
