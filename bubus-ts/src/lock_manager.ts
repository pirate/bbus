import type { BaseEvent } from './base_event.js'
import type { EventResult } from './event_result.js'

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

export const EVENT_CONCURRENCY_MODES = ['global-serial', 'bus-serial', 'parallel'] as const
export type EventConcurrencyMode = (typeof EVENT_CONCURRENCY_MODES)[number]

export const EVENT_HANDLER_CONCURRENCY_MODES = ['serial', 'parallel'] as const
export type EventHandlerConcurrencyMode = (typeof EVENT_HANDLER_CONCURRENCY_MODES)[number]

export const EVENT_HANDLER_COMPLETION_MODES = ['all', 'first'] as const
export type EventHandlerCompletionMode = (typeof EVENT_HANDLER_COMPLETION_MODES)[number]

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

  // used by EventBus.processEventImmediately to yield the parent handler's lock to the child event so it can be processed immediately
  yieldHandlerLockForChildRun(): boolean {
    if (!this.semaphore || this.state !== 'held') {
      return false
    }
    this.state = 'yielded'
    this.semaphore.release()
    return true
  }

  // used by EventBus.processEventImmediately to reacquire the handler lock after the child event has been processed
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

  // used by EventResult.runHandler to exit the handler lock after the handler has finished executing
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

  // used by EventBus.processEventImmediately to yield the handler lock and reacquire it after the child event has been processed
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

// Interface that must be implemented by the EventBus class to be used by the LockManager
export type EventBusInterfaceForLockManager = {
  isIdleAndQueueEmpty: () => boolean
  event_concurrency_default: EventConcurrencyMode
}

// The LockManager is responsible for managing the concurrency of events and handlers
export class LockManager {
  private bus: EventBusInterfaceForLockManager // Live bus reference; used to read defaults and idle state.

  static global_event_semaphore = new AsyncSemaphore(1) // used for the global-serial concurrency mode
  readonly bus_event_semaphore: AsyncSemaphore // Per-bus event semaphore; created with LockManager and never swapped.
  private pause_depth: number // Re-entrant pause counter; increments on requestRunloopPause, decrements on release.
  private pause_waiters: Array<() => void> // Resolvers for waitUntilRunloopResumed; drained when pause_depth hits 0.
  private active_handler_results: EventResult[] // Stack of active handler results for "inside handler" detection.

  private idle_waiters: Array<() => void> // Resolvers waiting for stable idle; cleared when idle confirmed.
  private idle_check_pending: boolean // Debounce flag to avoid scheduling redundant idle checks.
  private idle_check_streak: number // Counts consecutive idle checks; used to require two ticks of idle.

  constructor(bus: EventBusInterfaceForLockManager) {
    this.bus = bus
    this.bus_event_semaphore = new AsyncSemaphore(1) // used for the bus-serial concurrency mode

    this.pause_depth = 0
    this.pause_waiters = []
    this.active_handler_results = []

    this.idle_waiters = []
    this.idle_check_pending = false
    this.idle_check_streak = 0
  }

  // Low-level runloop pause: increments a re-entrant counter and returns a release
  // function. Used for broad, bus-scoped pauses during queue-jump across buses.
  requestRunloopPause(): () => void {
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

  waitUntilRunloopResumed(): Promise<void> {
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

  enterActiveHandlerContext(result: EventResult): void {
    this.active_handler_results.push(result)
  }

  exitActiveHandlerContext(result: EventResult): void {
    const idx = this.active_handler_results.indexOf(result)
    if (idx >= 0) {
      this.active_handler_results.splice(idx, 1)
    }
  }

  getActiveHandlerResult(): EventResult | undefined {
    return this.active_handler_results[this.active_handler_results.length - 1]
  }

  // Per-bus check: true only if this specific bus has a handler on its stack.
  // For cross-bus queue-jumping, EventBus.processEventImmediately uses getParentEventResultAcrossAllBusses()
  // to walk up the parent event tree, and the bus proxy passes handler_result
  // to processEventImmediately so it can yield/reacquire the correct semaphore.
  isAnyHandlerActive(): boolean {
    return this.active_handler_results.length > 0
  }

  waitForIdle(): Promise<void> {
    return new Promise((resolve) => {
      this.idle_waiters.push(resolve)
      this.scheduleIdleCheck()
    })
  }

  // Called by EventBus.markEventCompleted and EventBus.markHandlerCompleted to notify
  // waitUntilIdle() callers that the bus may now be idle.
  notifyIdleListeners(): void {
    // Fast-path: most completions have no waitUntilIdle() callers waiting,
    // so skip expensive idle snapshot scans in that common case.
    if (this.idle_waiters.length === 0) {
      this.idle_check_streak = 0
      return
    }

    if (!this.bus.isIdleAndQueueEmpty()) {
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

  // get the bus-level semaphore that prevents/allows multiple events to be processed concurrently on the same bus
  getSemaphoreForEvent(event: BaseEvent): AsyncSemaphore | null {
    const resolved = event.event_concurrency ?? this.bus.event_concurrency_default
    if (resolved === 'parallel') {
      return null
    }
    if (resolved === 'global-serial') {
      return LockManager.global_event_semaphore
    }
    return this.bus_event_semaphore
  }

  // Schedules a debounced idle check to run after a short delay. Used to gate
  // waitUntilIdle() calls during handler execution and after event completion.
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

  // Reset all state to initial values
  clear(): void {
    this.pause_depth = 0
    this.pause_waiters = []
    this.active_handler_results = []
    this.idle_waiters = []
    this.idle_check_pending = false
    this.idle_check_streak = 0
  }
}
