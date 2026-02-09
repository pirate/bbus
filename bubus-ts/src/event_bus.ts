import { BaseEvent } from './base_event.js'
import { EventResult } from './event_result.js'
import { captureAsyncContext, runWithAsyncContext } from './async_context.js'
import { AsyncSemaphore, type ConcurrencyMode, HandlerLock, LockManager, runWithSemaphore, withResolvers } from './lock_manager.js'
import {
  EventHandlerAbortedError,
  EventHandlerCancelledError,
  EventHandlerTimeoutError,
  EventHandlerResultSchemaError,
  EventHandler,
} from './event_handler.js'
import { logTree } from './logging.js'

import type { EventClass, EventHandlerFunction, EventKey, FindOptions, UntypedEventHandlerFunction } from './types.js'

type FindWaiter = {
  // similar to a handler, except its for .find() calls
  // needs to be different because it's resolved on dispatch not event processing time
  // also is ephemeral, gets unregistered the moment it resolves and
  // doesnt show up in event processing tree, doesn't block runloop, etc.
  event_key: EventKey
  matches: (event: BaseEvent) => boolean
  resolve: (event: BaseEvent) => void
  timeout_id?: ReturnType<typeof setTimeout>
}

type EventBusOptions = {
  max_history_size?: number | null
  event_concurrency?: ConcurrencyMode
  event_handler_concurrency?: ConcurrencyMode
  event_timeout?: number | null // default handler timeout in seconds, applied when event.event_timeout is undefined
  event_handler_slow_timeout?: number | null // threshold before a warning is logged about slow handler execution
  event_slow_timeout?: number | null // threshold before a warning is logged about slow event processing
}

// Global registry of all EventBus instances to allow for cross-bus coordination when global-serial concurrency mode is used
class GlobalEventBusInstanceRegistry {
  private _refs = new Set<WeakRef<EventBus>>()
  private _lookup = new WeakMap<EventBus, WeakRef<EventBus>>()
  private _gc =
    typeof FinalizationRegistry !== 'undefined'
      ? new FinalizationRegistry<WeakRef<EventBus>>((ref) => {
          this._refs.delete(ref)
        })
      : null

  add(bus: EventBus): void {
    const ref = new WeakRef(bus)
    this._refs.add(ref)
    this._lookup.set(bus, ref)
    this._gc?.register(bus, ref, bus)
  }

  delete(bus: EventBus): void {
    const ref = this._lookup.get(bus)
    if (!ref) return
    this._refs.delete(ref)
    this._lookup.delete(bus)
    this._gc?.unregister(bus)
  }

  has(bus: EventBus): boolean {
    return this._lookup.get(bus)?.deref() !== undefined
  }

  get size(): number {
    let n = 0
    for (const ref of this._refs) ref.deref() ? n++ : this._refs.delete(ref)
    return n
  }

  *[Symbol.iterator](): Iterator<EventBus> {
    for (const ref of this._refs) {
      const bus = ref.deref()
      if (bus) yield bus
      else this._refs.delete(ref)
    }
  }

  // find an event by its id across all buses
  findEventById(event_id: string): BaseEvent | null {
    for (const bus of this) {
      const event = bus.event_history.get(event_id)
      if (event) {
        return event
      }
    }
    return null
  }
}

export class EventBus {
  static _all_instances = new GlobalEventBusInstanceRegistry()

  name: string // name of the event bus, recommended to include the word "Bus" in the name for clarity in logs

  // configuration options
  max_history_size: number | null // max number of completed events kept in log, set to null for unlimited history
  event_concurrency_default: ConcurrencyMode
  event_handler_concurrency_default: ConcurrencyMode
  event_timeout_default: number | null
  event_handler_slow_timeout: number | null
  event_slow_timeout: number | null

  // public runtime state
  handlers: Map<string, EventHandler> // map of handler uuidv5 ids to EventHandler objects
  event_history: Map<string, BaseEvent> // map of event uuidv7 ids to processed BaseEvent objects

  // internal runtime state
  pending_event_queue: BaseEvent[] // queue of events that have been dispatched to the bus but not yet processed
  in_flight_event_ids: Set<string> // set of event ids that are currently being processed by the bus
  runloop_running: boolean
  locks: LockManager
  find_waiters: Set<FindWaiter> // set of FindWaiter objects that are waiting for a matching future event

  constructor(name: string = 'EventBus', options: EventBusOptions = {}) {
    this.name = name

    // set configuration options
    this.max_history_size = options.max_history_size === undefined ? 100 : options.max_history_size
    this.event_concurrency_default = options.event_concurrency ?? 'bus-serial'
    this.event_handler_concurrency_default = options.event_handler_concurrency ?? 'bus-serial'
    this.event_timeout_default = options.event_timeout === undefined ? 60 : options.event_timeout
    this.event_handler_slow_timeout = options.event_handler_slow_timeout === undefined ? 30 : options.event_handler_slow_timeout
    this.event_slow_timeout = options.event_slow_timeout === undefined ? 300 : options.event_slow_timeout

    // initialize runtime state
    this.handlers = new Map()
    this.event_history = new Map()
    this.pending_event_queue = []
    this.in_flight_event_ids = new Set()
    this.runloop_running = false
    this.locks = new LockManager(this)
    this.find_waiters = new Set()

    EventBus._all_instances.add(this)

    this.dispatch = this.dispatch.bind(this)
    this.emit = this.emit.bind(this)
  }

  toString(): string {
    if (this.name.toLowerCase().includes('bus')) {
      return `${this.name}`
    }
    return `EventBus(${this.name})` // for clarity that its a bus if bus is not in the name
  }

  // destroy the event bus and all its state to allow for garbage collection
  destroy(): void {
    EventBus._all_instances.delete(this)
    this.handlers.clear()
    for (const event of this.event_history.values()) {
      event._gc()
    }
    this.event_history.clear()
    this.pending_event_queue.length = 0
    this.in_flight_event_ids.clear()
    this.find_waiters.clear()
    this.locks.clear()
  }

  on<T extends BaseEvent>(
    event_key: EventClass<T>,
    handler: EventHandlerFunction<T>,
    options?: { event_handler_concurrency?: ConcurrencyMode; handler_timeout?: number | null }
  ): EventHandler
  on<T extends BaseEvent>(
    event_key: string | '*',
    handler: UntypedEventHandlerFunction<T>,
    options?: { event_handler_concurrency?: ConcurrencyMode; handler_timeout?: number | null }
  ): EventHandler
  on(
    event_key: EventKey | '*',
    handler: EventHandlerFunction | UntypedEventHandlerFunction,
    options: { event_handler_concurrency?: ConcurrencyMode; handler_timeout?: number | null } = {}
  ): EventHandler {
    const normalized_key = this.normalizeEventKey(event_key) // get string event_type or '*'
    const handler_name = handler.name || 'anonymous' // get handler function name or 'anonymous' if the handler is an anonymous/arrow function
    const { isostring: handler_registered_at, ts: handler_registered_ts } = BaseEvent.nextTimestamp()
    const handler_timeout = options.handler_timeout ?? this.event_timeout_default
    const handler_entry = new EventHandler({
      handler: handler as EventHandlerFunction,
      handler_name,
      handler_timeout,
      event_handler_concurrency: options.event_handler_concurrency,
      handler_registered_at,
      handler_registered_ts,
      event_key: normalized_key,
      eventbus_name: this.name,
    })

    this.handlers.set(handler_entry.id, handler_entry)
    return handler_entry
  }

  off<T extends BaseEvent>(event_key: EventKey<T> | '*', handler?: EventHandlerFunction<T> | string | EventHandler): void {
    const normalized_key = this.normalizeEventKey(event_key)
    if (typeof handler === 'object' && handler instanceof EventHandler && handler.id !== undefined) {
      handler = handler.id
    }
    const match_by_id = typeof handler === 'string'
    for (const entry of this.handlers.values()) {
      if (entry.event_key !== normalized_key) {
        continue
      }
      const handler_id = entry.id
      if (handler === undefined || (match_by_id ? handler_id === handler : entry.handler === (handler as EventHandlerFunction))) {
        this.handlers.delete(handler_id)
      }
    }
  }

  dispatch<T extends BaseEvent>(event: T, _event_key?: EventKey<T>): T {
    const original_event = event._event_original ?? event // if event is a bus-scoped proxy already, get the original underlying event object
    if (!original_event.bus) {
      // if we are the first bus to dispatch this event, set the bus property on the original event object
      original_event.bus = this
    }
    if (!Array.isArray(original_event.event_path)) {
      original_event.event_path = []
    }
    if (original_event._event_dispatch_context === undefined) {
      // when used in fastify/nextjs/other contexts with tracing based on AsyncLocalStorage in node
      // we want to capture the context at the dispatch site and use it when running handlers
      // because events may be handled async in a separate context than the dispatch site
      original_event._event_dispatch_context = captureAsyncContext()
    }
    if (original_event.event_timeout === null) {
      original_event.event_timeout = this.event_timeout_default
    }

    if (original_event.event_path.includes(this.name) || this.hasProcessedEvent(original_event)) {
      return this.getEventProxyScopedToThisBus(original_event) as T
    }

    if (!original_event.event_path.includes(this.name)) {
      original_event.event_path.push(this.name)
    }

    if (original_event.event_parent_id && original_event.event_emitted_by_handler_id) {
      const parent_result = original_event.event_parent?.event_results.get(original_event.event_emitted_by_handler_id)
      if (parent_result) {
        parent_result.linkEmittedChildEvent(original_event)
      }
    }

    this.event_history.set(original_event.event_id, original_event)
    this.trimHistory()

    original_event.event_pending_bus_count += 1
    this.pending_event_queue.push(original_event)
    this.startRunloop()

    return this.getEventProxyScopedToThisBus(original_event) as T
  }

  // alias for dispatch
  emit<T extends BaseEvent>(event: T, event_key?: EventKey<T>): T {
    return this.dispatch(event, event_key)
  }

  // find a recent event or wait for a future event that matches some criteria
  find<T extends BaseEvent>(event_key: EventKey<T>, options?: FindOptions): Promise<T | null>
  find<T extends BaseEvent>(event_key: EventKey<T>, where: (event: T) => boolean, options?: FindOptions): Promise<T | null>
  async find<T extends BaseEvent>(
    event_key: EventKey<T>,
    where_or_options: ((event: T) => boolean) | FindOptions = {},
    maybe_options: FindOptions = {}
  ): Promise<T | null> {
    const where = typeof where_or_options === 'function' ? where_or_options : () => true
    const options = typeof where_or_options === 'function' ? maybe_options : where_or_options

    const past = options.past ?? true
    const future = options.future ?? true
    const child_of = options.child_of ?? null

    if (past === false && future === false) {
      return null
    }

    const matches = (event: BaseEvent): boolean => {
      if (!this.eventMatchesKey(event, event_key)) {
        return false
      }
      if (!where(event as T)) {
        return false
      }
      if (child_of && !this.eventIsChildOf(event, child_of)) {
        return false
      }
      return true
    }

    // find an event in the history that matches the criteria
    if (past !== false || future !== false) {
      const now_ms = performance.timeOrigin + performance.now()
      const cutoff_ms = past === true ? null : now_ms - Math.max(0, Number(past)) * 1000

      const history_values = Array.from(this.event_history.values())
      for (let i = history_values.length - 1; i >= 0; i -= 1) {
        const event = history_values[i]
        if (!matches(event)) {
          continue
        }
        if (event.event_status === 'completed') {
          if (past === false) {
            continue
          }
          if (cutoff_ms !== null && Date.parse(event.event_created_at) < cutoff_ms) {
            continue
          }
          return this.getEventProxyScopedToThisBus(event) as T
        }
        if (future !== false) {
          return this.getEventProxyScopedToThisBus(event) as T
        }
      }
    }

    // if we are only looking for past events, return null when no match is found
    if (future === false) {
      return null
    }

    // if we are looking for future events, return a promise that resolves when a match is found
    return new Promise<T | null>((resolve) => {
      const waiter: FindWaiter = {
        event_key,
        matches,
        resolve: (event) => resolve(this.getEventProxyScopedToThisBus(event) as T),
      }

      if (future !== true) {
        const timeout_ms = Math.max(0, Number(future)) * 1000
        waiter.timeout_id = setTimeout(() => {
          this.find_waiters.delete(waiter)
          resolve(null)
        }, timeout_ms)
      }

      this.find_waiters.add(waiter)
    })
  }

  // Called when a handler does `await child.done()` — processes the child event
  // immediately ("queue-jump") instead of waiting for the runloop to pick it up.
  //
  // Yield-and-reacquire: if the calling handler holds a handler concurrency semaphore,
  // we temporarily release it so child handlers on the same bus can acquire it
  // (preventing deadlock for bus-serial/global-serial modes). We re-acquire after
  // the child completes so the parent handler can continue with the semaphore held.
  async processEventImmediately<T extends BaseEvent>(event: T, handler_result?: EventResult): Promise<T> {
    const original_event = event._event_original ?? event
    // Find the parent handler's result: prefer the proxy-provided one (only if
    // the handler is still running), then this bus's stack, then walk up the
    // parent event tree (cross-bus case). If none found, we're not inside a
    // handler and should fall back to waitForCompletion.
    const proxy_result = handler_result?.status === 'started' ? handler_result : undefined
    const currently_active_event_result =
      proxy_result ?? this.locks.getActiveHandlerResult() ?? this.getParentEventResultAcrossAllBusses(original_event) ?? undefined
    if (!currently_active_event_result) {
      // Not inside any handler scope — avoid queue-jump, but if this event is
      // next in line we can process it immediately without waiting on the runloop.
      const queue_index = this.pending_event_queue.indexOf(original_event)
      const can_process_now =
        queue_index === 0 &&
        !this.locks.isPaused() &&
        !this.in_flight_event_ids.has(original_event.event_id) &&
        !this.hasProcessedEvent(original_event)
      if (can_process_now) {
        this.pending_event_queue.shift()
        this.in_flight_event_ids.add(original_event.event_id)
        await this.scheduleEventProcessing(original_event)
        if (original_event.event_status !== 'completed') {
          await original_event.waitForCompletion()
        }
        return event
      }
      await original_event.waitForCompletion()
      return event
    }

    // ensure a pause request is set so the runloop pauses and (will resume when the event is completed)
    this.locks.requestRunloopPauseForQueueJumpEvent(currently_active_event_result)
    if (original_event.event_status === 'completed') {
      return event
    }

    const run_queue_jump = currently_active_event_result._lock
      ? (fn: () => Promise<T>) => currently_active_event_result._lock!.runQueueJump(fn)
      : (fn: () => Promise<T>) => fn()
    return await run_queue_jump(async () => {
      if (original_event.event_status === 'started') {
        await this.runImmediatelyAcrossBuses(original_event)
        return event
      }

      const index = this.pending_event_queue.indexOf(original_event)
      if (index >= 0) {
        this.pending_event_queue.splice(index, 1)
      }

      await this.runImmediatelyAcrossBuses(original_event)
      return event
    })
  }

  async waitUntilIdle(): Promise<void> {
    await this.locks.waitForIdle()
  }

  // Weak idle check: only checks if handlers are idle, doesnt check that the queue is empty
  isIdle(): boolean {
    for (const event of this.event_history.values()) {
      for (const result of event.event_results.values()) {
        if (result.eventbus_name !== this.name) {
          continue
        }
        if (result.status === 'pending' || result.status === 'started') {
          return false
        }
      }
    }
    return true // no handlers are pending or started
  }

  // Stronger idle check: no queued work, no in-flight processing, runloop not
  // active, and no handlers pending/running for this bus.
  isIdleAndQueueEmpty(): boolean {
    return this.pending_event_queue.length === 0 && this.in_flight_event_ids.size === 0 && this.isIdle() && !this.runloop_running
  }

  eventIsChildOf(event: BaseEvent, ancestor: BaseEvent): boolean {
    if (event.event_id === ancestor.event_id) {
      return false
    }

    let current_parent_id = event.event_parent_id
    while (current_parent_id) {
      if (current_parent_id === ancestor.event_id) {
        return true
      }
      const parent = this.event_history.get(current_parent_id)
      if (!parent) {
        return false
      }
      current_parent_id = parent.event_parent_id
    }
    return false
  }

  eventIsParentOf(parent_event: BaseEvent, child_event: BaseEvent): boolean {
    return this.eventIsChildOf(child_event, parent_event)
  }

  // return a full detailed tree diagram of all events and results on this bus
  logTree(): string {
    return logTree(this)
  }

  // Resolve an event id from this bus first, then across all known buses.
  findEventById(event_id: string): BaseEvent | null {
    return this.event_history.get(event_id) ?? EventBus._all_instances.findEventById(event_id)
  }

  // Walk up the parent event chain to find an in-flight ancestor handler result.
  // Returns the result if found, null otherwise. Used by processEventImmediately to detect
  // cross-bus queue-jump scenarios where the calling handler is on a different bus.
  getParentEventResultAcrossAllBusses(event: BaseEvent): EventResult | null {
    const original = event._event_original ?? event
    let current_parent_id = original.event_parent_id
    let current_handler_id = original.event_emitted_by_handler_id
    while (current_handler_id && current_parent_id) {
      const parent = EventBus._all_instances.findEventById(current_parent_id)
      if (!parent) break
      const handler_result = parent.event_results.get(current_handler_id)
      if (handler_result && handler_result.status === 'started') return handler_result
      current_parent_id = parent.event_parent_id
      current_handler_id = parent.event_emitted_by_handler_id
    }
    return null
  }

  // Processes a queue-jumped event across all buses that have it dispatched.
  // Called from processEventImmediately after the parent handler's semaphore has been yielded.
  //
  // Event semaphore bypass: the initiating bus (this) always bypasses its event semaphore
  // since we're inside a handler that already holds it. Other buses only bypass if
  // they resolve to the same semaphore instance (i.e. global-serial mode where all
  // buses share LockManager.global_event_semaphore).
  //
  // Handler semaphores are NOT bypassed — child handlers must acquire the handler
  // semaphore normally. This works because processEventImmediately already released the
  // parent's handler semaphore via yield-and-reacquire.
  private async runImmediatelyAcrossBuses(event: BaseEvent): Promise<void> {
    const buses = this.getBusesForImmediateRun(event)
    if (buses.length === 0) {
      await event.waitForCompletion()
      return
    }

    const pause_releases = buses.map((bus) => bus.locks.requestPause())

    // Determine which event semaphore the initiating bus resolves to, so we can
    // detect when other buses share the same instance (global-serial).
    const initiating_event_semaphore = this.locks.getSemaphoreForEvent(event)

    try {
      for (const bus of buses) {
        const index = bus.pending_event_queue.indexOf(event)
        if (index >= 0) {
          bus.pending_event_queue.splice(index, 1)
        }
        if (bus.hasProcessedEvent(event)) {
          continue
        }
        if (bus.in_flight_event_ids.has(event.event_id)) {
          continue
        }
        bus.in_flight_event_ids.add(event.event_id)

        // Bypass event semaphore on the initiating bus (we're already inside a handler
        // that acquired it). For other buses, only bypass if they resolve to the same
        // semaphore instance (global-serial shares one semaphore across all buses).
        const bus_event_semaphore = bus.locks.getSemaphoreForEvent(event)
        const should_bypass_event_semaphore =
          bus === this || (initiating_event_semaphore !== null && bus_event_semaphore === initiating_event_semaphore)

        await bus.scheduleEventProcessing(event, {
          bypass_event_semaphores: should_bypass_event_semaphore,
        })
      }

      if (event.event_status !== 'completed') {
        await event.waitForCompletion()
      }
    } finally {
      for (const release of pause_releases) {
        release()
      }
    }
  }

  // Collects buses that currently "own" this event so queue-jump can run it immediately
  // across all forwarded buses. Called by runImmediatelyAcrossBuses(), which itself is
  // invoked from processEventImmediately (via BaseEvent.done()) when an event is awaited inside
  // a handler. Uses event.event_path ordering to pick candidate buses and filters out
  // buses that haven't seen the event or already processed it.
  private getBusesForImmediateRun(event: BaseEvent): EventBus[] {
    const ordered: EventBus[] = []
    const seen = new Set<EventBus>()

    const event_path = Array.isArray(event.event_path) ? event.event_path : []
    for (const name of event_path) {
      for (const bus of EventBus._all_instances) {
        if (bus.name !== name) {
          continue
        }
        if (!bus.event_history.has(event.event_id)) {
          continue
        }
        if (bus.hasProcessedEvent(event)) {
          continue
        }
        if (!seen.has(bus)) {
          ordered.push(bus)
          seen.add(bus)
        }
      }
    }

    if (!seen.has(this) && this.event_history.has(event.event_id)) {
      ordered.push(this)
    }

    return ordered
  }

  private startRunloop(): void {
    if (this.runloop_running) {
      return
    }
    this.runloop_running = true
    queueMicrotask(() => {
      void this.runloop()
    })
  }

  // schedule the processing of an event on the event bus by its normal runloop
  // but set up the bus to process the given event immediately if it is a queue-jump event
  private async scheduleEventProcessing(
    event: BaseEvent,
    options: {
      bypass_event_semaphores?: boolean
      pre_acquired_semaphore?: AsyncSemaphore | null
    } = {}
  ): Promise<void> {
    try {
      const semaphore = options.bypass_event_semaphores ? null : this.locks.getSemaphoreForEvent(event)
      const pre_acquired_semaphore = options.pre_acquired_semaphore ?? null
      if (pre_acquired_semaphore) {
        await this.processEvent(event)
      } else {
        await runWithSemaphore(semaphore, async () => {
          await this.processEvent(event)
        })
      }
    } finally {
      if (options.pre_acquired_semaphore) {
        options.pre_acquired_semaphore.release()
      }
      this.in_flight_event_ids.delete(event.event_id)
      this.locks.notifyIdleListeners()
    }
  }

  private async runloop(): Promise<void> {
    for (;;) {
      while (this.pending_event_queue.length > 0) {
        await Promise.resolve()
        if (this.locks.isPaused()) {
          await this.locks.waitUntilRunloopResumed()
          continue
        }
        const next_event = this.pending_event_queue[0]
        if (!next_event) {
          continue
        }
        const original_event = next_event._event_original ?? next_event
        if (this.hasProcessedEvent(original_event)) {
          this.pending_event_queue.shift()
          continue
        }
        let pre_acquired_semaphore: AsyncSemaphore | null = null
        const event_semaphore = this.locks.getSemaphoreForEvent(original_event)
        if (event_semaphore) {
          await event_semaphore.acquire()
          pre_acquired_semaphore = event_semaphore
        }
        this.pending_event_queue.shift()
        if (this.in_flight_event_ids.has(original_event.event_id)) {
          if (pre_acquired_semaphore) {
            pre_acquired_semaphore.release()
          }
          continue
        }
        this.in_flight_event_ids.add(original_event.event_id)
        void this.scheduleEventProcessing(original_event, {
          bypass_event_semaphores: true,
          pre_acquired_semaphore,
        })
        await Promise.resolve()
      }
      this.runloop_running = false
      if (this.pending_event_queue.length > 0) {
        this.startRunloop()
        return
      }
      this.locks.notifyIdleListeners()
      return
    }
  }

  private async processEvent(event: BaseEvent): Promise<void> {
    if (this.hasProcessedEvent(event)) {
      return
    }
    event.markStarted()
    this.notifyFindListeners(event)

    const slow_event_warning_timer = this.createSlowEventWarningTimer(event)

    try {
      const handler_entries = this.createPendingHandlerResults(event)

      const handler_promises = handler_entries.map((entry) => this.runEventHandler(event, entry.handler, entry.result))

      if (event.event_handler_completion === 'first') {
        // first() mode: cancel remaining handlers once any handler returns a non-undefined result
        let first_found = false
        const monitored = handler_entries.map((entry, i) =>
          handler_promises[i].then(() => {
            if (!first_found && entry.result.status === 'completed' && entry.result.result !== undefined) {
              first_found = true
              this.cancelEventHandlersForFirstMode(event, entry.result)
            }
          })
        )
        await Promise.all(monitored)
      } else {
        await Promise.all(handler_promises)
      }

      event.event_pending_bus_count = Math.max(0, event.event_pending_bus_count - 1)
      event.markCompleted(false)
      if (event.event_status === 'completed') {
        this.notifyEventParentsOfCompletion(event)
      }
    } finally {
      if (slow_event_warning_timer) {
        clearTimeout(slow_event_warning_timer)
      }
    }
  }

  // Manually manages the handler concurrency semaphore instead of using runWithSemaphore,
  // because processEventImmediately may temporarily yield it during queue-jumping.
  async runEventHandler(event: BaseEvent, handler: EventHandler, result: EventResult): Promise<void> {
    if (result.status === 'error' && result.error instanceof EventHandlerCancelledError) {
      return
    }

    const handler_event = this.getEventProxyScopedToThisBus(event, result)
    const semaphore = this.locks.getSemaphoreForHandler(event, handler)

    if (semaphore) {
      await semaphore.acquire()
    }

    // if the result is already in an error or completed state, release the semaphore immediately and return
    // prevent double-processing of the event by the same handler
    if (result.status === 'error' || result.status === 'completed') {
      if (semaphore) semaphore.release()
      return
    }

    // exit the handler lock if it is already held
    if (result._lock) result._lock.exitHandlerRun()
    // create a new handler lock to track ownership of the semaphore during handler execution
    result._lock = new HandlerLock(semaphore)
    this.locks.enterActiveHandlerContext(result)

    // resolve the effective timeout by combining the event timeout and the handler timeout
    const effective_timeout = this.resolveEffectiveTimeout(event.event_timeout, result.handler.handler_timeout)
    const slow_handler_warning_timer = this.createSlowHandlerWarningTimer(event, result, effective_timeout)

    try {
      const abort_signal = result.markStarted()
      const handler_result = await Promise.race([this.runHandlerWithTimeout(event, handler, handler_event, result), abort_signal])
      if (event.event_result_schema && handler_result !== undefined) {
        // if there is a result schema to enforce, parse the handler's return value and mark the event as completed or errored if it doesn't match the schema
        const parsed = event.event_result_schema.safeParse(handler_result)
        if (parsed.success) {
          result.markCompleted(parsed.data)
        } else {
          // if the handler's return value doesn't match the schema, mark the event as errored with an error message
          const error = new EventHandlerResultSchemaError(
            `${this.toString()}.on(${event.toString()}, ${result.handler.toString()}) return value ${JSON.stringify(handler_result).slice(0, 20)}... did not match event_result_schema ${event.event_result_type}: ${parsed.error.message}`,
            { event_result: result, cause: parsed.error, raw_value: handler_result }
          )
          result.markError(error)
        }
      } else {
        // if there is no result schema to enforce, just mark the event as completed with the raw handler's return value
        result.markCompleted(handler_result)
      }
    } catch (error) {
      // if the handler timed out, cancel all pending descendants and mark the event as errored
      if (error instanceof EventHandlerTimeoutError) {
        result.markError(error)
        this.cancelPendingDescendants(event, error)
      } else {
        result.markError(error)
      }
    } finally {
      result._abort = null
      result._lock?.exitHandlerRun()
      this.locks.exitActiveHandlerContext(result)
      this.locks.releaseRunloopPauseForQueueJumpEvent(result)
      if (slow_handler_warning_timer) {
        clearTimeout(slow_handler_warning_timer)
      }
    }
  }

  // run a handler with a timeout, returning a promise that resolves or rejects with the handler's result or an error if the timeout is exceeded
  private async runHandlerWithTimeout(
    event: BaseEvent,
    handler: EventHandler,
    handler_event: BaseEvent = event,
    result: EventResult
  ): Promise<unknown> {
    // resolve the effective timeout by combining the event timeout and the handler timeout
    const effective_timeout = this.resolveEffectiveTimeout(event.event_timeout, result.handler.handler_timeout)
    const run_handler = () =>
      Promise.resolve().then(() => runWithAsyncContext(event._event_dispatch_context ?? null, () => handler.handler(handler_event)))

    if (effective_timeout === null) {
      // if there is no timeout to enforce, just run the handler directly and return the promise
      return run_handler()
    }

    const timeout_seconds = effective_timeout
    const timeout_ms = timeout_seconds * 1000

    const { promise, resolve, reject } = withResolvers<unknown>()
    let settled = false

    // finalize the promise by clearing the timeout and calling the resolve or reject function
    const finalize = (fn: (value?: unknown) => void) => {
      return (value?: unknown) => {
        if (settled) {
          return
        }
        settled = true
        clearTimeout(timer)
        fn(value)
      }
    }

    // set a timeout to reject the promise if the handler takes too long
    const timer = setTimeout(() => {
      finalize(reject)(
        new EventHandlerTimeoutError(
          `${this.toString()}.on(${event.toString()}, ${result.handler.toString()}) timed out after ${timeout_seconds}s`,
          {
            event_result: result,
            timeout_seconds,
          }
        )
      )
    }, timeout_ms)

    run_handler().then(finalize(resolve)).catch(finalize(reject))

    return promise
  }

  private createSlowEventWarningTimer(event: BaseEvent): ReturnType<typeof setTimeout> | null {
    const event_warn_ms = this.event_slow_timeout === null ? null : this.event_slow_timeout * 1000
    if (event_warn_ms === null) {
      return null
    }
    return setTimeout(() => {
      if (event.event_status === 'completed') {
        return
      }
      const running_handler_count = [...event.event_results.values()].filter((result) => result.status === 'started').length
      const started_ts = event.event_started_ts ?? event.event_created_ts ?? performance.now()
      const elapsed_ms = Math.max(0, performance.now() - started_ts)
      const elapsed_seconds = (elapsed_ms / 1000).toFixed(2)
      console.warn(
        `[bubus] Slow event processing: ${this.name}.on(${event.event_type}#${event.event_id.slice(-4)}, ${running_handler_count} handlers) still running after ${elapsed_seconds}s`
      )
    }, event_warn_ms)
  }

  private createSlowHandlerWarningTimer(
    event: BaseEvent,
    result: EventResult,
    effective_timeout: number | null
  ): ReturnType<typeof setTimeout> | null {
    const warn_ms = this.event_handler_slow_timeout === null ? null : this.event_handler_slow_timeout * 1000
    const should_warn = warn_ms !== null && (effective_timeout === null || effective_timeout * 1000 > warn_ms)
    if (!should_warn || warn_ms === null) {
      return null
    }
    const started_at_ms = performance.now()
    return setTimeout(() => {
      if (result.status !== 'started') {
        return
      }
      const elapsed_ms = performance.now() - started_at_ms
      const elapsed_seconds = (elapsed_ms / 1000).toFixed(1)
      console.warn(
        `[bubus] Slow event handler: ${this.name}.on(${event.toString()}, ${result.handler.toString()}) still running after ${elapsed_seconds}s`
      )
    }, warn_ms)
  }

  private resolveEffectiveTimeout(event_timeout: number | null, handler_timeout: number | null): number | null {
    if (handler_timeout === null && event_timeout === null) {
      return null
    }
    if (handler_timeout === null) {
      return event_timeout
    }
    if (event_timeout === null) {
      return handler_timeout
    }
    return Math.min(handler_timeout, event_timeout)
  }

  // check if an event has been processed (and completed) by this bus
  hasProcessedEvent(event: BaseEvent): boolean {
    const results = Array.from(event.event_results.values()).filter((result) => result.eventbus_name === this.name)
    if (results.length === 0) {
      return false
    }
    return results.every((result) => result.status === 'completed' || result.status === 'error')
  }

  private notifyEventParentsOfCompletion(event: BaseEvent): void {
    const visited = new Set<string>()
    let parent_id = event.event_parent_id
    while (parent_id && !visited.has(parent_id)) {
      visited.add(parent_id)
      const parent = EventBus._all_instances.findEventById(parent_id)
      if (!parent) {
        break
      }
      parent.markCompleted(false)
      if (parent.event_status !== 'completed') {
        break
      }
      parent_id = parent.event_parent_id
    }
  }

  // get a proxy wrapper around an Event that will automatically link emitted child events to this bus and handler
  // proxy is what gets passed into the handler, if handler does event.bus.emit(...) to dispatch child events,
  // the proxy auto-sets event.parent_event_id and event.event_emitted_by_handler_id
  getEventProxyScopedToThisBus<T extends BaseEvent>(event: T, handler_result?: EventResult): T {
    const original_event = event._event_original ?? event
    const bus = this
    const parent_event_id = original_event.event_id
    const bus_proxy = new Proxy(bus, {
      get(target, prop, receiver) {
        if (prop === 'processEventImmediately') {
          return (child_event: BaseEvent) => {
            const runner = Reflect.get(target, prop, receiver) as (event: BaseEvent, handler_result?: EventResult) => Promise<BaseEvent>
            return runner.call(target, child_event, handler_result)
          }
        }
        if (prop === 'dispatch' || prop === 'emit') {
          return (child_event: BaseEvent, event_key?: EventKey) => {
            const original_child = child_event._event_original ?? child_event
            if (handler_result) {
              handler_result.linkEmittedChildEvent(original_child)
            } else if (!original_child.event_parent_id) {
              // fallback for non-handler scoped dispatch
              original_child.event_parent_id = parent_event_id
            }
            const dispatcher = Reflect.get(target, prop, receiver) as (event: BaseEvent, event_key?: EventKey) => BaseEvent
            const dispatched = dispatcher.call(target, original_child, event_key)
            return target.getEventProxyScopedToThisBus(dispatched, handler_result)
          }
        }
        return Reflect.get(target, prop, receiver)
      },
    })
    const scoped = new Proxy(original_event, {
      get(target, prop, receiver) {
        if (prop === 'bus') {
          return bus_proxy
        }
        if (prop === '_event_original') {
          return target
        }
        return Reflect.get(target, prop, receiver)
      },
      set(target, prop, value) {
        if (prop === 'bus') {
          return true
        }
        return Reflect.set(target, prop, value, target)
      },
      has(target, prop) {
        if (prop === 'bus') {
          return true
        }
        if (prop === '_event_original') {
          return true
        }
        return Reflect.has(target, prop)
      },
    })

    return scoped as T
  }

  // force-abort processing of all pending descendants of an event regardless of whether they have already started
  cancelPendingDescendants(event: BaseEvent, reason: unknown): void {
    const cancellation_cause = this.normalizeCancellationCause(reason)
    const visited = new Set<string>()
    const cancelChildEvent = (child: BaseEvent): void => {
      const original_child = child._event_original ?? child
      if (visited.has(original_child.event_id)) {
        return
      }
      visited.add(original_child.event_id)

      // Depth-first: cancel grandchildren before parent so
      // eventAreAllChildrenComplete() returns true when we get back up.
      for (const grandchild of original_child.event_children) {
        cancelChildEvent(grandchild)
      }

      const path = Array.isArray(original_child.event_path) ? original_child.event_path : []
      const buses_to_cancel = new Set<string>(path)
      for (const bus of EventBus._all_instances) {
        if (!buses_to_cancel.has(bus.name)) {
          continue
        }
        bus.cancelEvent(original_child, cancellation_cause)
      }

      // Force-complete the child event. In JS we can't stop running async
      // handlers, but markCompleted() resolves the done() promise so callers
      // aren't blocked waiting for background work to finish. The background
      // handler's eventual markCompleted/markError is a no-op (terminal guard).
      if (original_child.event_status !== 'completed') {
        original_child.markCompleted()
      }
    }

    for (const child of event.event_children) {
      cancelChildEvent(child)
    }
  }

  // Cancel all handler results for an event except the winner, used by first() mode.
  // Cancels pending handlers immediately, aborts started handlers via signalAbort(),
  // and cancels any child events emitted by the losing handlers.
  private cancelEventHandlersForFirstMode(event: BaseEvent, winner: EventResult): void {
    const cause = new Error('first() resolved: another handler returned a result first')

    for (const result of event.event_results.values()) {
      if (result === winner) continue
      if (result.eventbus_name !== this.name) continue

      if (result.status === 'pending') {
        result.markError(
          new EventHandlerCancelledError(`Cancelled: first() resolved`, {
            event_result: result,
            cause,
          })
        )
      } else if (result.status === 'started') {
        // Cancel child events emitted by this handler before aborting it
        for (const child of result.event_children) {
          const original_child = child._event_original ?? child
          this.cancelPendingDescendants(original_child, cause)
          const child_path = Array.isArray(original_child.event_path) ? original_child.event_path : []
          for (const bus of EventBus._all_instances) {
            if (child_path.includes(bus.name)) {
              bus.cancelEvent(original_child, cause)
            }
          }
          if (original_child.event_status !== 'completed') {
            original_child.markCompleted()
          }
        }

        // Abort the handler itself
        result._lock?.exitHandlerRun()
        const aborted_error = new EventHandlerAbortedError(`Aborted: first() resolved`, {
          event_result: result,
          cause,
        })
        result.markError(aborted_error)
        result.signalAbort(aborted_error)
      }
    }
  }

  private normalizeCancellationCause(reason: unknown): Error {
    if (reason instanceof EventHandlerCancelledError || reason instanceof EventHandlerAbortedError) {
      return reason.cause instanceof Error ? reason.cause : reason
    }
    if (reason instanceof EventHandlerTimeoutError) {
      return reason
    }
    return reason instanceof Error ? reason : new Error(String(reason))
  }

  // force-abort processing of an event regardless of whether it is pending or has already started
  private cancelEvent(event: BaseEvent, cause: Error): void {
    const original_event = event._event_original ?? event
    const handler_entries = this.createPendingHandlerResults(original_event)
    let updated = false
    for (const entry of handler_entries) {
      if (entry.result.status === 'pending') {
        const cancelled_error = new EventHandlerCancelledError(`Cancelled pending handler due to parent error: ${cause.message}`, {
          event_result: entry.result,
          cause,
        })
        entry.result.markError(cancelled_error)
        updated = true
      } else if (entry.result.status === 'started') {
        // Abort running handlers. In JS we can't actually stop a running async
        // function, but marking it as error means the event system treats it as
        // done. The background handler will finish silently (its markCompleted/
        // markError call is a no-op once in terminal state).
        //
        // Exit handler-run ownership immediately so any held lock is released.
        // If reacquire is currently pending, exit closes ownership and the
        // reacquire path auto-releases when it wakes.
        entry.result._lock?.exitHandlerRun()

        const aborted_error = new EventHandlerAbortedError(`Aborted running handler due to parent error: ${cause.message}`, {
          event_result: entry.result,
          cause,
        })
        entry.result.markError(aborted_error)
        entry.result.signalAbort(aborted_error)
        updated = true
      }
    }

    let removed = 0
    if (this.pending_event_queue.length > 0) {
      const before_len = this.pending_event_queue.length
      this.pending_event_queue = this.pending_event_queue.filter(
        (queued) => (queued._event_original ?? queued).event_id !== original_event.event_id
      )
      removed = before_len - this.pending_event_queue.length
    }

    if (removed > 0 && !this.in_flight_event_ids.has(original_event.event_id)) {
      original_event.event_pending_bus_count = Math.max(0, original_event.event_pending_bus_count - 1)
    }

    if (updated || removed > 0) {
      original_event.markCompleted(false)
      if (original_event.event_status === 'completed') {
        this.notifyEventParentsOfCompletion(original_event)
      }
    }
  }

  private notifyFindListeners(event: BaseEvent): void {
    for (const waiter of Array.from(this.find_waiters)) {
      if (!this.eventMatchesKey(event, waiter.event_key)) {
        continue
      }
      if (!waiter.matches(event)) {
        continue
      }
      if (waiter.timeout_id) {
        clearTimeout(waiter.timeout_id)
      }
      this.find_waiters.delete(waiter)
      waiter.resolve(event)
    }
  }

  private createPendingHandlerResults(event: BaseEvent): Array<{
    handler: EventHandler
    result: EventResult
  }> {
    const handlers = this.getHandlersForEvent(event)
    return handlers.map((entry) => {
      const handler_id = entry.id
      const existing = event.event_results.get(handler_id)
      const result = existing ?? new EventResult({ event, handler: entry })
      if (!existing) {
        event.event_results.set(handler_id, result)
      }
      return { handler: entry, result }
    })
  }

  getHandlersForEvent(event: BaseEvent): EventHandler[] {
    const handlers: EventHandler[] = []

    // Exact-match handlers first, then wildcard — preserves original ordering
    for (const entry of this.handlers.values()) {
      if (entry.event_key === event.event_type) {
        handlers.push(entry)
      }
    }
    for (const entry of this.handlers.values()) {
      if (entry.event_key === '*') {
        handlers.push(entry)
      }
    }

    return handlers
  }

  private eventMatchesKey(event: BaseEvent, event_key: EventKey): boolean {
    if (event_key === '*') {
      return true
    }
    const normalized = this.normalizeEventKey(event_key)
    if (normalized === '*') {
      return true
    }
    return event.event_type === normalized
  }

  private normalizeEventKey(event_key: EventKey | '*'): string | '*' {
    if (event_key === '*') {
      return '*'
    }
    if (typeof event_key === 'string') {
      return event_key
    }
    const event_type = (event_key as { event_type?: unknown }).event_type
    if (typeof event_type === 'string' && event_type.length > 0 && event_type !== 'BaseEvent') {
      return event_type
    }
    throw new Error(
      'bus.on(match_pattern, ...) must be a string event type, "*", or a BaseEvent class, got: ' + JSON.stringify(event_key).slice(0, 30)
    )
  }

  private trimHistory(): void {
    if (this.max_history_size === null) {
      return
    }
    if (this.event_history.size <= this.max_history_size) {
      return
    }

    let remaining_overage = this.event_history.size - this.max_history_size

    // First pass: remove completed events (oldest first, Map iterates in insertion order)
    for (const [event_id, event] of this.event_history) {
      if (remaining_overage <= 0) {
        break
      }
      if (event.event_status !== 'completed') {
        continue
      }
      this.event_history.delete(event_id)
      event._gc()
      remaining_overage -= 1
    }

    // Second pass: force-remove oldest events regardless of status
    let dropped_pending_events = 0
    if (remaining_overage > 0) {
      for (const [event_id, event] of this.event_history) {
        if (remaining_overage <= 0) {
          break
        }
        if (event.event_status !== 'completed') {
          dropped_pending_events += 1
        }
        this.event_history.delete(event_id)
        event._gc()
        remaining_overage -= 1
      }
      if (dropped_pending_events > 0) {
        console.error(
          `[bubus] ⚠️ Bus ${this.toString()} has exceeded its limit of ${this.max_history_size} inflight events and has started dropping oldest pending events! Increase bus.max_history_size or reduce the event volume.`
        )
      }
    }
  }
}
