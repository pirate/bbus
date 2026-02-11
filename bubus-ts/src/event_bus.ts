import { BaseEvent, type BaseEventJSON } from './base_event.js'
import { EventResult } from './event_result.js'
import { captureAsyncContext } from './async_context.js'
import {
  AsyncSemaphore,
  type EventConcurrencyMode,
  type EventHandlerConcurrencyMode,
  type EventHandlerCompletionMode,
  LockManager,
  runWithSemaphore,
} from './lock_manager.js'
import { EventHandler, FindWaiter, type EphemeralFindEventHandler, type EventHandlerJSON, type FindWaiterJSON } from './event_handler.js'
import { logTree } from './logging.js'
import { v7 as uuidv7 } from 'uuid'

import type { EventClass, EventHandlerFunction, EventKey, FindOptions, UntypedEventHandlerFunction } from './types.js'

type EventBusOptions = {
  id?: string
  max_history_size?: number | null

  // per-event options
  event_concurrency?: EventConcurrencyMode | null
  event_timeout?: number | null // default handler timeout in seconds, applied when event.event_timeout is undefined
  event_slow_timeout?: number | null // threshold before a warning is logged about slow event processing

  // per-event-handler options
  event_handler_concurrency?: EventHandlerConcurrencyMode | null
  event_handler_completion?: EventHandlerCompletionMode
  event_handler_slow_timeout?: number | null // threshold before a warning is logged about slow handler execution
  event_handler_detect_file_paths?: boolean // autodetect source code file and lineno where handlers are defined for better logs (slightly slower because Error().stack introspection to fine files is expensive)
}

export type EventBusJSON = {
  id: string
  name: string
  max_history_size: number | null
  event_concurrency: EventConcurrencyMode
  event_timeout: number | null
  event_slow_timeout: number | null
  event_handler_concurrency: EventHandlerConcurrencyMode
  event_handler_completion: EventHandlerCompletionMode
  event_handler_slow_timeout: number | null
  event_handler_detect_file_paths: boolean
  handlers: EventHandlerJSON[]
  handlers_by_key: Array<[string, string[]]>
  event_history: BaseEventJSON[]
  pending_event_queue: BaseEventJSON[]
  in_flight_event_ids: string[]
  runloop_running: boolean
  find_waiters: FindWaiterJSON[]
}

// Global registry of all EventBus instances to allow for cross-bus coordination when global-serial concurrency mode is used
class GlobalEventBusInstanceRegistry {
  private _event_buses = new Set<WeakRef<EventBus>>()

  add(bus: EventBus): void {
    const ref = new WeakRef(bus)
    this._event_buses.add(ref)
  }

  delete(bus: EventBus): void {
    for (const ref of this._event_buses) {
      const current = ref.deref()
      if (!current || current === bus) {
        this._event_buses.delete(ref)
      }
    }
  }

  has(bus: EventBus): boolean {
    for (const ref of this._event_buses) {
      const current = ref.deref()
      if (!current) {
        this._event_buses.delete(ref)
        continue
      }
      if (current === bus) {
        return true
      }
    }
    return false
  }

  get size(): number {
    let n = 0
    for (const ref of this._event_buses) ref.deref() ? n++ : this._event_buses.delete(ref)
    return n
  }

  *[Symbol.iterator](): Iterator<EventBus> {
    for (const ref of this._event_buses) {
      const bus = ref.deref()
      if (bus) yield bus
      else this._event_buses.delete(ref)
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
  get _all_instances(): GlobalEventBusInstanceRegistry {
    return EventBus._all_instances
  }

  id: string // unique uuidv7 identifier for the event bus
  name: string // name of the event bus, recommended to include the word "Bus" in the name for clarity in logs

  // configuration options
  max_history_size: number | null // max number of completed events kept in log, set to null for unlimited history
  event_timeout_default: number | null
  event_concurrency_default: EventConcurrencyMode
  event_handler_concurrency_default: EventHandlerConcurrencyMode
  event_handler_completion_default: EventHandlerCompletionMode
  event_handler_detect_file_paths: boolean

  // slow processing warning timeout settings
  event_handler_slow_timeout: number | null
  event_slow_timeout: number | null

  // public runtime state
  handlers: Map<string, EventHandler> // map of handler uuidv5 ids to EventHandler objects
  handlers_by_key: Map<string, string[]> // map of normalized event_key to ordered handler ids
  event_history: Map<string, BaseEvent> // map of event uuidv7 ids to processed BaseEvent objects

  // internal runtime state
  pending_event_queue: BaseEvent[] // queue of events that have been dispatched to the bus but not yet processed
  in_flight_event_ids: Set<string> // set of event ids that are currently being processed by the bus
  runloop_running: boolean
  locks: LockManager
  find_waiters: Set<EphemeralFindEventHandler> // set of EphemeralFindEventHandler objects that are waiting for a matching future event

  constructor(name: string = 'EventBus', options: EventBusOptions = {}) {
    this.id = options.id ?? uuidv7()
    this.name = name

    // set configuration options
    this.max_history_size = options.max_history_size === undefined ? 100 : options.max_history_size
    this.event_concurrency_default = options.event_concurrency ?? 'bus-serial'
    this.event_handler_concurrency_default = options.event_handler_concurrency ?? 'serial'
    this.event_handler_completion_default = options.event_handler_completion ?? 'all'
    this.event_handler_detect_file_paths = options.event_handler_detect_file_paths ?? true
    this.event_timeout_default = options.event_timeout === undefined ? 60 : options.event_timeout
    this.event_handler_slow_timeout = options.event_handler_slow_timeout === undefined ? 30 : options.event_handler_slow_timeout
    this.event_slow_timeout = options.event_slow_timeout === undefined ? 300 : options.event_slow_timeout

    // initialize runtime state
    this.runloop_running = false
    this.handlers = new Map()
    this.handlers_by_key = new Map()
    this.find_waiters = new Set()
    this.event_history = new Map()
    this.pending_event_queue = []
    this.in_flight_event_ids = new Set()
    this.locks = new LockManager(this)

    EventBus._all_instances.add(this)

    this.dispatch = this.dispatch.bind(this)
    this.emit = this.emit.bind(this)
  }

  toString(): string {
    return `${this.name}#${this.id.slice(-4)}`
  }

  toJSON(): EventBusJSON {
    return {
      id: this.id,
      name: this.name,
      max_history_size: this.max_history_size,
      event_concurrency: this.event_concurrency_default,
      event_timeout: this.event_timeout_default,
      event_slow_timeout: this.event_slow_timeout,
      event_handler_concurrency: this.event_handler_concurrency_default,
      event_handler_completion: this.event_handler_completion_default,
      event_handler_slow_timeout: this.event_handler_slow_timeout,
      event_handler_detect_file_paths: this.event_handler_detect_file_paths,
      handlers: EventHandler.toJSONArray(this.handlers.values()),
      handlers_by_key: Array.from(this.handlers_by_key.entries()).map(([key, ids]) => [key, [...ids]]),
      event_history: BaseEvent.toJSONArray(this.event_history.values()),
      pending_event_queue: BaseEvent.toJSONArray(this.pending_event_queue),
      in_flight_event_ids: Array.from(this.in_flight_event_ids),
      runloop_running: this.runloop_running,
      find_waiters: FindWaiter.toJSONArray(this.find_waiters),
    }
  }

  static fromJSON(data: unknown): EventBus {
    if (!data || typeof data !== 'object') {
      throw new Error('EventBus.fromJSON(data) requires an object')
    }
    const record = data as Record<string, unknown>
    const name = typeof record.name === 'string' ? record.name : 'EventBus'
    const options: EventBusOptions = {}

    if (typeof record.id === 'string') options.id = record.id
    if (typeof record.max_history_size === 'number' || record.max_history_size === null) options.max_history_size = record.max_history_size
    if (record.event_concurrency === 'global-serial' || record.event_concurrency === 'bus-serial' || record.event_concurrency === 'parallel') {
      options.event_concurrency = record.event_concurrency
    }
    if (typeof record.event_timeout === 'number' || record.event_timeout === null) options.event_timeout = record.event_timeout
    else if (typeof record.event_timeout_default === 'number' || record.event_timeout_default === null) {
      options.event_timeout = record.event_timeout_default
    }
    if (typeof record.event_slow_timeout === 'number' || record.event_slow_timeout === null) options.event_slow_timeout = record.event_slow_timeout
    if (record.event_handler_concurrency === 'serial' || record.event_handler_concurrency === 'parallel') {
      options.event_handler_concurrency = record.event_handler_concurrency
    } else if (record.event_handler_concurrency_default === 'serial' || record.event_handler_concurrency_default === 'parallel') {
      options.event_handler_concurrency = record.event_handler_concurrency_default
    }
    if (record.event_handler_completion === 'all' || record.event_handler_completion === 'first') {
      options.event_handler_completion = record.event_handler_completion
    } else if (record.event_handler_completion_default === 'all' || record.event_handler_completion_default === 'first') {
      options.event_handler_completion = record.event_handler_completion_default
    }
    if (typeof record.event_handler_slow_timeout === 'number' || record.event_handler_slow_timeout === null) {
      options.event_handler_slow_timeout = record.event_handler_slow_timeout
    }
    if (typeof record.event_handler_detect_file_paths === 'boolean') {
      options.event_handler_detect_file_paths = record.event_handler_detect_file_paths
    }
    const bus = new EventBus(name, options)

    const handler_entries = EventHandler.fromJSONArray(record.handlers)
    for (const handler_entry of handler_entries) {
      bus.handlers.set(handler_entry.id, handler_entry)
    }

    const raw_handlers_by_key = Array.isArray(record.handlers_by_key) ? record.handlers_by_key : []
    if (raw_handlers_by_key.length > 0) {
      bus.handlers_by_key.clear()
      for (const entry of raw_handlers_by_key) {
        if (!Array.isArray(entry) || entry.length !== 2) {
          continue
        }
        const [raw_key, raw_ids] = entry
        if (typeof raw_key !== 'string' || !Array.isArray(raw_ids)) {
          continue
        }
        const ids = raw_ids.filter((id): id is string => typeof id === 'string')
        bus.handlers_by_key.set(raw_key, ids)
      }
    } else {
      for (const handler_entry of bus.handlers.values()) {
        const ids = bus.handlers_by_key.get(handler_entry.event_key)
        if (ids) ids.push(handler_entry.id)
        else bus.handlers_by_key.set(handler_entry.event_key, [handler_entry.id])
      }
    }

    const history_events = BaseEvent.fromJSONArray(record.event_history)
    for (const event of history_events) {
      event.bus = bus
      bus.event_history.set(event.event_id, event)
    }

    const pending_queue_events = BaseEvent.fromJSONArray(record.pending_event_queue)
    bus.pending_event_queue = pending_queue_events.map((event) => {
      event.bus = bus
      const existing = bus.event_history.get(event.event_id)
      if (existing) {
        return existing
      }
      bus.event_history.set(event.event_id, event)
      return event
    })

    const raw_in_flight = Array.isArray(record.in_flight_event_ids) ? record.in_flight_event_ids : []
    bus.in_flight_event_ids = new Set(raw_in_flight.filter((id): id is string => typeof id === 'string'))

    // Reset runtime execution state after restore. Queue/history/handlers are restored,
    // but lock/semaphore internals should always restart from a clean default state.
    bus.runloop_running = false
    bus.locks.clear()
    bus.find_waiters = new Set(FindWaiter.fromJSONArray(record.find_waiters))

    return bus
  }

  get label(): string {
    return `${this.name}#${this.id.slice(-4)}`
  }

  // destroy the event bus and all its state to allow for garbage collection
  destroy(): void {
    EventBus._all_instances.delete(this)
    this.handlers.clear()
    this.handlers_by_key.clear()
    for (const event of this.event_history.values()) {
      event._gc()
    }
    this.event_history.clear()
    this.pending_event_queue.length = 0
    this.in_flight_event_ids.clear()
    this.find_waiters.clear()
    this.locks.clear()
  }

  on<T extends BaseEvent>(event_key: EventClass<T>, handler: EventHandlerFunction<T>, options?: Partial<EventHandler>): EventHandler
  on<T extends BaseEvent>(event_key: string | '*', handler: UntypedEventHandlerFunction<T>, options?: Partial<EventHandler>): EventHandler
  on(
    event_key: EventKey | '*',
    handler: EventHandlerFunction | UntypedEventHandlerFunction,
    options: Partial<EventHandler> = {}
  ): EventHandler {
    const normalized_key = this.normalizeEventKey(event_key) // get string event_type or '*'
    const handler_name = handler.name || 'anonymous' // get handler function name or 'anonymous' if the handler is an anonymous/arrow function
    const { isostring: handler_registered_at, ts: handler_registered_ts } = BaseEvent.nextTimestamp()
    const handler_entry = new EventHandler({
      handler: handler as EventHandlerFunction,
      handler_name,
      handler_registered_at,
      handler_registered_ts,
      event_key: normalized_key,
      eventbus_name: this.name,
      eventbus_id: this.id,
      ...options,
    })
    if (this.event_handler_detect_file_paths) {
      // optionally peform (expensive) file path detection for the handler using Error().stack introspection
      // makes logs much more useful for debugging, but is expensive to do if not needed
      handler_entry.detectHandlerFilePath()
    }

    this.handlers.set(handler_entry.id, handler_entry)
    const ids = this.handlers_by_key.get(handler_entry.event_key)
    if (ids) ids.push(handler_entry.id)
    else this.handlers_by_key.set(handler_entry.event_key, [handler_entry.id])
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
        this.removeIndexedHandler(entry.event_key, handler_id)
      }
    }
  }

  dispatch<T extends BaseEvent>(event: T): T {
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
    if (original_event.event_handler_completion === undefined) {
      original_event.event_handler_completion = this.event_handler_completion_default
    }

    if (original_event.event_path.includes(this.label) || this.hasProcessedEvent(original_event)) {
      return this.getEventProxyScopedToThisBus(original_event) as T
    }

    if (!original_event.event_path.includes(this.label)) {
      original_event.event_path.push(this.label)
    }

    if (original_event.event_parent_id && original_event.event_emitted_by_handler_id) {
      const parent_result = original_event.event_parent?.event_results.get(original_event.event_emitted_by_handler_id)
      if (parent_result) {
        parent_result.linkEmittedChildEvent(original_event)
      }
    }

    this.event_history.set(original_event.event_id, original_event)
    this.trimHistory()
    this.notifyFindListeners(original_event)

    original_event.event_pending_bus_count += 1
    this.pending_event_queue.push(original_event)
    this.startRunloop()

    return this.getEventProxyScopedToThisBus(original_event) as T
  }

  // alias for dispatch
  emit<T extends BaseEvent>(event: T): T {
    return this.dispatch(event)
  }

  // find a recent event or wait for a future event that matches some criteria
  find(event_key: '*', options?: FindOptions): Promise<BaseEvent | null>
  find(event_key: '*', where: (event: BaseEvent) => boolean, options?: FindOptions): Promise<BaseEvent | null>
  find<T extends BaseEvent>(event_key: EventKey<T>, options?: FindOptions): Promise<T | null>
  find<T extends BaseEvent>(event_key: EventKey<T>, where: (event: T) => boolean, options?: FindOptions): Promise<T | null>
  async find<T extends BaseEvent>(
    event_key: EventKey<T> | '*',
    where_or_options: ((event: T) => boolean) | FindOptions = {},
    maybe_options: FindOptions = {}
  ): Promise<T | null> {
    const where = typeof where_or_options === 'function' ? where_or_options : () => true
    const options = typeof where_or_options === 'function' ? maybe_options : where_or_options

    const past = options.past === undefined && options.future === undefined ? true : (options.past ?? true)
    const future = options.past === undefined && options.future === undefined ? false : (options.future ?? true)
    const child_of = options.child_of ?? null
    const event_field_filters = Object.entries(options).filter(
      ([key, value]) => key.startsWith('event_') && value !== undefined
    ) as Array<[`event_${string}`, unknown]>

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
      for (const [event_key, expected] of event_field_filters) {
        if ((event as unknown as Record<string, unknown>)[event_key] !== expected) {
          return false
        }
      }
      return true
    }

    // find a dispatched event in history that matches the criteria
    if (past !== false) {
      const now_ms = performance.timeOrigin + performance.now()
      const cutoff_ms = past === true ? null : now_ms - Math.max(0, Number(past)) * 1000

      const history_values = Array.from(this.event_history.values())
      for (let i = history_values.length - 1; i >= 0; i -= 1) {
        const event = history_values[i]
        if (!matches(event)) {
          continue
        }
        if (cutoff_ms !== null && Date.parse(event.event_created_at) < cutoff_ms) {
          continue
        }
        return this.getEventProxyScopedToThisBus(event) as T
      }
    }

    // if we are only looking for past events, return null when no match is found
    if (future === false) {
      return null
    }

    // if we are looking for future events, return a promise that resolves when a match is found
    return new Promise<T | null>((resolve) => {
      const waiter: EphemeralFindEventHandler = {
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

  async waitUntilIdle(): Promise<void> {
    await this.locks.waitForIdle()
  }

  // Weak idle check: only checks if handlers are idle, doesnt check that the queue is empty
  isIdle(): boolean {
    for (const event of this.event_history.values()) {
      for (const result of event.event_results.values()) {
        if (result.eventbus_id !== this.id) {
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

  eventIsChildOf(child_event: BaseEvent, parent_event: BaseEvent): boolean {
    if (child_event.event_id === parent_event.event_id) {
      return false
    }

    let current_parent_id = child_event.event_parent_id
    while (current_parent_id) {
      if (current_parent_id === parent_event.event_id) {
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
  // optionally using a pre-acquired semaphore if we're inside handling of a parent event
  private async processEvent(
    event: BaseEvent,
    options: {
      bypass_event_semaphores?: boolean
      pre_acquired_semaphore?: AsyncSemaphore | null
    } = {}
  ): Promise<void> {
    try {
      if (this.hasProcessedEvent(event)) {
        return
      }
      event.markStarted()
      const slow_event_warning_timer = event.createSlowEventWarningTimer()
      const semaphore = options.bypass_event_semaphores ? null : this.locks.getSemaphoreForEvent(event)
      const pre_acquired_semaphore = options.pre_acquired_semaphore ?? null
      try {
        if (pre_acquired_semaphore) {
          const pending_entries = event.createPendingHandlerResults(this)
          await this.getEventProxyScopedToThisBus(event).processEvent(pending_entries)
        } else {
          await runWithSemaphore(semaphore, async () => {
            const pending_entries = event.createPendingHandlerResults(this)
            await this.getEventProxyScopedToThisBus(event).processEvent(pending_entries)
          })
        }
        event.event_pending_bus_count = Math.max(0, event.event_pending_bus_count - 1)
        event.markCompleted(false)
      } finally {
        if (slow_event_warning_timer) {
          clearTimeout(slow_event_warning_timer)
        }
      }
    } finally {
      if (options.pre_acquired_semaphore) {
        options.pre_acquired_semaphore.release()
      }
      this.in_flight_event_ids.delete(event.event_id)
      this.locks.notifyIdleListeners()
    }
  }

  // Called when a handler does `await child.done()` — processes the child event
  // immediately ("queue-jump") instead of waiting for the runloop to pick it up.
  //
  // Yield-and-reacquire: if the calling handler holds a handler concurrency semaphore,
  // we temporarily release it so child handlers on the same bus can acquire it
  // (preventing deadlock for serial handler mode). We re-acquire after
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
        await this.processEvent(original_event)
        if (original_event.event_status !== 'completed') {
          await original_event.waitForCompletion()
        }
        return event
      }
      await original_event.waitForCompletion()
      return event
    }

    // ensure a pause request is set so the bus runloop pauses and (will resume when the handler exits)
    currently_active_event_result.ensureQueueJumpPause(this)
    if (original_event.event_status === 'completed') {
      return event
    }

    // re-endter event-level handler lock if needed
    if (currently_active_event_result._lock) {
      await currently_active_event_result._lock.runQueueJump(this.processEventImmediatelyAcrossBuses.bind(this, original_event))
      return event
    }

    await this.processEventImmediatelyAcrossBuses(original_event)
    return event
  }

  // Processes a queue-jumped event across all buses that have it dispatched.
  // Called from processEventImmediately after the parent handler's semaphore has been yielded.
  private async processEventImmediatelyAcrossBuses(event: BaseEvent): Promise<void> {
    // Use event_path ordering to pick candidate buses and filter out buses that
    // haven't seen the event or already processed it.
    const ordered: EventBus[] = []
    const seen = new Set<EventBus>()
    const event_path = Array.isArray(event.event_path) ? event.event_path : []
    for (const label of event_path) {
      for (const bus of EventBus._all_instances) {
        if (bus.label !== label) {
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
    if (ordered.length === 0) {
      await event.waitForCompletion()
      return
    }

    // Determine which event semaphore the initiating bus resolves to, so we can
    // detect when other buses share the same instance (global-serial).
    const initiating_event_semaphore = this.locks.getSemaphoreForEvent(event)
    const pause_releases: Array<() => void> = []

    try {
      for (const bus of ordered) {
        if (bus !== this) {
          pause_releases.push(bus.locks.requestRunloopPause())
        }
      }

      for (const bus of ordered) {
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

        await bus.processEvent(event, {
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
        void this.processEvent(original_event, {
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

  // check if an event has been processed (and completed) by this bus
  hasProcessedEvent(event: BaseEvent): boolean {
    const results = Array.from(event.event_results.values()).filter((result) => result.eventbus_id === this.id)
    if (results.length === 0) {
      return false
    }
    return results.every((result) => result.status === 'completed' || result.status === 'error')
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
          return (child_event: BaseEvent) => {
            const original_child = child_event._event_original ?? child_event
            if (handler_result) {
              handler_result.linkEmittedChildEvent(original_child)
            } else if (!original_child.event_parent_id) {
              // fallback for non-handler scoped dispatch
              original_child.event_parent_id = parent_event_id
            }
            const dispatcher = Reflect.get(target, prop, receiver) as (event: BaseEvent) => BaseEvent
            const dispatched = dispatcher.call(target, original_child)
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

  getHandlersForEvent(event: BaseEvent): EventHandler[] {
    const handlers: EventHandler[] = []
    for (const key of [event.event_type, '*']) {
      const ids = this.handlers_by_key.get(key)
      if (!ids) continue
      for (const id of ids) {
        const entry = this.handlers.get(id)
        if (entry) handlers.push(entry)
      }
    }
    return handlers
  }

  private removeIndexedHandler(event_key: string | '*', handler_id: string): void {
    const ids = this.handlers_by_key.get(event_key)
    if (!ids) return
    const idx = ids.indexOf(handler_id)
    if (idx >= 0) ids.splice(idx, 1)
    if (ids.length === 0) this.handlers_by_key.delete(event_key)
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
