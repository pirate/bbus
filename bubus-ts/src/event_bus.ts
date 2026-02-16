import { BaseEvent, type BaseEventJSON } from './base_event.js'
import { EventHistory } from './event_history.js'
import { EventResult } from './event_result.js'
import { captureAsyncContext } from './async_context.js'
import { withSlowMonitor, withTimeout } from './timing.js'
import {
  AsyncLock,
  type EventConcurrencyMode,
  type EventHandlerConcurrencyMode,
  type EventHandlerCompletionMode,
  LockManager,
} from './lock_manager.js'
import {
  EventHandler,
  EventHandlerAbortedError,
  EventHandlerCancelledError,
  EventHandlerTimeoutError,
  type EphemeralFindEventHandler,
  type EventHandlerJSON,
} from './event_handler.js'
import type { EventBusMiddleware, EventBusMiddlewareCtor, EventBusMiddlewareInput } from './middlewares.js'
import { logTree } from './logging.js'
import { v7 as uuidv7 } from 'uuid'

import type { EventClass, EventHandlerCallable, EventPattern, FindOptions, UntypedEventHandlerFunction } from './types.js'

export type EventBusOptions = {
  id?: string
  max_history_size?: number | null
  max_history_drop?: boolean

  // per-event options
  event_concurrency?: EventConcurrencyMode | null
  event_timeout?: number | null // default handler timeout in seconds, applied when event.event_timeout is undefined
  event_slow_timeout?: number | null // threshold before a warning is logged about slow event processing

  // per-event-handler options
  event_handler_concurrency?: EventHandlerConcurrencyMode | null
  event_handler_completion?: EventHandlerCompletionMode
  event_handler_slow_timeout?: number | null // threshold before a warning is logged about slow handler execution
  event_handler_detect_file_paths?: boolean // autodetect source code file and lineno where handlers are defined for better logs (slightly slower because Error().stack introspection to fine files is expensive)
  middlewares?: EventBusMiddlewareInput[]
}

export type EventBusJSON = {
  id: string
  name: string
  max_history_size: number | null
  max_history_drop: boolean
  event_concurrency: EventConcurrencyMode
  event_timeout: number | null
  event_slow_timeout: number | null
  event_handler_concurrency: EventHandlerConcurrencyMode
  event_handler_completion: EventHandlerCompletionMode
  event_handler_slow_timeout: number | null
  event_handler_detect_file_paths: boolean
  handlers: Record<string, EventHandlerJSON>
  handlers_by_key: Record<string, string[]>
  event_history: Record<string, BaseEventJSON>
  pending_event_queue: string[]
}

// Global registry of all EventBus instances to allow for cross-bus coordination
// when global-serial concurrency mode is used.
export class GlobalBusRegistry {
  private _bus_refs = new Set<WeakRef<EventBus>>()

  add(bus: EventBus): void {
    this._bus_refs.add(new WeakRef(bus))
  }

  discard(bus: EventBus): void {
    for (const ref of this._bus_refs) {
      const current = ref.deref()
      if (!current || current === bus) {
        this._bus_refs.delete(ref)
      }
    }
  }

  has(bus: EventBus): boolean {
    for (const ref of this._bus_refs) {
      const current = ref.deref()
      if (!current) {
        this._bus_refs.delete(ref)
        continue
      }
      if (current === bus) {
        return true
      }
    }
    return false
  }

  get size(): number {
    let count = 0
    for (const ref of this._bus_refs) {
      if (ref.deref()) {
        count += 1
      } else {
        this._bus_refs.delete(ref)
      }
    }
    return count
  }

  iter(): Iterator<EventBus> {
    return this[Symbol.iterator]()
  }

  *[Symbol.iterator](): Iterator<EventBus> {
    for (const ref of this._bus_refs) {
      const bus = ref.deref()
      if (bus) {
        yield bus
      } else {
        this._bus_refs.delete(ref)
      }
    }
  }

  findBusById(bus_id: string): EventBus | undefined {
    for (const bus of this) {
      if (bus.id === bus_id) {
        return bus
      }
    }
    return undefined
  }

  findEventById(event_id: string): BaseEvent | null {
    for (const bus of this) {
      const event = bus.event_history.getEvent(event_id)
      if (event) {
        return event
      }
    }
    return null
  }
}

export class EventBus {
  static all_instances: GlobalBusRegistry = new GlobalBusRegistry()

  get all_instances(): GlobalBusRegistry {
    return EventBus.all_instances
  }

  id: string // unique uuidv7 identifier for the event bus
  name: string // name of the event bus, recommended to include the word "Bus" in the name for clarity in logs

  // configuration options
  event_timeout: number | null
  event_concurrency: EventConcurrencyMode
  event_handler_concurrency_default: EventHandlerConcurrencyMode
  event_handler_completion_default: EventHandlerCompletionMode
  event_handler_detect_file_paths: boolean

  // slow processing warning timeout settings
  event_handler_slow_timeout: number | null
  event_slow_timeout: number | null

  // public runtime state
  handlers: Map<string, EventHandler> // map of handler uuidv5 ids to EventHandler objects
  handlers_by_key: Map<string, string[]> // map of normalized event_pattern to ordered handler ids
  event_history: EventHistory<BaseEvent> // map of event uuidv7 ids to processed BaseEvent objects

  // internal runtime state
  pending_event_queue: BaseEvent[] // queue of events that have been emitted to the bus but not yet processed
  in_flight_event_ids: Set<string> // set of event ids that are currently being processed by the bus
  runloop_running: boolean
  locks: LockManager
  find_waiters: Set<EphemeralFindEventHandler> // set of EphemeralFindEventHandler objects that are waiting for a matching future event
  middlewares: EventBusMiddleware[]

  private static normalizeMiddlewares(middlewares?: EventBusMiddlewareInput[]): EventBusMiddleware[] {
    const normalized: EventBusMiddleware[] = []
    for (const middleware of middlewares ?? []) {
      if (!middleware) {
        continue
      }
      if (typeof middleware === 'function') {
        normalized.push(new (middleware as EventBusMiddlewareCtor)())
      } else {
        normalized.push(middleware as EventBusMiddleware)
      }
    }
    return normalized
  }

  constructor(name: string = 'EventBus', options: EventBusOptions = {}) {
    this.id = options.id ?? uuidv7()
    this.name = name

    // set configuration options
    this.event_concurrency = options.event_concurrency ?? 'bus-serial'
    this.event_handler_concurrency_default = options.event_handler_concurrency ?? 'serial'
    this.event_handler_completion_default = options.event_handler_completion ?? 'all'
    this.event_handler_detect_file_paths = options.event_handler_detect_file_paths ?? true
    this.event_timeout = options.event_timeout === undefined ? 60 : options.event_timeout
    this.event_handler_slow_timeout = options.event_handler_slow_timeout === undefined ? 30 : options.event_handler_slow_timeout
    this.event_slow_timeout = options.event_slow_timeout === undefined ? 300 : options.event_slow_timeout

    // initialize runtime state
    this.runloop_running = false
    this.handlers = new Map()
    this.handlers_by_key = new Map()
    this.find_waiters = new Set()
    this.event_history = new EventHistory({
      max_history_size: options.max_history_size === undefined ? 100 : options.max_history_size,
      max_history_drop: options.max_history_drop ?? false,
    })
    this.pending_event_queue = []
    this.in_flight_event_ids = new Set()
    this.locks = new LockManager(this)
    this.middlewares = EventBus.normalizeMiddlewares(options.middlewares)

    EventBus.all_instances.add(this)

    this.dispatch = this.dispatch.bind(this)
    this.emit = this.emit.bind(this)
  }

  toString(): string {
    return `${this.name}#${this.id.slice(-4)}`
  }

  scheduleMicrotask(fn: () => void): void {
    if (typeof queueMicrotask === 'function') {
      queueMicrotask(fn)
      return
    }
    void Promise.resolve().then(fn)
  }

  private async runMiddlewareHook(hook: keyof EventBusMiddleware, args: unknown[]): Promise<void> {
    if (this.middlewares.length === 0) {
      return
    }
    for (const middleware of this.middlewares) {
      const callback = middleware[hook]
      if (!callback) {
        continue
      }
      await (callback as (...hook_args: unknown[]) => void | Promise<void>).apply(middleware, args)
    }
  }

  async onEventChange(event: BaseEvent, status: 'pending' | 'started' | 'completed'): Promise<void> {
    await this._onEventChange(event, status)
  }

  async onEventResultChange(event: BaseEvent, result: EventResult, status: 'pending' | 'started' | 'completed'): Promise<void> {
    await this._onEventResultChange(event, result, status)
  }

  private async _onEventChange(event: BaseEvent, status: 'pending' | 'started' | 'completed'): Promise<void> {
    await this.runMiddlewareHook('onEventChange', [this, event, status])
  }

  private async _onEventResultChange(event: BaseEvent, result: EventResult, status: 'pending' | 'started' | 'completed'): Promise<void> {
    await this.runMiddlewareHook('onEventResultChange', [this, event, result, status])
  }

  private async _onBusHandlersChange(handler: EventHandler, registered: boolean): Promise<void> {
    await this.runMiddlewareHook('onBusHandlersChange', [this, handler, registered])
  }

  private _finalizeEventTimeout(
    event: BaseEvent,
    pending_entries: Array<{
      handler: EventHandler
      result: EventResult
    }>,
    timeout_error: EventHandlerTimeoutError
  ): void {
    const timeout_seconds = timeout_error.timeout_seconds ?? event.event_timeout ?? null
    event.eventCancelPendingChildProcessing(timeout_error)

    for (const entry of pending_entries) {
      const result = entry.result
      if (result.status === 'completed') {
        continue
      }
      if (result.status === 'error') {
        continue
      }
      if (result.status === 'started') {
        result._lock?.exitHandlerRun()
        result.releaseQueueJumpPauses()
        const aborted_error = new EventHandlerAbortedError(`Aborted running handler due to event timeout`, {
          event_result: result,
          timeout_seconds,
          cause: timeout_error,
        })
        result.markError(aborted_error)
        result.signalAbort(aborted_error)
        continue
      }
      const cancelled_error = new EventHandlerCancelledError(`Cancelled pending handler due to event timeout`, {
        event_result: result,
        timeout_seconds,
        cause: timeout_error,
      })
      result.markError(cancelled_error)
    }

    event.event_pending_bus_count = Math.max(0, event.event_pending_bus_count - 1)
    event.markCompleted()
  }

  private _createEventTimeoutError(
    event: BaseEvent,
    pending_entries: Array<{
      handler: EventHandler
      result: EventResult
    }>,
    timeout_seconds: number
  ): EventHandlerTimeoutError {
    const timeout_anchor =
      pending_entries.find((entry) => entry.result.status === 'started') ??
      pending_entries.find((entry) => entry.result.status === 'pending') ??
      pending_entries[0]!
    return new EventHandlerTimeoutError(
      `${this.toString()}.on(${event.toString()}, ${timeout_anchor.result.handler.toString()}) timed out after ${timeout_seconds}s`,
      {
        event_result: timeout_anchor.result,
        timeout_seconds,
      }
    )
  }

  private async _runHandlersWithTimeout(
    event: BaseEvent,
    pending_entries: Array<{
      handler: EventHandler
      result: EventResult
    }>,
    event_timeout: number | null,
    fn: () => Promise<void>
  ): Promise<void> {
    try {
      if (event_timeout === null || pending_entries.length === 0) {
        await fn()
      } else {
        await withTimeout(event_timeout, () => this._createEventTimeoutError(event, pending_entries, event_timeout), fn)
      }
    } catch (error) {
      if (error instanceof EventHandlerTimeoutError) {
        this._finalizeEventTimeout(event, pending_entries, error)
        return
      }
      throw error
    }
  }

  private _markEventCompletedIfNeeded(event: BaseEvent): void {
    if (event.event_status !== 'completed') {
      event.event_pending_bus_count = Math.max(0, event.event_pending_bus_count - 1)
      event.markCompleted(false)
    }
    if (
      this.event_history.max_history_size !== null &&
      this.event_history.max_history_size > 0 &&
      this.event_history.size > this.event_history.max_history_size
    ) {
      this.event_history.trimEventHistory({
        isEventComplete: (candidate_event) => candidate_event.event_status === 'completed',
        onDropEvent: (candidate_event) => candidate_event._gc(),
        ownerLabel: this.toString(),
        max_history_size: this.event_history.max_history_size,
        max_history_drop: this.event_history.max_history_drop,
      })
    }
  }

  toJSON(): EventBusJSON {
    const handlers: Record<string, EventHandlerJSON> = {}
    for (const [handler_id, handler] of this.handlers.entries()) {
      handlers[handler_id] = handler.toJSON()
    }

    const handlers_by_key: Record<string, string[]> = {}
    for (const [key, ids] of this.handlers_by_key.entries()) {
      handlers_by_key[key] = [...ids]
    }

    const event_history: Record<string, BaseEventJSON> = {}
    for (const [event_id, event] of this.event_history.entries()) {
      event_history[event_id] = event.toJSON()
    }

    const pending_event_queue: string[] = []
    for (const event of this.pending_event_queue) {
      const event_id = event.event_id
      if (!event_history[event_id]) {
        event_history[event_id] = event.toJSON()
      }
      pending_event_queue.push(event_id)
    }

    return {
      id: this.id,
      name: this.name,
      max_history_size: this.event_history.max_history_size,
      max_history_drop: this.event_history.max_history_drop,
      event_concurrency: this.event_concurrency,
      event_timeout: this.event_timeout,
      event_slow_timeout: this.event_slow_timeout,
      event_handler_concurrency: this.event_handler_concurrency_default,
      event_handler_completion: this.event_handler_completion_default,
      event_handler_slow_timeout: this.event_handler_slow_timeout,
      event_handler_detect_file_paths: this.event_handler_detect_file_paths,
      handlers,
      handlers_by_key,
      event_history,
      pending_event_queue,
    }
  }

  private static _stubHandlerFn(): EventHandlerCallable {
    return (() => undefined) as EventHandlerCallable
  }

  private static _upsertHandlerIndex(bus: EventBus, event_pattern: string, handler_id: string): void {
    const ids = bus.handlers_by_key.get(event_pattern)
    if (ids) {
      if (!ids.includes(handler_id)) {
        ids.push(handler_id)
      }
      return
    }
    bus.handlers_by_key.set(event_pattern, [handler_id])
  }

  private static _linkEventResultHandlers(event: BaseEvent, bus: EventBus): void {
    for (const [map_key, result] of Array.from(event.event_results.entries())) {
      const handler_id = result.handler_id
      const existing_handler = bus.handlers.get(handler_id)
      if (existing_handler) {
        result.handler = existing_handler
      } else {
        const source = result.handler
        const handler_entry = EventHandler.fromJSON(
          {
            ...source.toJSON(),
            id: handler_id,
            event_pattern: source.event_pattern || event.event_type,
            eventbus_name: source.eventbus_name || bus.name,
            eventbus_id: source.eventbus_id || bus.id,
          },
          EventBus._stubHandlerFn()
        )
        bus.handlers.set(handler_entry.id, handler_entry)
        EventBus._upsertHandlerIndex(bus, handler_entry.event_pattern, handler_entry.id)
        result.handler = handler_entry
      }

      if (map_key !== handler_id) {
        event.event_results.delete(map_key)
        event.event_results.set(handler_id, result)
      }
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
    if (typeof record.max_history_drop === 'boolean') options.max_history_drop = record.max_history_drop
    if (
      record.event_concurrency === 'global-serial' ||
      record.event_concurrency === 'bus-serial' ||
      record.event_concurrency === 'parallel'
    ) {
      options.event_concurrency = record.event_concurrency
    }
    if (typeof record.event_timeout === 'number' || record.event_timeout === null) options.event_timeout = record.event_timeout
    if (typeof record.event_slow_timeout === 'number' || record.event_slow_timeout === null)
      options.event_slow_timeout = record.event_slow_timeout
    if (record.event_handler_concurrency === 'serial' || record.event_handler_concurrency === 'parallel') {
      options.event_handler_concurrency = record.event_handler_concurrency
    }
    if (record.event_handler_completion === 'all' || record.event_handler_completion === 'first') {
      options.event_handler_completion = record.event_handler_completion
    }
    if (typeof record.event_handler_slow_timeout === 'number' || record.event_handler_slow_timeout === null) {
      options.event_handler_slow_timeout = record.event_handler_slow_timeout
    }
    if (typeof record.event_handler_detect_file_paths === 'boolean') {
      options.event_handler_detect_file_paths = record.event_handler_detect_file_paths
    }
    const bus = new EventBus(name, options)

    if (!record.handlers || typeof record.handlers !== 'object' || Array.isArray(record.handlers)) {
      throw new Error('EventBus.fromJSON(data) requires handlers as an id-keyed object')
    }
    for (const [handler_id, payload] of Object.entries(record.handlers as Record<string, unknown>)) {
      if (!payload || typeof payload !== 'object') {
        continue
      }
      const parsed = EventHandler.fromJSON(
        {
          ...(payload as Record<string, unknown>),
          id: typeof (payload as { id?: unknown }).id === 'string' ? (payload as { id: string }).id : handler_id,
        },
        EventBus._stubHandlerFn()
      )
      bus.handlers.set(parsed.id, parsed)
    }

    if (!record.handlers_by_key || typeof record.handlers_by_key !== 'object' || Array.isArray(record.handlers_by_key)) {
      throw new Error('EventBus.fromJSON(data) requires handlers_by_key as an object')
    }
    bus.handlers_by_key.clear()
    for (const [raw_key, raw_ids] of Object.entries(record.handlers_by_key as Record<string, unknown>)) {
      if (!Array.isArray(raw_ids)) {
        continue
      }
      const ids = raw_ids.filter((id): id is string => typeof id === 'string')
      bus.handlers_by_key.set(raw_key, ids)
    }

    if (!record.event_history || typeof record.event_history !== 'object' || Array.isArray(record.event_history)) {
      throw new Error('EventBus.fromJSON(data) requires event_history as an id-keyed object')
    }
    for (const [event_id, payload] of Object.entries(record.event_history as Record<string, unknown>)) {
      if (!payload || typeof payload !== 'object') {
        continue
      }
      const event = BaseEvent.fromJSON({
        ...(payload as Record<string, unknown>),
        event_id: typeof (payload as { event_id?: unknown }).event_id === 'string' ? (payload as { event_id: string }).event_id : event_id,
      })
      event.bus = bus
      bus.event_history.set(event.event_id, event)
    }

    if (!Array.isArray(record.pending_event_queue)) {
      throw new Error('EventBus.fromJSON(data) requires pending_event_queue as an array of event ids')
    }
    const raw_pending_event_queue = record.pending_event_queue
    const pending_event_ids: string[] = []
    for (const item of raw_pending_event_queue) {
      if (typeof item === 'string') {
        pending_event_ids.push(item)
      }
    }
    bus.pending_event_queue = pending_event_ids
      .map((event_id) => bus.event_history.get(event_id))
      .filter((event): event is BaseEvent => Boolean(event))

    for (const event of bus.event_history.values()) {
      EventBus._linkEventResultHandlers(event, bus)
    }

    // Reset runtime execution state after restore. Queue/history/handlers are restored,
    // but lock internals should always restart from a clean default state.
    bus.in_flight_event_ids.clear()
    bus.runloop_running = false
    bus.locks.clear()
    bus.find_waiters.clear()

    return bus
  }

  get label(): string {
    return `${this.name}#${this.id.slice(-4)}`
  }

  removeEventFromPendingQueue(event: BaseEvent): number {
    const original_event = event._event_original ?? event
    let removed_count = 0
    for (let index = this.pending_event_queue.length - 1; index >= 0; index -= 1) {
      const queued_event = this.pending_event_queue[index]
      const queued_original = queued_event._event_original ?? queued_event
      if (queued_original.event_id !== original_event.event_id) {
        continue
      }
      this.pending_event_queue.splice(index, 1)
      removed_count += 1
    }
    return removed_count
  }

  isEventInFlightOrQueued(event_id: string): boolean {
    if (this.in_flight_event_ids.has(event_id)) {
      return true
    }
    for (const queued_event of this.pending_event_queue) {
      const queued_original = queued_event._event_original ?? queued_event
      if (queued_original.event_id === event_id) {
        return true
      }
    }
    return false
  }

  removeEventFromHistory(event_id: string): boolean {
    return this.event_history.delete(event_id)
  }

  // destroy the event bus and all its state to allow for garbage collection
  destroy(): void {
    EventBus.all_instances.discard(this)
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

  on<T extends BaseEvent>(event_pattern: EventClass<T>, handler: EventHandlerCallable<T>, options?: Partial<EventHandler>): EventHandler
  on<T extends BaseEvent>(
    event_pattern: string | '*',
    handler: UntypedEventHandlerFunction<T>,
    options?: Partial<EventHandler>
  ): EventHandler
  on(
    event_pattern: EventPattern | '*',
    handler: EventHandlerCallable | UntypedEventHandlerFunction,
    options: Partial<EventHandler> = {}
  ): EventHandler {
    const normalized_key = this.normalizeEventPattern(event_pattern) // get string event_type or '*'
    const handler_name = handler.name || 'anonymous' // get handler function name or 'anonymous' if the handler is an anonymous/arrow function
    const { isostring: handler_registered_at, ts: handler_registered_ts } = BaseEvent.nextTimestamp()
    const handler_entry = new EventHandler({
      handler: handler as EventHandlerCallable,
      handler_name,
      handler_registered_at,
      handler_registered_ts,
      event_pattern: normalized_key,
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
    const ids = this.handlers_by_key.get(handler_entry.event_pattern)
    if (ids) ids.push(handler_entry.id)
    else this.handlers_by_key.set(handler_entry.event_pattern, [handler_entry.id])
    this.scheduleMicrotask(() => {
      void this._onBusHandlersChange(handler_entry, true)
    })
    return handler_entry
  }

  off<T extends BaseEvent>(event_pattern: EventPattern<T> | '*', handler?: EventHandlerCallable<T> | string | EventHandler): void {
    const normalized_key = this.normalizeEventPattern(event_pattern)
    if (typeof handler === 'object' && handler instanceof EventHandler && handler.id !== undefined) {
      handler = handler.id
    }
    const match_by_id = typeof handler === 'string'
    for (const entry of this.handlers.values()) {
      if (entry.event_pattern !== normalized_key) {
        continue
      }
      const handler_id = entry.id
      if (handler === undefined || (match_by_id ? handler_id === handler : entry.handler === (handler as EventHandlerCallable))) {
        this.handlers.delete(handler_id)
        this.removeIndexedHandler(entry.event_pattern, handler_id)
        this.scheduleMicrotask(() => {
          void this._onBusHandlersChange(entry, false)
        })
      }
    }
  }

  emit<T extends BaseEvent>(event: T): T {
    const original_event = event._event_original ?? event // if event is a bus-scoped proxy already, get the original underlying event object
    if (!original_event.bus) {
      // if we are the first bus to emit this event, set the bus property on the original event object
      original_event.bus = this
    }
    if (!Array.isArray(original_event.event_path)) {
      original_event.event_path = []
    }
    if (original_event.eventGetDispatchContext() === undefined) {
      // when used in fastify/nextjs/other contexts with tracing based on AsyncLocalStorage in node
      // we want to capture the context at the emit site and use it when running handlers
      // because events may be handled async in a separate context than the emit site
      original_event.eventSetDispatchContext(captureAsyncContext())
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

    if (
      this.event_history.max_history_size !== null &&
      this.event_history.max_history_size > 0 &&
      !this.event_history.max_history_drop &&
      this.event_history.size >= this.event_history.max_history_size
    ) {
      throw new Error(
        `${this.toString()}.emit(${original_event.event_type}) rejected: history limit reached (${this.event_history.size}/${this.event_history.max_history_size}); set event_history.max_history_drop=true to drop old history instead.`
      )
    }

    this.event_history.addEvent(original_event)
    this.event_history.trimEventHistory({
      isEventComplete: (candidate_event) => candidate_event.event_status === 'completed',
      onDropEvent: (candidate_event) => candidate_event._gc(),
      ownerLabel: this.toString(),
      max_history_size: this.event_history.max_history_size,
      max_history_drop: this.event_history.max_history_drop,
    })
    this.notifyFindListeners(original_event)

    original_event.event_pending_bus_count += 1
    this.pending_event_queue.push(original_event)
    this.scheduleMicrotask(() => {
      void this._onEventChange(this.getEventProxyScopedToThisBus(original_event), 'pending')
    })
    this.startRunloop()

    return this.getEventProxyScopedToThisBus(original_event) as T
  }

  // alias for emit
  dispatch<T extends BaseEvent>(event: T): T {
    return this.emit(event)
  }

  // find a recent event or wait for a future event that matches some criteria
  find(event_pattern: '*', options?: FindOptions<BaseEvent>): Promise<BaseEvent | null>
  find(event_pattern: '*', where: (event: BaseEvent) => boolean, options?: FindOptions<BaseEvent>): Promise<BaseEvent | null>
  find<T extends BaseEvent>(event_pattern: EventPattern<T>, options?: FindOptions<T>): Promise<T | null>
  find<T extends BaseEvent>(event_pattern: EventPattern<T>, where: (event: T) => boolean, options?: FindOptions<T>): Promise<T | null>
  async find<T extends BaseEvent>(
    event_pattern: EventPattern<T> | '*',
    where_or_options: ((event: T) => boolean) | FindOptions<T> = {},
    maybe_options: FindOptions<T> = {}
  ): Promise<T | null> {
    const where = typeof where_or_options === 'function' ? where_or_options : () => true
    const options = typeof where_or_options === 'function' ? maybe_options : where_or_options
    const match = await this.event_history.find(event_pattern as EventPattern<T> | '*', where, {
      ...options,
      eventIsChildOf: (event, ancestor) => this.eventIsChildOf(event, ancestor),
      waitForFutureMatch: (normalized_event_pattern, matches, future) =>
        this._waitForFutureMatch(normalized_event_pattern, matches, future),
    })
    if (!match) {
      return null
    }
    return this.getEventProxyScopedToThisBus(match) as T
  }

  private async _waitForFutureMatch(
    event_pattern: string | '*',
    matches: (event: BaseEvent) => boolean,
    future: boolean | number
  ): Promise<BaseEvent | null> {
    if (future === false) {
      return null
    }
    return await new Promise<BaseEvent | null>((resolve) => {
      const waiter: EphemeralFindEventHandler = {
        event_pattern,
        matches,
        resolve: (event) => resolve(event),
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

  async waitUntilIdle(timeout: number | null = null): Promise<void> {
    await this.locks.waitForIdle(timeout)
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
    return this.event_history.get(event_id) ?? EventBus.all_instances.findEventById(event_id)
  }

  // Walk up the parent event chain to find an in-flight ancestor handler result.
  // Returns the result if found, null otherwise. Used by processEventImmediately to detect
  // cross-bus queue-jump scenarios where the calling handler is on a different bus.
  getParentEventResultAcrossAllBuses(event: BaseEvent): EventResult | null {
    const original = event._event_original ?? event
    let current_parent_id = original.event_parent_id
    let current_handler_id = original.event_emitted_by_handler_id
    while (current_handler_id && current_parent_id) {
      const parent = EventBus.all_instances.findEventById(current_parent_id)
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
    this.scheduleMicrotask(() => {
      void this.runloop()
    })
  }

  // schedule the processing of an event on the event bus by its normal runloop
  // optionally using a pre-acquired lock if we're inside handling of a parent event
  private async processEvent(
    event: BaseEvent,
    options: {
      bypass_event_locks?: boolean
      pre_acquired_lock?: AsyncLock | null
    } = {}
  ): Promise<void> {
    let pending_entries: Array<{
      handler: EventHandler
      result: EventResult
    }> = []
    try {
      if (this.hasProcessedEvent(event)) {
        return
      }
      event.markStarted()
      pending_entries = event.eventCreatePendingHandlerResults(this)
      const resolved_event_timeout = event.event_timeout ?? this.event_timeout
      const scoped_event = this.getEventProxyScopedToThisBus(event)
      if (this.middlewares.length > 0) {
        for (const entry of pending_entries) {
          await this._onEventResultChange(scoped_event, entry.result, 'pending')
        }
      }
      await this.locks.withEventLock(
        event,
        () =>
          this._runHandlersWithTimeout(event, pending_entries, resolved_event_timeout, () =>
            withSlowMonitor(event.createSlowEventWarningTimer(), () => scoped_event.runHandlers(pending_entries))
          ),
        options
      )
      this._markEventCompletedIfNeeded(event)
    } finally {
      if (options.pre_acquired_lock) {
        options.pre_acquired_lock.release()
      }
      this.in_flight_event_ids.delete(event.event_id)
      this.locks.notifyIdleListeners()
    }
  }

  // Called when a handler does `await child.done()` — processes the child event
  // immediately ("queue-jump") instead of waiting for the runloop to pick it up.
  //
  // Yield-and-reacquire: if the calling handler holds a handler concurrency lock,
  // we temporarily release it so child handlers on the same bus can acquire it
  // (preventing deadlock for serial handler mode). We re-acquire after
  // the child completes so the parent handler can continue with the lock held.
  async processEventImmediately<T extends BaseEvent>(event: T, handler_result?: EventResult): Promise<T> {
    const original_event = event._event_original ?? event
    // Find the parent handler's result: prefer the proxy-provided one (only if
    // the handler is still running), then this bus's stack, then walk up the
    // parent event tree (cross-bus case). If none found, we're not inside a
    // handler and should fall back to waitForCompletion.
    const proxy_result = handler_result?.status === 'started' ? handler_result : undefined
    const currently_active_event_result =
      proxy_result ?? this.locks.getActiveHandlerResult() ?? this.getParentEventResultAcrossAllBuses(original_event) ?? undefined
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

  // Processes a queue-jumped event across all buses that have it emitted.
  // Called from processEventImmediately after the parent handler's lock has been yielded.
  private async processEventImmediatelyAcrossBuses(event: BaseEvent): Promise<void> {
    // Use event_path ordering to pick candidate buses and filter out buses that
    // haven't seen the event or already processed it.
    const ordered: EventBus[] = []
    const seen = new Set<EventBus>()
    const event_path = Array.isArray(event.event_path) ? event.event_path : []
    for (const label of event_path) {
      for (const bus of EventBus.all_instances) {
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

    // Determine which event lock the initiating bus resolves to, so we can
    // detect when other buses share the same instance (global-serial).
    const initiating_event_lock = this.locks.getLockForEvent(event)
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

        // Bypass event lock on the initiating bus (we're already inside a handler
        // that acquired it). For other buses, only bypass if they resolve to the same
        // lock instance (global-serial shares one lock across all buses).
        const bus_event_lock = bus.locks.getLockForEvent(event)
        const should_bypass_event_lock = bus === this || (initiating_event_lock !== null && bus_event_lock === initiating_event_lock)

        await bus.processEvent(event, {
          bypass_event_locks: should_bypass_event_lock,
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
        let pre_acquired_lock: AsyncLock | null = null
        const event_lock = this.locks.getLockForEvent(original_event)
        if (event_lock) {
          await event_lock.acquire()
          pre_acquired_lock = event_lock
        }
        this.pending_event_queue.shift()
        if (this.in_flight_event_ids.has(original_event.event_id)) {
          if (pre_acquired_lock) {
            pre_acquired_lock.release()
          }
          continue
        }
        this.in_flight_event_ids.add(original_event.event_id)
        void this.processEvent(original_event, {
          bypass_event_locks: true,
          pre_acquired_lock,
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
          const runner = Reflect.get(target, prop, receiver) as EventBus['processEventImmediately']
          const process_event_immediately = <TChild extends BaseEvent>(child_event: TChild): Promise<TChild> => {
            return runner.call(target, child_event, handler_result) as Promise<TChild>
          }
          return process_event_immediately
        }
        if (prop === 'dispatch' || prop === 'emit') {
          const emit_child_event = <TChild extends BaseEvent>(child_event: TChild): TChild => {
            const original_child = child_event._event_original ?? child_event
            if (handler_result) {
              handler_result.linkEmittedChildEvent(original_child)
            } else if (!original_child.event_parent_id && original_child.event_id !== parent_event_id) {
              // fallback for non-handler scoped emit/dispatch
              original_child.event_parent_id = parent_event_id
            }
            const dispatcher = Reflect.get(target, prop, receiver) as EventBus['dispatch']
            const dispatched = dispatcher.call(target, original_child)
            return target.getEventProxyScopedToThisBus(dispatched as TChild, handler_result)
          }
          return emit_child_event
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
      if (!this.eventMatchesKey(event, waiter.event_pattern)) {
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

  private removeIndexedHandler(event_pattern: string | '*', handler_id: string): void {
    const ids = this.handlers_by_key.get(event_pattern)
    if (!ids) return
    const idx = ids.indexOf(handler_id)
    if (idx >= 0) ids.splice(idx, 1)
    if (ids.length === 0) this.handlers_by_key.delete(event_pattern)
  }

  private eventMatchesKey(event: BaseEvent, event_pattern: EventPattern): boolean {
    if (event_pattern === '*') {
      return true
    }
    const normalized = this.normalizeEventPattern(event_pattern)
    if (normalized === '*') {
      return true
    }
    return event.event_type === normalized
  }

  private normalizeEventPattern(event_pattern: EventPattern | '*'): string | '*' {
    if (event_pattern === '*') {
      return '*'
    }
    if (typeof event_pattern === 'string') {
      return event_pattern
    }
    const event_type = (event_pattern as { event_type?: unknown }).event_type
    if (typeof event_type === 'string' && event_type.length > 0 && event_type !== 'BaseEvent') {
      return event_type
    }
    const class_name = (event_pattern as { name?: unknown }).name
    if (typeof class_name === 'string' && class_name.length > 0 && class_name !== 'BaseEvent') {
      return class_name
    }
    let preview: string
    try {
      const encoded = JSON.stringify(event_pattern)
      preview = typeof encoded === 'string' ? encoded.slice(0, 30) : String(event_pattern).slice(0, 30)
    } catch {
      preview = String(event_pattern).slice(0, 30)
    }
    throw new Error('bus.on(match_pattern, ...) must be a string event type, "*", or a BaseEvent class, got: ' + preview)
  }

}
