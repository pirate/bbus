import { z } from 'zod'
import { v7 as uuidv7 } from 'uuid'

import type { EventBus } from './event_bus.js'
import type { EventHandler } from './event_handler.js'
import { EventResult } from './event_result.js'
import { EventHandlerAbortedError, EventHandlerCancelledError, EventHandlerTimeoutError } from './event_handler.js'
import type { EventConcurrencyMode, EventHandlerConcurrencyMode, EventHandlerCompletionMode, Deferred } from './lock_manager.js'
import {
  AsyncLock,
  EVENT_CONCURRENCY_MODES,
  EVENT_HANDLER_CONCURRENCY_MODES,
  EVENT_HANDLER_COMPLETION_MODES,
  withResolvers,
} from './lock_manager.js'
import { extractZodShape, normalizeEventResultType, toJsonSchema } from './types.js'
import type { EventResultType } from './types.js'

export const BaseEventSchema = z
  .object({
    event_id: z.string().uuid(),
    event_created_at: z.string().datetime(),
    event_created_ts: z.number().optional(),
    event_type: z.string(),
    event_version: z.string().default('0.0.1'),
    event_timeout: z.number().positive().nullable(),
    event_handler_timeout: z.number().positive().nullable().optional(),
    event_handler_slow_timeout: z.number().positive().nullable().optional(),
    event_parent_id: z.string().uuid().nullable().optional(),
    event_path: z.array(z.string()).optional(),
    event_result_type: z.unknown().optional(),
    event_emitted_by_handler_id: z.string().uuid().nullable().optional(),
    event_pending_bus_count: z.number().nonnegative().optional(),
    event_status: z.enum(['pending', 'started', 'completed']).optional(),
    event_started_at: z.string().datetime().nullable().optional(),
    event_started_ts: z.number().nullable().optional(),
    event_completed_at: z.string().datetime().nullable().optional(),
    event_completed_ts: z.number().nullable().optional(),
    event_results: z.array(z.unknown()).optional(),
    event_concurrency: z.enum(EVENT_CONCURRENCY_MODES).nullable().optional(),
    event_handler_concurrency: z.enum(EVENT_HANDLER_CONCURRENCY_MODES).nullable().optional(),
    event_handler_completion: z.enum(EVENT_HANDLER_COMPLETION_MODES).nullable().optional(),
  })
  .loose()

export type BaseEventData = z.infer<typeof BaseEventSchema>
export type BaseEventJSON = BaseEventData & Record<string, unknown>
type BaseEventFields = Pick<
  BaseEventData,
  | 'event_id'
  | 'event_created_at'
  | 'event_created_ts'
  | 'event_type'
  | 'event_version'
  | 'event_timeout'
  | 'event_handler_timeout'
  | 'event_handler_slow_timeout'
  | 'event_parent_id'
  | 'event_path'
  | 'event_result_type'
  | 'event_emitted_by_handler_id'
  | 'event_pending_bus_count'
  | 'event_status'
  | 'event_started_at'
  | 'event_started_ts'
  | 'event_completed_at'
  | 'event_completed_ts'
  | 'event_results'
  | 'event_concurrency'
  | 'event_handler_concurrency'
  | 'event_handler_completion'
>

export type BaseEventInit<TFields extends Record<string, unknown>> = TFields & Partial<BaseEventFields>

type BaseEventSchemaShape = typeof BaseEventSchema.shape

export type EventSchema<TShape extends z.ZodRawShape> = z.ZodObject<BaseEventSchemaShape & TShape>
type EventPayload<TShape extends z.ZodRawShape> = TShape extends Record<string, never> ? {} : z.infer<z.ZodObject<TShape>>

type EventInput<TShape extends z.ZodRawShape> = z.input<EventSchema<TShape>>
export type EventInit<TShape extends z.ZodRawShape> = Omit<EventInput<TShape>, keyof BaseEventFields> & Partial<BaseEventFields>

type EventWithResultSchema<TResult> = BaseEvent & { __event_result_type__?: TResult }

type ResultTypeFromEventResultTypeInput<TInput> = TInput extends z.ZodTypeAny
  ? z.infer<TInput>
  : TInput extends StringConstructor
    ? string
    : TInput extends NumberConstructor
      ? number
      : TInput extends BooleanConstructor
        ? boolean
        : TInput extends ArrayConstructor
          ? unknown[]
          : TInput extends ObjectConstructor
            ? Record<string, unknown>
            : unknown

type ResultSchemaFromShape<TShape> = TShape extends { event_result_type: infer S } ? ResultTypeFromEventResultTypeInput<S> : unknown

const EVENT_CLASS_DEFAULTS = new WeakMap<Function, Record<string, unknown>>()

export type EventFactory<TShape extends z.ZodRawShape, TResult = unknown> = {
  (data: EventInit<TShape>): EventWithResultSchema<TResult> & EventPayload<TShape>
  new (data: EventInit<TShape>): EventWithResultSchema<TResult> & EventPayload<TShape>
  schema: EventSchema<TShape>
  class?: new (data: EventInit<TShape>) => EventWithResultSchema<TResult> & EventPayload<TShape>
  event_type?: string
  event_version?: string
  event_result_type?: z.ZodTypeAny
  fromJSON?: (data: unknown) => EventWithResultSchema<TResult> & EventPayload<TShape>
}

type ZodShapeFrom<TShape extends Record<string, unknown>> = {
  [K in keyof TShape as K extends 'event_result_type' ? never : TShape[K] extends z.ZodTypeAny ? K : never]: Extract<
    TShape[K],
    z.ZodTypeAny
  >
}

export class BaseEvent {
  // event metadata fields
  event_id!: string // unique uuidv7 identifier for the event
  event_created_at!: string // ISO datetime string version of event_created_at
  event_created_ts!: number // nanosecond monotonic version of event_created_at
  event_type!: string // should match the class name of the event, e.g. BaseEvent.extend("MyEvent").event_type === "MyEvent"
  event_version!: string // event schema/version tag managed by callers for migration-friendly payload handling
  event_timeout!: number | null // maximum time in seconds that the event is allowed to run before it is aborted
  event_handler_timeout?: number | null // optional per-event handler timeout override in seconds
  event_handler_slow_timeout?: number | null // optional per-event slow handler warning threshold in seconds
  event_parent_id!: string | null // id of the parent event that triggered this event, if this event was emitted during handling of another event, else null
  event_path!: string[] // list of bus labels (name#id) that the event has been dispatched to, including the current bus
  event_result_type?: z.ZodTypeAny // optional zod schema to enforce the shape of return values from handlers
  event_results!: Map<string, EventResult<this>> // map of handler ids to EventResult objects for the event
  event_emitted_by_handler_id!: string | null // if event was emitted inside a handler while it was running, this is set to the enclosing handler's handler id, else null
  event_pending_bus_count!: number // number of buses that have accepted this event and not yet finished processing or removed it from their queues (for queue-jump processing)
  event_status!: 'pending' | 'started' | 'completed' // processing status of the event as a whole, no separate 'error' state because events can not error, only individual handlers can
  event_started_at?: string | null // ISO datetime string version of event_started_ts
  event_started_ts?: number | null // nanosecond monotonic version of event_started_at
  event_completed_at?: string | null // ISO datetime string version of event_completed_ts
  event_completed_ts?: number | null // nanosecond monotonic version of event_completed_at
  event_concurrency?: EventConcurrencyMode | null // concurrency mode for the event as a whole in relation to other events
  event_handler_concurrency?: EventHandlerConcurrencyMode | null // concurrency mode for the handlers within the event
  event_handler_completion?: EventHandlerCompletionMode | null // completion strategy: 'all' (default) waits for every handler, 'first' returns earliest non-undefined result and cancels the rest

  static event_type?: string // class name of the event, e.g. BaseEvent.extend("MyEvent").event_type === "MyEvent"
  static event_version = '0.0.1'
  static schema = BaseEventSchema // zod schema for the event data fields, used to parse and validate event data when creating a new event

  // internal runtime state
  bus?: EventBus // shortcut to the bus that dispatched this event, for event.bus.emit(event) auto-child tracking via proxy wrapping
  _event_original?: BaseEvent // underlying event object that was dispatched, if this is a bus-scoped proxy wrapping it
  _event_dispatch_context?: unknown | null // captured AsyncLocalStorage context at dispatch site, used to restore that context when running handlers

  _event_completed_signal: Deferred<this> | null
  _lock_for_event_handler: AsyncLock | null

  constructor(data: BaseEventInit<Record<string, unknown>> = {}) {
    const ctor = this.constructor as typeof BaseEvent & {
      event_version?: string
      event_result_type?: z.ZodTypeAny
    }
    const ctor_defaults = EVENT_CLASS_DEFAULTS.get(ctor) ?? {}
    const merged_data = {
      ...ctor_defaults,
      ...data,
    } as BaseEventInit<Record<string, unknown>>
    const event_type = merged_data.event_type ?? ctor.event_type ?? ctor.name
    const event_version = merged_data.event_version ?? ctor.event_version ?? '0.0.1'
    const raw_event_result_type = merged_data.event_result_type ?? ctor.event_result_type
    const event_result_type = normalizeEventResultType(raw_event_result_type)
    const event_id = merged_data.event_id ?? uuidv7()
    const { isostring: default_event_created_at, ts: default_event_created_ts } = BaseEvent.nextTimestamp()
    const event_created_at = merged_data.event_created_at ?? default_event_created_at
    const event_created_ts = merged_data.event_created_ts === undefined ? default_event_created_ts : merged_data.event_created_ts
    const event_timeout = merged_data.event_timeout ?? null

    const base_data = {
      ...merged_data,
      event_id,
      event_created_at,
      event_created_ts,
      event_type,
      event_version,
      event_timeout,
      event_result_type,
    }

    const schema = ctor.schema ?? BaseEventSchema
    const parsed = schema.parse(base_data) as BaseEventData & Record<string, unknown>

    Object.assign(this, parsed)

    const parsed_path = (parsed as { event_path?: string[] }).event_path
    this.event_path = Array.isArray(parsed_path) ? [...parsed_path] : []

    // load event results from potentially raw objects from JSON to proper EventResult objects
    this.event_results = hydrateEventResults(this, (parsed as { event_results?: unknown }).event_results)
    this.event_pending_bus_count =
      typeof (parsed as { event_pending_bus_count?: unknown }).event_pending_bus_count === 'number'
        ? Math.max(0, Number((parsed as { event_pending_bus_count?: number }).event_pending_bus_count))
        : 0
    const parsed_status = (parsed as { event_status?: unknown }).event_status
    this.event_status =
      parsed_status === 'pending' || parsed_status === 'started' || parsed_status === 'completed' ? parsed_status : 'pending'

    this.event_started_at = parsed.event_started_at ?? null
    this.event_started_ts = parsed.event_started_ts ?? null
    this.event_completed_at = parsed.event_completed_at ?? null
    this.event_completed_ts = parsed.event_completed_ts ?? null
    this.event_parent_id =
      typeof (parsed as { event_parent_id?: unknown }).event_parent_id === 'string'
        ? (parsed as { event_parent_id: string }).event_parent_id
        : null
    this.event_emitted_by_handler_id =
      typeof (parsed as { event_emitted_by_handler_id?: unknown }).event_emitted_by_handler_id === 'string'
        ? (parsed as { event_emitted_by_handler_id: string }).event_emitted_by_handler_id
        : null

    this.event_result_type = event_result_type
    this.event_created_ts = parsed.event_created_ts ?? event_created_ts

    this._event_completed_signal = null
    this._lock_for_event_handler = null
    this._event_dispatch_context = undefined
  }

  // "MyEvent#a48f"
  toString(): string {
    return `${this.event_type}#${this.event_id.slice(-4)}`
  }

  // get the next monotonic timestamp for global ordering of all operations
  static nextTimestamp(): { date: Date; isostring: string; ts: number } {
    const ts = performance.now()
    const date = new Date(performance.timeOrigin + ts)
    return { date, isostring: date.toISOString(), ts }
  }

  // main entry point for users to define their own event types
  // BaseEvent.extend("MyEvent", { some_custom_field: z.string(), event_result_type: z.string(), event_timeout: 25, ... }) -> MyEvent
  static extend<TShape extends z.ZodRawShape>(event_type: string, shape?: TShape): EventFactory<TShape, ResultSchemaFromShape<TShape>>
  static extend<TShape extends Record<string, unknown>>(
    event_type: string,
    shape?: TShape
  ): EventFactory<ZodShapeFrom<TShape>, ResultSchemaFromShape<TShape>>
  static extend<TShape extends Record<string, unknown>>(
    event_type: string,
    shape: TShape = {} as TShape
  ): EventFactory<ZodShapeFrom<TShape>, ResultSchemaFromShape<TShape>> {
    const raw_shape = shape as Record<string, unknown>
    const raw_event_result_type = raw_shape.event_result_type
    const event_result_type = normalizeEventResultType(raw_event_result_type)
    const event_version = typeof raw_shape.event_version === 'string' ? raw_shape.event_version : undefined
    const event_defaults = Object.fromEntries(
      Object.entries(raw_shape).filter(
        ([key, value]) => key !== 'event_result_type' && key !== 'event_version' && !(value instanceof z.ZodType)
      )
    )

    const zod_shape = extractZodShape(raw_shape)
    const full_schema = BaseEventSchema.extend(zod_shape)

    // create a new event class that extends BaseEvent and adds the custom fields
    class ExtendedEvent extends BaseEvent {
      static schema = full_schema as unknown as typeof BaseEvent.schema
      static event_type = event_type
      static event_version = event_version ?? BaseEvent.event_version
      static event_result_type = event_result_type

      constructor(data: EventInit<ZodShapeFrom<TShape>>) {
        super(data as BaseEventInit<Record<string, unknown>>)
      }
    }

    type FactoryResult = EventWithResultSchema<ResultSchemaFromShape<TShape>> & EventPayload<ZodShapeFrom<TShape>>

    function EventFactory(data: EventInit<ZodShapeFrom<TShape>>): FactoryResult {
      return new ExtendedEvent(data) as FactoryResult
    }

    EventFactory.schema = full_schema as EventSchema<ZodShapeFrom<TShape>>
    EventFactory.event_type = event_type
    EventFactory.event_version = event_version ?? BaseEvent.event_version
    EventFactory.event_result_type = event_result_type
    EventFactory.class = ExtendedEvent as unknown as new (
      data: EventInit<ZodShapeFrom<TShape>>
    ) => EventWithResultSchema<ResultSchemaFromShape<TShape>> & EventPayload<ZodShapeFrom<TShape>>
    EventFactory.fromJSON = (data: unknown) => (ExtendedEvent.fromJSON as (data: unknown) => FactoryResult)(data)
    EventFactory.prototype = ExtendedEvent.prototype
    EVENT_CLASS_DEFAULTS.set(ExtendedEvent, event_defaults)

    return EventFactory as unknown as EventFactory<ZodShapeFrom<TShape>, ResultSchemaFromShape<TShape>>
  }

  static fromJSON<T extends typeof BaseEvent>(this: T, data: unknown): InstanceType<T> {
    if (!data || typeof data !== 'object') {
      const schema = this.schema ?? BaseEventSchema
      const parsed = schema.parse(data)
      return new this(parsed) as InstanceType<T>
    }
    const record = { ...(data as Record<string, unknown>) }
    if (record.event_result_type !== undefined && record.event_result_type !== null) {
      record.event_result_type = normalizeEventResultType(record.event_result_type)
    }
    return new this(record as BaseEventInit<Record<string, unknown>>) as InstanceType<T>
  }

  static toJSONArray(events: Iterable<BaseEvent>): BaseEventJSON[] {
    return Array.from(events, (event) => {
      const original = event._event_original ?? event
      return original.toJSON()
    })
  }

  static fromJSONArray(data: unknown): BaseEvent[] {
    if (!Array.isArray(data)) {
      return []
    }
    return data.map((item) => BaseEvent.fromJSON(item))
  }

  toJSON(): BaseEventJSON {
    const record: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(this as unknown as Record<string, unknown>)) {
      if (key.startsWith('_') || key === 'bus' || key === 'event_results') continue
      if (value === undefined || typeof value === 'function') continue
      record[key] = value
    }
    const event_results = Array.from(this.event_results.values()).map((result) => result.toJSON())

    return {
      ...record,
      event_id: this.event_id,
      event_type: this.event_type,
      event_version: this.event_version,
      event_result_type: this.event_result_type ? toJsonSchema(this.event_result_type) : this.event_result_type,

      // static configuration options
      event_timeout: this.event_timeout,
      event_concurrency: this.event_concurrency,
      event_handler_concurrency: this.event_handler_concurrency,
      event_handler_completion: this.event_handler_completion,
      event_handler_slow_timeout: this.event_handler_slow_timeout,
      event_handler_timeout: this.event_handler_timeout,

      // mutable parent/child/bus tracking runtime state
      event_parent_id: this.event_parent_id,
      event_path: this.event_path,
      event_emitted_by_handler_id: this.event_emitted_by_handler_id,
      event_pending_bus_count: this.event_pending_bus_count,

      // mutable runtime status and timestamps
      event_status: this.event_status,
      event_created_at: this.event_created_at,
      event_created_ts: this.event_created_ts,
      event_started_at: this.event_started_at,
      event_started_ts: this.event_started_ts,
      event_completed_at: this.event_completed_at,
      event_completed_ts: this.event_completed_ts,

      // mutable result state
      ...(event_results.length > 0 ? { event_results } : {}),
    }
  }

  createSlowEventWarningTimer(): ReturnType<typeof setTimeout> | null {
    const event_slow_timeout = (this as { event_slow_timeout?: number | null }).event_slow_timeout ?? this.bus?.event_slow_timeout ?? null
    const event_warn_ms = event_slow_timeout === null ? null : event_slow_timeout * 1000
    if (event_warn_ms === null) {
      return null
    }
    const name = this.bus?.name ?? 'EventBus'
    return setTimeout(() => {
      if (this.event_status === 'completed') {
        return
      }
      const running_handler_count = [...this.event_results.values()].filter((result) => result.status === 'started').length
      const started_ts = this.event_started_ts ?? this.event_created_ts ?? performance.now()
      const elapsed_ms = Math.max(0, performance.now() - started_ts)
      const elapsed_seconds = (elapsed_ms / 1000).toFixed(2)
      console.warn(
        `[bubus] Slow event processing: ${name}.on(${this.event_type}#${this.event_id.slice(-4)}, ${running_handler_count} handlers) still running after ${elapsed_seconds}s`
      )
    }, event_warn_ms)
  }

  eventCreatePendingHandlerResults(bus: EventBus): Array<{
    handler: EventHandler
    result: EventResult
  }> {
    const original_event = this._event_original ?? this
    const scoped_event = bus.getEventProxyScopedToThisBus(original_event)
    const handlers = bus.getHandlersForEvent(original_event)
    return handlers.map((entry) => {
      const handler_id = entry.id
      const existing = original_event.event_results.get(handler_id)
      const result = existing ?? new EventResult({ event: scoped_event, handler: entry })
      if (!existing) {
        original_event.event_results.set(handler_id, result)
      } else if (existing.event !== scoped_event) {
        existing.event = scoped_event
      }
      return { handler: entry, result }
    })
  }

  private _collectPendingResults(
    original: BaseEvent,
    pending_entries?: Array<{
      handler: EventHandler
      result: EventResult
    }>
  ): EventResult[] {
    if (pending_entries) {
      return pending_entries.map((entry) => entry.result)
    }
    if (!this.bus?.id) {
      return Array.from(original.event_results.values())
    }
    return Array.from(original.event_results.values()).filter((result) => result.eventbus_id === this.bus!.id)
  }

  private _isFirstModeWinningResult(entry: EventResult): boolean {
    return entry.status === 'completed' && entry.result !== undefined && entry.result !== null && !(entry.result instanceof BaseEvent)
  }

  private _markFirstModeWinnerIfNeeded(original: BaseEvent, entry: EventResult, first_state: { found: boolean }): void {
    if (first_state.found) {
      return
    }
    if (!this._isFirstModeWinningResult(entry)) {
      return
    }
    first_state.found = true
    original.cancelEventHandlersForFirstMode(entry)
  }

  private async _runHandlerWithLock(original: BaseEvent, entry: EventResult): Promise<void> {
    if (!this.bus) {
      throw new Error('event has no bus attached')
    }
    await this.bus.locks.withHandlerLock(original, this.bus.event_handler_concurrency_default, async (handler_lock) => {
      await entry.runHandler(handler_lock)
    })
  }

  // Run all pending handler results for the current bus context.
  async runHandlers(
    pending_entries?: Array<{
      handler: EventHandler
      result: EventResult
    }>
  ): Promise<void> {
    const original = this._event_original ?? this
    const pending_results = this._collectPendingResults(original, pending_entries)
    if (pending_results.length === 0) {
      return
    }
    const resolved_completion = original.event_handler_completion ?? this.bus?.event_handler_completion_default ?? 'all'
    if (resolved_completion === 'first') {
      if (original.eventGetHandlerLock() !== null) {
        for (const entry of pending_results) {
          await this._runHandlerWithLock(original, entry)
          if (!this._isFirstModeWinningResult(entry)) {
            continue
          }
          original.cancelEventHandlersForFirstMode(entry)
          break
        }
        return
      }
      const first_state = { found: false }
      const handler_promises = pending_results.map((entry) => this._runHandlerWithLock(original, entry))
      const monitored = pending_results.map((entry, index) =>
        handler_promises[index].then(() => {
          this._markFirstModeWinnerIfNeeded(original, entry, first_state)
        })
      )
      await Promise.all(monitored)
      return
    } else {
      const handler_promises = pending_results.map((entry) => this._runHandlerWithLock(original, entry))
      await Promise.all(handler_promises)
    }
  }

  eventGetHandlerLock(default_concurrency?: EventHandlerConcurrencyMode): AsyncLock | null {
    const original = this._event_original ?? this
    const resolved =
      original.event_handler_concurrency ?? default_concurrency ?? original.bus?.event_handler_concurrency_default ?? 'serial'
    if (resolved === 'parallel') {
      return null
    }
    if (!original._lock_for_event_handler) {
      original._lock_for_event_handler = new AsyncLock(1)
    }
    return original._lock_for_event_handler
  }

  eventSetHandlerLock(lock: AsyncLock | null): void {
    const original = this._event_original ?? this
    original._lock_for_event_handler = lock
  }

  eventGetDispatchContext(): unknown | null | undefined {
    const original = this._event_original ?? this
    return original._event_dispatch_context
  }

  eventSetDispatchContext(dispatch_context: unknown | null | undefined): void {
    const original = this._event_original ?? this
    original._event_dispatch_context = dispatch_context
  }

  // Backward-compatible alias; prefer eventGetHandlerLock().
  getHandlerLock(default_concurrency?: EventHandlerConcurrencyMode): AsyncLock | null {
    return this.eventGetHandlerLock(default_concurrency)
  }

  // Get parent event object from event_parent_id (checks across all busses)
  get event_parent(): BaseEvent | undefined {
    const original = this._event_original ?? this
    const parent_id = original.event_parent_id
    if (!parent_id) {
      return undefined
    }
    return original.bus?.findEventById(parent_id) ?? undefined
  }

  // get all direct children of this event
  get event_children(): BaseEvent[] {
    const children: BaseEvent[] = []
    const seen = new Set<string>()
    for (const result of this.event_results.values()) {
      for (const child of result.event_children) {
        if (!seen.has(child.event_id)) {
          seen.add(child.event_id)
          children.push(child)
        }
      }
    }
    return children
  }

  // get all children grandchildren etc. recursively
  get event_descendants(): BaseEvent[] {
    const descendants: BaseEvent[] = []
    const visited = new Set<string>()
    const root_id = this.event_id
    const stack = [...this.event_children]

    while (stack.length > 0) {
      const child = stack.pop()
      if (!child) {
        continue
      }
      const child_id = child.event_id
      if (child_id === root_id) {
        continue
      }
      if (visited.has(child_id)) {
        continue
      }
      visited.add(child_id)
      descendants.push(child)
      if (child.event_children.length > 0) {
        stack.push(...child.event_children)
      }
    }

    return descendants
  }

  // force-abort processing of all pending descendants of an event regardless of whether they have already started
  eventCancelPendingChildProcessing(reason: unknown): void {
    const original = this._event_original ?? this
    const cancellation_cause =
      reason instanceof EventHandlerTimeoutError
        ? reason
        : reason instanceof EventHandlerCancelledError || reason instanceof EventHandlerAbortedError
          ? reason.cause instanceof Error
            ? reason.cause
            : reason
          : reason instanceof Error
            ? reason
            : new Error(String(reason))
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

      original_child.markCancelled(cancellation_cause)

      // Force-complete the child event. In JS we can't stop running async
      // handlers, but markCompleted() resolves the done() promise so callers
      // aren't blocked waiting for background work to finish. The background
      // handler's eventual markCompleted/markError is a no-op (terminal guard).
      if (original_child.event_status !== 'completed') {
        original_child.markCompleted()
      }
    }

    for (const child of original.event_children) {
      cancelChildEvent(child)
    }
  }

  // Cancel all handler results for an event except the winner, used by first() mode.
  // Cancels pending handlers immediately, aborts started handlers via signalAbort(),
  // and cancels any child events emitted by the losing handlers.
  cancelEventHandlersForFirstMode(winner: EventResult): void {
    const cause = new Error('first() resolved: another handler returned a result first')
    const bus_id = winner.eventbus_id

    for (const result of this.event_results.values()) {
      if (result === winner) continue
      if (result.eventbus_id !== bus_id) continue

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
          original_child.eventCancelPendingChildProcessing(cause)
          original_child.markCancelled(cause)
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

  // force-abort processing of this event regardless of whether it is pending or has already started
  markCancelled(cause: Error): void {
    const original = this._event_original ?? this
    if (!this.bus) {
      if (original.event_status !== 'completed') {
        original.markCompleted()
      }
      return
    }
    const path = Array.isArray(original.event_path) ? original.event_path : []
    const buses_to_cancel = new Set<string>(path)
    for (const bus of this.bus.all_instances) {
      if (!buses_to_cancel.has(bus.label)) {
        continue
      }

      const handler_entries = original.eventCreatePendingHandlerResults(bus)
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

      const removed = bus.removeEventFromPendingQueue(original)

      if (removed > 0 && !bus.isEventInFlightOrQueued(original.event_id)) {
        original.event_pending_bus_count = Math.max(0, original.event_pending_bus_count - 1)
      }

      if (updated || removed > 0) {
        original.markCompleted(false)
      }
    }

    if (original.event_status !== 'completed') {
      original.markCompleted()
    }
  }

  notifyEventParentsOfCompletion(): void {
    const original = this._event_original ?? this
    if (!this.bus) {
      return
    }
    const visited = new Set<string>()
    let parent_id = original.event_parent_id
    while (parent_id && !visited.has(parent_id)) {
      visited.add(parent_id)
      const parent = this.bus.findEventById(parent_id)
      if (!parent) {
        break
      }
      parent.markCompleted(false, false)
      if (parent.event_status !== 'completed') {
        break
      }
      parent_id = parent.event_parent_id
    }
  }

  // awaitable that triggers immediate (queue-jump) processing of the event on all buses where it is queued
  // use event.waitForCompletion() or event.finished() to wait for the event to be processed in normal queue order
  done(): Promise<this> {
    if (!this.bus) {
      return Promise.reject(new Error('event has no bus attached'))
    }
    if (this.event_status === 'completed') {
      return Promise.resolve(this)
    }
    // Always delegate to processEventImmediately — it walks up the parent event tree
    // to determine whether we're inside a handler (works cross-bus). If no
    // ancestor handler is in-flight, it falls back to waitForCompletion().
    return this.bus.processEventImmediately(this)
  }

  // clearer alias for done() to indicate that the event will be processed immediately
  // await bus.emit(event).immediate() is less ambiguous than await event.done()
  immediate(): Promise<this> {
    return this.done()
  }

  // returns the first non-undefined handler result value, cancelling remaining handlers
  // when any handler completes. Works with all event_handler_concurrency modes:
  //   parallel: races all handlers, returns first non-undefined, aborts the rest
  //   serial: runs handlers sequentially, returns first non-undefined, skips remaining
  first(): Promise<EventResultType<this> | undefined> {
    if (!this.bus) {
      return Promise.reject(new Error('event has no bus attached'))
    }
    const original = this._event_original ?? this
    original.event_handler_completion = 'first'
    return this.done().then((completed_event) => {
      const orig = completed_event._event_original ?? completed_event
      return Array.from(orig.event_results.values())
        .filter(
          (result) =>
            result.status === 'completed' && result.result !== undefined && result.result !== null && !(result.result instanceof BaseEvent)
        )
        .sort((a, b) => (a.completed_ts ?? 0) - (b.completed_ts ?? 0))
        .map((result) => result.result as EventResultType<this>)
        .at(0)
    })
  }

  // awaitable that waits for the event to be processed in normal queue order by the runloop
  waitForCompletion(): Promise<this> {
    if (this.event_status === 'completed') {
      return Promise.resolve(this)
    }
    this._notifyDoneListeners()
    return this._event_completed_signal!.promise
  }

  // convenience alias for await event.waitForCompletion()
  finished(): Promise<this> {
    return this.waitForCompletion()
  }

  markPending(): this {
    const original = this._event_original ?? this
    original.event_status = 'pending'
    original.event_started_at = null
    original.event_started_ts = null
    original.event_completed_at = null
    original.event_completed_ts = null
    original.event_results.clear()
    original.event_pending_bus_count = 0
    original.eventSetDispatchContext(undefined)
    original._event_completed_signal = null
    original._lock_for_event_handler = null
    original.bus = undefined
    return this
  }

  reset(): this {
    const original = this._event_original ?? this
    const ctor = original.constructor as typeof BaseEvent
    const fresh_event = ctor.fromJSON(original.toJSON()) as this
    fresh_event.event_id = uuidv7()
    return fresh_event.markPending()
  }

  markStarted(): void {
    const original = this._event_original ?? this
    if (original.event_status !== 'pending') {
      return
    }
    original.event_status = 'started'
    const { isostring: event_started_at, ts: event_started_ts } = BaseEvent.nextTimestamp()
    original.event_started_at = event_started_at
    original.event_started_ts = event_started_ts
    if (original.bus) {
      const bus_for_hook = original.bus
      const event_for_bus = bus_for_hook.getEventProxyScopedToThisBus(original)
      bus_for_hook.scheduleMicrotask(() => {
        void bus_for_hook.onEventChange(event_for_bus, 'started')
      })
    }
  }

  markCompleted(force: boolean = true, notify_parents: boolean = true): void {
    const original = this._event_original ?? this
    if (original.event_status === 'completed') {
      return
    }
    if (!force) {
      if (original.event_pending_bus_count > 0) {
        return
      }
      if (!original.eventAreAllChildrenComplete()) {
        return
      }
    }
    original.event_status = 'completed'
    const { isostring: event_completed_at, ts: event_completed_ts } = BaseEvent.nextTimestamp()
    original.event_completed_at = event_completed_at
    original.event_completed_ts = event_completed_ts
    if (original.bus) {
      const bus_for_hook = original.bus
      const event_for_bus = bus_for_hook.getEventProxyScopedToThisBus(original)
      bus_for_hook.scheduleMicrotask(() => {
        void bus_for_hook.onEventChange(event_for_bus, 'completed')
      })
    }
    original.eventSetDispatchContext(null)
    original._notifyDoneListeners()
    original._event_completed_signal!.resolve(original)
    original._event_completed_signal = null
    original.dropFromZeroHistoryBuses()
    if (notify_parents && original.bus) {
      original.notifyEventParentsOfCompletion()
    }
  }

  private dropFromZeroHistoryBuses(): void {
    if (!this.bus) {
      return
    }
    const original = this._event_original ?? this
    for (const bus of this.bus.all_instances) {
      if (bus.event_history.max_history_size !== 0) {
        continue
      }
      bus.removeEventFromHistory(original.event_id)
    }
  }

  get event_errors(): unknown[] {
    // const errors: unknown[] = []
    // for (const result of this.event_results.values()) {
    //   if (result.error !== undefined) {
    //     errors.push(result.error)
    //   }
    // }
    // return errors
    return (
      Array.from(this.event_results.values())
        // filter for events that have completed + have non-undefined error values
        .filter((event_result) => event_result.error !== undefined && event_result.completed_ts !== null)
        // sort by completion time
        .sort((event_result_a, event_result_b) => (event_result_a.completed_ts ?? 0) - (event_result_b.completed_ts ?? 0))
        // assemble array of flat error values
        .map((event_result) => event_result.error)
    )
  }

  // all non-undefined handler result values in completion order
  get all_results(): EventResultType<this>[] {
    return (
      Array.from(this.event_results.values())
        // only events that have completed + have non-undefined result values
        .filter((event_result) => event_result.completed_ts !== null && event_result.result !== undefined)
        // sort by completion time
        .sort((event_result_a, event_result_b) => (event_result_a.completed_ts ?? 0) - (event_result_b.completed_ts ?? 0))
        // assemble array of flat parsed handler return values
        .map((event_result) => event_result.result as EventResultType<this>)
    )
  }

  // Returns the first non-undefined completed handler result, sorted by completion time.
  // Useful after first() or done() to get the winning result value.
  get event_result(): EventResultType<this> | undefined {
    return this.all_results.at(0)
  }

  // Returns the last non-undefined completed handler result, sorted by completion time.
  // Useful after first() or done() to get the winning result value.
  get last_result(): EventResultType<this> | undefined {
    return this.all_results.at(-1)
  }

  eventAreAllChildrenComplete(): boolean {
    for (const descendant of this.event_descendants) {
      if (descendant.event_status !== 'completed') {
        return false
      }
    }
    return true
  }

  private _notifyDoneListeners(): void {
    if (this._event_completed_signal) {
      return
    }
    this._event_completed_signal = withResolvers<this>()
  }

  // Break internal reference chains so a completed event can be GC'd when
  // Evicted from event_history. Called by EventHistory.trimEventHistory().
  _gc(): void {
    this._event_completed_signal = null
    this.eventSetDispatchContext(null)
    this.bus = undefined
    this._lock_for_event_handler = null
    for (const result of this.event_results.values()) {
      result.event_children = []
    }
    this.event_results.clear()
  }
}

const hydrateEventResults = <TEvent extends BaseEvent>(event: TEvent, raw_event_results: unknown): Map<string, EventResult<TEvent>> => {
  const event_results = new Map<string, EventResult<TEvent>>()
  if (!Array.isArray(raw_event_results)) {
    return event_results
  }
  for (const item of raw_event_results) {
    const result = EventResult.fromJSON(event, item)
    const map_key = typeof result.handler_id === 'string' && result.handler_id.length > 0 ? result.handler_id : result.id
    event_results.set(map_key, result)
  }
  return event_results
}
