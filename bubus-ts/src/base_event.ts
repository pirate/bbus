import { z } from 'zod'
import { v7 as uuidv7 } from 'uuid'

import type { EventBus } from './event_bus.js'
import { EventResult } from './event_result.js'
import type { ConcurrencyMode, Deferred } from './lock_manager.js'
import { CONCURRENCY_MODES, withResolvers } from './lock_manager.js'
import { extractZodShape, getStringTypeName, isZodSchema, toJsonSchema } from './types.js'
import type { EventResultType } from './types.js'

export const BaseEventSchema = z
  .object({
    event_id: z.string().uuid(),
    event_created_at: z.string().datetime(),
    event_created_ts: z.number().optional(),
    event_type: z.string(),
    event_timeout: z.number().positive().nullable(),
    event_parent_id: z.string().uuid().optional(),
    event_path: z.array(z.string()).optional(),
    event_result_type: z.string().optional(),
    event_result_schema: z.unknown().optional(),
    event_emitted_by_handler_id: z.string().uuid().optional(),
    event_pending_bus_count: z.number().nonnegative().optional(),
    event_status: z.enum(['pending', 'started', 'completed']).optional(),
    event_started_at: z.string().datetime().optional(),
    event_started_ts: z.number().optional(),
    event_completed_at: z.string().datetime().optional(),
    event_completed_ts: z.number().optional(),
    event_results: z.array(z.unknown()).optional(),
    event_concurrency: z.enum(CONCURRENCY_MODES).optional(),
    event_handler_concurrency: z.enum(CONCURRENCY_MODES).optional(),
  })
  .loose()

export type BaseEventData = z.infer<typeof BaseEventSchema>
type BaseEventFields = Pick<
  BaseEventData,
  | 'event_id'
  | 'event_created_at'
  | 'event_created_ts'
  | 'event_type'
  | 'event_timeout'
  | 'event_parent_id'
  | 'event_path'
  | 'event_result_type'
  | 'event_result_schema'
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
>

export type BaseEventInit<TFields extends Record<string, unknown>> = TFields & Partial<BaseEventFields>

type BaseEventSchemaShape = typeof BaseEventSchema.shape

export type EventSchema<TShape extends z.ZodRawShape> = z.ZodObject<BaseEventSchemaShape & TShape>
type EventPayload<TShape extends z.ZodRawShape> = z.infer<z.ZodObject<TShape>>

type EventInput<TShape extends z.ZodRawShape> = z.input<EventSchema<TShape>>
export type EventInit<TShape extends z.ZodRawShape> = Omit<EventInput<TShape>, keyof BaseEventFields> & Partial<BaseEventFields>

type EventWithResult<TResult> = BaseEvent & { __event_result_type__?: TResult }

type ResultTypeFromShape<TShape> = TShape extends { event_result_schema: infer S }
  ? S extends z.ZodTypeAny
    ? z.infer<S>
    : unknown
  : unknown

export type EventFactory<TShape extends z.ZodRawShape, TResult = unknown> = {
  (data: EventInit<TShape>): EventWithResult<TResult> & EventPayload<TShape>
  new (data: EventInit<TShape>): EventWithResult<TResult> & EventPayload<TShape>
  schema: EventSchema<TShape>
  event_type?: string
  event_result_schema?: z.ZodTypeAny
  event_result_type?: string
  fromJSON?: (data: unknown) => EventWithResult<TResult> & EventPayload<TShape>
}

type ZodShapeFrom<TShape extends Record<string, unknown>> = {
  [K in keyof TShape as K extends 'event_result_schema' | 'event_result_type' | 'event_result_schema_json'
    ? never
    : TShape[K] extends z.ZodTypeAny
      ? K
      : never]: Extract<TShape[K], z.ZodTypeAny>
}

export class BaseEvent {
  // event metadata fields
  event_id!: string // unique uuidv7 identifier for the event
  event_created_at!: string // ISO datetime string version of event_created_at
  event_created_ts!: number // nanosecond monotonic version of event_created_at
  event_type!: string // should match the class name of the event, e.g. BaseEvent.extend("MyEvent").event_type === "MyEvent"
  event_timeout!: number | null // maximum time in seconds that the event is allowed to run before it is aborted
  event_parent_id?: string // id of the parent event that triggered this event, if this event was emitted during handling of another event
  event_path!: string[] // list of bus names that the event has been dispatched to, including the current bus
  event_result_schema?: z.ZodTypeAny // optional zod schema to enforce the shape of return values from handlers
  event_result_type?: string // optional string identifier of the type of the return values from handlers, to make it easier to reference common shapes across networkboundaries e.g. ScreenshotEventResultType
  event_results!: Map<string, EventResult<this>> // map of handler ids to EventResult objects for the event
  event_emitted_by_handler_id?: string // if event was emitted inside a handler while it was running, this will be set to the enclosing handler's handler id
  event_pending_bus_count!: number // number of buses that have accepted this event and not yet finished processing or removed it from their queues (for queue-jump processing)
  event_status!: 'pending' | 'started' | 'completed' // processing status of the event as a whole, no separate 'error' state because events can not error, only individual handlers can
  event_started_at?: string // ISO datetime string version of event_started_ts
  event_started_ts?: number // nanosecond monotonic version of event_started_at
  event_completed_at?: string // ISO datetime string version of event_completed_ts
  event_completed_ts?: number // nanosecond monotonic version of event_completed_at
  event_concurrency?: ConcurrencyMode // concurrency mode for the event as a whole in relation to other events
  event_handler_concurrency?: ConcurrencyMode // concurrency mode for the handlers within the event

  static event_type?: string // class name of the event, e.g. BaseEvent.extend("MyEvent").event_type === "MyEvent"
  static schema = BaseEventSchema // zod schema for the event data fields, used to parse and validate event data when creating a new event

  // internal runtime state
  bus?: EventBus // shortcut to the bus that dispatched this event, for event.bus.dispatch(event) auto-child tracking via proxy wrapping
  _event_original?: BaseEvent // underlying event object that was dispatched, if this is a bus-scoped proxy wrapping it
  _event_dispatch_context?: unknown | null // captured AsyncLocalStorage context at dispatch site, used to restore that context when running handlers

  _event_done_signal: Deferred<this> | null

  // first() mode: when set, processEvent cancels remaining handlers after the first non-undefined result
  _first_mode: boolean

  constructor(data: BaseEventInit<Record<string, unknown>> = {}) {
    const ctor = this.constructor as typeof BaseEvent & {
      event_result_schema?: z.ZodTypeAny
      event_result_type?: string
    }
    const event_type = data.event_type ?? ctor.event_type ?? ctor.name
    const event_result_schema = (data.event_result_schema ?? ctor.event_result_schema) as z.ZodTypeAny | undefined
    const event_result_type = data.event_result_type ?? ctor.event_result_type ?? getStringTypeName(event_result_schema)
    const event_id = data.event_id ?? uuidv7()
    const { isostring: default_event_created_at, ts: event_created_ts } = BaseEvent.nextTimestamp()
    const event_created_at = data.event_created_at ?? default_event_created_at
    const event_timeout = data.event_timeout ?? null

    const base_data = {
      ...data,
      event_id,
      event_created_at,
      event_type,
      event_timeout,
      event_result_schema,
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

    this.event_started_at =
      typeof (parsed as { event_started_at?: unknown }).event_started_at === 'string'
        ? (parsed as { event_started_at: string }).event_started_at
        : undefined
    this.event_started_ts =
      typeof (parsed as { event_started_ts?: unknown }).event_started_ts === 'number'
        ? (parsed as { event_started_ts: number }).event_started_ts
        : undefined
    this.event_completed_at =
      typeof (parsed as { event_completed_at?: unknown }).event_completed_at === 'string'
        ? (parsed as { event_completed_at: string }).event_completed_at
        : undefined
    this.event_completed_ts =
      typeof (parsed as { event_completed_ts?: unknown }).event_completed_ts === 'number'
        ? (parsed as { event_completed_ts: number }).event_completed_ts
        : undefined
    this.event_emitted_by_handler_id =
      typeof (parsed as { event_emitted_by_handler_id?: unknown }).event_emitted_by_handler_id === 'string'
        ? (parsed as { event_emitted_by_handler_id: string }).event_emitted_by_handler_id
        : undefined

    this.event_result_schema = event_result_schema
    this.event_result_type = event_result_type
    this.event_created_ts =
      typeof (parsed as { event_created_ts?: unknown }).event_created_ts === 'number'
        ? (parsed as { event_created_ts: number }).event_created_ts
        : event_created_ts

    this._event_done_signal = null
    this._event_dispatch_context = undefined
    this._first_mode = false
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
  // BaseEvent.extend("MyEvent", { some_custom_field: z.string(), event_result_schema: z.string(), event_timeout: 25, ... }) -> MyEvent
  static extend<TShape extends z.ZodRawShape>(event_type: string, shape?: TShape): EventFactory<TShape, ResultTypeFromShape<TShape>>
  static extend<TShape extends Record<string, unknown>>(
    event_type: string,
    shape?: TShape
  ): EventFactory<ZodShapeFrom<TShape>, ResultTypeFromShape<TShape>>
  static extend<TShape extends Record<string, unknown>>(
    event_type: string,
    shape: TShape = {} as TShape
  ): EventFactory<ZodShapeFrom<TShape>, ResultTypeFromShape<TShape>> {
    const raw_shape = shape as Record<string, unknown>

    const event_result_schema = isZodSchema(raw_shape.event_result_schema) ? (raw_shape.event_result_schema as z.ZodTypeAny) : undefined
    const explicit_event_result_type = typeof raw_shape.event_result_type === 'string' ? raw_shape.event_result_type : undefined
    const event_result_type = explicit_event_result_type ?? getStringTypeName(event_result_schema)

    const zod_shape = extractZodShape(raw_shape)
    const full_schema = BaseEventSchema.extend(zod_shape)

    // create a new event class that extends BaseEvent and adds the custom fields
    class ExtendedEvent extends BaseEvent {
      static schema = full_schema as unknown as typeof BaseEvent.schema
      static event_type = event_type
      static event_result_schema = event_result_schema
      static event_result_type = event_result_type

      constructor(data: EventInit<ZodShapeFrom<TShape>>) {
        super(data as BaseEventInit<Record<string, unknown>>)
      }
    }

    type FactoryResult = EventWithResult<ResultTypeFromShape<TShape>> & EventPayload<ZodShapeFrom<TShape>>

    function EventFactory(data: EventInit<ZodShapeFrom<TShape>>): FactoryResult {
      return new ExtendedEvent(data) as FactoryResult
    }

    EventFactory.schema = full_schema as EventSchema<ZodShapeFrom<TShape>>
    EventFactory.event_type = event_type
    EventFactory.event_result_schema = event_result_schema
    EventFactory.event_result_type = event_result_type
    EventFactory.fromJSON = (data: unknown) => (ExtendedEvent.fromJSON as (data: unknown) => FactoryResult)(data)
    EventFactory.prototype = ExtendedEvent.prototype
    ;(EventFactory as unknown as { class: typeof ExtendedEvent }).class = ExtendedEvent

    return EventFactory as unknown as EventFactory<ZodShapeFrom<TShape>, ResultTypeFromShape<TShape>>
  }

  // parse raw event data into a new event object
  static parse<T extends typeof BaseEvent>(this: T, data: unknown): InstanceType<T> {
    const schema = this.schema ?? BaseEventSchema
    const parsed = schema.parse(data)
    return new this(parsed) as InstanceType<T>
  }

  static fromJSON<T extends typeof BaseEvent>(this: T, data: unknown): InstanceType<T> {
    if (!data || typeof data !== 'object') {
      return this.parse(data)
    }
    const record = { ...(data as Record<string, unknown>) }
    if (record.event_result_schema && !isZodSchema(record.event_result_schema)) {
      const zod_any = z as unknown as { fromJSONSchema?: (schema: unknown) => z.ZodTypeAny }
      if (typeof zod_any.fromJSONSchema === 'function') {
        record.event_result_schema = zod_any.fromJSONSchema(record.event_result_schema)
      }
    }
    return new this(record as BaseEventInit<Record<string, unknown>>) as InstanceType<T>
  }

  toJSON(): BaseEventData {
    return {
      event_id: this.event_id,
      event_created_at: this.event_created_at,
      event_created_ts: this.event_created_ts,
      event_type: this.event_type,
      event_timeout: this.event_timeout,
      event_parent_id: this.event_parent_id,
      event_path: this.event_path,
      event_result_type: this.event_result_type,
      event_emitted_by_handler_id: this.event_emitted_by_handler_id,
      event_pending_bus_count: this.event_pending_bus_count,
      event_status: this.event_status,
      event_started_at: this.event_started_at,
      event_started_ts: this.event_started_ts,
      event_completed_at: this.event_completed_at,
      event_completed_ts: this.event_completed_ts,
      event_results: Array.from(this.event_results.values()).map((result) => result.toJSON()),
      event_concurrency: this.event_concurrency,
      event_handler_concurrency: this.event_handler_concurrency,
      event_result_schema: this.event_result_schema ? toJsonSchema(this.event_result_schema) : this.event_result_schema,
    }
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
    const runner_bus = this.bus as {
      processEventImmediately: (event: BaseEvent) => Promise<BaseEvent>
    }
    return runner_bus.processEventImmediately(this) as Promise<this>
  }

  // clearer alias for done() to indicate that the event will be processed immediately
  // await bus.dispatch(event).immediate() is less ambiguous than await event.done()
  immediate(): Promise<this> {
    return this.done()
  }

  // returns the first non-undefined handler result value, cancelling remaining handlers
  // when any handler completes. Works with all event_handler_concurrency modes:
  //   parallel: races all handlers, returns first non-undefined, aborts the rest
  //   bus-serial/global-serial: runs handlers sequentially, returns first non-undefined, skips remaining
  first(): Promise<EventResultType<this> | undefined> {
    if (!this.bus) {
      return Promise.reject(new Error('event has no bus attached'))
    }
    const original = this._event_original ?? this
    original._first_mode = true
    return this.done().then((completed_event) => {
      const orig = completed_event._event_original ?? completed_event
      return orig.first_result as EventResultType<this> | undefined
    })
  }

  // awaitable that waits for the event to be processed in normal queue order by the runloop
  waitForCompletion(): Promise<this> {
    if (this.event_status === 'completed') {
      return Promise.resolve(this)
    }
    this._notifyDoneListeners()
    return this._event_done_signal!.promise
  }

  // convenience alias for await event.waitForCompletion()
  finished(): Promise<this> {
    return this.waitForCompletion()
  }

  markStarted(): void {
    if (this.event_status !== 'pending') {
      return
    }
    this.event_status = 'started'
    const { isostring: event_started_at, ts: event_started_ts } = BaseEvent.nextTimestamp()
    this.event_started_at = event_started_at
    this.event_started_ts = event_started_ts
  }

  markCompleted(force: boolean = true): void {
    if (this.event_status === 'completed') {
      return
    }
    if (!force) {
      if (this.event_pending_bus_count > 0) {
        return
      }
      if (!this.eventAreAllChildrenComplete()) {
        return
      }
    }
    this.event_status = 'completed'
    const { isostring: event_completed_at, ts: event_completed_ts } = BaseEvent.nextTimestamp()
    this.event_completed_at = event_completed_at
    this.event_completed_ts = event_completed_ts
    this._event_dispatch_context = null
    this._notifyDoneListeners()
    this._event_done_signal!.resolve(this)
    this._event_done_signal = null
  }

  get event_errors(): unknown[] {
    const errors: unknown[] = []
    for (const result of this.event_results.values()) {
      if (result.error !== undefined) {
        errors.push(result.error)
      }
    }
    return errors
  }

  // Returns the first non-undefined completed handler result, sorted by completion time.
  // Useful after first() or done() to get the winning result value.
  get first_result(): EventResultType<this> | undefined {
    const completed = Array.from(this.event_results.values())
      .filter((r): r is EventResult<this> & { completed_ts: number } =>
        r.status === 'completed' && r.result !== undefined && typeof r.completed_ts === 'number'
      )
      .sort((a, b) => a.completed_ts - b.completed_ts)
    return completed.length > 0 ? completed[0].result as EventResultType<this> : undefined
  }

  eventAreAllChildrenComplete(): boolean {
    for (const descendant of this.event_descendants) {
      if (descendant.event_status !== 'completed') {
        return false
      }
    }
    return true
  }

  _notifyDoneListeners(): void {
    if (this._event_done_signal) {
      return
    }
    this._event_done_signal = withResolvers<this>()
  }

  // Break internal reference chains so a completed event can be GC'd when
  // evicted from event_history. Called by EventBus.trimHistory().
  _gc(): void {
    this._event_done_signal = null
    this._event_dispatch_context = null
    this.bus = undefined
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
    if (!result) {
      continue
    }
    const map_key = typeof result.handler_id === 'string' && result.handler_id.length > 0 ? result.handler_id : result.id
    event_results.set(map_key, result)
  }
  return event_results
}
