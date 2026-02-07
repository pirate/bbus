import { z } from 'zod'
import { v7 as uuidv7 } from 'uuid'

import type { EventBus } from './event_bus.js'
import { EventResult } from './event_result.js'
import type { ConcurrencyMode, Deferred } from './lock_manager.js'
import { CONCURRENCY_MODES, withResolvers } from './lock_manager.js'

export const BaseEventSchema = z
  .object({
    event_id: z.string().uuid(),
    event_created_at: z.string().datetime(),
    event_type: z.string(),
    event_timeout: z.number().positive().nullable(),
    event_parent_id: z.string().uuid().optional(),
    event_path: z.array(z.string()).optional(),
    event_result_type: z.string().optional(),
    event_result_schema: z.unknown().optional(),
    event_concurrency: z.enum(CONCURRENCY_MODES).optional(),
    handler_concurrency: z.enum(CONCURRENCY_MODES).optional(),
  })
  .passthrough()

export type BaseEventData = z.infer<typeof BaseEventSchema>
type BaseEventFields = Pick<
  BaseEventData,
  | 'event_id'
  | 'event_created_at'
  | 'event_type'
  | 'event_timeout'
  | 'event_parent_id'
  | 'event_result_type'
  | 'event_result_schema'
  | 'event_concurrency'
  | 'handler_concurrency'
>

export type BaseEventInit<TFields extends Record<string, unknown>> = TFields & Partial<BaseEventFields>

type BaseEventSchemaShape = typeof BaseEventSchema.shape

export type EventSchema<TShape extends z.ZodRawShape> = z.ZodObject<BaseEventSchemaShape & TShape>

type EventInput<TShape extends z.ZodRawShape> = z.input<EventSchema<TShape>>
export type EventInit<TShape extends z.ZodRawShape> = Omit<EventInput<TShape>, keyof BaseEventFields> & Partial<BaseEventFields>

export type EventFactory<TShape extends z.ZodRawShape> = {
  (data: EventInit<TShape>): BaseEvent & z.infer<EventSchema<TShape>>
  new (data: EventInit<TShape>): BaseEvent & z.infer<EventSchema<TShape>>
  schema: EventSchema<TShape>
  event_type?: string
  event_result_schema?: z.ZodTypeAny
  event_result_type?: string
  fromJSON?: (data: unknown) => BaseEvent & z.infer<EventSchema<TShape>>
}

type ZodShapeFrom<TShape extends Record<string, unknown>> = {
  [K in keyof TShape as K extends 'event_result_schema' | 'event_result_type' | 'event_result_schema_json'
    ? never
    : TShape[K] extends z.ZodTypeAny
      ? K
      : never]: Extract<TShape[K], z.ZodTypeAny>
}

export class BaseEvent {
  event_id!: string                   // unique uuidv7 identifier for the event
  event_created_at!: string           // ISO datetime string version of event_created_ts
  event_created_ts!: number           // nanosecond monotonic version of event_created_at
  event_type!: string                 // should match the class name of the event, e.g. BaseEvent.extend("MyEvent").event_type === "MyEvent"
  event_timeout!: number | null       // maximum time in seconds that each handler for the event is allowed to run before it is aborted
  event_parent_id?: string            // id of the parent event that triggered this event, if this event was emitted during handling of another event
  event_path!: string[]               // list of bus names that the event has been dispatched to, including the current bus
  event_result_schema?: z.ZodTypeAny  // optional zod schema to enforce the shape of return values from handlers
  event_result_type?: string          // optional string identifier of the type of the return values from handlers, to make it easier to reference common shapes across networkboundaries e.g. ScreenshotEventResultType
  event_results!: Map<string, EventResult>
  event_emitted_by_handler_id?: string  // if event was emitted inside a handler while it was running, this will be set to the enclosing handler's handler id
  event_pending_bus_count!: number // Number of buses that have accepted this event and not yet finished processing or removed it from their queues.
  event_status!: 'pending' | 'started' | 'completed'
  event_started_at?: string
  event_started_ts?: number
  event_completed_at?: string
  event_completed_ts?: number
  event_concurrency?: ConcurrencyMode
  handler_concurrency?: ConcurrencyMode
  
  bus?: EventBus                       // shortcut to the bus that dispatched this event, for event.bus.dispatch(event) auto-child tracking via proxy wrapping
  _original_event?: BaseEvent          // underlying event object that was dispatched, if this is a bus-scoped proxy wrapping it
  _dispatch_context?: unknown | null   // captured AsyncLocalStorage context at dispatch site, used to restore that context when running handlers

  static schema = BaseEventSchema
  static event_type?: string

  _done: Deferred<this> | null

  constructor(data: BaseEventInit<Record<string, unknown>> = {}) {
    const ctor = this.constructor as typeof BaseEvent & {
      event_result_schema?: z.ZodTypeAny
      event_result_type?: string
    }
    const event_type = data.event_type ?? ctor.event_type ?? ctor.name
    const event_result_schema = (data.event_result_schema ?? ctor.event_result_schema) as z.ZodTypeAny | undefined
    const event_result_type = data.event_result_type ?? ctor.event_result_type
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
    this.event_pending_bus_count = 0
    this.event_status = 'pending'
    this.event_result_schema = event_result_schema
    this.event_result_type = event_result_type
    this.event_results = new Map()
    this.event_created_ts = event_created_ts

    this._done = null
    this._dispatch_context = undefined
  }

  static nextTimestamp(): { date: Date; isostring: string; ts: number } {
    const ts = performance.now()
    const date = new Date(performance.timeOrigin + ts)
    return { date, isostring: date.toISOString(), ts }
  }

  static extend<TShape extends z.ZodRawShape>(event_type: string, shape?: TShape): EventFactory<TShape>
  static extend<TShape extends Record<string, unknown>>(event_type: string, shape?: TShape): EventFactory<ZodShapeFrom<TShape>>
  static extend<TShape extends Record<string, unknown>>(event_type: string, shape: TShape = {} as TShape): EventFactory<ZodShapeFrom<TShape>> {
    const raw_shape = shape as Record<string, unknown>

    const event_result_schema = is_zod_schema(raw_shape.event_result_schema) ? (raw_shape.event_result_schema as z.ZodTypeAny) : undefined
    const event_result_type = typeof raw_shape.event_result_type === 'string' ? raw_shape.event_result_type : undefined

    const zod_shape = extract_zod_shape(raw_shape)
    const full_schema = BaseEventSchema.extend(zod_shape)

    class ExtendedEvent extends BaseEvent {
      static schema = full_schema as unknown as typeof BaseEvent.schema
      static event_type = event_type
      static event_result_schema = event_result_schema
      static event_result_type = event_result_type

      constructor(data: EventInit<ZodShapeFrom<TShape>>) {
        super(data as BaseEventInit<Record<string, unknown>>)
      }
    }

    type FactoryResult = BaseEvent & z.infer<EventSchema<ZodShapeFrom<TShape>>>

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

    return EventFactory as unknown as EventFactory<ZodShapeFrom<TShape>>
  }

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
    if (record.event_result_schema && !is_zod_schema(record.event_result_schema)) {
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
      event_type: this.event_type,
      event_timeout: this.event_timeout,
      event_parent_id: this.event_parent_id,
      event_path: this.event_path,
      event_result_type: this.event_result_type,
      event_concurrency: this.event_concurrency,
      handler_concurrency: this.handler_concurrency,
      event_result_schema: this.event_result_schema ? to_json_schema(this.event_result_schema) : this.event_result_schema,
    }
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
    const descendants: BaseEvent[] = [];
    const visited = new Set<string>();
    const root_id = this.event_id;
    const stack = [...this.event_children];

    while (stack.length > 0) {
      const child = stack.pop();
      if (!child) {
        continue;
      }
      const child_id = child.event_id;
      if (child_id === root_id) {
        continue;
      }
      if (visited.has(child_id)) {
        continue;
      }
      visited.add(child_id);
      descendants.push(child);
      if (child.event_children.length > 0) {
        stack.push(...child.event_children);
      }
    }

    return descendants;
  }

  // awaitable to trigger immediate processing of the event on all buses where it is queued
  done(): Promise<this> {
    if (!this.bus) {
      return Promise.reject(new Error('event has no bus attached'))
    }
    if (this.event_status === 'completed') {
      return Promise.resolve(this)
    }
    // Always delegate to _runImmediately — it walks up the parent event tree
    // to determine whether we're inside a handler (works cross-bus). If no
    // ancestor handler is in-flight, it falls back to waitForCompletion().
    const runner_bus = this.bus as {
      _runImmediately: (event: BaseEvent) => Promise<BaseEvent>
    }
    return runner_bus._runImmediately(this) as Promise<this>
  }

  waitForCompletion(): Promise<this> {
    if (this.event_status === 'completed') {
      return Promise.resolve(this)
    }
    this._notifyDoneListeners()
    return this._done!.promise
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
    this._dispatch_context = null
    this._notifyDoneListeners()
    this._done!.resolve(this)
    this._done = null
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

  eventAreAllChildrenComplete(): boolean {
    for (const descendant of this.event_descendants) {
      if (descendant.event_status !== 'completed') {
        return false
      }
    }
    return true
  }

  _notifyDoneListeners(): void {
    if (this._done) {
      return
    }
    this._done = withResolvers<this>()
  }

  // Break internal reference chains so a completed event can be GC'd when
  // evicted from event_history. Called by EventBus.trimHistory().
  _gc(): void {
    this._done = null
    this._dispatch_context = null
    this.bus = undefined
    for (const result of this.event_results.values()) {
      result.event_children = []
    }
    this.event_results.clear()
  }
}

const is_zod_schema = (value: unknown): value is z.ZodTypeAny => !!value && typeof (value as z.ZodTypeAny).safeParse === 'function'

const extract_zod_shape = (raw: Record<string, unknown>): z.ZodRawShape => {
  const shape: Record<string, z.ZodTypeAny> = {}
  for (const [key, value] of Object.entries(raw)) {
    if (key === 'event_result_schema' || key === 'event_result_type') {
      continue
    }
    if (is_zod_schema(value)) {
      shape[key] = value
    }
  }
  return shape as z.ZodRawShape
}

const to_json_schema = (schema: unknown): unknown => {
  if (!schema) {
    return schema
  }
  if (!is_zod_schema(schema)) {
    return schema
  }
  const zod_any = z as unknown as { toJSONSchema?: (schema: z.ZodTypeAny) => unknown }
  if (typeof zod_any.toJSONSchema === 'function') {
    return zod_any.toJSONSchema(schema)
  }
  return undefined
}
