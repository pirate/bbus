import { z } from "zod";
import { v7 as uuidv7 } from "uuid";

import type { EventBus } from "./event_bus.js";
import { EventResult } from "./event_result.js";
import type { ConcurrencyMode, Deferred } from "./semaphores.js";
import { CONCURRENCY_MODES, withResolvers } from "./semaphores.js";


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
    handler_concurrency: z.enum(CONCURRENCY_MODES).optional()
  })
  .passthrough();

export type BaseEventData = z.infer<typeof BaseEventSchema>;
type BaseEventFields = Pick<
  BaseEventData,
  | "event_id"
  | "event_created_at"
  | "event_type"
  | "event_timeout"
  | "event_parent_id"
  | "event_result_type"
  | "event_result_schema"
  | "event_concurrency"
  | "handler_concurrency"
>;

export type BaseEventInit<TFields extends Record<string, unknown>> = TFields &
  Partial<BaseEventFields>;

type BaseEventSchemaShape = typeof BaseEventSchema.shape;

export type EventSchema<TShape extends z.ZodRawShape> = z.ZodObject<
  BaseEventSchemaShape & TShape
>;

type EventInput<TShape extends z.ZodRawShape> = z.input<EventSchema<TShape>>;
export type EventInit<TShape extends z.ZodRawShape> = Omit<EventInput<TShape>, keyof BaseEventFields> &
  Partial<BaseEventFields>;

export type EventFactory<TShape extends z.ZodRawShape> = {
  (data: EventInit<TShape>): BaseEvent & z.infer<EventSchema<TShape>>;
  new (data: EventInit<TShape>): BaseEvent & z.infer<EventSchema<TShape>>;
  schema: EventSchema<TShape>;
  event_type?: string;
  event_result_schema?: z.ZodTypeAny;
  event_result_type?: string;
  fromJSON?: (data: unknown) => BaseEvent & z.infer<EventSchema<TShape>>;
};

type ZodShapeFrom<TShape extends Record<string, unknown>> = {
  [K in keyof TShape as K extends
    | "event_result_schema"
    | "event_result_type"
    | "event_result_schema_json"
    ? never
    : TShape[K] extends z.ZodTypeAny
    ? K
    : never]: Extract<TShape[K], z.ZodTypeAny>;
};

export class BaseEvent {
  static _last_timestamp_ms = 0;
  event_id: string;
  event_created_at: string;
  event_type: string;
  event_timeout: number | null;
  event_parent_id?: string;
  event_path: string[];
  event_factory?: Function;
  event_result_schema?: z.ZodTypeAny;
  event_result_type?: string;
  event_results: Map<string, EventResult>;
  event_emitted_by_handler_id?: string;
  event_pending_buses: number;
  event_status: "pending" | "started" | "completed";
  event_created_at_ms: number;
  event_started_at?: string;
  event_completed_at?: string;
  event_errors: unknown[];
  bus?: EventBus;
  event_concurrency?: ConcurrencyMode;
  handler_concurrency?: ConcurrencyMode;
  _original_event?: BaseEvent;
  _dispatch_context?: unknown | null;

  static schema = BaseEventSchema;
  static event_type?: string;

  _done: Deferred<this> | null;

  constructor(data: BaseEventInit<Record<string, unknown>> = {}) {
    const ctor = this.constructor as typeof BaseEvent & {
      factory?: Function;
      event_result_schema?: z.ZodTypeAny;
      event_result_type?: string;
    };
    const event_type = data.event_type ?? ctor.event_type ?? ctor.name;
    const event_result_schema = data.event_result_schema ?? ctor.event_result_schema;
    const event_result_type = data.event_result_type ?? ctor.event_result_type;
    const event_id = data.event_id ?? uuidv7();
    const event_created_at =
      data.event_created_at ?? new Date().toISOString();
    const event_timeout = data.event_timeout ?? null;

    const base_data = {
      ...data,
      event_id,
      event_created_at,
      event_type,
      event_timeout,
      event_result_schema,
      event_result_type
    };

    const schema = ctor.schema ?? BaseEventSchema;
    const parsed = schema.parse(base_data) as BaseEventData & Record<string, unknown>;

    Object.assign(this, parsed);

    this.event_path = Array.isArray((parsed as { event_path?: string[] }).event_path)
      ? ([...(parsed as { event_path?: string[] }).event_path] as string[])
      : [];
    this.event_pending_buses = 0;
    this.event_status = "pending";
    this.event_created_at_ms = Date.parse(this.event_created_at);
    this.event_errors = [];
    this.event_factory = ctor.factory;
    this.event_result_schema = event_result_schema;
    this.event_result_type = event_result_type;
    this.event_results = new Map();

    this._done = null;
    this._dispatch_context = undefined;
  }

  static nextIsoTimestamp(): string {
    const now_ms = Date.now();
    const next_ms = Math.max(now_ms, BaseEvent._last_timestamp_ms + 1);
    BaseEvent._last_timestamp_ms = next_ms;
    return new Date(next_ms).toISOString();
  }

  static extend<TShape extends z.ZodRawShape>(
    shape: TShape
  ): EventFactory<TShape>;
  static extend<TShape extends Record<string, unknown>>(
    shape: TShape
  ): EventFactory<ZodShapeFrom<TShape>>;
  static extend<TShape extends Record<string, unknown>>(
    event_type: string,
    shape: TShape
  ): EventFactory<ZodShapeFrom<TShape>>;
  static extend<TShape extends Record<string, unknown>>(
    arg1: string | TShape,
    arg2?: TShape
  ): EventFactory<ZodShapeFrom<TShape>> {
    const event_type = typeof arg1 === "string" ? arg1 : undefined;
    const raw_shape = (typeof arg1 === "string" ? arg2 ?? {} : arg1) as Record<
      string,
      unknown
    >;

    const event_result_schema = is_zod_schema(raw_shape.event_result_schema)
      ? (raw_shape.event_result_schema as z.ZodTypeAny)
      : undefined;
    const event_result_type =
      typeof raw_shape.event_result_type === "string" ? raw_shape.event_result_type : undefined;

    const shape = extract_zod_shape(raw_shape);
    const full_schema = BaseEventSchema.extend(shape);

    class ExtendedEvent extends BaseEvent {
      static schema = full_schema;
      static event_type = event_type;
      static factory?: Function;
      static event_result_schema = event_result_schema;
      static event_result_type = event_result_type;

      constructor(data: EventInit<ZodShapeFrom<TShape>>) {
        super(data as BaseEventInit<Record<string, unknown>>);
      }
    }

    function EventFactory(
      data: EventInit<ZodShapeFrom<TShape>>
    ): BaseEvent & z.infer<EventSchema<ZodShapeFrom<TShape>>> {
      return new ExtendedEvent(data);
    }

    EventFactory.schema = full_schema as EventSchema<ZodShapeFrom<TShape>>;
    EventFactory.event_type = event_type;
    EventFactory.event_result_schema = event_result_schema;
    EventFactory.event_result_type = event_result_type;
    EventFactory.fromJSON = (data: unknown) =>
      ExtendedEvent.fromJSON(data) as BaseEvent & z.infer<EventSchema<ZodShapeFrom<TShape>>>;
    EventFactory.prototype = ExtendedEvent.prototype;
    (EventFactory as unknown as { class: typeof ExtendedEvent }).class = ExtendedEvent;
    (ExtendedEvent as unknown as { factory?: Function }).factory = EventFactory;

    return EventFactory as EventFactory<ZodShapeFrom<TShape>>;
  }

  static parse<T extends typeof BaseEvent>(this: T, data: unknown): InstanceType<T> {
    const schema = this.schema ?? BaseEventSchema;
    const parsed = schema.parse(data);
    return new this(parsed) as InstanceType<T>;
  }

  static fromJSON<T extends typeof BaseEvent>(this: T, data: unknown): InstanceType<T> {
    if (!data || typeof data !== "object") {
      return this.parse(data);
    }
    const record = { ...(data as Record<string, unknown>) };
    if (record.event_result_schema && !is_zod_schema(record.event_result_schema)) {
      const zod_any = z as unknown as { fromJSONSchema?: (schema: unknown) => z.ZodTypeAny };
      if (typeof zod_any.fromJSONSchema === "function") {
        record.event_result_schema = zod_any.fromJSONSchema(record.event_result_schema);
      }
    }
    return new this(record as BaseEventInit<Record<string, unknown>>) as InstanceType<T>;
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
      event_result_schema: this.event_result_schema
        ? to_json_schema(this.event_result_schema)
        : this.event_result_schema
    };
  }

  get type(): string {
    return this.event_type;
  }

  get event_children(): BaseEvent[] {
    const children: BaseEvent[] = [];
    const seen = new Set<string>();
    for (const result of this.event_results.values()) {
      for (const child of result.event_children) {
        if (!seen.has(child.event_id)) {
          seen.add(child.event_id);
          children.push(child);
        }
      }
    }
    return children;
  }

  done(): Promise<this> {
    if (!this.bus) {
      return Promise.reject(new Error("event has no bus attached"));
    }
    if (this.event_status === "completed") {
      return Promise.resolve(this);
    }
    // Always delegate to _runImmediately — it walks up the parent event tree
    // to determine whether we're inside a handler (works cross-bus). If no
    // ancestor handler is in-flight, it falls back to waitForCompletion().
    const runner_bus = this.bus as {
      _runImmediately: (event: BaseEvent) => Promise<BaseEvent>;
    };
    return runner_bus._runImmediately(this) as Promise<this>;
  }

  waitForCompletion(): Promise<this> {
    this.ensureDonePromise();
    return this._done!.promise;
  }

  markStarted(): void {
    if (this.event_status !== "pending") {
      return;
    }
    this.event_status = "started";
    this.event_started_at = BaseEvent.nextIsoTimestamp();
  }

  markCompleted(): void {
    if (this.event_status === "completed") {
      return;
    }
    this.event_status = "completed";
    this.event_completed_at = BaseEvent.nextIsoTimestamp();
    this.ensureDonePromise();
    this._done!.resolve(this);
  }

  markFailed(error: unknown): void {
    this.event_errors.push(error);
  }

  cancelPendingChildProcessing(reason: unknown): void {
    for (const child of this.event_children) {
      for (const result of child.event_results.values()) {
        if (result.status === "pending") {
          result.markError(reason);
        }
      }
      child.cancelPendingChildProcessing(reason);
    }
  }

  eventAreAllChildrenComplete(visited: Set<string> = new Set()): boolean {
    if (visited.has(this.event_id)) {
      return true;
    }
    visited.add(this.event_id);
    for (const child of this.event_children) {
      if (child.event_status !== "completed") {
        return false;
      }
      if (!child.eventAreAllChildrenComplete(visited)) {
        return false;
      }
    }
    return true;
  }

  tryFinalizeCompletion(): void {
    if (this.event_pending_buses > 0) {
      return;
    }
    if (!this.eventAreAllChildrenComplete()) {
      return;
    }
    this.markCompleted();
  }

  ensureDonePromise(): void {
    if (this._done) {
      return;
    }
    this._done = withResolvers<this>();
  }
}

const is_zod_schema = (value: unknown): value is z.ZodTypeAny =>
  !!value && typeof (value as z.ZodTypeAny).safeParse === "function";

const extract_zod_shape = (raw: Record<string, unknown>): z.ZodRawShape => {
  const shape: z.ZodRawShape = {};
  for (const [key, value] of Object.entries(raw)) {
    if (key === "event_result_schema" || key === "event_result_type") {
      continue;
    }
    if (is_zod_schema(value)) {
      shape[key] = value;
    }
  }
  return shape;
};

const to_json_schema = (schema: unknown): unknown => {
  if (!schema) {
    return schema;
  }
  if (!is_zod_schema(schema)) {
    return schema;
  }
  const zod_any = z as unknown as { toJSONSchema?: (schema: z.ZodTypeAny) => unknown };
  if (typeof zod_any.toJSONSchema === "function") {
    return zod_any.toJSONSchema(schema);
  }
  return undefined;
};
