import { z } from "zod";
import { v7 as uuidv7 } from "uuid";

import type { EventBus } from "./event_bus.js";
import { EventResult } from "./event_result.js";


export const BaseEventSchema = z
  .object({
    event_id: z.string().uuid(),
    event_created_at: z.string().datetime(),
    event_type: z.string(),
    event_timeout: z.number().positive().nullable(),
    event_parent_id: z.string().uuid().optional(),
    event_path: z.array(z.string()).optional()
  })
  .passthrough();

export type BaseEventData = z.infer<typeof BaseEventSchema>;
type BaseEventFields = Pick<
  BaseEventData,
  "event_id" | "event_created_at" | "event_type" | "event_timeout" | "event_parent_id"
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
};

export type EventExtendOptions = {
  event_result_schema?: z.ZodTypeAny;
  event_result_type?: string;
};

export class BaseEvent {
  static _last_timestamp_ms = 0;
  event_id: string;
  event_created_at: string;
  event_type: string;
  event_timeout: number | null;
  event_parent_id?: string;
  event_path: string[];
  event_processed_path: string[];
  event_factory?: Function;
  event_result_schema?: z.ZodTypeAny;
  event_result_type?: string;
  event_results: Map<string, EventResult>;
  event_children: BaseEvent[];
  event_emitted_by_handler_id?: string;
  event_pending_buses: number;
  event_status: "pending" | "started" | "completed";
  event_created_at_ms: number;
  event_started_at?: string;
  event_completed_at?: string;
  event_errors: unknown[];
  event_key_symbol?: symbol;
  bus?: EventBus;
  _original_event?: BaseEvent;

  static schema = BaseEventSchema;
  static event_type?: string;

  _done_promise: Promise<this> | null;
  _done_resolve: ((event: this) => void) | null;
  _done_reject: ((reason: unknown) => void) | null;

  constructor(data: BaseEventInit<Record<string, unknown>> = {}) {
    const ctor = this.constructor as typeof BaseEvent & {
      factory?: Function;
      event_result_schema?: z.ZodTypeAny;
      event_result_type?: string;
    };
    const event_type = data.event_type ?? ctor.event_type ?? ctor.name;
    const event_id = data.event_id ?? uuidv7();
    const event_created_at =
      data.event_created_at ?? new Date().toISOString();
    const event_timeout =
      data.event_timeout === undefined ? BaseEvent.defaultTimeout() : data.event_timeout;

    const base_data = {
      ...data,
      event_id,
      event_created_at,
      event_type,
      event_timeout
    };

    const schema = ctor.schema ?? BaseEventSchema;
    const parsed = schema.parse(base_data) as BaseEventData & Record<string, unknown>;

    Object.assign(this, parsed);

    this.event_path = Array.isArray((parsed as { event_path?: string[] }).event_path)
      ? ([...(parsed as { event_path?: string[] }).event_path] as string[])
      : [];
    this.event_processed_path = [];
    this.event_pending_buses = 0;
    this.event_status = "pending";
    this.event_created_at_ms = Date.parse(this.event_created_at);
    this.event_errors = [];
    this.event_factory = ctor.factory;
    this.event_result_schema = ctor.event_result_schema;
    this.event_result_type = ctor.event_result_type;
    this.event_results = new Map();
    this.event_children = [];

    this._done_promise = null;
    this._done_resolve = null;
    this._done_reject = null;
  }

  static defaultTimeout(): number {
    return 300;
  }

  static nextIsoTimestamp(): string {
    const now_ms = Date.now();
    const next_ms = Math.max(now_ms, BaseEvent._last_timestamp_ms + 1);
    BaseEvent._last_timestamp_ms = next_ms;
    return new Date(next_ms).toISOString();
  }

  static extend<TShape extends z.ZodRawShape>(
    shape: TShape,
    options?: EventExtendOptions
  ): EventFactory<TShape>;
  static extend<TShape extends z.ZodRawShape>(
    event_type: string,
    shape: TShape,
    options?: EventExtendOptions
  ): EventFactory<TShape>;
  static extend<TShape extends z.ZodRawShape>(
    arg1: string | TShape,
    arg2?: TShape | EventExtendOptions,
    arg3?: EventExtendOptions
  ): EventFactory<TShape> {
    return extendEvent(
      arg1 as string | TShape,
      arg2 as TShape | EventExtendOptions | undefined,
      arg3
    );
  }

  static parse<T extends typeof BaseEvent>(this: T, data: unknown): InstanceType<T> {
    const schema = this.schema ?? BaseEventSchema;
    const parsed = schema.parse(data);
    return new this(parsed) as InstanceType<T>;
  }

  toJSON(): BaseEventData {
    return {
      event_id: this.event_id,
      event_created_at: this.event_created_at,
      event_type: this.event_type,
      event_timeout: this.event_timeout,
      event_parent_id: this.event_parent_id,
      event_path: this.event_path
    };
  }

  get type(): string {
    return this.event_type;
  }

  done(): Promise<this> {
    if (!this.bus) {
      return Promise.reject(new Error("event has no bus attached"));
    }
    const runner_bus = this.bus as {
      _runImmediately: (event: BaseEvent) => Promise<BaseEvent>;
      isInsideHandler: () => boolean;
    };
    if (this.event_status === "completed") {
      return Promise.resolve(this);
    }
    if (runner_bus.isInsideHandler()) {
      return runner_bus._runImmediately(this) as Promise<this>;
    }
    return this.waitForCompletion();
  }

  waitForCompletion(): Promise<this> {
    this.ensureDonePromise();
    return this._done_promise as Promise<this>;
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
    if (this._done_resolve) {
      this._done_resolve(this as this);
    }
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
    if (this._done_promise) {
      return;
    }
    this._done_promise = new Promise<this>((resolve, reject) => {
      this._done_resolve = resolve;
      this._done_reject = reject;
    });
  }
}

export function extendEvent<TShape extends z.ZodRawShape>(
  shape: TShape
): EventFactory<TShape>;
export function extendEvent<TShape extends z.ZodRawShape>(
  event_type: string,
  shape: TShape,
  options?: EventExtendOptions
): EventFactory<TShape>;
export function extendEvent<TShape extends z.ZodRawShape>(
  arg1: string | TShape,
  arg2?: TShape | EventExtendOptions,
  arg3?: EventExtendOptions
): EventFactory<TShape> {
  const event_type = typeof arg1 === "string" ? arg1 : undefined;
  const shape = (typeof arg1 === "string" ? arg2 : arg1) as TShape;
  const options = (typeof arg1 === "string" ? arg3 : arg2) as EventExtendOptions | undefined;

  const full_schema = BaseEventSchema.extend(shape);

  class ExtendedEvent extends BaseEvent {
    static schema = full_schema;
    static event_type = event_type;
    static factory?: Function;
    static event_result_schema = options?.event_result_schema;
    static event_result_type = options?.event_result_type;

    constructor(data: EventInit<TShape>) {
      super(data as BaseEventInit<Record<string, unknown>>);
    }
  }

  function EventFactory(data: EventInit<TShape>): BaseEvent & z.infer<EventSchema<TShape>> {
    return new ExtendedEvent(data);
  }

  EventFactory.schema = full_schema;
  EventFactory.event_type = event_type;
  EventFactory.event_result_schema = options?.event_result_schema;
  EventFactory.event_result_type = options?.event_result_type;
  EventFactory.prototype = ExtendedEvent.prototype;
  (EventFactory as unknown as { class: typeof ExtendedEvent }).class = ExtendedEvent;
  (ExtendedEvent as unknown as { factory?: Function }).factory = EventFactory;

  return EventFactory as EventFactory<TShape>;
}
