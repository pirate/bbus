import { BaseEvent } from "./base_event.js";
import { EventResult } from "./event_result.js";
import { capture_async_context, run_with_async_context } from "./async_context.js";
import { v7 as uuidv7 } from "uuid";


export class EventHandlerTimeoutError extends Error {
  event_type: string;
  handler_name: string;
  timeout_seconds: number;

  constructor(
    message: string,
    params: { event_type: string; handler_name: string; timeout_seconds: number }
  ) {
    super(message);
    this.name = "EventHandlerTimeoutError";
    this.event_type = params.event_type;
    this.handler_name = params.handler_name;
    this.timeout_seconds = params.timeout_seconds;
  }
}

export class EventHandlerCancelledError extends Error {
  event_type: string;
  handler_name: string;
  parent_error: Error;

  constructor(
    message: string,
    params: { event_type: string; handler_name: string; parent_error: Error }
  ) {
    super(message);
    this.name = "EventHandlerCancelledError";
    this.event_type = params.event_type;
    this.handler_name = params.handler_name;
    this.parent_error = params.parent_error;
  }
}

const with_resolvers = <T>() => {
  if (typeof Promise.withResolvers === "function") {
    return Promise.withResolvers<T>();
  }

  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolve_fn, reject_fn) => {
    resolve = resolve_fn;
    reject = reject_fn;
  });

  return { promise, resolve, reject };
};
import type {
  EventClass,
  EventHandler,
  EventKey,
  FindOptions
} from "./types.js";

type FindWaiter = {
  event_key: EventKey;
  matches: (event: BaseEvent) => boolean;
  resolve: (event: BaseEvent) => void;
  timeout_id?: ReturnType<typeof setTimeout>;
};

type EventBusOptions = {
  max_history_size?: number | null;
};

export class EventBus {
  static instances: Set<EventBus> = new Set();
  static findEventById(event_id: string): BaseEvent | null {
    for (const bus of EventBus.instances) {
      const event = bus.event_history_by_id.get(event_id);
      if (event) {
        return event;
      }
    }
    return null;
  }

  name: string;
  max_history_size: number | null;
  handlers_by_key: Map<EventKey | "*", Set<EventHandler>>;
  event_history: BaseEvent[];
  event_history_by_id: Map<string, BaseEvent>;
  pending_queue: BaseEvent[];
  is_running: boolean;
  idle_waiters: Array<() => void>;
  find_waiters: Set<FindWaiter>;
  handler_stack: EventResult[];
  handler_file_paths: Map<EventHandler, string>;
  handler_ids: Map<EventHandler, string>;
  run_now_depth: number;
  run_now_waiters: Array<() => void>;
  inside_handler_depth: number;

  constructor(name: string = "EventBus", options: EventBusOptions = {}) {
    this.name = name;
    this.max_history_size =
      options.max_history_size === undefined ? 100 : options.max_history_size;
    this.handlers_by_key = new Map();
    this.event_history = [];
    this.event_history_by_id = new Map();
    this.pending_queue = [];
    this.is_running = false;
    this.idle_waiters = [];
    this.find_waiters = new Set();
    this.handler_stack = [];
    this.handler_file_paths = new Map();
    this.handler_ids = new Map();
    this.run_now_depth = 0;
    this.run_now_waiters = [];
    this.inside_handler_depth = 0;

    EventBus.instances.add(this);

    this.dispatch = this.dispatch.bind(this);
    this.emit = this.emit.bind(this);
  }

  on<T extends BaseEvent>(event_key: EventKey<T> | "*", handler: EventHandler<T>): void {
    const handler_set = this.handlers_by_key.get(event_key) ?? new Set();
    handler_set.add(handler as EventHandler);
    this.handlers_by_key.set(event_key, handler_set);

    if (!this.handler_file_paths.has(handler as EventHandler)) {
      const file_path = this.inferHandlerFilePath();
      if (file_path) {
        this.handler_file_paths.set(handler as EventHandler, file_path);
      }
    }
  }

  off<T extends BaseEvent>(event_key: EventKey<T> | "*", handler: EventHandler<T>): void {
    const handler_set = this.handlers_by_key.get(event_key);
    if (!handler_set) {
      return;
    }
    handler_set.delete(handler as EventHandler);
  }

  private getHandlerId(handler: EventHandler): string {
    const existing = this.handler_ids.get(handler);
    if (existing) {
      return existing;
    }
    const handler_id = uuidv7();
    this.handler_ids.set(handler, handler_id);
    return handler_id;
  }

  dispatch<T extends BaseEvent>(event: T, event_key?: EventKey<T>): T {
    const original_event = event._original_event ?? event;
    if (!original_event.bus) {
      original_event.bus = this;
    }
    if (!Array.isArray(original_event.event_path)) {
      original_event.event_path = [];
    }
    if (original_event._dispatch_context === undefined) {
      original_event._dispatch_context = capture_async_context();
    }

    if (typeof event_key === "symbol") {
      original_event.event_key_symbol = event_key;
    }

    if (original_event.event_path.includes(this.name) || this.eventHasVisited(original_event)) {
      return this._getBusScopedEvent(original_event) as T;
    }

    if (!original_event.event_path.includes(this.name)) {
      original_event.event_path.push(this.name);
    }

    const current_handler = this.handler_stack[this.handler_stack.length - 1];
    if (current_handler) {
      const parent_event = this.event_history_by_id.get(current_handler.event_id);
      if (parent_event) {
        if (!original_event.event_parent_id) {
          original_event.event_parent_id = parent_event.event_id;
        }
        if (original_event.event_parent_id === parent_event.event_id) {
          this.recordChildEvent(parent_event.event_id, original_event);
        }
      }
    }

    this.event_history.push(original_event);
    this.event_history_by_id.set(original_event.event_id, original_event);
    this.trimHistory();

    this.createPendingHandlerResults(original_event);

    original_event.event_pending_buses += 1;
    this.pending_queue.push(original_event);
    this.startRunloop();

    return this._getBusScopedEvent(original_event) as T;
  }

  emit<T extends BaseEvent>(event: T, event_key?: EventKey<T>): T {
    return this.dispatch(event, event_key);
  }

  find<T extends BaseEvent>(event_key: EventKey<T>, options?: FindOptions<T>): Promise<T | null>;
  find<T extends BaseEvent>(
    event_key: EventKey<T>,
    where: (event: T) => boolean,
    options?: FindOptions<T>
  ): Promise<T | null>;
  async find<T extends BaseEvent>(
    event_key: EventKey<T>,
    where_or_options: ((event: T) => boolean) | FindOptions<T> = {},
    maybe_options: FindOptions<T> = {}
  ): Promise<T | null> {
    const where = typeof where_or_options === "function" ? where_or_options : (() => true);
    const options = typeof where_or_options === "function" ? maybe_options : where_or_options;

    return this.findInternal(event_key, where, options);
  }

  private async findInternal<T extends BaseEvent>(
    event_key: EventKey<T>,
    where: (event: T) => boolean,
    options: FindOptions<T>
  ): Promise<T | null> {
    const past = options.past ?? true;
    const future = options.future ?? true;
    const child_of = options.child_of ?? null;

    if (past === false && future === false) {
      return null;
    }

    const matches = (event: BaseEvent): boolean => {
      if (!this.eventMatchesKey(event, event_key)) {
        return false;
      }
      if (!where(event as T)) {
        return false;
      }
      if (child_of && !this.eventIsChildOf(event, child_of)) {
        return false;
      }
      return true;
    };

    if (past !== false) {
      const now_ms = Date.now();
      const cutoff_ms =
        past === true ? null : now_ms - Math.max(0, Number(past)) * 1000;

      for (let i = this.event_history.length - 1; i >= 0; i -= 1) {
        const event = this.event_history[i];
        if (event.event_status !== "completed") {
          continue;
        }
        if (cutoff_ms !== null && event.event_created_at_ms < cutoff_ms) {
          continue;
        }
        if (matches(event)) {
          return event as T;
        }
      }
    }

    if (future === false) {
      return null;
    }

    return new Promise<T | null>((resolve, _reject) => {
      const waiter: FindWaiter = {
        event_key,
        matches,
        resolve: (event) => resolve(event as T)
      };

      if (future !== true) {
        const timeout_ms = Math.max(0, Number(future)) * 1000;
        waiter.timeout_id = setTimeout(() => {
          this.find_waiters.delete(waiter);
          resolve(null);
        }, timeout_ms);
      }

      this.find_waiters.add(waiter);
    });
  }

  async _runImmediately<T extends BaseEvent>(event: T): Promise<T> {
    const original_event = event._original_event ?? event;
    if (original_event.event_status === "completed") {
      return event;
    }
    if (original_event.event_status === "started") {
      await this.runImmediatelyAcrossBuses(original_event);
      return event;
    }

    const index = this.pending_queue.indexOf(original_event);
    if (index >= 0) {
      this.pending_queue.splice(index, 1);
    }

    await this.runImmediatelyAcrossBuses(original_event);
    return event;
  }

  async waitUntilIdle(): Promise<void> {
    if (!this.is_running && this.pending_queue.length === 0) {
      return;
    }
    return new Promise((resolve) => {
      this.idle_waiters.push(resolve);
    });
  }

  eventIsChildOf(event: BaseEvent, ancestor: BaseEvent): boolean {
    if (event.event_id === ancestor.event_id) {
      return false;
    }

    let current_parent_id = event.event_parent_id;
    while (current_parent_id) {
      if (current_parent_id === ancestor.event_id) {
        return true;
      }
      const parent = this.event_history_by_id.get(current_parent_id);
      if (!parent) {
        return false;
      }
      current_parent_id = parent.event_parent_id;
    }
    return false;
  }

  eventIsParentOf(event: BaseEvent, descendant: BaseEvent): boolean {
    return this.eventIsChildOf(descendant, event);
  }

  recordChildEvent(parent_event_id: string, child_event: BaseEvent): void {
    const original_child = child_event._original_event ?? child_event;
    const parent_event = this.event_history_by_id.get(parent_event_id);
    if (parent_event) {
      if (!parent_event.event_children.some((child) => child.event_id === original_child.event_id)) {
        parent_event.event_children.push(original_child);
      }
    }

    const current_result = this.handler_stack[this.handler_stack.length - 1];
    if (current_result) {
      if (!current_result.event_children.some((child) => child.event_id === original_child.event_id)) {
        current_result.event_children.push(original_child);
      }
      original_child.event_emitted_by_handler_id = current_result.handler_id;
    }
  }

  logTree(): string {
    const parent_to_children = new Map<string | null, BaseEvent[]>();

    const add_child = (parent_id: string | null, child: BaseEvent): void => {
      const existing = parent_to_children.get(parent_id) ?? [];
      existing.push(child);
      parent_to_children.set(parent_id, existing);
    };

    for (const event of this.event_history) {
      add_child(event.event_parent_id ?? null, event);
    }

    for (const children of parent_to_children.values()) {
      children.sort((a, b) => a.event_created_at_ms - b.event_created_at_ms);
    }

    const root_events: BaseEvent[] = [];
    const seen = new Set<string>();

    for (const event of this.event_history) {
      const parent_id = event.event_parent_id;
      if (!parent_id || parent_id === event.event_id || !this.event_history_by_id.has(parent_id)) {
        if (!seen.has(event.event_id)) {
          root_events.push(event);
          seen.add(event.event_id);
        }
      }
    }

    if (root_events.length === 0) {
      return "(No events in history)";
    }

    const lines: string[] = [];
    lines.push(`📊 Event History Tree for ${this.name}`);
    lines.push("=".repeat(80));

    root_events.sort((a, b) => a.event_created_at_ms - b.event_created_at_ms);
    const visited = new Set<string>();
    root_events.forEach((event, index) => {
      lines.push(
        this.buildTreeLine(
          event,
          "",
          index === root_events.length - 1,
          parent_to_children,
          visited
        )
      );
    });

    lines.push("=".repeat(80));

    return lines.join("\n");
  }

  isInsideHandler(): boolean {
    return this.inside_handler_depth > 0;
  }

  private async runImmediatelyAcrossBuses(event: BaseEvent): Promise<void> {
    const buses = this.getBusesForImmediateRun(event);
    if (buses.length === 0) {
      await event.waitForCompletion();
      return;
    }

    for (const bus of buses) {
      bus.run_now_depth += 1;
    }

    try {
      for (const bus of buses) {
        const index = bus.pending_queue.indexOf(event);
        if (index >= 0) {
          bus.pending_queue.splice(index, 1);
        }
        if (!event.event_processed_path.includes(bus.name)) {
          await bus.processEvent(event);
        }
      }

      if (event.event_status !== "completed") {
        await event.waitForCompletion();
      }
    } finally {
      for (const bus of buses) {
        bus.run_now_depth = Math.max(0, bus.run_now_depth - 1);
        bus.releaseRunNowWaiters();
      }
    }
  }

  private getBusesForImmediateRun(event: BaseEvent): EventBus[] {
    const ordered: EventBus[] = [];
    const seen = new Set<EventBus>();

    const event_path = Array.isArray(event.event_path) ? event.event_path : [];
    for (const name of event_path) {
      for (const bus of EventBus.instances) {
        if (bus.name !== name) {
          continue;
        }
        if (!bus.event_history_by_id.has(event.event_id)) {
          continue;
        }
        if (event.event_processed_path.includes(bus.name)) {
          continue;
        }
        if (!seen.has(bus)) {
          ordered.push(bus);
          seen.add(bus);
        }
      }
    }

    if (!seen.has(this) && this.event_history_by_id.has(event.event_id)) {
      ordered.push(this);
    }

    return ordered;
  }

  private releaseRunNowWaiters(): void {
    if (this.run_now_depth !== 0 || this.run_now_waiters.length === 0) {
      return;
    }
    const waiters = this.run_now_waiters;
    this.run_now_waiters = [];
    for (const resolve of waiters) {
      resolve();
    }
  }


  private startRunloop(): void {
    if (this.is_running) {
      return;
    }
    this.is_running = true;
    setTimeout(() => {
      setTimeout(() => {
        void this.runloop();
      }, 0);
    }, 0);
  }

  private async runloop(): Promise<void> {
    while (this.pending_queue.length > 0) {
      await Promise.resolve();
      if (this.run_now_depth > 0) {
        await new Promise<void>((resolve) => {
          this.run_now_waiters.push(resolve);
        });
        continue;
      }
      const next_event = this.pending_queue.shift();
      if (!next_event) {
        continue;
      }
      if (this.eventHasVisited(next_event)) {
        continue;
      }
      await this.processEvent(next_event);
      await Promise.resolve();
    }
    this.is_running = false;
    const idle_waiters = this.idle_waiters;
    this.idle_waiters = [];
    for (const resolve of idle_waiters) {
      resolve();
    }
  }

  private async processEvent(event: BaseEvent): Promise<void> {
    if (this.eventHasVisited(event)) {
      return;
    }
    if (!Array.isArray(event.event_processed_path)) {
      event.event_processed_path = [];
    }
    if (!event.event_processed_path.includes(this.name)) {
      event.event_processed_path.push(this.name);
    }
    event.markStarted();
    this.notifyFinders(event);

    const handlers = this.collectHandlers(event);
    const handler_results = handlers.map((handler) => {
      const handler_name = handler.name || "anonymous";
      const handler_id = this.getHandlerId(handler);
      const existing = event.event_results.get(handler_id);
      const result =
        existing ??
        new EventResult({
          event_id: event.event_id,
          handler_id,
          handler_name,
          handler_file_path: this.handler_file_paths.get(handler) ?? undefined,
          eventbus_name: this.name
        });
      if (!existing) {
        event.event_results.set(handler_id, result);
      }
      return { handler, result };
    });

    const handler_event = this._getBusScopedEvent(event);

    for (const { handler, result } of handler_results) {
      if (result.status === "error" && result.error instanceof EventHandlerCancelledError) {
        continue;
      }
      this.inside_handler_depth += 1;
      this.handler_stack.push(result);

      try {
        result.markStarted();
        const handler_result = await this.runHandlerWithTimeout(event, handler, handler_event);
        if (event.event_result_schema) {
          const parsed = event.event_result_schema.safeParse(handler_result);
          if (parsed.success) {
            result.markCompleted(parsed.data);
          } else {
            const error = new Error(
              `handler result did not match event_result_schema: ${parsed.error.message}`
            );
            result.markError(error);
            event.markFailed(error);
          }
        } else {
          result.markCompleted(handler_result);
        }
      } catch (error) {
        if (error instanceof EventHandlerTimeoutError) {
          result.markError(error);
          event.markFailed(error);
          const cancelled_error = new EventHandlerCancelledError(
            `Cancelled pending handler due to parent timeout: ${error.message}`,
            {
              event_type: event.event_type,
              handler_name: result.handler_name,
              parent_error: error
            }
          );
          event.cancelPendingChildProcessing(cancelled_error);
        } else {
          result.markError(error);
          event.markFailed(error);
        }
      } finally {
        this.handler_stack.pop();
        this.inside_handler_depth = Math.max(0, this.inside_handler_depth - 1);
      }
    }

    event.event_pending_buses = Math.max(0, event.event_pending_buses - 1);
    event.tryFinalizeCompletion();
    if (event.event_status === "completed") {
      this.notifyParentsFor(event);
    }
  }

  

  private async runHandlerWithTimeout(
    event: BaseEvent,
    handler: EventHandler,
    handler_event: BaseEvent = event
  ): Promise<unknown> {
    if (event.event_timeout === null) {
      return run_with_async_context(event._dispatch_context ?? null, () => handler(handler_event));
    }

    const timeout_seconds = event.event_timeout;
    const timeout_ms = timeout_seconds * 1000;

    const { promise, resolve, reject } = with_resolvers<unknown>();
    let settled = false;

    const timer = setTimeout(() => {
      if (settled) {
        return;
      }
      settled = true;
      reject(
        new EventHandlerTimeoutError(
          `handler ${handler.name || "anonymous"} timed out after ${timeout_seconds}s`,
          {
            event_type: event.event_type,
            handler_name: handler.name || "anonymous",
            timeout_seconds
          }
        )
      );
    }, timeout_ms);

    Promise.resolve()
      .then(() => run_with_async_context(event._dispatch_context ?? null, () => handler(handler_event)))
      .then((value) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        resolve(value);
      })
      .catch((error) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        reject(error);
      });

    return promise;
  }

  private eventHasVisited(event: BaseEvent): boolean {
    return (
      Array.isArray(event.event_processed_path) &&
      event.event_processed_path.includes(this.name)
    );
  }

  private notifyParentsFor(event: BaseEvent): void {
    const visited = new Set<string>();
    let parent_id = event.event_parent_id;
    while (parent_id && !visited.has(parent_id)) {
      visited.add(parent_id);
      const parent = EventBus.findEventById(parent_id);
      if (!parent) {
        break;
      }
      parent.tryFinalizeCompletion();
      if (parent.event_status !== "completed") {
        break;
      }
      parent_id = parent.event_parent_id;
    }
  }

  _getBusScopedEvent<T extends BaseEvent>(event: T): T {
    const original_event = event._original_event ?? event;
    const bus = this;
    const parent_event_id = original_event.event_id;
    const bus_proxy = new Proxy(bus, {
      get(target, prop, receiver) {
        if (prop === "dispatch" || prop === "emit") {
          return (child_event: BaseEvent, event_key?: EventKey) => {
            const original_child = child_event._original_event ?? child_event;
            if (!original_child.event_parent_id) {
              original_child.event_parent_id = parent_event_id;
            }
            const dispatcher = Reflect.get(target, prop, receiver) as (
              event: BaseEvent,
              event_key?: EventKey
            ) => BaseEvent;
            return dispatcher.call(target, original_child, event_key);
          };
        }
        return Reflect.get(target, prop, receiver);
      }
    });
    const scoped = new Proxy(original_event, {
      get(target, prop, receiver) {
        if (prop === "bus") {
          return bus_proxy;
        }
        if (prop === "_original_event") {
          return target;
        }
        return Reflect.get(target, prop, receiver);
      },
      set(target, prop, value) {
        if (prop === "bus") {
          return true;
        }
        return Reflect.set(target, prop, value, target);
      },
      has(target, prop) {
        if (prop === "bus") {
          return true;
        }
        if (prop === "_original_event") {
          return true;
        }
        return Reflect.has(target, prop);
      }
    });

    return scoped as T;
  }

  private buildTreeLine(
    event: BaseEvent,
    indent: string,
    is_last: boolean,
    parent_to_children: Map<string | null, BaseEvent[]>,
    visited: Set<string>
  ): string {
    const connector = is_last ? "└── " : "├── ";
    const status_icon =
      event.event_status === "completed"
        ? "✅"
        : event.event_status === "started"
          ? "🏃"
          : "⏳";

    const created_at = this.formatTimestamp(event.event_created_at);
    let timing = `[${created_at}`;
    if (event.event_completed_at) {
      const created_ms = Date.parse(event.event_created_at);
      const completed_ms = Date.parse(event.event_completed_at);
      if (!Number.isNaN(created_ms) && !Number.isNaN(completed_ms)) {
        const duration = (completed_ms - created_ms) / 1000;
        timing += ` (${duration.toFixed(3)}s)`;
      }
    }
    timing += "]";

    const line = `${indent}${connector}${status_icon} ${event.event_type}#${event.event_id.slice(-4)} ${timing}`;

    if (visited.has(event.event_id)) {
      return line;
    }
    visited.add(event.event_id);

    const extension = is_last ? "    " : "│   ";
    const new_indent = indent + extension;

    const result_items: Array<{ type: "result"; result: EventResult } | { type: "child"; child: BaseEvent }> =
      [];
    const printed_child_ids = new Set<string>();

    const results = Array.from(event.event_results.values()).sort((a, b) => {
      const a_time = a.started_at ? Date.parse(a.started_at) : 0;
      const b_time = b.started_at ? Date.parse(b.started_at) : 0;
      return a_time - b_time;
    });

    results.forEach((result) => {
      result_items.push({ type: "result", result });
      result.event_children.forEach((child) => {
        printed_child_ids.add(child.event_id);
      });
    });

    const children = parent_to_children.get(event.event_id) ?? [];
    children.forEach((child) => {
      if (!printed_child_ids.has(child.event_id) && !child.event_emitted_by_handler_id) {
        result_items.push({ type: "child", child });
      }
    });

    if (result_items.length === 0) {
      return line;
    }

    const child_lines: string[] = [];
    result_items.forEach((item, index) => {
      const is_last_item = index === result_items.length - 1;
      if (item.type === "result") {
        child_lines.push(
          this.buildResultLine(
            item.result,
            new_indent,
            is_last_item,
            parent_to_children,
            visited
          )
        );
      } else {
        child_lines.push(
          this.buildTreeLine(
            item.child,
            new_indent,
            is_last_item,
            parent_to_children,
            visited
          )
        );
      }
    });

    return [line, ...child_lines].join("\n");
  }

  private buildResultLine(
    result: EventResult,
    indent: string,
    is_last: boolean,
    parent_to_children: Map<string | null, BaseEvent[]>,
    visited: Set<string>
  ): string {
    const connector = is_last ? "└── " : "├── ";
    const status_icon =
      result.status === "completed"
        ? "✅"
        : result.status === "error"
          ? "❌"
          : result.status === "started"
            ? "🏃"
            : "⏳";

    const handler_label =
      result.handler_name && result.handler_name !== "anonymous"
        ? result.handler_name
        : result.handler_file_path
          ? result.handler_file_path
          : "anonymous";
    const handler_display = `${result.eventbus_name}.${handler_label}#${result.handler_id.slice(-4)}`;
    let line = `${indent}${connector}${status_icon} ${handler_display}`;

    if (result.started_at) {
      line += ` [${this.formatTimestamp(result.started_at)}`;
      if (result.completed_at) {
        const started_ms = Date.parse(result.started_at);
        const completed_ms = Date.parse(result.completed_at);
        if (!Number.isNaN(started_ms) && !Number.isNaN(completed_ms)) {
          const duration = (completed_ms - started_ms) / 1000;
          line += ` (${duration.toFixed(3)}s)`;
        }
      }
      line += "]";
    }

    if (result.status === "error" && result.error) {
      if (result.error instanceof EventHandlerTimeoutError) {
        line += ` ⏱️ Timeout: ${result.error.message}`;
      } else if (result.error instanceof EventHandlerCancelledError) {
        line += ` 🚫 Cancelled: ${result.error.message}`;
      } else {
        const error_name = result.error instanceof Error ? result.error.name : "Error";
        const error_message = result.error instanceof Error ? result.error.message : String(result.error);
        line += ` ☠️ ${error_name}: ${error_message}`;
      }
    } else if (result.status === "completed") {
      line += ` → ${this.formatResultValue(result.result)}`;
    }

    const extension = is_last ? "    " : "│   ";
    const new_indent = indent + extension;

    if (result.event_children.length === 0) {
      return line;
    }

    const child_lines: string[] = [];
    const direct_children = result.event_children;
    const parent_children = parent_to_children.get(result.event_id) ?? [];
    const emitted_children = parent_children.filter(
      (child) => child.event_emitted_by_handler_id === result.handler_id
    );
    const children_by_id = new Map<string, BaseEvent>();
    direct_children.forEach((child) => {
      children_by_id.set(child.event_id, child);
    });
    emitted_children.forEach((child) => {
      if (!children_by_id.has(child.event_id)) {
        children_by_id.set(child.event_id, child);
      }
    });
    const children_to_print = Array.from(children_by_id.values()).filter(
      (child) => !visited.has(child.event_id)
    );

    children_to_print.forEach((child, index) => {
      child_lines.push(
        this.buildTreeLine(
          child,
          new_indent,
          index === children_to_print.length - 1,
          parent_to_children,
          visited
        )
      );
    });

    return [line, ...child_lines].join("\n");
  }

  private formatTimestamp(value?: string): string {
    if (!value) {
      return "N/A";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return "N/A";
    }
    return date.toISOString().slice(11, 23);
  }

  private inferHandlerFilePath(): string | null {
    const stack = new Error().stack;
    if (!stack) {
      return null;
    }
    const lines = stack.split("\n").map((line) => line.trim());
    for (const line of lines) {
      if (!line || line.startsWith("Error")) {
        continue;
      }
      if (
        line.includes("event_bus.ts") ||
        line.includes("node:internal") ||
        line.includes("/node_modules/")
      ) {
        continue;
      }
      const match = line.match(/\(?(.+?:\d+:\d+)\)?$/);
      if (match && match[1]) {
        return match[1];
      }
    }
    return null;
  }

  private formatResultValue(value: unknown): string {
    if (value === null || value === undefined) {
      return "None";
    }
    if (value instanceof BaseEvent) {
      return `Event(${value.event_type}#${value.event_id.slice(-4)})`;
    }
    if (typeof value === "string") {
      return JSON.stringify(value);
    }
    if (typeof value === "number" || typeof value === "boolean") {
      return String(value);
    }
    if (Array.isArray(value)) {
      return `list(${value.length} items)`;
    }
    if (typeof value === "object") {
      return `dict(${Object.keys(value as Record<string, unknown>).length} items)`;
    }
    return `${typeof value}(...)`;
  }

  private notifyFinders(event: BaseEvent): void {
    for (const waiter of Array.from(this.find_waiters)) {
      if (!this.eventMatchesKey(event, waiter.event_key)) {
        continue;
      }
      if (!waiter.matches(event)) {
        continue;
      }
      if (waiter.timeout_id) {
        clearTimeout(waiter.timeout_id);
      }
      this.find_waiters.delete(waiter);
      waiter.resolve(event);
    }
  }

  private createPendingHandlerResults(event: BaseEvent): void {
    const handlers = this.collectHandlers(event);
    handlers.forEach((handler) => {
      const handler_id = this.getHandlerId(handler);
      if (event.event_results.has(handler_id)) {
        return;
      }
      const handler_name = handler.name || "anonymous";
      const result = new EventResult({
        event_id: event.event_id,
        handler_id,
        handler_name,
        handler_file_path: this.handler_file_paths.get(handler) ?? undefined,
        eventbus_name: this.name
      });
      event.event_results.set(handler_id, result);
    });
  }

  private collectHandlers(event: BaseEvent): EventHandler[] {
    const handlers: EventHandler[] = [];

    const string_handlers = this.handlers_by_key.get(event.event_type);
    if (string_handlers) {
      handlers.push(...string_handlers);
    }

    const class_handlers = this.handlers_by_key.get(event.constructor as EventClass);
    if (class_handlers) {
      handlers.push(...class_handlers);
    }

    if (event.event_factory) {
      const factory_handlers = this.handlers_by_key.get(event.event_factory as EventKey);
      if (factory_handlers) {
        handlers.push(...factory_handlers);
      }
    }

    if (event.event_key_symbol) {
      const symbol_handlers = this.handlers_by_key.get(event.event_key_symbol);
      if (symbol_handlers) {
        handlers.push(...symbol_handlers);
      }
    }

    const wildcard_handlers = this.handlers_by_key.get("*");
    if (wildcard_handlers) {
      handlers.push(...wildcard_handlers);
    }

    return handlers;
  }

  private eventMatchesKey(event: BaseEvent, event_key: EventKey): boolean {
    if (event_key === "*") {
      return true;
    }
    if (typeof event_key === "string") {
      return event.event_type === event_key;
    }
    if (typeof event_key === "symbol") {
      return event.event_key_symbol === event_key;
    }
    if (event.event_factory && event_key === event.event_factory) {
      return true;
    }
    const ctor = event.constructor as EventClass & { factory?: Function };
    if (ctor.factory && event_key === ctor.factory) {
      return true;
    }
    return event.constructor === event_key;
  }

  private trimHistory(): void {
    if (this.max_history_size === null) {
      return;
    }
    if (this.event_history.length <= this.max_history_size) {
      return;
    }

    let remaining_overage = this.event_history.length - this.max_history_size;

    for (let i = 0; i < this.event_history.length && remaining_overage > 0; i += 1) {
      const event = this.event_history[i];
      if (event.event_status !== "completed") {
        continue;
      }
      this.event_history_by_id.delete(event.event_id);
      this.event_history.splice(i, 1);
      i -= 1;
      remaining_overage -= 1;
    }

    while (remaining_overage > 0 && this.event_history.length > 0) {
      const event = this.event_history.shift();
      if (event) {
        this.event_history_by_id.delete(event.event_id);
      }
      remaining_overage -= 1;
    }
  }
}
