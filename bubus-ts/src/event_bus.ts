import { BaseEvent } from "./base_event.js";
import { EventResult } from "./event_result.js";
import { captureAsyncContext, runWithAsyncContext } from "./async_context.js";
import { v5 as uuidv5, v7 as uuidv7 } from "uuid";
import {
  AsyncLimiter,
  type ConcurrencyMode,
  limiterForMode,
  resolveConcurrencyMode,
  runWithLimiter,
  withResolvers
} from "./semaphores.js";


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

import type { EventHandler, EventKey, FindOptions, HandlerOptions } from "./types.js";

type FindWaiter = {
  event_key: EventKey;
  matches: (event: BaseEvent) => boolean;
  resolve: (event: BaseEvent) => void;
  timeout_id?: ReturnType<typeof setTimeout>;
};

type HandlerEntry = {
  id: string;
  handler: EventHandler;
  handler_name: string;
  handler_file_path?: string;
  handler_registered_at: string;
  options?: HandlerOptions;
  event_key: string | "*";
};

const HANDLER_ID_NAMESPACE = uuidv5("bubus-handler", uuidv5.DNS);

type EventBusOptions = {
  max_history_size?: number | null;
  event_concurrency?: ConcurrencyMode;
  handler_concurrency?: ConcurrencyMode;
  event_timeout?: number | null;
};

class EventBusInstanceRegistry {
  private _refs = new Set<WeakRef<EventBus>>();
  private _lookup = new WeakMap<EventBus, WeakRef<EventBus>>();
  private _gc = typeof FinalizationRegistry !== "undefined"
    ? new FinalizationRegistry<WeakRef<EventBus>>((ref) => { this._refs.delete(ref); })
    : null;

  add(bus: EventBus): void {
    const ref = new WeakRef(bus);
    this._refs.add(ref);
    this._lookup.set(bus, ref);
    this._gc?.register(bus, ref, bus);
  }

  delete(bus: EventBus): void {
    const ref = this._lookup.get(bus);
    if (!ref) return;
    this._refs.delete(ref);
    this._lookup.delete(bus);
    this._gc?.unregister(bus);
  }

  has(bus: EventBus): boolean {
    return this._lookup.get(bus)?.deref() !== undefined;
  }

  get size(): number {
    let n = 0;
    for (const ref of this._refs) ref.deref() ? n++ : this._refs.delete(ref);
    return n;
  }

  *[Symbol.iterator](): Iterator<EventBus> {
    for (const ref of this._refs) {
      const bus = ref.deref();
      if (bus) yield bus; else this._refs.delete(ref);
    }
  }
}

export class EventBus {
  static instances = new EventBusInstanceRegistry();
  static global_event_limiter = new AsyncLimiter(1);
  static global_handler_limiter = new AsyncLimiter(1);
  static findEventById(event_id: string): BaseEvent | null {
    for (const bus of EventBus.instances) {
      const event = bus.event_history.get(event_id);
      if (event) {
        return event;
      }
    }
    return null;
  }

  name: string;
  max_history_size: number | null;
  event_concurrency_default: ConcurrencyMode;
  handler_concurrency_default: ConcurrencyMode;
  event_timeout_default: number | null;
  bus_event_limiter: AsyncLimiter;
  bus_handler_limiter: AsyncLimiter;
  handlers: Map<string, HandlerEntry>;
  event_history: Map<string, BaseEvent>;
  pending_event_queue: BaseEvent[];
  in_flight_event_ids: Set<string>;
  runloop_running: boolean;
  // Resolves for callers of waitUntilIdle(); only drained when idle is confirmed twice.
  idle_waiters: Array<() => void>;
  // True while an idle check timeout is scheduled.
  idle_check_pending: boolean;
  // Number of consecutive idle snapshots seen; must reach 2 to resolve waiters.
  idle_check_streak: number;
  // Pending find() callers waiting for a matching future event.
  find_waiters: Set<FindWaiter>;
  // Depth counter for "immediate processing" (queue-jump) inside handlers.
  // While > 0, the runloop pauses to avoid processing unrelated events.
  immediate_processing_stack_depth: number;
  // Runloop waiters that resume once immediate_processing_stack_depth returns to 0.
  immediate_processing_waiters: Array<() => void>;
  // Stack of EventResults for handlers currently executing on this bus.
  // Enables per-bus isInsideHandler() and gives _runImmediately access to the
  // calling handler's result even when called on raw (non-proxied) events.
  _event_result_stack: EventResult[];

  constructor(name: string = "EventBus", options: EventBusOptions = {}) {
    this.name = name;
    this.max_history_size =
      options.max_history_size === undefined ? 100 : options.max_history_size;
    this.event_concurrency_default = options.event_concurrency ?? "bus-serial";
    this.handler_concurrency_default = options.handler_concurrency ?? "bus-serial";
    this.event_timeout_default =
      options.event_timeout === undefined ? 60 : options.event_timeout;
    this.bus_event_limiter = new AsyncLimiter(1);
    this.bus_handler_limiter = new AsyncLimiter(1);
    this.handlers = new Map();
    this.event_history = new Map();
    this.pending_event_queue = [];
    this.in_flight_event_ids = new Set();
    this.runloop_running = false;
    this.idle_waiters = [];
    this.idle_check_pending = false;
    this.idle_check_streak = 0;
    this.find_waiters = new Set();
    this.immediate_processing_stack_depth = 0;
    this.immediate_processing_waiters = [];
    this._event_result_stack = [];

    EventBus.instances.add(this);

    this.dispatch = this.dispatch.bind(this);
    this.emit = this.emit.bind(this);
  }

  destroy(): void {
    EventBus.instances.delete(this);
    this.handlers.clear();
    this.event_history.clear();
    this.pending_event_queue.length = 0;
    this.in_flight_event_ids.clear();
    this.find_waiters.clear();
    this.idle_waiters.length = 0;
    this.immediate_processing_waiters.length = 0;
  }

  on<T extends BaseEvent>(
    event_key: EventKey<T> | "*",
    handler: EventHandler<T>,
    options: HandlerOptions = {}
  ): void {
    const normalized_key = this.normalizeEventKey(event_key);
    const handler_name = handler.name || "anonymous";
    const handler_file_path = this.inferHandlerFilePath() ?? undefined;
    const handler_registered_at = BaseEvent.nextIsoTimestamp();
    const handler_id = this.computeHandlerId(
      normalized_key,
      handler_name,
      handler_file_path,
      handler_registered_at
    );

    this.handlers.set(handler_id, {
      id: handler_id,
      handler: handler as EventHandler,
      handler_name,
      handler_file_path,
      handler_registered_at,
      options: Object.keys(options).length > 0 ? options : undefined,
      event_key: normalized_key
    });
  }

  off<T extends BaseEvent>(event_key: EventKey<T> | "*", handler?: EventHandler<T> | string): void {
    const normalized_key = this.normalizeEventKey(event_key);
    const match_by_id = typeof handler === "string";
    for (const [handler_id, entry] of this.handlers) {
      if (entry.event_key !== normalized_key) {
        continue;
      }
      if (handler === undefined || (match_by_id ? handler_id === handler : entry.handler === (handler as EventHandler))) {
        this.handlers.delete(handler_id);
      }
    }
  }

  private computeHandlerId(
    event_key: string | "*",
    handler_name: string,
    handler_file_path: string | undefined,
    handler_registered_at: string
  ): string {
    const file_path = handler_file_path ?? "unknown";
    const seed = `${this.name}|${event_key}|${handler_name}|${file_path}|${handler_registered_at}`;
    return uuidv5(seed, HANDLER_ID_NAMESPACE);
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
      original_event._dispatch_context = captureAsyncContext();
    }
    if (original_event.event_timeout === null) {
      original_event.event_timeout = this.event_timeout_default;
    }


    if (original_event.event_path.includes(this.name) || this.eventHasVisited(original_event)) {
      return this._getBusScopedEvent(original_event) as T;
    }

    if (!original_event.event_path.includes(this.name)) {
      original_event.event_path.push(this.name);
    }

    if (original_event.event_parent_id) {
      const parent_event = this.event_history.get(original_event.event_parent_id);
      if (parent_event) {
        this.recordChildEvent(
          parent_event.event_id,
          original_event,
          original_event.event_emitted_by_handler_id
        );
      }
    }

    this.event_history.set(original_event.event_id, original_event);
    this.trimHistory();

    original_event.event_pending_buses += 1;
    this.pending_event_queue.push(original_event);
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

    if (past !== false || future !== false) {
      const now_ms = Date.now();
      const cutoff_ms =
        past === true ? null : now_ms - Math.max(0, Number(past)) * 1000;

      const history_values = Array.from(this.event_history.values());
      for (let i = history_values.length - 1; i >= 0; i -= 1) {
        const event = history_values[i];
        if (!matches(event)) {
          continue;
        }
        if (event.event_status === "completed") {
          if (past === false) {
            continue;
          }
          if (cutoff_ms !== null && event.event_created_at_ms < cutoff_ms) {
            continue;
          }
          return this._getBusScopedEvent(event) as T;
        }
        if (future !== false) {
          return this._getBusScopedEvent(event) as T;
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
        resolve: (event) => resolve(this._getBusScopedEvent(event) as T)
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

  // Called when a handler does `await child.done()` — processes the child event
  // immediately ("queue-jump") instead of waiting for the runloop to pick it up.
  //
  // Yield-and-reacquire: if the calling handler holds a handler concurrency limiter,
  // we temporarily release it so child handlers on the same bus can acquire it
  // (preventing deadlock for bus-serial/global-serial modes). We re-acquire after
  // the child completes so the parent handler can continue with the limiter held.
  async _runImmediately<T extends BaseEvent>(
    event: T,
    handler_result?: EventResult
  ): Promise<T> {
    const original_event = event._original_event ?? event;
    // Find the parent handler's result: prefer the proxy-provided one (only if
    // the handler is still running), then this bus's stack, then walk up the
    // parent event tree (cross-bus case). If none found, we're not inside a
    // handler and should fall back to waitForCompletion.
    const proxy_result = handler_result?.status === "started" ? handler_result : undefined;
    const effective_result = proxy_result
      ?? this._event_result_stack[this._event_result_stack.length - 1]
      ?? this._findInFlightAncestorResult(original_event)
      ?? undefined;
    if (!effective_result) {
      // Not inside any handler — fall back to normal completion waiting
      await original_event.waitForCompletion();
      return event;
    }
    if (!effective_result.queue_jump_hold) {
      effective_result.queue_jump_hold = true;
      this.immediate_processing_stack_depth += 1;
    }
    if (original_event.event_status === "completed") {
      return event;
    }

    // Yield the parent handler's limiter so child handlers can use it.
    // Null out _held_handler_limiter so concurrent calls from the same handler
    // (e.g. Promise.all([child1.done(), child2.done()])) don't double-release.
    const limiter_to_yield = effective_result?._held_handler_limiter ?? null;
    if (limiter_to_yield) {
      effective_result!._held_handler_limiter = null;
      limiter_to_yield.release();
    }

    try {
      if (original_event.event_status === "started") {
        await this.runImmediatelyAcrossBuses(original_event);
        return event;
      }

      const index = this.pending_event_queue.indexOf(original_event);
      if (index >= 0) {
        this.pending_event_queue.splice(index, 1);
      }

      await this.runImmediatelyAcrossBuses(original_event);
      return event;
    } finally {
      // Re-acquire the parent handler's limiter before returning control.
      // Only the call that actually released it will re-acquire.
      if (limiter_to_yield) {
        await limiter_to_yield.acquire();
        effective_result!._held_handler_limiter = limiter_to_yield;
      }
    }
  }

  async waitUntilIdle(): Promise<void> {
    if (this.isIdleSnapshot()) {
      return;
    }
    return new Promise((resolve) => {
      this.idle_waiters.push(resolve);
      this.scheduleIdleCheck();
    });
  }

  private scheduleIdleCheck(): void {
    if (this.idle_check_pending) {
      return;
    }
    this.idle_check_pending = true;
    setTimeout(() => {
      this.idle_check_pending = false;
      this.resolveIdleWaitersIfDone();
    }, 0);
  }

  private isIdleSnapshot(): boolean {
    return (
      this.pending_event_queue.length === 0 &&
      this.in_flight_event_ids.size === 0 &&
      !this.hasPendingResults() &&
      !this.runloop_running
    );
  }

  private resolveIdleWaitersIfDone(): void {
    if (!this.isIdleSnapshot()) {
      this.idle_check_streak = 0;
      if (this.idle_waiters.length > 0) {
        this.scheduleIdleCheck();
      }
      return;
    }
    this.idle_check_streak += 1;
    if (this.idle_check_streak < 2) {
      if (this.idle_waiters.length > 0) {
        this.scheduleIdleCheck();
      }
      return;
    }
    this.idle_check_streak = 0;
    const idle_waiters = this.idle_waiters;
    this.idle_waiters = [];
    for (const resolve of idle_waiters) {
      resolve();
    }
  }

  private hasPendingResults(): boolean {
    for (const event of this.event_history.values()) {
      for (const result of event.event_results.values()) {
        if (result.eventbus_name !== this.name) {
          continue;
        }
        if (result.status === "pending") {
          return true;
        }
      }
    }
    return false;
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
      const parent = this.event_history.get(current_parent_id);
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

  recordChildEvent(
    parent_event_id: string,
    child_event: BaseEvent,
    handler_id?: string
  ): void {
    const original_child = child_event._original_event ?? child_event;
    const parent_event = this.event_history.get(parent_event_id);

    const target_handler_id =
      handler_id ?? original_child.event_emitted_by_handler_id ?? undefined;
    if (target_handler_id) {
      const current_result = parent_event?.event_results.get(target_handler_id);
      if (current_result) {
        if (!current_result.event_children.some((child) => child.event_id === original_child.event_id)) {
          current_result.event_children.push(original_child);
        }
      }
      original_child.event_emitted_by_handler_id = target_handler_id;
    }
  }

  logTree(): string {
    const parent_to_children = new Map<string | null, BaseEvent[]>();

    const add_child = (parent_id: string | null, child: BaseEvent): void => {
      const existing = parent_to_children.get(parent_id) ?? [];
      existing.push(child);
      parent_to_children.set(parent_id, existing);
    };

    for (const event of this.event_history.values()) {
      add_child(event.event_parent_id ?? null, event);
    }

    for (const children of parent_to_children.values()) {
      children.sort((a, b) => a.event_created_at_ms - b.event_created_at_ms);
    }

    const root_events: BaseEvent[] = [];
    const seen = new Set<string>();

    for (const event of this.event_history.values()) {
      const parent_id = event.event_parent_id;
      if (!parent_id || parent_id === event.event_id || !this.event_history.has(parent_id)) {
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

  // Per-bus check: true only if this specific bus has a handler on its stack.
  // For cross-bus queue-jumping, done() uses the _is_handler_scoped flag on
  // the bus proxy instead (set by _getBusScopedEvent when handler_result exists).
  isInsideHandler(): boolean {
    return this._event_result_stack.length > 0;
  }

  // Walk up the parent event chain to find an in-flight ancestor handler result.
  // Returns the result if found, null otherwise. Used by _runImmediately to detect
  // cross-bus queue-jump scenarios where the calling handler is on a different bus.
  _findInFlightAncestorResult(event: BaseEvent): EventResult | null {
    const original = event._original_event ?? event;
    let current_parent_id = original.event_parent_id;
    let current_handler_id = original.event_emitted_by_handler_id;
    while (current_handler_id && current_parent_id) {
      const parent = EventBus.findEventById(current_parent_id);
      if (!parent) break;
      const handler_result = parent.event_results.get(current_handler_id);
      if (handler_result && handler_result.status === "started") return handler_result;
      current_parent_id = parent.event_parent_id;
      current_handler_id = parent.event_emitted_by_handler_id;
    }
    return null;
  }

  // Processes a queue-jumped event across all buses that have it dispatched.
  // Called from _runImmediately after the parent handler's limiter has been yielded.
  //
  // Event limiter bypass: the initiating bus (this) always bypasses its event limiter
  // since we're inside a handler that already holds it. Other buses only bypass if
  // they resolve to the same limiter instance (i.e. global-serial mode where all
  // buses share EventBus.global_event_limiter).
  //
  // Handler limiters are NOT bypassed — child handlers must acquire the handler
  // limiter normally. This works because _runImmediately already released the
  // parent's handler limiter via yield-and-reacquire.
  private async runImmediatelyAcrossBuses(event: BaseEvent): Promise<void> {
    const buses = this.getBusesForImmediateRun(event);
    if (buses.length === 0) {
      await event.waitForCompletion();
      return;
    }

    for (const bus of buses) {
      bus.immediate_processing_stack_depth += 1;
    }

    // Determine which event limiter the initiating bus resolves to, so we can
    // detect when other buses share the same instance (global-serial).
    const initiating_event_limiter = this.resolveEventLimiter(event);

    try {
      for (const bus of buses) {
        const index = bus.pending_event_queue.indexOf(event);
        if (index >= 0) {
          bus.pending_event_queue.splice(index, 1);
        }
        if (bus.eventHasVisited(event)) {
          continue;
        }
        if (bus.in_flight_event_ids.has(event.event_id)) {
          continue;
        }
        bus.in_flight_event_ids.add(event.event_id);

        // Bypass event limiter on the initiating bus (we're already inside a handler
        // that acquired it). For other buses, only bypass if they resolve to the same
        // limiter instance (global-serial shares one limiter across all buses).
        const bus_event_limiter = bus.resolveEventLimiter(event);
        const should_bypass_event_limiter =
          bus === this ||
          (initiating_event_limiter !== null &&
            bus_event_limiter === initiating_event_limiter);

        await bus.scheduleEventProcessing(event, {
          bypass_event_limiters: should_bypass_event_limiter
        });
      }

      if (event.event_status !== "completed") {
        await event.waitForCompletion();
      }
    } finally {
      for (const bus of buses) {
        bus.immediate_processing_stack_depth = Math.max(
          0,
          bus.immediate_processing_stack_depth - 1
        );
        bus.releaseImmediateProcessingWaiters();
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
        if (!bus.event_history.has(event.event_id)) {
          continue;
        }
        if (bus.eventHasVisited(event)) {
          continue;
        }
        if (!seen.has(bus)) {
          ordered.push(bus);
          seen.add(bus);
        }
      }
    }

    if (!seen.has(this) && this.event_history.has(event.event_id)) {
      ordered.push(this);
    }

    return ordered;
  }

  private releaseImmediateProcessingWaiters(): void {
    if (
      this.immediate_processing_stack_depth !== 0 ||
      this.immediate_processing_waiters.length === 0
    ) {
      return;
    }
    const waiters = this.immediate_processing_waiters;
    this.immediate_processing_waiters = [];
    for (const resolve of waiters) {
      try {
        // Each waiter is a Promise resolver created by runloop() while it was paused.
        // Resolving it resumes that runloop tick so it can continue draining the queue.
        resolve();
      } catch (error) {
        // Should never happen: these are internal Promise resolve callbacks.
        console.error("[bubus] immediate processing waiter threw", error);
      }
    }
  }


  private startRunloop(): void {
    if (this.runloop_running) {
      return;
    }
    this.runloop_running = true;
    queueMicrotask(() => {
      void this.runloop();
    });
  }

  private async scheduleEventProcessing(
    event: BaseEvent,
    options: {
      bypass_event_limiters?: boolean;
      pre_acquired_limiter?: AsyncLimiter | null;
    } = {}
  ): Promise<void> {
    try {
      const limiter = options.bypass_event_limiters ? null : this.resolveEventLimiter(event);
      const pre_acquired_limiter = options.pre_acquired_limiter ?? null;
      if (pre_acquired_limiter) {
        await this.processEvent(event);
      } else {
        await runWithLimiter(limiter, async () => {
          await this.processEvent(event);
        });
      }
    } finally {
      if (options.pre_acquired_limiter) {
        options.pre_acquired_limiter.release();
      }
      this.in_flight_event_ids.delete(event.event_id);
      this.resolveIdleWaitersIfDone();
    }
  }

  private async runloop(): Promise<void> {
    for (;;) {
      while (this.pending_event_queue.length > 0) {
        await Promise.resolve();
        if (this.immediate_processing_stack_depth > 0) {
          await new Promise<void>((resolve) => {
            this.immediate_processing_waiters.push(resolve);
          });
          continue;
        }
        const next_event = this.pending_event_queue[0];
        if (!next_event) {
          continue;
        }
        const original_event = next_event._original_event ?? next_event;
        if (this.eventHasVisited(original_event)) {
          this.pending_event_queue.shift();
          continue;
        }
        let pre_acquired_limiter: AsyncLimiter | null = null;
        const event_limiter = this.resolveEventLimiter(original_event);
        if (event_limiter) {
          await event_limiter.acquire();
          pre_acquired_limiter = event_limiter;
        }
        this.pending_event_queue.shift();
        if (this.in_flight_event_ids.has(original_event.event_id)) {
          if (pre_acquired_limiter) {
            pre_acquired_limiter.release();
          }
          continue;
        }
        this.in_flight_event_ids.add(original_event.event_id);
        void this.scheduleEventProcessing(original_event, {
          bypass_event_limiters: true,
          pre_acquired_limiter
        });
        await Promise.resolve();
      }
      this.runloop_running = false;
      if (this.pending_event_queue.length > 0) {
        this.startRunloop();
        return;
      }
      this.resolveIdleWaitersIfDone();
      return;
    }
  }

  private async processEvent(event: BaseEvent): Promise<void> {
    if (this.eventHasVisited(event)) {
      return;
    }
    event.markStarted();
    this.notifyFinders(event);

    const deadlock_timer =
      event.event_timeout === null
        ? null
        : setTimeout(() => {
            if (event.event_status === "completed") {
              return;
            }
            const started_at = event.event_started_at ?? event.event_created_at;
            const elapsed_ms = Date.now() - Date.parse(started_at);
            const elapsed_seconds = (elapsed_ms / 1000).toFixed(1);
            console.warn(
              `[bubus] Possible deadlock: ${event.event_type}#${event.event_id} still ${event.event_status} on ${this.name} after ${elapsed_seconds}s (timeout ${event.event_timeout}s)`
            );
          }, event.event_timeout * 1000);

    try {
      const handler_entries = this.createPendingHandlerResults(event);

      const handler_promises = handler_entries.map((entry) =>
        this.runHandlerEntry(event, entry.handler, entry.result, entry.options)
      );
      await Promise.all(handler_promises);

      event.event_pending_buses = Math.max(0, event.event_pending_buses - 1);
      event.tryFinalizeCompletion();
      if (event.event_status === "completed") {
        this.notifyParentsFor(event);
      }
    } finally {
      if (deadlock_timer) {
        clearTimeout(deadlock_timer);
      }
    }
  }

  private resolveEventLimiter(event: BaseEvent): AsyncLimiter | null {
    const resolved = resolveConcurrencyMode(
      event.event_concurrency,
      this.event_concurrency_default
    );
    return limiterForMode(resolved, EventBus.global_event_limiter, this.bus_event_limiter);
  }

  private resolveHandlerLimiter(
    event: BaseEvent,
    options?: HandlerOptions
  ): AsyncLimiter | null {
    const event_override =
      event.handler_concurrency && event.handler_concurrency !== "auto"
        ? event.handler_concurrency
        : undefined;
    const handler_override =
      options?.handler_concurrency && options.handler_concurrency !== "auto"
        ? options.handler_concurrency
        : undefined;
    const fallback = this.handler_concurrency_default;
    const resolved = resolveConcurrencyMode(
      event_override ?? handler_override ?? fallback,
      fallback
    );
    return limiterForMode(resolved, EventBus.global_handler_limiter, this.bus_handler_limiter);
  }

  private async runHandlerEntry(
    event: BaseEvent,
    handler: EventHandler,
    result: EventResult,
    options?: HandlerOptions
  ): Promise<void> {
    if (result.status === "error" && result.error instanceof EventHandlerCancelledError) {
      return;
    }

    const handler_event = this._getBusScopedEvent(event, result);
    const limiter = this.resolveHandlerLimiter(event, options);

    await runWithLimiter(limiter, async () => {
      if (result.status === "error" && result.error instanceof EventHandlerCancelledError) {
        return;
      }

      // Track which limiter this handler holds so _runImmediately can yield it
      // (release before child processing, re-acquire after) to prevent deadlock.
      result._held_handler_limiter = limiter;
      this._event_result_stack.push(result);
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
          this.cancelPendingChildProcessing(event, cancelled_error);
        } else {
          result.markError(error);
          event.markFailed(error);
        }
      } finally {
        result._held_handler_limiter = null;
        const stack_idx = this._event_result_stack.indexOf(result);
        if (stack_idx >= 0) {
          this._event_result_stack.splice(stack_idx, 1);
        }
        if (result.queue_jump_hold) {
          result.queue_jump_hold = false;
          this.immediate_processing_stack_depth = Math.max(
            0,
            this.immediate_processing_stack_depth - 1
          );
          this.releaseImmediateProcessingWaiters();
        }
      }
    });
  }

  

  private async runHandlerWithTimeout(
    event: BaseEvent,
    handler: EventHandler,
    handler_event: BaseEvent = event
  ): Promise<unknown> {
    const handler_name = handler.name || "anonymous";
    const warn_ms = 15000;
    const started_at_ms = Date.now();
    const should_warn =
      event.event_timeout === null || event.event_timeout * 1000 > warn_ms;
    const warn_timer = should_warn
      ? setTimeout(() => {
          const elapsed_ms = Date.now() - started_at_ms;
          const elapsed_seconds = (elapsed_ms / 1000).toFixed(1);
          console.warn(
            `[bubus] Slow handler: ${event.event_type}.${handler_name} running ${elapsed_seconds}s on ${this.name}`
          );
        }, warn_ms)
      : null;
    const clear_warn = () => {
      if (warn_timer) {
        clearTimeout(warn_timer);
      }
    };
    const run_handler = () =>
      Promise.resolve().then(() =>
        runWithAsyncContext(event._dispatch_context ?? null, () => handler(handler_event))
      );

    if (event.event_timeout === null) {
      return run_handler().finally(clear_warn);
    }

    const timeout_seconds = event.event_timeout;
    const timeout_ms = timeout_seconds * 1000;

    const { promise, resolve, reject } = withResolvers<unknown>();
    let settled = false;

    const finalize = (fn: (value?: unknown) => void) => {
      return (value?: unknown) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        clear_warn();
        fn(value);
      };
    };

    const timer = setTimeout(() => {
      finalize(reject)(
        new EventHandlerTimeoutError(
          `handler ${handler_name} timed out after ${timeout_seconds}s`,
          {
            event_type: event.event_type,
            handler_name,
            timeout_seconds
          }
        )
      );
    }, timeout_ms);

    run_handler().then(finalize(resolve)).catch(finalize(reject));

    return promise;
  }

  private eventHasVisited(event: BaseEvent): boolean {
    const results = Array.from(event.event_results.values()).filter(
      (result) => result.eventbus_name === this.name
    );
    if (results.length === 0) {
      return false;
    }
    return results.every(
      (result) => result.status === "completed" || result.status === "error"
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

  _getBusScopedEvent<T extends BaseEvent>(event: T, handler_result?: EventResult): T {
    const original_event = event._original_event ?? event;
    const bus = this;
    const parent_event_id = original_event.event_id;
    const handler_id = handler_result?.handler_id;
    const bus_proxy = new Proxy(bus, {
      get(target, prop, receiver) {
        if (prop === "_runImmediately") {
          return (child_event: BaseEvent) => {
            const runner = Reflect.get(target, prop, receiver) as (
              event: BaseEvent,
              handler_result?: EventResult
            ) => Promise<BaseEvent>;
            return runner.call(target, child_event, handler_result);
          };
        }
        if (prop === "dispatch" || prop === "emit") {
          return (child_event: BaseEvent, event_key?: EventKey) => {
            const original_child = child_event._original_event ?? child_event;
            if (!original_child.event_parent_id) {
              original_child.event_parent_id = parent_event_id;
            }
            if (handler_id && !original_child.event_emitted_by_handler_id) {
              original_child.event_emitted_by_handler_id = handler_id;
            }
            const dispatcher = Reflect.get(target, prop, receiver) as (
              event: BaseEvent,
              event_key?: EventKey
            ) => BaseEvent;
            const dispatched = dispatcher.call(target, original_child, event_key);
            return target._getBusScopedEvent(dispatched, handler_result);
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

  private cancelPendingChildProcessing(
    event: BaseEvent,
    error: EventHandlerCancelledError
  ): void {
    const visited = new Set<string>();
    const cancel_child = (child: BaseEvent): void => {
      const original_child = child._original_event ?? child;
      if (visited.has(original_child.event_id)) {
        return;
      }
      visited.add(original_child.event_id);

      const path = Array.isArray(original_child.event_path)
        ? original_child.event_path
        : [];
      const buses_to_cancel = new Set<string>(path);
      for (const bus of EventBus.instances) {
        if (!buses_to_cancel.has(bus.name)) {
          continue;
        }
        bus.cancelEventOnBus(original_child, error);
      }

      for (const grandchild of original_child.event_children) {
        cancel_child(grandchild);
      }
    };

    for (const child of event.event_children) {
      cancel_child(child);
    }
  }

  private cancelEventOnBus(event: BaseEvent, error: EventHandlerCancelledError): void {
    const original_event = event._original_event ?? event;
    const handler_entries = this.createPendingHandlerResults(original_event);
    let updated = false;
    for (const entry of handler_entries) {
      if (entry.result.status === "pending") {
        entry.result.markError(error);
        updated = true;
      }
    }

    let removed = 0;
    if (this.pending_event_queue.length > 0) {
      const before_len = this.pending_event_queue.length;
      this.pending_event_queue = this.pending_event_queue.filter(
        (queued) => (queued._original_event ?? queued).event_id !== original_event.event_id
      );
      removed = before_len - this.pending_event_queue.length;
    }

    if (removed > 0 && !this.in_flight_event_ids.has(original_event.event_id)) {
      original_event.event_pending_buses = Math.max(0, original_event.event_pending_buses - 1);
    }

    if (updated || removed > 0) {
      original_event.tryFinalizeCompletion();
      if (original_event.event_status === "completed") {
        this.notifyParentsFor(original_event);
      }
    }
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

  private createPendingHandlerResults(
    event: BaseEvent
  ): Array<{
    handler: EventHandler;
    result: EventResult;
    options?: HandlerOptions;
  }> {
    const handlers = this.collectHandlers(event);
    return handlers.map(({ handler_id, handler, handler_name, handler_file_path, options }) => {
      const existing = event.event_results.get(handler_id);
      const result =
        existing ??
        new EventResult({
          event_id: event.event_id,
          handler_id,
          handler_name,
          handler_file_path,
          eventbus_name: this.name
        });
      if (!existing) {
        event.event_results.set(handler_id, result);
      }
      return { handler, result, options };
    });
  }

  private collectHandlers(
    event: BaseEvent
  ): Array<{
    handler_id: string;
    handler: EventHandler;
    handler_name: string;
    handler_file_path?: string;
    options?: HandlerOptions;
  }> {
    const handlers: Array<{
      handler_id: string;
      handler: EventHandler;
      handler_name: string;
      handler_file_path?: string;
      options?: HandlerOptions;
    }> = [];

    for (const [handler_id, entry] of this.handlers) {
      if (entry.event_key !== event.event_type && entry.event_key !== "*") {
        continue;
      }
      handlers.push({
        handler_id,
        handler: entry.handler,
        handler_name: entry.handler_name,
        handler_file_path: entry.handler_file_path,
        options: entry.options
      });
    }

    return handlers;
  }

  private eventMatchesKey(event: BaseEvent, event_key: EventKey): boolean {
    if (event_key === "*") {
      return true;
    }
    const normalized = this.normalizeEventKey(event_key);
    if (normalized === "*") {
      return true;
    }
    return event.event_type === normalized;
  }

  private normalizeEventKey(event_key: EventKey | "*"): string | "*" {
    if (event_key === "*") {
      return "*";
    }
    if (typeof event_key === "string") {
      return event_key;
    }
    const event_type = (event_key as { event_type?: unknown }).event_type;
    if (typeof event_type === "string" && event_type.length > 0 && event_type !== "BaseEvent") {
      return event_type;
    }
    throw new Error(
      "event_key must be a string or an event class with a static event_type (not BaseEvent)"
    );
  }

  private trimHistory(): void {
    if (this.max_history_size === null) {
      return;
    }
    if (this.event_history.size <= this.max_history_size) {
      return;
    }

    let remaining_overage = this.event_history.size - this.max_history_size;

    // First pass: remove completed events (oldest first, Map iterates in insertion order)
    for (const [event_id, event] of this.event_history) {
      if (remaining_overage <= 0) {
        break;
      }
      if (event.event_status !== "completed") {
        continue;
      }
      this.event_history.delete(event_id);
      remaining_overage -= 1;
    }

    // Second pass: force-remove oldest events regardless of status
    if (remaining_overage > 0) {
      for (const event_id of this.event_history.keys()) {
        if (remaining_overage <= 0) {
          break;
        }
        this.event_history.delete(event_id);
        remaining_overage -= 1;
      }
    }
  }
}
