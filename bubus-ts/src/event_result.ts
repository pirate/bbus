import { v7 as uuidv7 } from "uuid";

import type { BaseEvent } from "./base_event.js";
import type { AsyncLimiter } from "./semaphores.js";

export type EventResultStatus = "pending" | "started" | "completed" | "error";

export class EventResult {
  id: string;
  status: EventResultStatus;
  event_id: string;
  handler_id: string;
  handler_name: string;
  handler_file_path?: string;
  eventbus_name: string;
  started_at?: string;
  completed_at?: string;
  result?: unknown;
  error?: unknown;
  event_children: BaseEvent[];
  // Tracks whether this handler's execution has triggered a queue-jump via done().
  //
  // Lifecycle:
  //   1. Starts as `false` when the EventResult is created.
  //   2. Set to `true` in _runImmediately() when the handler (or its raw event's
  //      done()) triggers immediate processing. At the same time,
  //      immediate_processing_stack_depth is incremented by 1 on the bus.
  //      The guard (!queue_jump_hold) prevents double-incrementing if the
  //      handler calls done() on multiple children.
  //   3. Checked in runHandlerEntry()'s finally block: if true, decrements
  //      immediate_processing_stack_depth and releases runloop waiters.
  //      This keeps the runloop paused between when runImmediatelyAcrossBuses()
  //      returns (its own try/finally decrements) and when the handler itself
  //      finishes — without this hold, the runloop would resume prematurely
  //      while the handler is still executing after `await child.done()`.
  //   4. Reset to `false` in the same finally block after decrementing.
  queue_jump_hold: boolean;
  // The handler concurrency limiter currently held by this handler execution.
  // Set by runHandlerEntry so that _runImmediately can temporarily release it
  // (yield-and-reacquire) to let child event handlers use the same limiter
  // without deadlocking.
  _held_handler_limiter: AsyncLimiter | null;

  constructor(params: {
    event_id: string;
    handler_id: string;
    handler_name: string;
    handler_file_path?: string;
    eventbus_name: string;
  }) {
    this.id = uuidv7();
    this.status = "pending";
    this.event_id = params.event_id;
    this.handler_id = params.handler_id;
    this.handler_name = params.handler_name;
    this.handler_file_path = params.handler_file_path;
    this.eventbus_name = params.eventbus_name;
    this.event_children = [];
    this.queue_jump_hold = false;
    this._held_handler_limiter = null;
  }

  markStarted(): void {
    this.status = "started";
    this.started_at = new Date().toISOString();
  }

  markCompleted(result: unknown): void {
    this.status = "completed";
    this.result = result;
    this.completed_at = new Date().toISOString();
  }

  markError(error: unknown): void {
    this.status = "error";
    this.error = error;
    this.completed_at = new Date().toISOString();
  }
}
