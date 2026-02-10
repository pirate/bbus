import { v7 as uuidv7 } from 'uuid'

import { z } from 'zod'

import { BaseEvent } from './base_event.js'
import type { EventBus } from './event_bus.js'
import {
  EventHandler,
  EventHandlerCancelledError,
  EventHandlerJSONSchema,
  EventHandlerResultSchemaError,
  EventHandlerTimeoutError,
} from './event_handler.js'
import { HandlerLock, withResolvers } from './lock_manager.js'
import type { Deferred } from './lock_manager.js'
import type { EventHandlerFunction, EventResultType } from './types.js'
import { runWithAsyncContext } from './async_context.js'
import { RetryTimeoutError } from './retry.js'

// More precise than event.event_status, includes separate 'error' state for handlers that throw errors during execution
export type EventResultStatus = 'pending' | 'started' | 'completed' | 'error'

export const EventResultJSONSchema = z
  .object({
    id: z.string(),
    status: z.enum(['pending', 'started', 'completed', 'error']),
    event_id: z.string(),
    handler: EventHandlerJSONSchema,
    started_at: z.string().optional(),
    started_ts: z.number().optional(),
    completed_at: z.string().optional(),
    completed_ts: z.number().optional(),
    result: z.unknown().optional(),
    error: z.unknown().optional(),
    event_children: z.array(z.string()).optional(),
  })
  .strict()

export type EventResultJSON = z.infer<typeof EventResultJSONSchema>

// Object that tracks the pending or completed execution of a single event handler
export class EventResult<TEvent extends BaseEvent = BaseEvent> {
  id: string // unique uuidv7 identifier for the event result
  status: EventResultStatus // 'pending', 'started', 'completed', or 'error'
  event: TEvent // the Event that the handler is processing
  handler: EventHandler // the EventHandler object that going to process the event
  started_at?: string // ISO datetime string version of started_ts
  started_ts?: number // nanosecond monotonic version of started_at
  completed_at?: string // ISO datetime string version of completed_ts
  completed_ts?: number // nanosecond monotonic version of completed_at
  result?: EventResultType<TEvent> // parsed return value from the event handler
  error?: unknown // error object thrown by the event handler, or null if the handler completed successfully
  event_children: BaseEvent[] // any child events that were emitted during handler execution are captured automatically and stored here to track hierarchy

  // Abort signal: created when handler starts, rejected by signalAbort() to
  // interrupt runHandler's await via Promise.race.
  _abort: Deferred<never> | null
  // Handler lock: tracks ownership of the handler concurrency semaphore
  // during handler execution. Set by runHandler(), used by
  // processEventImmediately for yield-and-reacquire during queue-jumps.
  _lock: HandlerLock | null

  constructor(params: { event: TEvent; handler: EventHandler }) {
    this.id = uuidv7()
    this.status = 'pending'
    this.event = params.event
    this.handler = params.handler
    this.event_children = []
    this.result = undefined
    this.error = undefined
    this._abort = null
    this._lock = null
  }

  toString(): string {
    return `${this.result ?? 'null'} (${this.status})`
  }

  get event_id(): string {
    return this.event.event_id
  }

  get bus(): EventBus {
    return this.event.bus!
  }

  get handler_id(): string {
    return this.handler.id
  }

  get handler_name(): string {
    return this.handler.handler_name
  }

  get handler_file_path(): string | undefined {
    return this.handler.handler_file_path
  }

  get eventbus_name(): string {
    return this.handler.eventbus_name
  }

  // shortcut for the result value so users can do event_result.value instead of event_result.result
  get value(): EventResultType<TEvent> | undefined {
    return this.result
  }

  // Link a child event emitted by this handler run to the parent event/result.
  linkEmittedChildEvent(child_event: BaseEvent): void {
    const original_child = child_event._event_original ?? child_event
    const parent_event = this.event._event_original ?? this.event
    if (!original_child.event_parent_id) {
      original_child.event_parent_id = parent_event.event_id
    }
    if (!original_child.event_emitted_by_handler_id) {
      original_child.event_emitted_by_handler_id = this.handler_id
    }
    if (!this.event_children.some((child) => child.event_id === original_child.event_id)) {
      this.event_children.push(original_child)
    }
  }

  // Get the raw return value from the handler, even if it threw an error / failed validation
  get raw_value(): EventResultType<TEvent> | undefined {
    if (this.error && (this.error as any).raw_value !== undefined) {
      return (this.error as any).raw_value
    }
    return this.result
  }

  // Resolve handler timeout in seconds using precedence: handler -> event -> bus defaults.
  get handler_timeout(): number | null {
    const original = this.event._event_original ?? this.event
    const bus = this.bus
    const resolved_handler_timeout =
      this.handler.handler_timeout !== undefined
        ? this.handler.handler_timeout
        : original.event_handler_timeout !== undefined
          ? original.event_handler_timeout
          : (bus?.event_timeout_default ?? null)
    const resolved_event_timeout = original.event_timeout ?? null
    if (resolved_handler_timeout === null && resolved_event_timeout === null) {
      return null
    }
    if (resolved_handler_timeout === null) {
      return resolved_event_timeout
    }
    if (resolved_event_timeout === null) {
      return resolved_handler_timeout
    }
    return Math.min(resolved_handler_timeout, resolved_event_timeout)
  }

  // Resolve slow handler warning threshold in seconds using precedence: handler -> event -> bus defaults.
  get handler_slow_timeout(): number | null {
    const original = this.event._event_original ?? this.event
    const bus = this.bus

    if (this.handler.handler_slow_timeout !== undefined) {
      return this.handler.handler_slow_timeout
    }
    if (original.event_handler_slow_timeout !== undefined) {
      return original.event_handler_slow_timeout
    }
    const event_slow_timeout = (original as { event_slow_timeout?: number | null }).event_slow_timeout
    if (event_slow_timeout !== undefined) {
      return event_slow_timeout
    }
    const slow_timeout = (original as { slow_timeout?: number | null }).slow_timeout
    if (slow_timeout !== undefined) {
      return slow_timeout
    }
    if (bus?.event_handler_slow_timeout !== undefined) {
      return bus.event_handler_slow_timeout
    }
    return bus?.event_slow_timeout ?? null
  }

  // Create a slow-handler warning timer that logs if the handler runs too long.
  createSlowHandlerWarningTimer(effective_timeout: number | null): ReturnType<typeof setTimeout> | null {
    const handler_warn_timeout = this.handler_slow_timeout
    const warn_ms = handler_warn_timeout === null ? null : handler_warn_timeout * 1000
    const should_warn = warn_ms !== null && (effective_timeout === null || effective_timeout * 1000 > warn_ms)
    if (!should_warn || warn_ms === null) {
      return null
    }
    const event = this.event._event_original ?? this.event
    const bus_name = this.handler.eventbus_name
    const started_at_ms = performance.now()
    return setTimeout(() => {
      if (this.status !== 'started') {
        return
      }
      const elapsed_ms = performance.now() - started_at_ms
      const elapsed_seconds = (elapsed_ms / 1000).toFixed(1)
      console.warn(
        `[bubus] Slow event handler: ${bus_name}.on(${event.toString()}, ${this.handler.toString()}) still running after ${elapsed_seconds}s`
      )
    }, warn_ms)
  }

  // Run the handler end-to-end, including concurrency locks, timeouts, and result tracking.
  async runHandler(): Promise<void> {
    if (this.status === 'error' && this.error instanceof EventHandlerCancelledError) {
      return
    }

    const event = this.event._event_original ?? this.event
    const bus = this.bus
    const handler_event = bus ? bus.getEventProxyScopedToThisBus(event, this) : event
    const semaphore = event.getHandlerSemaphore(bus?.event_handler_concurrency_default)

    if (semaphore) {
      await semaphore.acquire()
    }

    // if the result is already in an error or completed state, release the semaphore immediately and return
    if (this.status === 'error' || this.status === 'completed') {
      if (semaphore) semaphore.release()
      return
    }

    // exit the handler lock if it is already held
    if (this._lock) this._lock.exitHandlerRun()
    // create a new handler lock to track ownership of the semaphore during handler execution
    this._lock = new HandlerLock(semaphore)
    if (bus) {
      bus.locks.enterActiveHandlerContext(this)
    }

    // resolve the effective timeout by combining the event timeout and the handler timeout
    const effective_timeout = this.handler_timeout
    const slow_handler_warning_timer = this.createSlowHandlerWarningTimer(effective_timeout)

    const run_handler = () =>
      Promise.resolve().then(() => runWithAsyncContext(event._event_dispatch_context ?? null, () => this.handler.handler(handler_event)))

    try {
      const abort_signal = this.markStarted()
      let handler_result: unknown

      if (effective_timeout === null) {
        handler_result = await Promise.race([run_handler(), abort_signal])
      } else {
        const timeout_seconds = effective_timeout
        const timeout_ms = timeout_seconds * 1000

        const { promise, resolve, reject } = withResolvers<unknown>()
        let settled = false

        const finalize = (fn: (value?: unknown) => void) => {
          return (value?: unknown) => {
            if (settled) {
              return
            }
            settled = true
            clearTimeout(timer)
            fn(value)
          }
        }

        const bus_label = bus?.toString() ?? this.handler.eventbus_name
        const timer = setTimeout(() => {
          finalize(reject)(
            new EventHandlerTimeoutError(
              `${bus_label}.on(${event.toString()}, ${this.handler.toString()}) timed out after ${timeout_seconds}s`,
              {
                event_result: this,
                timeout_seconds,
              }
            )
          )
        }, timeout_ms)

        run_handler().then(finalize(resolve)).catch(finalize(reject))

        handler_result = await Promise.race([promise, abort_signal])
      }

      if (event.event_result_schema && handler_result !== undefined) {
        const parsed = event.event_result_schema.safeParse(handler_result)
        if (parsed.success) {
          this.markCompleted(parsed.data as EventResultType<TEvent>)
        } else {
          const bus_label = bus?.toString() ?? this.handler.eventbus_name
          const error = new EventHandlerResultSchemaError(
            `${bus_label}.on(${event.toString()}, ${this.handler.toString()}) return value ${JSON.stringify(handler_result).slice(0, 20)}... did not match event_result_schema ${event.event_result_type}: ${parsed.error.message}`,
            { event_result: this, cause: parsed.error, raw_value: handler_result }
          )
          this.markError(error)
        }
      } else {
        this.markCompleted(handler_result as EventResultType<TEvent> | undefined)
      }
    } catch (error) {
      const normalized_error =
        error instanceof RetryTimeoutError
          ? new EventHandlerTimeoutError(error.message, { event_result: this, timeout_seconds: error.timeout_seconds, cause: error })
          : error
      if (normalized_error instanceof EventHandlerTimeoutError) {
        this.markError(normalized_error)
        event.cancelPendingDescendants(normalized_error)
      } else {
        this.markError(normalized_error)
      }
    } finally {
      this._abort = null
      this._lock?.exitHandlerRun()
      if (bus) {
        bus.locks.exitActiveHandlerContext(this)
        bus.locks.releaseRunloopPauseForQueueJumpEvent(this)
      }
      if (slow_handler_warning_timer) {
        clearTimeout(slow_handler_warning_timer)
      }
    }
  }

  // Reject the abort promise, causing runHandler's Promise.race to
  // throw immediately — even if the handler has no timeout.
  signalAbort(error: Error): void {
    if (this._abort) {
      this._abort.reject(error)
      this._abort = null
    }
  }

  // Mark started and return the abort promise for Promise.race.
  markStarted(): Promise<never> {
    if (!this._abort) {
      this._abort = withResolvers<never>()
    }
    if (this.status === 'pending') {
      this.status = 'started'
      const { isostring: started_at, ts: started_ts } = BaseEvent.nextTimestamp()
      this.started_at = started_at
      this.started_ts = started_ts
    }
    return this._abort.promise
  }

  markCompleted(result: EventResultType<TEvent> | undefined): void {
    if (this.status === 'completed' || this.status === 'error') return
    this.status = 'completed'
    this.result = result
    const { isostring: completed_at, ts: completed_ts } = BaseEvent.nextTimestamp()
    this.completed_at = completed_at
    this.completed_ts = completed_ts
  }

  markError(error: unknown): void {
    if (this.status === 'completed' || this.status === 'error') return
    this.status = 'error'
    this.error = error
    const { isostring: completed_at, ts: completed_ts } = BaseEvent.nextTimestamp()
    this.completed_at = completed_at
    this.completed_ts = completed_ts
  }

  toJSON(): EventResultJSON {
    return {
      id: this.id,
      status: this.status,
      event_id: this.event.event_id,
      handler: this.handler.toJSON(),
      started_at: this.started_at,
      started_ts: this.started_ts,
      completed_at: this.completed_at,
      completed_ts: this.completed_ts,
      result: this.result,
      error: this.error,
      event_children: this.event_children.map((child) => child.event_id),
    }
  }

  static fromJSON<TEvent extends BaseEvent>(event: TEvent, data: unknown): EventResult<TEvent> {
    const record = EventResultJSONSchema.parse(data)
    const handler_stub = EventHandler.fromJSON(record.handler, (() => undefined) as EventHandlerFunction)

    const result = new EventResult<TEvent>({ event, handler: handler_stub })
    result.id = record.id
    result.status = record.status
    result.started_at = record.started_at
    result.started_ts = record.started_ts
    result.completed_at = record.completed_at
    result.completed_ts = record.completed_ts
    if ('result' in record) {
      result.result = record.result as EventResultType<TEvent>
    }
    if ('error' in record) {
      result.error = record.error
    }
    result.event_children = []
    return result
  }
}
