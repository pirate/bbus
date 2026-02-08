import { v7 as uuidv7 } from 'uuid'

import { BaseEvent } from './base_event.js'
import type { EventHandler } from './event_handler.js'
import { HandlerLock, type ConcurrencyMode, withResolvers } from './lock_manager.js'
import type { Deferred } from './lock_manager.js'
import type { EventHandlerFunction, EventResultType } from './types.js'

// More precise than event.event_status, includes separate 'error' state for handlers that throw errors during execution
export type EventResultStatus = 'pending' | 'started' | 'completed' | 'error'

export type EventResultData = {
  id?: string
  status?: EventResultStatus
  event_id?: string
  handler?: {
    id?: string
    handler_name?: string
    handler_file_path?: string
    handler_timeout?: number | null
    event_handler_concurrency?: ConcurrencyMode
    handler_registered_at?: string
    handler_registered_ts?: number
    event_key?: string | '*'
    eventbus_name?: string
  }
  started_at?: string
  started_ts?: number
  completed_at?: string
  completed_ts?: number
  result?: unknown
  error?: unknown
  event_children?: string[]
}

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
  // interrupt runEventHandler's await via Promise.race.
  _abort: Deferred<never> | null
  // Handler lock: tracks ownership of the handler concurrency semaphore
  // during handler execution. Set by EventBus.runEventHandler, used by
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

  get handler_id(): string {
    return this.handler.id
  }

  get handler_name(): string {
    return this.handler.handler_name
  }

  get handler_file_path(): string | undefined {
    return this.handler.handler_file_path
  }

  get handler_timeout(): number | null {
    return this.handler.handler_timeout
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

  // Reject the abort promise, causing runEventHandler's Promise.race to
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

  toJSON(): EventResultData {
    return {
      id: this.id,
      status: this.status,
      event_id: this.event.event_id,
      handler: {
        id: this.handler.id,
        handler_name: this.handler.handler_name,
        handler_file_path: this.handler.handler_file_path,
        handler_timeout: this.handler.handler_timeout,
        event_handler_concurrency: this.handler.event_handler_concurrency,
        handler_registered_at: this.handler.handler_registered_at,
        handler_registered_ts: this.handler.handler_registered_ts,
        event_key: this.handler.event_key,
        eventbus_name: this.handler.eventbus_name,
      },
      started_at: this.started_at,
      started_ts: this.started_ts,
      completed_at: this.completed_at,
      completed_ts: this.completed_ts,
      result: this.result,
      error: this.error,
      event_children: this.event_children.map((child) => child.event_id),
    }
  }

  static fromJSON<TEvent extends BaseEvent>(event: TEvent, data: unknown): EventResult<TEvent> | null {
    if (!data || typeof data !== 'object') {
      return null
    }
    const record = data as EventResultData
    const handler_record = record.handler ?? {}

    const handler_stub = {
      id: typeof handler_record.id === 'string' ? handler_record.id : `deserialized_handler_${uuidv7()}`,
      handler: (() => undefined) as EventHandlerFunction,
      handler_name: typeof handler_record.handler_name === 'string' ? handler_record.handler_name : 'deserialized_handler',
      handler_file_path: typeof handler_record.handler_file_path === 'string' ? handler_record.handler_file_path : undefined,
      handler_timeout:
        typeof handler_record.handler_timeout === 'number' || handler_record.handler_timeout === null
          ? handler_record.handler_timeout
          : null,
      event_handler_concurrency: handler_record.event_handler_concurrency,
      handler_registered_at:
        typeof handler_record.handler_registered_at === 'string' ? handler_record.handler_registered_at : event.event_created_at,
      handler_registered_ts:
        typeof handler_record.handler_registered_ts === 'number' ? handler_record.handler_registered_ts : event.event_created_ts,
      event_key:
        handler_record.event_key === '*' || typeof handler_record.event_key === 'string' ? handler_record.event_key : event.event_type,
      eventbus_name: typeof handler_record.eventbus_name === 'string' ? handler_record.eventbus_name : (event.bus?.name ?? 'unknown'),
      toString: () => {
        const name = typeof handler_record.handler_name === 'string' ? handler_record.handler_name : 'deserialized_handler'
        const file = typeof handler_record.handler_file_path === 'string' ? handler_record.handler_file_path : 'unknown'
        return `${name}() (${file})`
      },
    } as unknown as EventHandler

    const result = new EventResult<TEvent>({ event, handler: handler_stub })
    if (typeof record.id === 'string') {
      result.id = record.id
    }
    if (record.status === 'pending' || record.status === 'started' || record.status === 'completed' || record.status === 'error') {
      result.status = record.status
    }
    if (typeof record.started_at === 'string') {
      result.started_at = record.started_at
    }
    if (typeof record.started_ts === 'number') {
      result.started_ts = record.started_ts
    }
    if (typeof record.completed_at === 'string') {
      result.completed_at = record.completed_at
    }
    if (typeof record.completed_ts === 'number') {
      result.completed_ts = record.completed_ts
    }
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
