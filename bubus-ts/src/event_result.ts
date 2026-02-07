import { v7 as uuidv7 } from 'uuid'

import { BaseEvent } from './base_event.js'
import type { EventHandler } from './event_handler.js'
import { HandlerLock, withResolvers } from './lock_manager.js'
import type { Deferred } from './lock_manager.js'

export type EventResultStatus = 'pending' | 'started' | 'completed' | 'error'

export class EventResult {
  id: string
  status: EventResultStatus
  event: BaseEvent
  handler: EventHandler
  started_at?: string
  started_ts?: number
  completed_at?: string
  completed_ts?: number
  result?: unknown // raw return value from the event handler
  error?: unknown // error object thrown by the event handler
  event_children: BaseEvent[]
  // Abort signal: created when handler starts, rejected by signalAbort() to
  // interrupt runEventHandler's await via Promise.race.
  _abort: Deferred<never> | null
  // Handler lock: tracks ownership of the handler concurrency semaphore
  // during handler execution. Set by EventBus.runEventHandler, used by
  // processEventImmediately for yield-and-reacquire during queue-jumps.
  _lock: HandlerLock | null

  constructor(params: { event: BaseEvent; handler: EventHandler }) {
    this.id = uuidv7()
    this.status = 'pending'
    this.event = params.event
    this.handler = params.handler
    this.event_children = []
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

  markCompleted(result: unknown): void {
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
}
