import { v7 as uuidv7 } from 'uuid'

import type { BaseEvent } from './base_event.js'
import { HandlerLock, withResolvers } from './lock_manager.js'
import type { Deferred } from './lock_manager.js'

export type EventResultStatus = 'pending' | 'started' | 'completed' | 'error'

export class EventResult {
  id: string
  status: EventResultStatus
  event_id: string
  handler_id: string
  handler_name: string
  handler_file_path?: string
  eventbus_name: string
  started_at?: string
  completed_at?: string
  result?: unknown
  error?: unknown
  event_children: BaseEvent[]
  // Abort signal: created when handler starts, rejected by signalAbort() to
  // interrupt runHandlerEntry's await via Promise.race.
  _abort: Deferred<never> | null
  // Handler lock: tracks ownership of the handler concurrency semaphore
  // during handler execution. Set by EventBus.runHandlerEntry, used by
  // _runImmediately for yield-and-reacquire during queue-jumps.
  _lock: HandlerLock | null

  constructor(params: { event_id: string; handler_id: string; handler_name: string; handler_file_path?: string; eventbus_name: string }) {
    this.id = uuidv7()
    this.status = 'pending'
    this.event_id = params.event_id
    this.handler_id = params.handler_id
    this.handler_name = params.handler_name
    this.handler_file_path = params.handler_file_path
    this.eventbus_name = params.eventbus_name
    this.event_children = []
    this._abort = null
    this._lock = null
  }

  // Create the abort deferred so runHandlerEntry can race against it.
  ensureAbortSignal(): Promise<never> {
    if (!this._abort) {
      this._abort = withResolvers<never>()
    }
    return this._abort.promise
  }

  // Reject the abort promise, causing runHandlerEntry's Promise.race to
  // throw immediately — even if the handler has no timeout.
  signalAbort(error: Error): void {
    if (this._abort) {
      this._abort.reject(error)
      this._abort = null
    }
  }

  markStarted(): void {
    this.status = 'started'
    this.started_at = new Date().toISOString()
  }

  markCompleted(result: unknown): void {
    if (this.status === 'completed' || this.status === 'error') return
    this.status = 'completed'
    this.result = result
    this.completed_at = new Date().toISOString()
  }

  markError(error: unknown): void {
    if (this.status === 'completed' || this.status === 'error') return
    this.status = 'error'
    this.error = error
    this.completed_at = new Date().toISOString()
  }
}
