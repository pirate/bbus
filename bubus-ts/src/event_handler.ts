import { v5 as uuidv5 } from 'uuid'

import type { EventHandlerFunction, HandlerOptions } from './types.js'
import { BaseEvent } from './base_event.js'
import { EventResult } from './event_result.js'

const HANDLER_ID_NAMESPACE = uuidv5('bubus-handler', uuidv5.DNS)

export class EventHandler {
  // an entry in the list of handlers that are registered on a bus
  id: string // unique uuidv5 based on hash of bus name, handler name, handler file path:lineno, registered at timestamp, and event key
  handler: EventHandlerFunction
  handler_name: string
  handler_file_path?: string
  handler_timeout: number | null
  handler_registered_at: string
  handler_registered_ts: number
  options?: HandlerOptions
  event_key: string | '*'
  eventbus_name: string

  constructor(params: {
    id?: string
    handler: EventHandlerFunction
    handler_name: string
    handler_file_path?: string
    handler_timeout: number | null
    handler_registered_at: string
    handler_registered_ts: number
    options?: HandlerOptions
    event_key: string | '*'
    eventbus_name: string
  }) {
    const handler_file_path = EventHandler.detectHandlerFilePath(params.handler_file_path)
    this.id =
      params.id ??
      EventHandler.computeHandlerId({
        eventbus_name: params.eventbus_name,
        handler_name: params.handler_name,
        handler_file_path,
        handler_registered_at: params.handler_registered_at,
        event_key: params.event_key,
      })
    this.handler = params.handler
    this.handler_name = params.handler_name
    this.handler_file_path = handler_file_path
    this.handler_timeout = params.handler_timeout
    this.handler_registered_at = params.handler_registered_at
    this.handler_registered_ts = params.handler_registered_ts
    this.options = params.options
    this.event_key = params.event_key
    this.eventbus_name = params.eventbus_name
  }

  // compute globally unique handler uuid as a hash of the bus name, handler name, handler file path, registered at timestamp, and event key
  static computeHandlerId(params: {
    eventbus_name: string
    handler_name: string
    handler_file_path?: string
    handler_registered_at: string
    event_key: string | '*'
  }): string {
    const file_path = EventHandler.detectHandlerFilePath(params.handler_file_path, 'unknown') ?? 'unknown'
    const seed = `${params.eventbus_name}|${params.handler_name}|${file_path}|${params.handler_registered_at}|${params.event_key}`
    return uuidv5(seed, HANDLER_ID_NAMESPACE)
  }

  toString(): string {
    const label = this.handler_name && this.handler_name !== 'anonymous' ? `${this.handler_name}()` : `function#${this.id.slice(-4)}()`
    const file_path = this.handler_file_path ?? 'unknown'
    return `${label} (${file_path})`
  }

  private static detectHandlerFilePath(file_path?: string, fallback: string = 'unknown'): string | undefined {
    const extract = (value: string): string =>
      value.trim().match(/\(([^)]+)\)$/)?.[1] ??
      value.trim().match(/^\s*at\s+(.+)$/)?.[1] ??
      value.trim().match(/^[^@]+@(.+)$/)?.[1] ??
      value.trim()
    let resolved_path = file_path ? extract(file_path) : file_path
    if (!resolved_path) {
      const line = new Error().stack?.split('\n').map((l) => l.trim()).filter(Boolean)[4]
      if (line) resolved_path = extract(line)
    }
    if (!resolved_path) return fallback
    const match = resolved_path.match(/^(.*?):(\d+)(?::\d+)?$/)
    let normalized = match ? match[1] : resolved_path
    const line_number = match?.[2]
    if (normalized.startsWith('file://')) {
      let path = normalized.slice('file://'.length)
      if (path.startsWith('localhost/')) path = path.slice('localhost'.length)
      if (!path.startsWith('/')) path = `/${path}`
      try {
        normalized = decodeURIComponent(path)
      } catch {
        normalized = path
      }
    }
    normalized = normalized.replace(/\/Users\/[^/]+\//, '~/')
    return line_number ? `${normalized}:${line_number}` : normalized
  }
}
export class TimeoutError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'TimeoutError'
  }
}

export class EventHandlerError extends Error {
  event_result: EventResult
  timeout_seconds: number | null
  cause: Error

  constructor(message: string, params: { event_result: EventResult; timeout_seconds?: number | null; cause: Error }) {
    super(message)
    this.name = 'EventHandlerError'
    this.event_result = params.event_result
    this.cause = params.cause
    this.timeout_seconds = params.timeout_seconds ?? this.event_result.event.event_timeout ?? null
  }

  get event(): BaseEvent {
    return this.event_result.event
  }

  get event_type(): string {
    return this.event.event_type
  }

  get handler_name(): string {
    return this.event_result.handler_name
  }

  get handler_id(): string {
    return this.event_result.handler_id
  }

  get event_timeout(): number | null {
    return this.event.event_timeout
  }
}
// EventHandlerTimeoutError: when the handler itself timed out while executing (due to event.event_timeout being exceeded)

export class EventHandlerTimeoutError extends EventHandlerError {
  constructor(message: string, params: { event_result: EventResult; timeout_seconds?: number | null; cause?: Error }) {
    super(message, {
      event_result: params.event_result,
      timeout_seconds: params.timeout_seconds,
      cause: params.cause ?? new TimeoutError(message),
    })
    this.name = 'EventHandlerTimeoutError'
  }
}
// EventHandlerCancelledError: when a pending handler was cancelled and never run due to an error (e.g. timeout) in a parent scope

export class EventHandlerCancelledError extends EventHandlerError {
  constructor(message: string, params: { event_result: EventResult; timeout_seconds?: number | null; cause: Error }) {
    super(message, params)
    this.name = 'EventHandlerCancelledError'
  }
}
// EventHandlerAbortedError: when a handler that was already running was aborted due to an error in the parent scope, not due to an error in its own logic / exceeding its own timeout

export class EventHandlerAbortedError extends EventHandlerError {
  constructor(message: string, params: { event_result: EventResult; timeout_seconds?: number | null; cause: Error }) {
    super(message, params)
    this.name = 'EventHandlerAbortedError'
  }
}

// EventHandlerResultSchemaError: when a handler returns a value that fails event_result_schema validation
export class EventHandlerResultSchemaError extends EventHandlerError {
  raw_value: unknown

  constructor(message: string, params: { event_result: EventResult; timeout_seconds?: number | null; cause: Error, raw_value: unknown }) {
    super(message, params)
    this.name = 'EventHandlerResultSchemaError'
    this.raw_value = params.raw_value
  }
}
