import { z } from 'zod'
import { v5 as uuidv5 } from 'uuid'

import { normalizeEventPattern, type EventHandlerCallable, type EventPattern } from './types.js'
import { BaseEvent } from './base_event.js'
import type { EventResult } from './event_result.js'

const HANDLER_ID_NAMESPACE = uuidv5('bubus-handler', uuidv5.DNS)
const HandlerRegisteredTsSchema = z.number().int().finite().nonnegative().max(Number.MAX_SAFE_INTEGER)

export type EphemeralFindEventHandler = {
  // Similar to a handler, except it's for .find() calls.
  // Resolved on dispatch, ephemeral, and never shows up in the processing tree.
  event_pattern: EventPattern
  matches: (event: BaseEvent) => boolean
  resolve: (event: BaseEvent) => void
  timeout_id?: ReturnType<typeof setTimeout>
}

export const FindWaiterJSONSchema = z
  .object({
    event_pattern: z.union([z.string(), z.literal('*')]),
    has_timeout: z.boolean(),
  })
  .strict()

export type FindWaiterJSON = z.infer<typeof FindWaiterJSONSchema>

export class FindWaiter {
  static toJSON(waiter: EphemeralFindEventHandler): FindWaiterJSON {
    return {
      event_pattern: normalizeEventPattern(waiter.event_pattern),
      has_timeout: waiter.timeout_id !== undefined,
    }
  }

  static fromJSON(
    data: unknown,
    overrides: {
      matches?: (event: BaseEvent) => boolean
      resolve?: (event: BaseEvent) => void
    } = {}
  ): EphemeralFindEventHandler {
    const record = FindWaiterJSONSchema.parse(data)
    const event_pattern = record.event_pattern
    const defaultMatches = (event: BaseEvent): boolean => event_pattern === '*' || event.event_type === event_pattern
    return {
      event_pattern,
      matches: overrides.matches ?? defaultMatches,
      resolve: overrides.resolve ?? (() => {}),
    }
  }

  static toJSONArray(waiters: Iterable<EphemeralFindEventHandler>): FindWaiterJSON[] {
    return Array.from(waiters, (waiter) => FindWaiter.toJSON(waiter))
  }

  static fromJSONArray(
    data: unknown,
    overrides: {
      matches?: (event: BaseEvent) => boolean
      resolve?: (event: BaseEvent) => void
    } = {}
  ): EphemeralFindEventHandler[] {
    if (!Array.isArray(data)) {
      return []
    }
    return data.map((item) => FindWaiter.fromJSON(item, overrides))
  }
}

export const EventHandlerJSONSchema = z
  .object({
    id: z.string(),
    eventbus_name: z.string(),
    eventbus_id: z.string().uuid(),
    event_pattern: z.union([z.string(), z.literal('*')]),
    handler_name: z.string(),
    handler_file_path: z.string().nullable().optional(),
    handler_timeout: z.number().nullable().optional(),
    handler_slow_timeout: z.number().nullable().optional(),
    handler_registered_at: z.string(),
    handler_registered_ts: HandlerRegisteredTsSchema,
  })
  .strict()

export type EventHandlerJSON = z.infer<typeof EventHandlerJSONSchema>

// an entry in the list of event handlers that are registered on a bus
export class EventHandler {
  id: string // unique uuidv5 based on hash of bus name, handler name, handler file path:lineno, registered at timestamp, and event key
  handler: EventHandlerCallable // normalized async handler callable used by runtime execution
  handler_name: string // name of the handler function, or 'anonymous' if the handler is an anonymous/arrow function
  handler_file_path: string | null // ~/path/to/source/file.ts:123, or null when unknown
  handler_timeout?: number | null // maximum time in seconds that the handler is allowed to run before it is aborted, resolved at runtime if not set
  handler_slow_timeout?: number | null // warning threshold in seconds for slow handler execution
  handler_registered_at: string // ISO datetime used in the deterministic handler-id seed
  handler_registered_ts: number // integer offset component paired with handler_registered_at in the handler-id seed
  event_pattern: string | '*' // event_type string to match against, or '*' to match all events
  eventbus_name: string // name of the event bus that the handler is registered on
  eventbus_id: string // uuidv7 identifier of the event bus that the handler is registered on

  private static _normalized_handler_by_raw = new WeakMap<EventHandlerCallable, EventHandlerCallable>()
  private static _raw_handler_by_normalized = new WeakMap<EventHandlerCallable, EventHandlerCallable>()

  constructor(params: {
    id?: string
    handler: EventHandlerCallable
    handler_name: string
    handler_file_path?: string | null
    handler_timeout?: number | null
    handler_slow_timeout?: number | null
    handler_registered_at: string
    handler_registered_ts: number
    event_pattern: string | '*'
    eventbus_name: string
    eventbus_id: string
  }) {
    const handler_registered_ts = HandlerRegisteredTsSchema.parse(params.handler_registered_ts)
    this.id =
      params.id ??
      EventHandler.computeHandlerId({
        eventbus_id: params.eventbus_id,
        handler_name: params.handler_name,
        handler_file_path: params.handler_file_path,
        handler_registered_at: params.handler_registered_at,
        handler_registered_ts,
        event_pattern: params.event_pattern,
      })
    this.handler = EventHandler.resolveAsyncHandler(params.handler)
    this.handler_name = params.handler_name
    this.handler_file_path = params.handler_file_path ?? null
    this.handler_timeout = params.handler_timeout
    this.handler_slow_timeout = params.handler_slow_timeout
    this.handler_registered_at = params.handler_registered_at
    this.handler_registered_ts = handler_registered_ts
    this.event_pattern = params.event_pattern
    this.eventbus_name = params.eventbus_name
    this.eventbus_id = params.eventbus_id
  }

  private static isAsyncFunction(handler: EventHandlerCallable): boolean {
    return Object.prototype.toString.call(handler) === '[object AsyncFunction]'
  }

  static resolveAsyncHandler(handler: EventHandlerCallable): EventHandlerCallable {
    const cached_normalized_handler = this._normalized_handler_by_raw.get(handler)
    if (cached_normalized_handler) {
      return cached_normalized_handler
    }
    if (this._raw_handler_by_normalized.has(handler)) {
      return handler
    }

    const normalized_handler = this.isAsyncFunction(handler)
      ? handler
      : ((async (event: BaseEvent): Promise<unknown> => await handler(event as never)) as EventHandlerCallable)

    this._normalized_handler_by_raw.set(handler, normalized_handler)
    this._raw_handler_by_normalized.set(normalized_handler, handler)
    return normalized_handler
  }

  static _getRawHandler(handler: EventHandlerCallable): EventHandlerCallable {
    return this._raw_handler_by_normalized.get(handler) ?? handler
  }

  static _handlersMatch(registered_handler: EventHandlerCallable, candidate_handler: EventHandlerCallable): boolean {
    if (registered_handler === candidate_handler) {
      return true
    }
    const registered_raw_handler = this._getRawHandler(registered_handler)
    const candidate_raw_handler = this._getRawHandler(candidate_handler)
    return registered_raw_handler === candidate_handler || registered_raw_handler === candidate_raw_handler
  }

  // compute globally unique handler uuid as a hash of the bus name, handler name, handler file path, registered at timestamp, and event key
  static computeHandlerId(params: {
    eventbus_id: string
    handler_name: string
    handler_file_path?: string | null
    handler_registered_at: string
    handler_registered_ts: number
    event_pattern: string | '*'
  }): string {
    const handler_registered_ts = HandlerRegisteredTsSchema.parse(params.handler_registered_ts)
    const file_path = params.handler_file_path ?? 'unknown'
    const seed = `${params.eventbus_id}|${params.handler_name}|${file_path}|${params.handler_registered_at}|${handler_registered_ts}|${params.event_pattern}`
    return uuidv5(seed, HANDLER_ID_NAMESPACE)
  }

  // "someHandlerName() @ ~/path/to/source/file.ts:123"  <- best case when file path is available and its a named function
  // "function#1234()"  <- worst case when no file path is available and its an anonymous/arrow function defined inline
  toString(): string {
    const label = this.handler_name && this.handler_name !== 'anonymous' ? `${this.handler_name}()` : `function#${this.id.slice(-4)}()`
    return this.handler_file_path ? `${label} @ ${this.handler_file_path}` : label
  }

  // autodetect the path/to/source/file.ts:lineno where the handler is defined for better logs
  // optional (controlled by EventBus.event_handler_detect_file_paths) because it can slow down performance to introspect stack traces and find file paths
  _detectHandlerFilePath(): void {
    const line = new Error().stack
      ?.split('\n')
      .map((l) => l.trim())
      .filter(Boolean)[4]
    if (!line) return
    const resolved_path =
      line.trim().match(/\(([^)]+)\)$/)?.[1] ??
      line.trim().match(/^\s*at\s+(.+)$/)?.[1] ??
      line.trim().match(/^[^@]+@(.+)$/)?.[1] ??
      line.trim()
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
    normalized = normalized.replace(/\/users\/[^/]+\//i, '~/').replace(/\/home\/[^/]+\//i, '~/')
    this.handler_file_path = line_number ? `${normalized}:${line_number}` : normalized
  }

  toJSON(): EventHandlerJSON {
    return {
      id: this.id,
      eventbus_name: this.eventbus_name,
      eventbus_id: this.eventbus_id,
      event_pattern: this.event_pattern,
      handler_name: this.handler_name,
      handler_file_path: this.handler_file_path,
      handler_timeout: this.handler_timeout,
      handler_slow_timeout: this.handler_slow_timeout,
      handler_registered_at: this.handler_registered_at,
      handler_registered_ts: this.handler_registered_ts,
    }
  }

  static fromJSON(data: unknown, handler?: EventHandlerCallable): EventHandler {
    const record = EventHandlerJSONSchema.parse(data)
    const handler_fn = handler ?? ((() => undefined) as EventHandlerCallable)
    const handler_name = record.handler_name || handler_fn.name || 'anonymous' // 'anonymous' is the default name for anonymous/arrow functions
    return new EventHandler({
      id: record.id,
      handler: handler_fn,
      handler_name,
      handler_file_path: record.handler_file_path ?? null,
      handler_timeout: record.handler_timeout,
      handler_slow_timeout: record.handler_slow_timeout,
      handler_registered_at: record.handler_registered_at,
      handler_registered_ts: record.handler_registered_ts,
      event_pattern: record.event_pattern,
      eventbus_name: record.eventbus_name,
      eventbus_id: record.eventbus_id,
    })
  }

  static toJSONArray(handlers: Iterable<EventHandler>): EventHandlerJSON[] {
    return Array.from(handlers, (handler) => handler.toJSON())
  }

  static fromJSONArray(data: unknown, handler?: EventHandlerCallable): EventHandler[] {
    if (!Array.isArray(data)) {
      return []
    }
    return data.map((item) => EventHandler.fromJSON(item, handler))
  }

  get eventbus_label(): string {
    return `${this.eventbus_name}#${this.eventbus_id.slice(-4)}`
  }
}

// Generic base TimeoutError used for EventHandlerTimeoutError.cause default value if
export class TimeoutError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'TimeoutError'
  }
}

// Base class for all errors that can occur while running an event handler
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

// When the handler itself timed out while executing (due to handler.handler_timeout being exceeded)
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

// When a pending handler was cancelled and never run due to an error (e.g. timeout) in a parent scope
export class EventHandlerCancelledError extends EventHandlerError {
  constructor(message: string, params: { event_result: EventResult; timeout_seconds?: number | null; cause: Error }) {
    super(message, params)
    this.name = 'EventHandlerCancelledError'
  }
}

// When a handler that was already running was aborted due to an error in the parent scope, not due to an error in its own logic / exceeding its own timeout
export class EventHandlerAbortedError extends EventHandlerError {
  constructor(message: string, params: { event_result: EventResult; timeout_seconds?: number | null; cause: Error }) {
    super(message, params)
    this.name = 'EventHandlerAbortedError'
  }
}

// When a handler run succesfully but returned a value that failed event_result_type validation
export class EventHandlerResultSchemaError extends EventHandlerError {
  raw_value: unknown

  constructor(message: string, params: { event_result: EventResult; timeout_seconds?: number | null; cause: Error; raw_value: unknown }) {
    super(message, params)
    this.name = 'EventHandlerResultSchemaError'
    this.raw_value = params.raw_value
  }

  get expected_schema(): any {
    return this.event_result.event.event_result_type
  }
}
