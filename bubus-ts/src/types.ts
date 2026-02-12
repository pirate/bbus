import { z } from 'zod'
import type { BaseEvent } from './base_event.js'

export type EventStatus = 'pending' | 'started' | 'completed'

export type EventClass<T extends BaseEvent = BaseEvent> = { event_type?: string } & (new (...args: any[]) => T)

export type EventPattern<T extends BaseEvent = BaseEvent> = string | EventClass<T>

export type EventWithResult<TResult> = BaseEvent & { __event_result_type__?: TResult }

export type EventResultType<TEvent extends BaseEvent> = TEvent extends { __event_result_type__?: infer TResult } ? TResult : unknown

export type EventHandlerFunction<T extends BaseEvent = BaseEvent> = (
  event: T
) => void | EventResultType<T> | Promise<void | EventResultType<T>>

// For string and wildcard subscriptions we cannot reliably infer which event
// type will arrive, so return type checking intentionally degrades to unknown.
export type UntypedEventHandlerFunction<T extends BaseEvent = BaseEvent> = (event: T) => void | unknown | Promise<void | unknown>

export type FindWindow = boolean | number

type FindEventFieldFilters = {
  [K in keyof BaseEvent as K extends `event_${string}` ? K : never]?: BaseEvent[K]
}

export type FindOptions = {
  past?: FindWindow
  future?: FindWindow
  child_of?: BaseEvent | null
} & FindEventFieldFilters

export const normalizeEventPattern = (event_pattern: EventPattern | '*'): string | '*' => {
  if (event_pattern === '*') {
    return '*'
  }
  if (typeof event_pattern === 'string') {
    return event_pattern
  }
  const event_type = (event_pattern as { event_type?: unknown }).event_type
  if (typeof event_type === 'string' && event_type.length > 0 && event_type !== 'BaseEvent') {
    return event_type
  }
  const class_name = (event_pattern as { name?: unknown }).name
  if (typeof class_name === 'string' && class_name.length > 0 && class_name !== 'BaseEvent') {
    return class_name
  }
  throw new Error(
    `Invalid event key: expected event type string, "*", or BaseEvent class, got: ${JSON.stringify(event_pattern).slice(0, 80)}`
  )
}

const WRAPPER_TYPES = new Set(['optional', 'nullable', 'default', 'catch', 'prefault', 'readonly', 'nonoptional', 'exact_optional'])

const OBJECT_LIKE_TYPES = new Set(['object', 'record', 'map', 'set'])

const TYPE_ALIASES: Record<string, string> = {
  enum: 'string',
  tuple: 'array',
  void: 'undefined',
  lazy: 'unknown',
}

export const isZodSchema = (value: unknown): value is z.ZodTypeAny => !!value && typeof (value as z.ZodTypeAny).safeParse === 'function'

export const extractZodShape = (raw: Record<string, unknown>): z.ZodRawShape => {
  const shape: Record<string, z.ZodTypeAny> = {}
  for (const [key, value] of Object.entries(raw)) {
    if (key === 'event_result_schema' || key === 'event_result_type') continue
    if (isZodSchema(value)) shape[key] = value
  }
  return shape as z.ZodRawShape
}

export const toJsonSchema = (schema: unknown): unknown => {
  if (!schema || !isZodSchema(schema)) return schema
  const zod_any = z as unknown as { toJSONSchema?: (input: z.ZodTypeAny) => unknown }
  return typeof zod_any.toJSONSchema === 'function' ? zod_any.toJSONSchema(schema) : undefined
}

export const getStringTypeName = (schema?: z.ZodTypeAny): string | undefined => {
  if (!schema) return undefined

  const visited = new Set<z.ZodTypeAny>()
  const infer = (value: z.ZodTypeAny): string => {
    if (visited.has(value)) return 'unknown'
    visited.add(value)

    const def = (value as unknown as { _def?: Record<string, unknown> })._def ?? {}
    const kind = typeof def.type === 'string' ? def.type : ''
    if (!kind) return 'unknown'

    if (WRAPPER_TYPES.has(kind)) {
      return isZodSchema(def.innerType) ? infer(def.innerType) : 'unknown'
    }
    if (kind === 'pipe') {
      return isZodSchema(def.out) ? infer(def.out) : 'unknown'
    }
    if (kind === 'union') {
      const options = (Array.isArray(def.options) ? def.options : []).filter(isZodSchema)
      if (options.length === 0) return 'unknown'
      const inferred = new Set(options.map((option) => infer(option)))
      return inferred.size === 1 ? [...inferred][0] : 'unknown'
    }
    if (kind === 'literal') {
      const literal = Array.isArray(def.values) ? def.values[0] : undefined
      if (literal === null) return 'null'
      if (typeof literal === 'object') return 'object'
      if (typeof literal === 'function') return 'function'
      return typeof literal
    }
    if (OBJECT_LIKE_TYPES.has(kind)) return 'object'
    return TYPE_ALIASES[kind] ?? kind
  }

  return infer(schema)
}
