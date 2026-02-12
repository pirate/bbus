import { z } from 'zod'
import type { BaseEvent } from './base_event.js'

export type EventStatus = 'pending' | 'started' | 'completed'

export type EventClass<T extends BaseEvent = BaseEvent> = { event_type?: string } & (new (...args: any[]) => T)

export type EventPattern<T extends BaseEvent = BaseEvent> = string | EventClass<T>

export type EventWithResultSchema<TResult> = BaseEvent & { __event_result_type__?: TResult }

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

export const isZodSchema = (value: unknown): value is z.ZodTypeAny => !!value && typeof (value as z.ZodTypeAny).safeParse === 'function'

export const extractZodShape = (raw: Record<string, unknown>): z.ZodRawShape => {
  const shape: Record<string, z.ZodTypeAny> = {}
  for (const [key, value] of Object.entries(raw)) {
    if (key === 'event_result_type') continue
    if (isZodSchema(value)) shape[key] = value
  }
  return shape as z.ZodRawShape
}

export const toJsonSchema = (schema: unknown): unknown => {
  if (!schema || !isZodSchema(schema)) return schema
  const zod_any = z as unknown as { toJSONSchema?: (input: z.ZodTypeAny) => unknown }
  return typeof zod_any.toJSONSchema === 'function' ? zod_any.toJSONSchema(schema) : undefined
}

const getJsonSchemaTypeName = (schema: unknown): string | undefined => {
  if (!schema || typeof schema !== 'object') return undefined
  const raw_type = (schema as { type?: unknown }).type
  let schema_type: string | undefined
  if (typeof raw_type === 'string') {
    schema_type = raw_type
  } else if (Array.isArray(raw_type)) {
    const non_null = raw_type.filter((value): value is string => typeof value === 'string' && value !== 'null')
    if (non_null.length === 1) {
      schema_type = non_null[0]
    }
  }
  if (!schema_type) return undefined
  if (schema_type === 'integer') return 'number'
  if (
    schema_type === 'string' ||
    schema_type === 'number' ||
    schema_type === 'boolean' ||
    schema_type === 'object' ||
    schema_type === 'array' ||
    schema_type === 'null'
  ) {
    return schema_type
  }
  return undefined
}

export const jsonSchemaToZodPrimitive = (schema: unknown): z.ZodTypeAny | undefined => {
  const schema_type = getJsonSchemaTypeName(schema)
  if (schema_type === 'string') return z.string()
  if (schema_type === 'number') return z.number()
  if (schema_type === 'boolean') return z.boolean()
  return undefined
}
