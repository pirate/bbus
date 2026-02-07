import type { BaseEvent } from './base_event.js'
import type { ConcurrencyMode } from './lock_manager.js'

export type EventStatus = 'pending' | 'started' | 'completed'

export type EventClass<T extends BaseEvent = BaseEvent> = { event_type?: string } & (new (...args: any[]) => T)

export type EventKey<T extends BaseEvent = BaseEvent> = string | EventClass<T>

export type EventWithResult<TResult> = BaseEvent & { __event_result_type__?: TResult }

export type EventResultType<TEvent extends BaseEvent> =
  TEvent extends { __event_result_type__?: infer TResult } ? TResult : unknown

export type EventHandlerFunction<T extends BaseEvent = BaseEvent> = (
  event: T
) => void | EventResultType<T> | Promise<void | EventResultType<T>>

// For string and wildcard subscriptions we cannot reliably infer which event
// type will arrive, so return type checking intentionally degrades to unknown.
export type UntypedEventHandlerFunction<T extends BaseEvent = BaseEvent> = (event: T) => void | unknown | Promise<void | unknown>

export type HandlerOptions = {
  handler_concurrency?: ConcurrencyMode
  handler_timeout?: number | null
}

export type FindWindow = boolean | number

export type FindOptions = {
  past?: FindWindow
  future?: FindWindow
  child_of?: BaseEvent | null
}
