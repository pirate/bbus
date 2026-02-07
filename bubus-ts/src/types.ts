import type { BaseEvent } from './base_event.js'
import type { ConcurrencyMode } from './lock_manager.js'

export type EventStatus = 'pending' | 'started' | 'completed'

export type EventClass<T extends BaseEvent = BaseEvent> = { event_type?: string } & (new (...args: any[]) => T)

export type EventKey<T extends BaseEvent = BaseEvent> = string | EventClass<T>

export type EventHandlerFunction<T extends BaseEvent = BaseEvent> = (event: T) => void | Promise<void>

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
