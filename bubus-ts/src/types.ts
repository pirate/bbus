import type { BaseEvent } from "./base_event.js";

export type EventStatus = "pending" | "started" | "completed";

export type EventClass<T extends BaseEvent = BaseEvent> = new (...args: any[]) => T;

export type EventKey<T extends BaseEvent = BaseEvent> = string | symbol | EventClass<T>;

export type EventHandler<T extends BaseEvent = BaseEvent> = (event: T) => void | Promise<void>;

export type FindWindow = boolean | number;

export type FindOptions<T extends BaseEvent = BaseEvent> = {
  past?: FindWindow;
  future?: FindWindow;
  child_of?: BaseEvent | null;
};
