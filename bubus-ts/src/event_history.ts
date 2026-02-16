import { BaseEvent } from './base_event.js'
import type { EventPattern, FindWindow } from './types.js'
import { normalizeEventPattern } from './types.js'

export type EventHistoryFindOptions = {
  past?: FindWindow
  future?: FindWindow
  child_of?: BaseEvent | null
  event_is_child_of?: (event: BaseEvent, ancestor: BaseEvent) => boolean
  wait_for_future_match?: (
    event_pattern: string | '*',
    matches: (event: BaseEvent) => boolean,
    future: FindWindow
  ) => Promise<BaseEvent | null>
} & Record<string, unknown>

export type EventHistoryTrimOptions<TEvent extends BaseEvent = BaseEvent> = {
  is_event_complete?: (event: TEvent) => boolean
  on_remove?: (event: TEvent) => void
  owner_label?: string
  max_history_size?: number | null
  max_history_drop?: boolean
}

export class EventHistory<TEvent extends BaseEvent = BaseEvent> implements Iterable<[string, TEvent]> {
  max_history_size: number | null
  max_history_drop: boolean

  private _events: Map<string, TEvent>
  private _warned_about_dropping_uncompleted_events: boolean

  constructor(options: { max_history_size?: number | null; max_history_drop?: boolean } = {}) {
    this.max_history_size = options.max_history_size === undefined ? 100 : options.max_history_size
    this.max_history_drop = options.max_history_drop ?? false
    this._events = new Map()
    this._warned_about_dropping_uncompleted_events = false
  }

  get size(): number {
    return this._events.size
  }

  [Symbol.iterator](): Iterator<[string, TEvent]> {
    return this._events[Symbol.iterator]()
  }

  entries(): IterableIterator<[string, TEvent]> {
    return this._events.entries()
  }

  keys(): IterableIterator<string> {
    return this._events.keys()
  }

  values(): IterableIterator<TEvent> {
    return this._events.values()
  }

  clear(): void {
    this._events.clear()
  }

  get(event_id: string): TEvent | undefined {
    return this._events.get(event_id)
  }

  set(event_id: string, event: TEvent): this {
    this._events.set(event_id, event)
    return this
  }

  has(event_id: string): boolean {
    return this._events.has(event_id)
  }

  delete(event_id: string): boolean {
    return this._events.delete(event_id)
  }

  addEvent(event: TEvent): void {
    this._events.set(event.event_id, event)
  }

  getEvent(event_id: string): TEvent | undefined {
    return this._events.get(event_id)
  }

  removeEvent(event_id: string): boolean {
    return this._events.delete(event_id)
  }

  hasEvent(event_id: string): boolean {
    return this._events.has(event_id)
  }

  static normalizeEventPattern(event_pattern: EventPattern | '*'): string | '*' {
    return normalizeEventPattern(event_pattern)
  }

  static isEventCompleteFast(event: BaseEvent): boolean {
    return event.event_status === 'completed'
  }

  find(event_pattern: '*', where?: (event: TEvent) => boolean, options?: EventHistoryFindOptions): Promise<TEvent | null>
  find<TMatch extends TEvent>(
    event_pattern: EventPattern<TMatch>,
    where?: (event: TMatch) => boolean,
    options?: EventHistoryFindOptions
  ): Promise<TMatch | null>
  async find(
    event_pattern: EventPattern<TEvent> | '*',
    where: (event: TEvent) => boolean = () => true,
    options: EventHistoryFindOptions = {}
  ): Promise<TEvent | null> {
    const past = options.past ?? true
    const future = options.future ?? false
    const child_of = options.child_of ?? null
    const eventIsChildOf = options.event_is_child_of ?? ((event: BaseEvent, ancestor: BaseEvent) => this.eventIsChildOf(event, ancestor))
    const waitForFutureMatch = options.wait_for_future_match
    if (past === false && future === false) {
      return null
    }

    const event_key = EventHistory.normalizeEventPattern(event_pattern)
    const cutoff_ms = past === true ? null : performance.timeOrigin + performance.now() - Math.max(0, Number(past)) * 1000

    const event_field_filters = Object.entries(options).filter(
      ([key, value]) =>
        key !== 'past' &&
        key !== 'future' &&
        key !== 'child_of' &&
        key !== 'event_is_child_of' &&
        key !== 'wait_for_future_match' &&
        value !== undefined
    )

    const matches = (event: BaseEvent): boolean => {
      if (event_key !== '*' && event.event_type !== event_key) {
        return false
      }
      if (child_of && !eventIsChildOf(event, child_of)) {
        return false
      }
      let field_mismatch = false
      for (const [field_name, expected] of event_field_filters) {
        if ((event as unknown as Record<string, unknown>)[field_name] !== expected) {
          field_mismatch = true
          break
        }
      }
      if (field_mismatch) {
        return false
      }
      if (!where(event as TEvent)) {
        return false
      }
      return true
    }

    if (past !== false) {
      const history_values = Array.from(this._events.values())
      for (let i = history_values.length - 1; i >= 0; i -= 1) {
        const event = history_values[i]
        if (cutoff_ms !== null && Date.parse(event.event_created_at) < cutoff_ms) {
          continue
        }
        if (matches(event)) {
          return event
        }
      }
    }

    if (future === false || !waitForFutureMatch) {
      return null
    }

    return (await waitForFutureMatch(event_key, matches, future)) as TEvent | null
  }

  cleanupExcessEvents(options: EventHistoryTrimOptions<TEvent> = {}): number {
    const max_history_size = options.max_history_size ?? this.max_history_size
    if (max_history_size === null) {
      return 0
    }
    if (max_history_size === 0) {
      return this.trimEventHistory(options)
    }
    if (this.size <= max_history_size) {
      return 0
    }

    const on_remove = options.on_remove
    const remove_count = this.size - max_history_size
    const event_ids_to_remove = Array.from(this._events.keys()).slice(0, remove_count)
    let removed_count = 0

    for (const event_id of event_ids_to_remove) {
      const event = this._events.get(event_id)
      if (!event) {
        continue
      }
      this._events.delete(event_id)
      if (on_remove) {
        on_remove(event)
      }
      removed_count += 1
    }

    return removed_count
  }

  trimEventHistory(options: EventHistoryTrimOptions<TEvent> = {}): number {
    const max_history_size = options.max_history_size ?? this.max_history_size
    const max_history_drop = options.max_history_drop ?? this.max_history_drop
    if (max_history_size === null) {
      return 0
    }

    const is_event_complete = options.is_event_complete ?? ((event: TEvent) => event.event_status === 'completed')
    const on_remove = options.on_remove

    if (max_history_size === 0) {
      let removed_count = 0
      for (const [event_id, event] of Array.from(this._events.entries())) {
        if (!is_event_complete(event)) {
          continue
        }
        this._events.delete(event_id)
        if (on_remove) {
          on_remove(event)
        }
        removed_count += 1
      }
      return removed_count
    }

    if (!max_history_drop) {
      return 0
    }

    if (this.size <= max_history_size) {
      return 0
    }

    let remaining_overage = this.size - max_history_size
    let removed_count = 0

    for (const [event_id, event] of Array.from(this._events.entries())) {
      if (remaining_overage <= 0) {
        break
      }
      if (!is_event_complete(event)) {
        continue
      }
      this._events.delete(event_id)
      if (on_remove) {
        on_remove(event)
      }
      removed_count += 1
      remaining_overage -= 1
    }

    let dropped_uncompleted = 0
    if (remaining_overage > 0) {
      for (const [event_id, event] of Array.from(this._events.entries())) {
        if (remaining_overage <= 0) {
          break
        }
        if (!is_event_complete(event)) {
          dropped_uncompleted += 1
        }
        this._events.delete(event_id)
        if (on_remove) {
          on_remove(event)
        }
        removed_count += 1
        remaining_overage -= 1
      }

      if (dropped_uncompleted > 0 && !this._warned_about_dropping_uncompleted_events) {
        this._warned_about_dropping_uncompleted_events = true
        const owner_label = options.owner_label ?? 'EventBus'
        console.error(
          `[bubus] ⚠️ Bus ${owner_label} has exceeded max_history_size=${max_history_size} and is dropping oldest history entries (even uncompleted events). Increase max_history_size or set max_history_drop=false to reject.`
        )
      }
    }

    return removed_count
  }

  private eventIsChildOf(event: BaseEvent, ancestor: BaseEvent): boolean {
    let current_parent_id = event.event_parent_id
    const visited = new Set<string>()

    while (current_parent_id && !visited.has(current_parent_id)) {
      if (current_parent_id === ancestor.event_id) {
        return true
      }
      visited.add(current_parent_id)
      const parent = this._events.get(current_parent_id)
      if (!parent) {
        return false
      }
      current_parent_id = parent.event_parent_id
    }

    return false
  }
}
