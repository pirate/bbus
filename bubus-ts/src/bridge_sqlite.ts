import { BaseEvent } from './base_event.js'
import { EventBus } from './event_bus.js'
import { assertOptionalDependencyAvailable, importOptionalDependency, isNodeRuntime } from './optional_deps.js'
import type { EventClass, EventHandlerFunction, EventKey, UntypedEventHandlerFunction } from './types.js'

const randomSuffix = (): string => Math.random().toString(36).slice(2, 10)

export class SQLiteEventBridge {
  readonly path: string
  readonly table: string
  readonly poll_interval: number
  readonly name: string

  private readonly inbound_bus: EventBus
  private running: boolean
  private last_row_id: number
  private listener_task: Promise<void> | null
  private db: any | null

  constructor(path: string, table: string = 'bubus_events', poll_interval: number = 0.25, name?: string) {
    assertOptionalDependencyAvailable('SQLiteEventBridge', 'better-sqlite3')

    this.path = path
    this.table = table
    this.poll_interval = poll_interval
    this.name = name ?? `SQLiteEventBridge_${randomSuffix()}`
    this.inbound_bus = new EventBus(this.name)
    this.running = false
    this.last_row_id = 0
    this.listener_task = null
    this.db = null

    this.dispatch = this.dispatch.bind(this)
    this.emit = this.emit.bind(this)
    this.on = this.on.bind(this)
  }

  on<T extends BaseEvent>(event_key: EventClass<T>, handler: EventHandlerFunction<T>): void
  on<T extends BaseEvent>(event_key: string | '*', handler: UntypedEventHandlerFunction<T>): void
  on(event_key: EventKey | '*', handler: EventHandlerFunction | UntypedEventHandlerFunction): void {
    this.ensureStarted()
    if (typeof event_key === 'string') {
      this.inbound_bus.on(event_key, handler as UntypedEventHandlerFunction<BaseEvent>)
      return
    }
    this.inbound_bus.on(event_key as EventClass<BaseEvent>, handler as EventHandlerFunction<BaseEvent>)
  }

  async dispatch<T extends BaseEvent>(event: T): Promise<void> {
    this.ensureStarted()
    if (!this.db) {
      await this.start()
    }
    const payload = JSON.stringify(event.toJSON())
    this.db.prepare(`INSERT INTO ${this.table} (payload) VALUES (?)`).run(payload)
  }

  async emit<T extends BaseEvent>(event: T): Promise<void> {
    return this.dispatch(event)
  }

  async start(): Promise<void> {
    if (this.running) return
    if (!isNodeRuntime()) {
      throw new Error('SQLiteEventBridge is only supported in Node.js runtimes')
    }

    const mod = await importOptionalDependency('SQLiteEventBridge', 'better-sqlite3')
    const Database = mod.default ?? mod
    this.db = new Database(this.path)
    this.db.pragma('journal_mode = WAL')
    this.db
      .prepare(
        `CREATE TABLE IF NOT EXISTS ${this.table} (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)`
      )
      .run()

    const row = this.db.prepare(`SELECT COALESCE(MAX(id), 0) AS max_id FROM ${this.table}`).get()
    this.last_row_id = Number(row?.max_id ?? 0)

    this.running = true
    this.listener_task = this.listenLoop()
  }

  async close(): Promise<void> {
    this.running = false
    await Promise.allSettled(this.listener_task ? [this.listener_task] : [])
    this.listener_task = null

    if (this.db) {
      this.db.close()
      this.db = null
    }

    this.inbound_bus.destroy()
  }

  private ensureStarted(): void {
    if (this.running || this.listener_task) return
    void this.start().catch((error: unknown) => {
      console.error('[bubus] SQLiteEventBridge failed to start', error)
    })
  }

  private async listenLoop(): Promise<void> {
    while (this.running) {
      try {
        if (this.db) {
          const rows = this.db
            .prepare(`SELECT id, payload FROM ${this.table} WHERE id > ? ORDER BY id ASC`)
            .all(this.last_row_id) as Array<{ id: number; payload: string }>
          for (const row of rows) {
            this.last_row_id = Math.max(this.last_row_id, Number(row.id))
            try {
              await this.dispatchInboundPayload(JSON.parse(row.payload))
            } catch {
              // Ignore malformed payloads.
            }
          }
        }
      } catch {
        // Keep polling on transient errors.
      }
      await new Promise((resolve) => setTimeout(resolve, Math.max(1, this.poll_interval * 1000)))
    }
  }

  private async dispatchInboundPayload(payload: unknown): Promise<void> {
    const parsed_event = BaseEvent.fromJSON(payload)
    const existing_event = EventBus._all_instances.findEventById(parsed_event.event_id)
    const event = existing_event ?? parsed_event
    this.inbound_bus.dispatch(event)
  }
}
