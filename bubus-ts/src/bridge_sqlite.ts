import { BaseEvent } from './base_event.js'
import { EventBus } from './event_bus.js'
import { assertOptionalDependencyAvailable, importOptionalDependency, isNodeRuntime } from './optional_deps.js'
import type { EventClass, EventHandlerFunction, EventKey, UntypedEventHandlerFunction } from './types.js'

const randomSuffix = (): string => Math.random().toString(36).slice(2, 10)
const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/

const validateIdentifier = (value: string, label: string): string => {
  if (!IDENTIFIER_RE.test(value)) {
    throw new Error(`Invalid ${label}: ${JSON.stringify(value)}. Use only [A-Za-z0-9_] and start with a letter/_`)
  }
  return value
}

export class SQLiteEventBridge {
  readonly path: string
  readonly table: string
  readonly poll_interval: number
  readonly name: string

  private readonly inbound_bus: EventBus
  private running: boolean
  private last_seen_event_created_at: string
  private last_seen_event_id: string
  private listener_task: Promise<void> | null
  private db: any | null
  private table_columns: Set<string>

  constructor(path: string, table: string = 'bubus_events', poll_interval: number = 0.25, name?: string) {
    assertOptionalDependencyAvailable('SQLiteEventBridge', 'better-sqlite3')

    this.path = path
    this.table = validateIdentifier(table, 'table name')
    this.poll_interval = poll_interval
    this.name = name ?? `SQLiteEventBridge_${randomSuffix()}`
    this.inbound_bus = new EventBus(this.name)
    this.running = false
    this.last_seen_event_created_at = ''
    this.last_seen_event_id = ''
    this.listener_task = null
    this.db = null
    this.table_columns = new Set(['event_id', 'event_created_at', 'event_type'])

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
    if (!this.running) {
      await this.start()
    }
    if (!this.db) {
      throw new Error('SQLiteEventBridge database not initialized')
    }

    const payload = event.toJSON() as Record<string, unknown>
    const payload_keys = Object.keys(payload).sort()
    this.ensureColumns(payload_keys)

    const columns_sql = payload_keys.map((key) => `"${key}"`).join(', ')
    const placeholders_sql = payload_keys.map(() => '?').join(', ')
    const values = payload_keys.map((key) => (payload[key] === null || payload[key] === undefined ? null : JSON.stringify(payload[key])))

    const update_fields = payload_keys.filter((key) => key !== 'event_id')
    let upsert_sql = `INSERT INTO "${this.table}" (${columns_sql}) VALUES (${placeholders_sql})`
    if (update_fields.length > 0) {
      const updates_sql = update_fields.map((key) => `"${key}" = excluded."${key}"`).join(', ')
      upsert_sql += ` ON CONFLICT("event_id") DO UPDATE SET ${updates_sql}`
    } else {
      upsert_sql += ' ON CONFLICT("event_id") DO NOTHING'
    }

    this.db.prepare(upsert_sql).run(...values)
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
      .prepare(`CREATE TABLE IF NOT EXISTS "${this.table}" ("event_id" TEXT PRIMARY KEY, "event_created_at" TEXT, "event_type" TEXT)`)
      .run()

    this.refreshColumnCache()
    this.ensureColumns(['event_id', 'event_created_at', 'event_type'])
    this.ensureBaseIndexes()
    this.setCursorToLatestRow()

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
            .prepare(
              `SELECT * FROM "${this.table}" WHERE COALESCE("event_created_at", '') > ? OR (COALESCE("event_created_at", '') = ? AND COALESCE("event_id", '') > ?) ORDER BY COALESCE("event_created_at", '') ASC, COALESCE("event_id", '') ASC`
            )
            .all(this.last_seen_event_created_at, this.last_seen_event_created_at, this.last_seen_event_id) as Array<
            Record<string, unknown>
          >

          for (const row of rows) {
            this.last_seen_event_created_at = String(row.event_created_at ?? '')
            this.last_seen_event_id = String(row.event_id ?? '')

            const payload: Record<string, unknown> = {}
            for (const [key, raw_value] of Object.entries(row)) {
              if (raw_value === null || raw_value === undefined) continue

              if (typeof raw_value !== 'string') {
                payload[key] = raw_value
                continue
              }

              try {
                payload[key] = JSON.parse(raw_value)
              } catch {
                payload[key] = raw_value
              }
            }

            await this.dispatchInboundPayload(payload)
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
    const event = existing_event ?? parsed_event.reset()
    this.inbound_bus.dispatch(event)
  }

  private refreshColumnCache(): void {
    if (!this.db) return
    const rows = this.db.prepare(`PRAGMA table_info("${this.table}")`).all() as Array<{ name: string }>
    this.table_columns = new Set(rows.map((row) => String(row.name)))
  }

  private ensureColumns(keys: string[]): void {
    if (!this.db) return

    for (const key of keys) {
      validateIdentifier(key, 'event field name')
    }

    const missing_columns = keys.filter((key) => !this.table_columns.has(key))
    for (const key of missing_columns) {
      this.db.prepare(`ALTER TABLE "${this.table}" ADD COLUMN "${key}" TEXT`).run()
      this.table_columns.add(key)
    }
  }

  private ensureBaseIndexes(): void {
    if (!this.db) return

    const event_created_at_index = `${this.table}_event_created_at_idx`
    const event_type_index = `${this.table}_event_type_idx`

    this.db.prepare(`CREATE INDEX IF NOT EXISTS "${event_created_at_index}" ON "${this.table}" ("event_created_at")`).run()
    this.db.prepare(`CREATE INDEX IF NOT EXISTS "${event_type_index}" ON "${this.table}" ("event_type")`).run()
  }

  private setCursorToLatestRow(): void {
    if (!this.db) return

    const row = this.db
      .prepare(
        `SELECT COALESCE("event_created_at", '') AS event_created_at, COALESCE("event_id", '') AS event_id FROM "${this.table}" ORDER BY COALESCE("event_created_at", '') DESC, COALESCE("event_id", '') DESC LIMIT 1`
      )
      .get() as { event_created_at?: string; event_id?: string } | undefined

    this.last_seen_event_created_at = String(row?.event_created_at ?? '')
    this.last_seen_event_id = String(row?.event_id ?? '')
  }
}
