import { BaseEvent } from './base_event.js'
import { EventBus } from './event_bus.js'
import type { EventClass, EventHandlerFunction, EventKey, UntypedEventHandlerFunction } from './types.js'

const isNodeRuntime = (): boolean => {
  const maybe_process = (globalThis as { process?: { versions?: { node?: string } } }).process
  return typeof maybe_process?.versions?.node === 'string'
}

const importNodeModule = async (specifier: string): Promise<any> => {
  const dynamic_import = Function('module_name', 'return import(module_name)') as (module_name: string) => Promise<unknown>
  return dynamic_import(specifier) as Promise<any>
}

const randomSuffix = (): string => Math.random().toString(36).slice(2, 10)

export class JSONLEventBridge {
  readonly path: string
  readonly poll_interval: number
  readonly name: string

  private readonly inbound_bus: EventBus
  private running: boolean
  private line_offset: number
  private listener_task: Promise<void> | null

  constructor(path: string, poll_interval: number = 0.25, name?: string) {
    this.path = path
    this.poll_interval = poll_interval
    this.name = name ?? `JSONLEventBridge_${randomSuffix()}`
    this.inbound_bus = new EventBus(this.name)
    this.running = false
    this.line_offset = 0
    this.listener_task = null

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
    const fs = await this.loadFs()
    await fs.promises.mkdir(this.dirname(this.path), { recursive: true })
    const payload = JSON.stringify(event.toJSON()) + '\n'
    await fs.promises.appendFile(this.path, payload, 'utf8')
  }

  async emit<T extends BaseEvent>(event: T): Promise<void> {
    return this.dispatch(event)
  }

  async start(): Promise<void> {
    if (this.running) return
    const fs = await this.loadFs()
    await fs.promises.mkdir(this.dirname(this.path), { recursive: true })
    await fs.promises.appendFile(this.path, '', 'utf8')
    this.line_offset = await this.countLines()
    this.running = true
    this.listener_task = this.listenLoop()
  }

  async close(): Promise<void> {
    this.running = false
    await Promise.allSettled(this.listener_task ? [this.listener_task] : [])
    this.listener_task = null
    this.inbound_bus.destroy()
  }

  private ensureStarted(): void {
    if (this.running || this.listener_task) return
    void this.start().catch((error: unknown) => {
      console.error('[bubus] JSONLEventBridge failed to start', error)
    })
  }

  private async listenLoop(): Promise<void> {
    while (this.running) {
      try {
        await this.pollNewLines()
      } catch {
        // Keep polling on transient errors.
      }
      await new Promise((resolve) => setTimeout(resolve, Math.max(1, this.poll_interval * 1000)))
    }
  }

  private async pollNewLines(): Promise<void> {
    const lines = await this.readLines()
    if (this.line_offset >= lines.length) return

    const new_lines = lines.slice(this.line_offset)
    this.line_offset = lines.length

    for (const line of new_lines) {
      const trimmed = line.trim()
      if (!trimmed) continue
      try {
        const payload = JSON.parse(trimmed)
        await this.dispatchInboundPayload(payload)
      } catch {
        // Ignore malformed line.
      }
    }
  }

  private async dispatchInboundPayload(payload: unknown): Promise<void> {
    const parsed_event = BaseEvent.fromJSON(payload)
    const existing_event = EventBus._all_instances.findEventById(parsed_event.event_id)
    const event = existing_event ?? parsed_event
    this.inbound_bus.dispatch(event)
  }

  private async readLines(): Promise<string[]> {
    const fs = await this.loadFs()
    const content = await fs.promises.readFile(this.path, 'utf8')
    if (!content) return []
    const lines = content.split(/\r?\n/)
    if (lines.length > 0 && lines[lines.length - 1] === '') {
      lines.pop()
    }
    return lines
  }

  private async countLines(): Promise<number> {
    const lines = await this.readLines()
    return lines.length
  }

  private dirname(path: string): string {
    const idx = path.lastIndexOf('/')
    return idx >= 0 ? path.slice(0, idx) || '.' : '.'
  }

  private async loadFs(): Promise<any> {
    if (!isNodeRuntime()) {
      throw new Error('JSONLEventBridge is only supported in Node.js runtimes')
    }
    return importNodeModule('node:fs')
  }
}
