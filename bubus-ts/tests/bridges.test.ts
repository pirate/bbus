import assert from 'node:assert/strict'
import { spawn, spawnSync, type ChildProcess } from 'node:child_process'
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { createConnection, createServer as createNetServer } from 'node:net'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { test } from 'node:test'

import { z } from 'zod'

import {
  BaseEvent,
  HTTPEventBridge,
  JSONLEventBridge,
  NATSEventBridge,
  PostgresEventBridge,
  RedisEventBridge,
  SQLiteEventBridge,
  SocketEventBridge,
} from '../src/index.js'

const tests_dir = dirname(fileURLToPath(import.meta.url))
const TEST_RUN_ID = `${process.pid}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`

const makeTempDir = (prefix: string): string => mkdtempSync(join(tmpdir(), `${prefix}-${TEST_RUN_ID}-`))

const IPCPingEvent = BaseEvent.extend('IPCPingEvent', {
  value: z.number(),
  label: z.string(),
  meta: z.record(z.string(), z.unknown()),
})

const getFreePort = async (): Promise<number> =>
  await new Promise<number>((resolve, reject) => {
    const server = createNetServer()
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      const address = server.address()
      if (!address || typeof address === 'string') {
        server.close(() => reject(new Error('failed to allocate test port')))
        return
      }
      server.close(() => resolve(address.port))
    })
  })

const sleep = async (ms: number): Promise<void> => await new Promise((resolve) => setTimeout(resolve, ms))

const canonical = (payload: Record<string, unknown>): Record<string, unknown> => {
  const normalized: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(payload)) {
    if (key.endsWith('_at') && typeof value === 'string') {
      const ts = Date.parse(value)
      if (!Number.isNaN(ts)) {
        normalized[key] = ts
        continue
      }
    }
    normalized[key] = value
  }
  return normalized
}

const normalizeRoundtripPayload = (payload: Record<string, unknown>): Record<string, unknown> => {
  const normalized = canonical(payload)
  const dynamic_keys = [
    'event_path',
    'event_processed_at',
    'event_result_type',
    'event_result_schema',
    'event_results',
    'event_pending_bus_count',
    'event_status',
    'event_started_at',
    'event_started_ts',
    'event_completed_at',
    'event_completed_ts',
    'event_timeout',
    'event_handler_completion',
    'event_handler_concurrency',
    'event_handler_slow_timeout',
    'event_handler_timeout',
    'event_parent_id',
    'event_emitted_by_handler_id',
    'event_concurrency',
  ]
  for (const key of dynamic_keys) {
    delete normalized[key]
  }
  for (const [key, value] of Object.entries(normalized)) {
    if (value === undefined) {
      delete normalized[key]
    }
  }
  return normalized
}

const waitForPort = async (port: number, timeout_ms = 15000): Promise<void> => {
  const started = Date.now()
  while (Date.now() - started < timeout_ms) {
    const ok = await new Promise<boolean>((resolve) => {
      const socket = createConnection({ host: '127.0.0.1', port }, () => {
        socket.end()
        resolve(true)
      })
      socket.once('error', () => resolve(false))
    })
    if (ok) return
    await sleep(50)
  }
  throw new Error(`port did not open in time: ${port}`)
}

const waitForPath = async (path: string, worker: ChildProcess, timeout_ms = 15000): Promise<void> => {
  const started = Date.now()
  while (Date.now() - started < timeout_ms) {
    if (existsSync(path)) return
    if (worker.exitCode !== null) {
      const stdout = worker.stdout?.read()?.toString?.() ?? ''
      const stderr = worker.stderr?.read()?.toString?.() ?? ''
      throw new Error(`worker exited early (${worker.exitCode})\nstdout:\n${stdout}\nstderr:\n${stderr}`)
    }
    await sleep(50)
  }
  throw new Error(`path did not appear in time: ${path}`)
}

const stopProcess = async (proc: ChildProcess): Promise<void> => {
  if (proc.exitCode !== null) return
  proc.kill('SIGTERM')
  await sleep(250)
  if (proc.exitCode === null) {
    proc.kill('SIGKILL')
    await sleep(250)
  }
}

const runChecked = (cmd: string, args: string[], cwd?: string): void => {
  const result = spawnSync(cmd, args, { cwd, encoding: 'utf8' })
  assert.equal(result.status, 0, `${cmd} failed\nstdout:\n${result.stdout ?? ''}\nstderr:\n${result.stderr ?? ''}`)
}

const makeSenderBridge = (kind: string, config: Record<string, string>): any => {
  if (kind === 'http') return new HTTPEventBridge({ send_to: config.endpoint })
  if (kind === 'socket') return new SocketEventBridge(config.path)
  if (kind === 'jsonl') return new JSONLEventBridge(config.path, 0.05)
  if (kind === 'sqlite') return new SQLiteEventBridge(config.path, config.table, 0.05)
  if (kind === 'redis') return new RedisEventBridge(config.url)
  if (kind === 'nats') return new NATSEventBridge(config.server, config.subject)
  if (kind === 'postgres') return new PostgresEventBridge(config.url)
  throw new Error(`unsupported bridge kind: ${kind}`)
}

const assertRoundtrip = async (kind: string, config: Record<string, string>): Promise<void> => {
  const temp_dir = makeTempDir(`bubus-bridge-${kind}`)
  const ready_path = join(temp_dir, 'worker.ready')
  const output_path = join(temp_dir, 'received.json')
  const config_path = join(temp_dir, 'worker_config.json')
  const worker_payload = {
    ...config,
    kind,
    ready_path,
    output_path,
  }
  writeFileSync(config_path, JSON.stringify(worker_payload), 'utf8')

  const sender = makeSenderBridge(kind, config)

  const worker = spawn(process.execPath, ['--import', 'tsx', join(tests_dir, 'bridge_listener_worker.ts'), config_path], {
    cwd: tests_dir,
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  try {
    await waitForPath(ready_path, worker)
    if (kind === 'postgres') {
      await sender.start()
    }
    const outbound = IPCPingEvent({ value: 17, label: `${kind}_ok`, meta: { kind, n: 1 } })
    await sender.emit(outbound)
    await waitForPath(output_path, worker)
    const received_payload = JSON.parse(readFileSync(output_path, 'utf8')) as Record<string, unknown>
    assert.deepEqual(normalizeRoundtripPayload(received_payload), normalizeRoundtripPayload(outbound.toJSON() as Record<string, unknown>))
  } finally {
    await sender.close()
    await stopProcess(worker)
    rmSync(temp_dir, { recursive: true, force: true })
  }
}

test('HTTPEventBridge roundtrip between processes', async () => {
  const endpoint = `http://127.0.0.1:${await getFreePort()}/events`
  await assertRoundtrip('http', { endpoint })
})

test('SocketEventBridge roundtrip between processes', async () => {
  const socket_path = `/tmp/bb-${TEST_RUN_ID}-${Math.random().toString(16).slice(2)}.sock`
  await assertRoundtrip('socket', { path: socket_path })
})

test('SocketEventBridge rejects long socket paths', async () => {
  const long_path = `/tmp/${'a'.repeat(100)}.sock`
  assert.throws(() => {
    new SocketEventBridge(long_path)
  })
})

test('JSONLEventBridge roundtrip between processes', async () => {
  const temp_dir = makeTempDir('bubus-jsonl')
  try {
    await assertRoundtrip('jsonl', { path: join(temp_dir, 'events.jsonl') })
  } finally {
    rmSync(temp_dir, { recursive: true, force: true })
  }
})

test('SQLiteEventBridge roundtrip between processes', async (t) => {
  try {
    const sqlite_module = (await import('better-sqlite3')) as { default?: new (path: string) => { close: () => void } }
    const SQLiteDatabase = sqlite_module.default
    if (!SQLiteDatabase) {
      t.skip('better-sqlite3 is unavailable in this runtime')
      return
    }
    const db = new SQLiteDatabase(':memory:')
    db.close()
  } catch {
    t.skip('better-sqlite3 is unavailable in this runtime')
    return
  }

  const temp_dir = makeTempDir('bubus-sqlite')
  try {
    const sqlite_path = join(temp_dir, 'events.sqlite3')
    runChecked('sqlite3', [sqlite_path, 'SELECT 1;'])
    await assertRoundtrip('sqlite', { path: sqlite_path, table: 'bubus_events' })
  } finally {
    rmSync(temp_dir, { recursive: true, force: true })
  }
})

test('RedisEventBridge roundtrip between processes', async () => {
  const temp_dir = makeTempDir('bubus-redis')
  const port = await getFreePort()
  const redis = spawn(
    'redis-server',
    ['--save', '', '--appendonly', 'no', '--bind', '127.0.0.1', '--port', String(port), '--dir', temp_dir],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  )
  try {
    await waitForPort(port)
    await assertRoundtrip('redis', { url: `redis://127.0.0.1:${port}/1/bubus_events` })
  } finally {
    await stopProcess(redis)
    rmSync(temp_dir, { recursive: true, force: true })
  }
})

test('NATSEventBridge roundtrip between processes', async () => {
  const port = await getFreePort()
  const nats = spawn('nats-server', ['-a', '127.0.0.1', '-p', String(port)], { stdio: ['ignore', 'pipe', 'pipe'] })
  try {
    await waitForPort(port)
    await assertRoundtrip('nats', { server: `nats://127.0.0.1:${port}`, subject: 'bubus_events' })
  } finally {
    await stopProcess(nats)
  }
})

test('PostgresEventBridge roundtrip between processes', async () => {
  const temp_dir = makeTempDir('bubus-postgres')
  const data_dir = join(temp_dir, 'pgdata')
  runChecked('initdb', ['-D', data_dir, '-A', 'trust', '-U', 'postgres'])
  const port = await getFreePort()
  const postgres = spawn('postgres', ['-D', data_dir, '-h', '127.0.0.1', '-p', String(port), '-k', '/tmp'], {
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  try {
    await waitForPort(port)
    await assertRoundtrip('postgres', { url: `postgresql://postgres@127.0.0.1:${port}/postgres/bubus_events` })
  } finally {
    await stopProcess(postgres)
    rmSync(temp_dir, { recursive: true, force: true })
  }
})
