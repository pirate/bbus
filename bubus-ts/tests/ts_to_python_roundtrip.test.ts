import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { test } from 'node:test'
import { z } from 'zod'

import { BaseEvent } from '../src/index.js'

const tests_dir = dirname(fileURLToPath(import.meta.url))
const ts_root = resolve(tests_dir, '..')
const repo_root = resolve(ts_root, '..')

const jsonSafe = (value: unknown): Record<string, unknown> => JSON.parse(JSON.stringify(value)) as Record<string, unknown>

const assertFieldEqual = (key: string, actual: unknown, expected: unknown, context: string): void => {
  if (key.endsWith('_at') && typeof actual === 'string' && typeof expected === 'string') {
    assert.equal(Date.parse(actual), Date.parse(expected), `${context}: ${key}`)
    return
  }
  assert.deepEqual(actual, expected, `${context}: ${key}`)
}

const runCommand = (cmd: string, args: string[], cwd = repo_root): ReturnType<typeof spawnSync> =>
  spawnSync(cmd, args, {
    cwd,
    env: process.env,
    encoding: 'utf8',
  })

const resolvePython = (): string | null => {
  const candidates = [
    process.env.BUBUS_PYTHON_BIN,
    resolve(repo_root, '.venv', 'bin', 'python'),
    resolve(repo_root, '.venv', 'Scripts', 'python.exe'),
    'python3',
    'python',
  ].filter((candidate): candidate is string => typeof candidate === 'string' && candidate.length > 0)

  for (const candidate of candidates) {
    if ((candidate.includes('/') || candidate.includes('\\')) && !existsSync(candidate)) {
      continue
    }
    const probe = runCommand(candidate, ['--version'])
    if (probe.status === 0) {
      return candidate
    }
  }
  return null
}

const assertPythonCanImportBubus = (python_bin: string): void => {
  const probe = runCommand(python_bin, ['-c', 'import pydantic; import bubus'])
  if (probe.status !== 0) {
    throw new Error(`python environment cannot import bubus/pydantic:\nstdout:\n${probe.stdout ?? ''}\nstderr:\n${probe.stderr ?? ''}`)
  }
}

const runPythonRoundtrip = (python_bin: string, payload: Array<Record<string, unknown>>): Array<Record<string, unknown>> => {
  const temp_dir = mkdtempSync(join(tmpdir(), 'bubus-ts-to-python-'))
  const input_path = join(temp_dir, 'ts_events.json')
  const output_path = join(temp_dir, 'python_events.json')

  const python_script = `
import json
import os
from typing import Any
from bubus import BaseEvent

input_path = os.environ.get('BUBUS_TS_PY_INPUT_PATH')
output_path = os.environ.get('BUBUS_TS_PY_OUTPUT_PATH')
if not input_path or not output_path:
    raise RuntimeError('missing BUBUS_TS_PY_INPUT_PATH or BUBUS_TS_PY_OUTPUT_PATH')

with open(input_path, 'r', encoding='utf-8') as f:
    raw = json.load(f)

if not isinstance(raw, list):
    raise TypeError('expected array payload')

roundtripped: list[dict[str, Any]] = []
for item in raw:
    event = BaseEvent[Any].model_validate(item)
    roundtripped.append(event.model_dump(mode='json'))

with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(roundtripped, f, indent=2)
`

  try {
    writeFileSync(input_path, JSON.stringify(payload, null, 2), 'utf8')
    const proc = spawnSync(python_bin, ['-c', python_script], {
      cwd: repo_root,
      env: {
        ...process.env,
        BUBUS_TS_PY_INPUT_PATH: input_path,
        BUBUS_TS_PY_OUTPUT_PATH: output_path,
      },
      encoding: 'utf8',
    })

    assert.equal(proc.status, 0, `python roundtrip failed:\nstdout:\n${proc.stdout ?? ''}\nstderr:\n${proc.stderr ?? ''}`)

    return JSON.parse(readFileSync(output_path, 'utf8')) as Array<Record<string, unknown>>
  } finally {
    rmSync(temp_dir, { recursive: true, force: true })
  }
}

test('ts_to_python_roundtrip preserves event fields and result schemas', (t) => {
  const python_bin = resolvePython()
  if (!python_bin) {
    t.skip('python is required for ts<->python roundtrip tests')
    return
  }

  try {
    assertPythonCanImportBubus(python_bin)
  } catch (error) {
    t.skip(String(error))
    return
  }

  const IntResultEvent = BaseEvent.extend('IntResultEvent', {
    value: z.number(),
    label: z.string(),
    event_result_schema: z.number(),
  })
  const StringListResultEvent = BaseEvent.extend('StringListResultEvent', {
    names: z.array(z.string()),
    attempt: z.number(),
    event_result_schema: z.array(z.string()),
  })
  const ScreenshotEvent = BaseEvent.extend('ScreenshotEvent', {
    target_id: z.string(),
    quality: z.string(),
    event_result_schema: z.object({
      image_url: z.string(),
      width: z.number(),
      height: z.number(),
      tags: z.array(z.string()),
    }),
  })
  const MetricsEvent = BaseEvent.extend('MetricsEvent', {
    bucket: z.string(),
    counters: z.record(z.string(), z.number()),
    event_result_schema: z.record(z.string(), z.array(z.number())),
  })

  const parent = IntResultEvent({
    value: 7,
    label: 'parent',
    event_path: ['TsBus#aaaa'],
    event_timeout: 12.5,
  })
  const child = ScreenshotEvent({
    target_id: 'tab-1',
    quality: 'high',
    event_parent_id: parent.event_id,
    event_path: ['TsBus#aaaa', 'PyBridge#bbbb'],
    event_timeout: 33.0,
  })
  const list_event = StringListResultEvent({
    names: ['alpha', 'beta', 'gamma'],
    attempt: 2,
    event_parent_id: parent.event_id,
    event_path: ['TsBus#aaaa'],
  })
  const metrics_event = MetricsEvent({
    bucket: 'images',
    counters: { ok: 12, failed: 1 },
    event_path: ['TsBus#aaaa'],
  })
  const adhoc_event = new BaseEvent({
    event_type: 'AdhocEvent',
    event_timeout: 4.0,
    event_parent_id: parent.event_id,
    event_path: ['TsBus#aaaa'],
    event_result_type: 'object',
    event_result_schema: z.record(z.string(), z.number()),
    custom_payload: { tab_id: 'tab-1', bytes: 12345 },
    nested_payload: { frames: [1, 2, 3], format: 'png' },
  })

  const events = [parent, child, list_event, metrics_event, adhoc_event]
  const ts_dumped = events.map((event) => jsonSafe(event.toJSON()))

  for (const event_dump of ts_dumped) {
    assert.ok('event_result_schema' in event_dump)
    assert.equal(typeof event_dump.event_result_schema, 'object')
  }

  const python_roundtripped = runPythonRoundtrip(python_bin, ts_dumped)
  assert.equal(python_roundtripped.length, ts_dumped.length)

  for (let i = 0; i < ts_dumped.length; i += 1) {
    const original = ts_dumped[i]
    const python_event = python_roundtripped[i]

    for (const [key, value] of Object.entries(original)) {
      assert.ok(key in python_event, `missing key after python roundtrip: ${key}`)
      assertFieldEqual(key, python_event[key], value, 'field changed after python roundtrip')
    }

    const restored = BaseEvent.fromJSON(python_event)
    const restored_dump = jsonSafe(restored.toJSON())

    for (const [key, value] of Object.entries(original)) {
      assert.ok(key in restored_dump, `missing key after ts reload: ${key}`)
      assertFieldEqual(key, restored_dump[key], value, 'field changed after ts reload')
    }
  }
})
