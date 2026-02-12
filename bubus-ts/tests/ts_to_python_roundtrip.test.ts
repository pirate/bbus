import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { test } from 'node:test'
import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'
import { fromJsonSchema } from '../src/types.js'

const tests_dir = dirname(fileURLToPath(import.meta.url))
const ts_root = resolve(tests_dir, '..')
const repo_root = resolve(ts_root, '..')

const jsonSafe = (value: unknown): Record<string, unknown> => JSON.parse(JSON.stringify(value)) as Record<string, unknown>

type ResultSemanticsCase = {
  event: BaseEvent
  valid_results: unknown[]
  invalid_results: unknown[]
}

const assertFieldEqual = (key: string, actual: unknown, expected: unknown, context: string): void => {
  if (key.endsWith('_at') && typeof actual === 'string' && typeof expected === 'string') {
    assert.equal(Date.parse(actual), Date.parse(expected), `${context}: ${key}`)
    return
  }
  assert.deepEqual(actual, expected, `${context}: ${key}`)
}

const stableValue = (value: unknown): string => {
  if (value === undefined) {
    return 'undefined'
  }
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

const assertSchemaSemanticsEqual = (
  original_schema_json: unknown,
  candidate_schema_json: unknown,
  valid_results: unknown[],
  invalid_results: unknown[],
  context: string
): void => {
  const original_schema = fromJsonSchema(original_schema_json)
  const candidate_schema = fromJsonSchema(candidate_schema_json)

  for (const result of valid_results) {
    const original_ok = original_schema.safeParse(result).success
    const candidate_ok = candidate_schema.safeParse(result).success
    assert.equal(original_ok, true, `${context}: original schema should accept ${stableValue(result)}`)
    assert.equal(candidate_ok, true, `${context}: candidate schema should accept ${stableValue(result)}`)
  }

  for (const result of invalid_results) {
    const original_ok = original_schema.safeParse(result).success
    const candidate_ok = candidate_schema.safeParse(result).success
    assert.equal(original_ok, false, `${context}: original schema should reject ${stableValue(result)}`)
    assert.equal(candidate_ok, false, `${context}: candidate schema should reject ${stableValue(result)}`)
  }

  for (const result of [...valid_results, ...invalid_results]) {
    const original_ok = original_schema.safeParse(result).success
    const candidate_ok = candidate_schema.safeParse(result).success
    assert.equal(
      candidate_ok,
      original_ok,
      `${context}: schema decision mismatch for ${stableValue(result)} (expected ${original_ok}, got ${candidate_ok})`
    )
  }
}

const buildRoundtripCases = (): ResultSemanticsCase[] => {
  const NumberResultEvent = BaseEvent.extend('TsPy_NumberResultEvent', {
    value: z.number(),
    label: z.string(),
    event_result_type: z.number(),
  })
  const StringResultEvent = BaseEvent.extend('TsPy_StringResultEvent', {
    id: z.string(),
    event_result_type: z.string(),
  })
  const BooleanResultEvent = BaseEvent.extend('TsPy_BooleanResultEvent', {
    id: z.string(),
    event_result_type: z.boolean(),
  })
  const NullResultEvent = BaseEvent.extend('TsPy_NullResultEvent', {
    id: z.string(),
    event_result_type: z.null(),
  })
  const StringCtorResultEvent = BaseEvent.extend('TsPy_StringCtorResultEvent', {
    id: z.string(),
    event_result_type: String,
  })
  const NumberCtorResultEvent = BaseEvent.extend('TsPy_NumberCtorResultEvent', {
    id: z.string(),
    event_result_type: Number,
  })
  const BooleanCtorResultEvent = BaseEvent.extend('TsPy_BooleanCtorResultEvent', {
    id: z.string(),
    event_result_type: Boolean,
  })
  const ArrayResultEvent = BaseEvent.extend('TsPy_ArrayResultEvent', {
    id: z.string(),
    event_result_type: z.array(z.string()),
  })
  const ArrayCtorResultEvent = BaseEvent.extend('TsPy_ArrayCtorResultEvent', {
    id: z.string(),
    event_result_type: Array,
  })
  const RecordResultEvent = BaseEvent.extend('TsPy_RecordResultEvent', {
    id: z.string(),
    event_result_type: z.record(z.string(), z.array(z.number())),
  })
  const ObjectCtorResultEvent = BaseEvent.extend('TsPy_ObjectCtorResultEvent', {
    id: z.string(),
    event_result_type: Object,
  })
  const ScreenshotResultEvent = BaseEvent.extend('TsPy_ScreenshotResultEvent', {
    target_id: z.string(),
    quality: z.string(),
    event_result_type: z.object({
      image_url: z.string(),
      width: z.number(),
      height: z.number(),
      tags: z.array(z.string()),
      is_animated: z.boolean(),
      confidence_scores: z.array(z.number()),
      metadata: z.record(z.string(), z.number()),
      regions: z.array(
        z.object({
          id: z.string(),
          label: z.string(),
          score: z.number(),
          visible: z.boolean(),
        })
      ),
    }),
  })

  const number_event = NumberResultEvent({
    value: 7,
    label: 'parent',
    event_path: ['TsBus#aaaa'],
    event_timeout: 12.5,
  })

  const screenshot_event = ScreenshotResultEvent({
    target_id: 'tab-1',
    quality: 'high',
    event_parent_id: number_event.event_id,
    event_path: ['TsBus#aaaa', 'PyBridge#bbbb'],
    event_timeout: 33.0,
  })

  const string_event = StringResultEvent({
    id: 's-1',
    event_parent_id: number_event.event_id,
    event_path: ['TsBus#aaaa'],
  })
  const bool_event = BooleanResultEvent({
    id: 'b-1',
    event_path: ['TsBus#aaaa'],
  })
  const null_event = NullResultEvent({
    id: 'n-1',
    event_path: ['TsBus#aaaa'],
  })
  const string_ctor_event = StringCtorResultEvent({
    id: 'cs-1',
    event_path: ['TsBus#aaaa'],
  })
  const number_ctor_event = NumberCtorResultEvent({
    id: 'cn-1',
    event_path: ['TsBus#aaaa'],
  })
  const boolean_ctor_event = BooleanCtorResultEvent({
    id: 'cb-1',
    event_path: ['TsBus#aaaa'],
  })
  const array_event = ArrayResultEvent({
    id: 'arr-1',
    event_path: ['TsBus#aaaa'],
  })
  const array_ctor_event = ArrayCtorResultEvent({
    id: 'carr-1',
    event_path: ['TsBus#aaaa'],
  })
  const record_event = RecordResultEvent({
    id: 'rec-1',
    event_path: ['TsBus#aaaa'],
  })
  const object_ctor_event = ObjectCtorResultEvent({
    id: 'obj-1',
    event_path: ['TsBus#aaaa'],
  })

  return [
    {
      event: number_event,
      valid_results: [0, -1, 1.5],
      invalid_results: ['1', true, { value: 1 }],
    },
    {
      event: string_event,
      valid_results: ['ok', ''],
      invalid_results: [123, false, ['x']],
    },
    {
      event: bool_event,
      valid_results: [true, false],
      invalid_results: ['false', 0, {}],
    },
    {
      event: null_event,
      valid_results: [null],
      invalid_results: [0, false, 'not-null', {}, []],
    },
    {
      event: string_ctor_event,
      valid_results: ['ok', ''],
      invalid_results: [123, false, ['x']],
    },
    {
      event: number_ctor_event,
      valid_results: [3.14, 42],
      invalid_results: ['42', false, {}],
    },
    {
      event: boolean_ctor_event,
      valid_results: [true, false],
      invalid_results: ['true', 1, []],
    },
    {
      event: array_event,
      valid_results: [['a', 'b'], []],
      invalid_results: [['a', 1], {}, 'not-array'],
    },
    {
      event: array_ctor_event,
      valid_results: [[1, 'two', false], []],
      invalid_results: ['not-array', { 0: 'x' }, true],
    },
    {
      event: record_event,
      valid_results: [{ a: [1, 2], b: [] }, {}],
      invalid_results: [{ a: ['1'] }, ['not-object'], 12],
    },
    {
      event: object_ctor_event,
      valid_results: [{ any: 'shape', count: 2 }, {}],
      invalid_results: ['not-object', [1, 2], true],
    },
    {
      event: screenshot_event,
      valid_results: [
        {
          image_url: 'https://img.local/1.png',
          width: 1920,
          height: 1080,
          tags: ['hero', 'dashboard'],
          is_animated: false,
          confidence_scores: [0.95, 0.89],
          metadata: { score: 0.99, variance: 0.01 },
          regions: [
            { id: 'r1', label: 'face', score: 0.9, visible: true },
            { id: 'r2', label: 'button', score: 0.7, visible: false },
          ],
        },
      ],
      invalid_results: [
        {
          image_url: 123,
          width: '1920',
          height: 1080,
          tags: ['hero'],
          is_animated: false,
          confidence_scores: [0.95],
          metadata: { score: 0.99 },
          regions: [{ id: 'r1', label: 'face', score: 0.9, visible: true }],
        },
        {
          image_url: 'https://img.local/1.png',
          width: 1920,
          height: 1080,
          tags: ['hero'],
          is_animated: false,
          confidence_scores: [0.95],
          metadata: { score: 0.99 },
          regions: [{ id: 123, label: 'face', score: 0.9, visible: true }],
        },
      ],
    },
  ]
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

test('ts_to_python_roundtrip preserves event fields and result type semantics', async (t) => {
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

  const roundtrip_cases = buildRoundtripCases()
  const events = roundtrip_cases.map((entry) => entry.event)
  const roundtrip_cases_by_type = new Map(roundtrip_cases.map((entry) => [entry.event.event_type, entry]))
  const ts_dumped = events.map((event) => jsonSafe(event.toJSON()))

  for (const event_dump of ts_dumped) {
    assert.ok('event_result_type' in event_dump)
    assert.equal(typeof event_dump.event_result_type, 'object')
  }

  const python_roundtripped = runPythonRoundtrip(python_bin, ts_dumped)
  assert.equal(python_roundtripped.length, ts_dumped.length)

  for (let i = 0; i < ts_dumped.length; i += 1) {
    const original = ts_dumped[i]
    const python_event = python_roundtripped[i]

    const event_type = String(original.event_type)
    const semantics_case = roundtrip_cases_by_type.get(event_type)
    assert.ok(semantics_case, `missing semantics case for event_type=${event_type}`)

    for (const [key, value] of Object.entries(original)) {
      assert.ok(key in python_event, `missing key after python roundtrip: ${key}`)
      if (key === 'event_result_type') {
        assert.equal(typeof python_event[key], 'object')
        assertSchemaSemanticsEqual(
          value,
          python_event[key],
          semantics_case.valid_results,
          semantics_case.invalid_results,
          `python roundtrip ${event_type}`
        )
        continue
      }
      assertFieldEqual(key, python_event[key], value, 'field changed after python roundtrip')
    }

    const restored = BaseEvent.fromJSON(python_event)
    const restored_dump = jsonSafe(restored.toJSON())

    for (const [key, value] of Object.entries(original)) {
      assert.ok(key in restored_dump, `missing key after ts reload: ${key}`)
      if (key === 'event_result_type') {
        assert.equal(typeof restored_dump[key], 'object')
        assertSchemaSemanticsEqual(
          value,
          restored_dump[key],
          semantics_case.valid_results,
          semantics_case.invalid_results,
          `ts reload ${event_type}`
        )
        continue
      }
      assertFieldEqual(key, restored_dump[key], value, 'field changed after ts reload')
    }
  }

  const screenshot_payload = python_roundtripped.find((event) => event.event_type === 'TsPy_ScreenshotResultEvent')
  assert.ok(screenshot_payload, 'missing TsPy_ScreenshotResultEvent in roundtrip payload')
  assert.equal(typeof screenshot_payload.event_result_type, 'object')

  const wrong_bus = new EventBus('TsPyTsWrongShape')
  wrong_bus.on('TsPy_ScreenshotResultEvent', () => ({
    image_url: 123,
    width: '1920',
    height: 1080,
    tags: ['hero', 'dashboard'],
    is_animated: 'false',
    confidence_scores: [0.95, 0.89],
    metadata: { score: 0.99 },
    regions: [{ id: 'r1', label: 'face', score: 0.9, visible: true }],
  }))
  const wrong_event = BaseEvent.fromJSON(screenshot_payload)
  assert.equal(typeof (wrong_event.event_result_type as { safeParse?: unknown } | undefined)?.safeParse, 'function')
  const wrong_dispatched = wrong_bus.dispatch(wrong_event)
  await wrong_dispatched.done()
  const wrong_result = Array.from(wrong_dispatched.event_results.values())[0]
  assert.equal(wrong_result.status, 'error')
  wrong_bus.destroy()

  const right_bus = new EventBus('TsPyTsRightShape')
  right_bus.on('TsPy_ScreenshotResultEvent', () => ({
    image_url: 'https://img.local/1.png',
    width: 1920,
    height: 1080,
    tags: ['hero', 'dashboard'],
    is_animated: false,
    confidence_scores: [0.95, 0.89],
    metadata: { score: 0.99, variance: 0.01 },
    regions: [
      { id: 'r1', label: 'face', score: 0.9, visible: true },
      { id: 'r2', label: 'button', score: 0.7, visible: false },
    ],
  }))
  const right_event = BaseEvent.fromJSON(screenshot_payload)
  assert.equal(typeof (right_event.event_result_type as { safeParse?: unknown } | undefined)?.safeParse, 'function')
  const right_dispatched = right_bus.dispatch(right_event)
  await right_dispatched.done()
  const right_result = Array.from(right_dispatched.event_results.values())[0]
  assert.equal(right_result.status, 'completed')
  assert.deepEqual(right_result.result, {
    image_url: 'https://img.local/1.png',
    width: 1920,
    height: 1080,
    tags: ['hero', 'dashboard'],
    is_animated: false,
    confidence_scores: [0.95, 0.89],
    metadata: { score: 0.99, variance: 0.01 },
    regions: [
      { id: 'r1', label: 'face', score: 0.9, visible: true },
      { id: 'r2', label: 'button', score: 0.7, visible: false },
    ],
  })
  right_bus.destroy()
})
