import assert from 'node:assert/strict'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

const typed_result_schema = z.object({
  value: z.string(),
  count: z.number(),
})

const TypedResultEvent = BaseEvent.extend('TypedResultEvent', {
  event_result_schema: typed_result_schema,
  event_result_type: 'TypedResult',
})

const StringResultEvent = BaseEvent.extend('StringResultEvent', {
  event_result_schema: z.string(),
  event_result_type: 'string',
})

const NumberResultEvent = BaseEvent.extend('NumberResultEvent', {
  event_result_schema: z.number(),
  event_result_type: 'number',
})

const ComplexResultEvent = BaseEvent.extend('ComplexResultEvent', {
  event_result_schema: z.object({
    items: z.array(z.string()),
    metadata: z.record(z.string(), z.number()),
  }),
})

const NoSchemaEvent = BaseEvent.extend('NoSchemaEvent', {})

const AutoObjectResultEvent = BaseEvent.extend('AutoObjectResultEvent', {
  event_result_schema: z.object({ ok: z.boolean() }),
})

const AutoRecordResultEvent = BaseEvent.extend('AutoRecordResultEvent', {
  event_result_schema: z.record(z.string(), z.number()),
})

const AutoMapResultEvent = BaseEvent.extend('AutoMapResultEvent', {
  event_result_schema: z.map(z.string(), z.number()),
})

const AutoStringResultEvent = BaseEvent.extend('AutoStringResultEvent', {
  event_result_schema: z.string(),
})

const AutoNumberResultEvent = BaseEvent.extend('AutoNumberResultEvent', {
  event_result_schema: z.number(),
})

const AutoBooleanResultEvent = BaseEvent.extend('AutoBooleanResultEvent', {
  event_result_schema: z.boolean(),
})

const ExplicitTypeWinsEvent = BaseEvent.extend('ExplicitTypeWinsEvent', {
  event_result_schema: z.string(),
  event_result_type: 'CustomResultType',
})

test('typed result schema validates and parses handler result', async () => {
  const bus = new EventBus('TypedResultBus')

  bus.on(TypedResultEvent, () => ({ value: 'hello', count: 42 }))

  const event = bus.dispatch(TypedResultEvent({}))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { value: 'hello', count: 42 })
  assert.equal(event.event_result_type, 'TypedResult')
})

test('built-in result schemas validate handler results', async () => {
  const bus = new EventBus('BuiltinResultBus')

  bus.on(StringResultEvent, () => '42')
  bus.on(NumberResultEvent, () => 123)

  const string_event = bus.dispatch(StringResultEvent({}))
  const number_event = bus.dispatch(NumberResultEvent({}))
  await string_event.done()
  await number_event.done()

  const string_result = Array.from(string_event.event_results.values())[0]
  const number_result = Array.from(number_event.event_results.values())[0]

  assert.equal(string_result.status, 'completed')
  assert.equal(string_result.result, '42')
  assert.equal(number_result.status, 'completed')
  assert.equal(number_result.result, 123)
})

test('invalid handler result marks error when schema is defined', async () => {
  const bus = new EventBus('ResultValidationErrorBus')

  bus.on(NumberResultEvent, () => 'not_a_number')

  const event = bus.dispatch(NumberResultEvent({}))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'error')
  assert.ok(result.error instanceof Error)
  assert.ok(event.event_errors.length > 0)
})

test('no schema leaves raw handler result untouched', async () => {
  const bus = new EventBus('NoSchemaResultBus')

  bus.on(NoSchemaEvent, () => ({ raw: true }))

  const event = bus.dispatch(NoSchemaEvent({}))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { raw: true })
})

test('complex result schema validates nested data', async () => {
  const bus = new EventBus('ComplexResultBus')

  bus.on(ComplexResultEvent, () => ({
    items: ['a', 'b'],
    metadata: { a: 1, b: 2 },
  }))

  const event = bus.dispatch(ComplexResultEvent({}))
  await event.done()

  const result = Array.from(event.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { items: ['a', 'b'], metadata: { a: 1, b: 2 } })
})

test('event_result_type auto-infers from common event_result_schema types', () => {
  assert.equal(AutoObjectResultEvent.event_result_type, 'object')
  assert.equal(AutoRecordResultEvent.event_result_type, 'object')
  assert.equal(AutoMapResultEvent.event_result_type, 'object')
  assert.equal(AutoStringResultEvent.event_result_type, 'string')
  assert.equal(AutoNumberResultEvent.event_result_type, 'number')
  assert.equal(AutoBooleanResultEvent.event_result_type, 'boolean')

  assert.equal(AutoObjectResultEvent({}).event_result_type, 'object')
  assert.equal(AutoRecordResultEvent({}).event_result_type, 'object')
  assert.equal(AutoMapResultEvent({}).event_result_type, 'object')
  assert.equal(AutoStringResultEvent({}).event_result_type, 'string')
  assert.equal(AutoNumberResultEvent({}).event_result_type, 'number')
  assert.equal(AutoBooleanResultEvent({}).event_result_type, 'boolean')
})

test('explicit event_result_type is not overridden by inference', () => {
  assert.equal(ExplicitTypeWinsEvent.event_result_type, 'CustomResultType')
  assert.equal(ExplicitTypeWinsEvent({}).event_result_type, 'CustomResultType')
})

test('fromJSON converts event_result_schema into zod schema', async () => {
  const bus = new EventBus('FromJsonResultBus')

  const original = TypedResultEvent({
    event_result_schema: typed_result_schema,
    event_result_type: 'TypedResult',
  })
  const json = original.toJSON()

  const restored = TypedResultEvent.fromJSON?.(json) ?? TypedResultEvent(json as never)

  assert.ok(restored.event_result_schema)
  assert.equal(typeof (restored.event_result_schema as { safeParse?: unknown }).safeParse, 'function')

  bus.on(TypedResultEvent, () => ({ value: 'from-json', count: 7 }))

  const dispatched = bus.dispatch(restored)
  await dispatched.done()

  const result = Array.from(dispatched.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, { value: 'from-json', count: 7 })
})

test('roundtrip preserves complex result schema types', async () => {
  const bus = new EventBus('RoundtripSchemaBus')

  const complex_schema = z.object({
    title: z.string(),
    count: z.number(),
    flags: z.array(z.boolean()),
    active: z.boolean(),
    meta: z.object({
      tags: z.array(z.string()),
      rating: z.number(),
    }),
  })

  const ComplexRoundtripEvent = BaseEvent.extend('ComplexRoundtripEvent', {
    event_result_schema: complex_schema,
    event_result_type: 'ComplexRoundtrip',
  })

  const original = ComplexRoundtripEvent({
    event_result_schema: complex_schema,
    event_result_type: 'ComplexRoundtrip',
  })

  const roundtripped = ComplexRoundtripEvent.fromJSON?.(original.toJSON()) ?? ComplexRoundtripEvent(original.toJSON() as never)

  const zod_any = z as unknown as {
    toJSONSchema?: (schema: unknown) => unknown
  }
  if (typeof zod_any.toJSONSchema === 'function') {
    const original_schema_json = zod_any.toJSONSchema(complex_schema)
    const roundtrip_schema_json = zod_any.toJSONSchema(roundtripped.event_result_schema)
    assert.deepEqual(roundtrip_schema_json, original_schema_json)
  }

  bus.on(ComplexRoundtripEvent, () => ({
    title: 'ok',
    count: 3,
    flags: [true, false, true],
    active: false,
    meta: { tags: ['a', 'b'], rating: 4 },
  }))

  const dispatched = bus.dispatch(roundtripped)
  await dispatched.done()

  const result = Array.from(dispatched.event_results.values())[0]
  assert.equal(result.status, 'completed')
  assert.deepEqual(result.result, {
    title: 'ok',
    count: 3,
    flags: [true, false, true],
    active: false,
    meta: { tags: ['a', 'b'], rating: 4 },
  })
})
