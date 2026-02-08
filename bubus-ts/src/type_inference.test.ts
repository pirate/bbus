/* eslint-disable @typescript-eslint/no-unused-vars */
// Do not remove the unused type/const names below; they are used to test type inference at compile time.

import { z } from 'zod'

import { BaseEvent } from './base_event.js'
import { EventBus } from './event_bus.js'
import type { EventResult } from './event_result.js'
import type { EventResultType } from './types.js'

type IsEqual<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false
type Assert<T extends true> = T

const InferableResultEvent = BaseEvent.extend('InferableResultEvent', {
  target_id: z.string(),
  event_result_schema: z.object({ ok: z.boolean() }),
})

type InferableResult = EventResultType<InstanceType<typeof InferableResultEvent>>
type _assert_inferable_result = Assert<IsEqual<InferableResult, { ok: boolean }>>
type InferableEventResultEntry =
  InstanceType<typeof InferableResultEvent>['event_results'] extends Map<string, infer TResultEntry> ? TResultEntry : never
type _assert_inferable_event_result_entry = Assert<
  IsEqual<InferableEventResultEntry, EventResult<InstanceType<typeof InferableResultEvent>>>
>
type InferableEventResultValue = InferableEventResultEntry extends { result?: infer TResultValue } ? TResultValue : never
type _assert_inferable_event_result_value = Assert<IsEqual<InferableEventResultValue, { ok: boolean }>>

const NoSchemaEvent = BaseEvent.extend('NoSchemaEventForInference', {})
type NoSchemaResult = EventResultType<InstanceType<typeof NoSchemaEvent>>
type _assert_no_schema_result = Assert<IsEqual<NoSchemaResult, unknown>>

const bus = new EventBus('TypeInferenceBus')

bus.on(InferableResultEvent, (event) => {
  const target: string = event.target_id
  return { ok: true }
})

bus.on(InferableResultEvent, () => undefined)

// @ts-expect-error non-void return must match event_result_schema for inferable event keys
bus.on(InferableResultEvent, () => 'not-ok')

// String/wildcard keys remain best-effort and do not strongly enforce return shapes.
bus.on('InferableResultEvent', () => 'anything')
bus.on('*', () => 123)
