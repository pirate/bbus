import assert from 'node:assert/strict'
import { test } from 'node:test'
import { z } from 'zod'

import { BaseEvent, EventBus, events_suck } from '../src/index.js'

test('events_suck.wrap builds imperative methods for emitting events', async () => {
  const bus = new EventBus('EventsSuckBus')
  const CreateEvent = BaseEvent.extend('EventsSuckCreateEvent', {
    name: z.string(),
    age: z.number(),
    event_result_type: z.string(),
  })
  const UpdateEvent = BaseEvent.extend('EventsSuckUpdateEvent', {
    id: z.string(),
    age: z.number().nullable().optional(),
    event_result_type: z.boolean(),
  })

  bus.on(CreateEvent, async (event) => {
    assert.equal((event as unknown as { nickname?: string }).nickname, 'bobby')
    return `user-${event.age}`
  })

  bus.on(UpdateEvent, async (event) => {
    assert.equal((event as unknown as { source?: string }).source, 'sync')
    return event.age === 46
  })

  const SDKClient = events_suck.wrap('SDKClient', {
    create: CreateEvent,
    update: UpdateEvent,
  })
  const client = new SDKClient(bus)

  const user_id = await client.create({ name: 'bob', age: 45 }, { nickname: 'bobby' })
  const updated = await client.update({ id: user_id ?? 'fallback-id', age: 46 }, { source: 'sync' })

  assert.equal(user_id, 'user-45')
  assert.equal(updated, true)
})

test('events_suck.make_events works with inline handlers', async () => {
  class LegacyService {
    calls: Array<[string, Record<string, unknown>]> = []

    create(id: string | null, name: string, age: number): string {
      this.calls.push(['create', { id, name, age }])
      return `${name}-${age}`
    }

    update(id: string, name?: string | null, age?: number | null, extra?: Record<string, unknown>): boolean {
      this.calls.push(['update', { id, name, age, ...(extra ?? {}) }])
      return true
    }
  }

  const ping_user = (user_id: string): string => `pong:${user_id}`

  const events = events_suck.make_events({
    FooBarAPICreateEvent: LegacyService.prototype.create,
    FooBarAPIUpdateEvent: LegacyService.prototype.update,
    FooBarAPIPingEvent: ping_user,
  })

  const service = new LegacyService()
  const bus = new EventBus('LegacyBus')
  bus.on(events.FooBarAPICreateEvent, ({ id, name, age }) => service.create(id, name, age))
  bus.on(events.FooBarAPIUpdateEvent, ({ id, name, age, ...event_fields }) => service.update(id, name, age, event_fields))
  bus.on(events.FooBarAPIPingEvent, ({ user_id }) => ping_user(user_id))

  const created = await bus.emit(events.FooBarAPICreateEvent({ id: null, name: 'bob', age: 45 })).first()
  const updated = await bus.emit(events.FooBarAPIUpdateEvent({ id: created, age: 46, source: 'sync' })).first()
  const pong = await bus.emit(events.FooBarAPIPingEvent({ user_id: 'u1' })).first()

  assert.equal(created, 'bob-45')
  assert.equal(updated, true)
  assert.equal(pong, 'pong:u1')
  assert.deepEqual(service.calls[0], ['create', { id: null, name: 'bob', age: 45 }])
  assert.equal(service.calls[1]?.[0], 'update')
  assert.equal(service.calls[1]?.[1].id, 'bob-45')
  assert.equal(service.calls[1]?.[1].age, 46)
  assert.equal(service.calls[1]?.[1].source, 'sync')
})
