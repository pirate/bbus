import assert from 'node:assert/strict'
import { createServer as createNetServer } from 'node:net'
import { test } from 'node:test'

import { z } from 'zod'

import { BaseEvent, EventBus, HTTPEventBridge, SocketEventBridge } from '../src/index.js'

const IPCPingEvent = BaseEvent.extend('IPCPingEvent', {
  value: z.number(),
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
      const { port } = address
      server.close(() => resolve(port))
    })
  })

test('HTTPEventBridge forwards events over HTTP', async () => {
  const port = await getFreePort()
  const endpoint = `http://127.0.0.1:${port}/events`

  const source_bus = new EventBus('SourceBus')
  const sink_bus = new EventBus('SinkBus')
  const sender = new HTTPEventBridge({ send_to: endpoint })
  const receiver = new HTTPEventBridge({ listen_on: endpoint })

  const seen_values: number[] = []

  sink_bus.on(IPCPingEvent, (event) => {
    seen_values.push(event.value)
  })
  source_bus.on('*', sender.emit)
  receiver.on('*', sink_bus.emit)

  await receiver.start()

  const outbound_event = source_bus.emit(IPCPingEvent({ value: 5 }))
  await outbound_event.done()
  await sink_bus.waitUntilIdle()

  const received = await sink_bus.find(IPCPingEvent, { past: true, future: false })
  assert.ok(received)
  assert.equal(received.value, 5)
  assert.deepEqual(seen_values, [5])

  await sender.close()
  await receiver.close()
  source_bus.destroy()
  sink_bus.destroy()
})

test('SocketEventBridge forwards events over unix sockets', async () => {
  const socket_path = `/tmp/bubus-ipc-${Date.now()}-${Math.random().toString(16).slice(2)}.sock`
  const source_bus = new EventBus('SourceBusUnix')
  const sink_bus = new EventBus('SinkBusUnix')
  const sender = new SocketEventBridge(socket_path)
  const receiver = new SocketEventBridge(socket_path)

  const seen_values: number[] = []

  sink_bus.on(IPCPingEvent, (event) => {
    seen_values.push(event.value)
  })
  source_bus.on('*', sender.emit)
  receiver.on('*', sink_bus.emit)

  await receiver.start()

  const outbound_event = source_bus.emit(IPCPingEvent({ value: 11 }))
  await outbound_event.done()
  await sink_bus.waitUntilIdle()

  const received = await sink_bus.find(IPCPingEvent, { past: true, future: false })
  assert.ok(received)
  assert.equal(received.value, 11)
  assert.deepEqual(seen_values, [11])

  await sender.close()
  await receiver.close()
  source_bus.destroy()
  sink_bus.destroy()
})

test('SocketEventBridge rejects long socket paths', async () => {
  const long_path = `/tmp/${'a'.repeat(100)}.sock`
  assert.throws(() => {
    new SocketEventBridge(long_path)
  })
})
