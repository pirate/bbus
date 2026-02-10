const defaultNow = () => performance.now()
const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const assert = (condition, message) => {
  if (!condition) {
    throw new Error(message)
  }
}

const mb = (bytes) => (bytes / 1024 / 1024).toFixed(1)
const kb = (bytes) => bytes / 1024
const clampNonNegative = (value) => (value < 0 ? 0 : value)
const formatMsPerEvent = (value) => `${value.toFixed(3)}ms/event`
const formatKbPerEvent = (value) => `${value.toFixed(3)}kb/event`
const formatMs = (value) => `${value.toFixed(3)}ms`

const HISTORY_LIMIT_STREAM = 2048
const HISTORY_LIMIT_ON_OFF = 1024
const HISTORY_LIMIT_EPHEMERAL_BUS = 128
const HISTORY_LIMIT_FIXED_HANDLERS = 128
const HISTORY_LIMIT_WORST_CASE = 1024
const TRIM_TARGET = 1

const heapDeltaNoiseFloorMb = (runtimeName) => (runtimeName === 'bun' ? 64.0 : 1.0)

const measureMemory = (hooks) => {
  if (typeof hooks.getMemoryUsage !== 'function') {
    return null
  }
  return hooks.getMemoryUsage()
}

const maybeForceGc = (hooks) => {
  if (typeof hooks.forceGc === 'function') {
    hooks.forceGc()
  }
}

const measureStableHeapUsed = async (hooks, mode = 'max', rounds = 12) => {
  const heaps = []
  for (let i = 0; i < rounds; i += 1) {
    await hooks.sleep(12)
    maybeForceGc(hooks)
    const mem = measureMemory(hooks)
    if (mem) heaps.push(mem.heapUsed)
  }
  if (heaps.length === 0) return null
  return mode === 'min' ? Math.min(...heaps) : Math.max(...heaps)
}

const measureHeapDeltaAfterGc = async (hooks, baselineHeapUsed) => {
  if (baselineHeapUsed === null || baselineHeapUsed === undefined) return null
  await hooks.sleep(120)
  const endHeapUsed = await measureStableHeapUsed(hooks, 'min', 24)
  if (endHeapUsed === null) return null
  return (endHeapUsed - baselineHeapUsed) / 1024 / 1024
}

const trimBusHistoryToOneEvent = async (hooks, bus, TrimEvent) => {
  bus.max_history_size = TRIM_TARGET
  let trimEvent = bus.dispatch(TrimEvent({}))
  await trimEvent.done()
  trimEvent = null
  await bus.waitUntilIdle()
  assert(bus.event_history.size <= TRIM_TARGET, `trim-to-1 failed for ${bus.toString()}: ${bus.event_history.size}/${TRIM_TARGET}`)
}

const waitForRegistrySize = async (hooks, EventBus, expectedSize, attempts = 150) => {
  for (let i = 0; i < attempts; i += 1) {
    maybeForceGc(hooks)
    await hooks.sleep(40)
    if (EventBus._all_instances.size <= expectedSize) {
      return true
    }
  }
  return EventBus._all_instances.size <= expectedSize
}

const runWarmup = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const { PerfWarmupEvent: WarmEvent, PerfWarmupTrimEvent: WarmTrimEvent } = getEventClasses(BaseEvent)

  const bus = new EventBus('PerfWarmupBus', { max_history_size: 512 })
  bus.on(WarmEvent, () => {})

  for (let i = 0; i < 2048; i += 256) {
    const pending = []
    for (let j = 0; j < 256; j += 1) {
      pending.push(bus.dispatch(WarmEvent({})))
    }
    await Promise.all(pending.map((event) => event.done()))
    await bus.waitUntilIdle()
  }

  await trimBusHistoryToOneEvent(hooks, bus, WarmTrimEvent)
  bus.destroy()
  await measureStableHeapUsed(hooks, 'min', 6)
}

const createMemoryTracker = (hooks) => {
  const baselineRaw = measureMemory(hooks)
  if (!baselineRaw) {
    return {
      baseline: null,
      peak: null,
      sample: () => null,
      peakRssKbPerEvent: () => null,
    }
  }

  const baseline = { rss: baselineRaw.rss, heapUsed: baselineRaw.heapUsed }
  const peak = { rss: baselineRaw.rss, heapUsed: baselineRaw.heapUsed }

  const sample = () => {
    const current = measureMemory(hooks)
    if (!current) return null
    if (current.rss > peak.rss) peak.rss = current.rss
    if (current.heapUsed > peak.heapUsed) peak.heapUsed = current.heapUsed
    return current
  }

  const peakRssKbPerEvent = (events) => {
    if (!events || !baseline) return null
    const deltaBytes = clampNonNegative(peak.rss - baseline.rss)
    return kb(deltaBytes) / events
  }

  return { baseline, peak, sample, peakRssKbPerEvent }
}

const record = (hooks, name, metrics) => {
  if (typeof hooks.log === 'function') {
    const perEventOnly = name === 'worst-case forwarding + timeouts'
    const parts = []
    if (!perEventOnly && typeof metrics.totalEvents === 'number') parts.push(`events=${metrics.totalEvents}`)
    if (!perEventOnly && typeof metrics.totalMs === 'number') parts.push(`total=${formatMs(metrics.totalMs)}`)
    if (typeof metrics.msPerEvent === 'number') parts.push(`latency=${formatMsPerEvent(metrics.msPerEvent)}`)
    if (typeof metrics.ramKbPerEvent === 'number') parts.push(`ram=${formatKbPerEvent(metrics.ramKbPerEvent)}`)
    if (typeof metrics.throughput === 'number') parts.push(`throughput=${metrics.throughput}/s`)
    if (typeof metrics.equivalent === 'boolean') parts.push(`equivalent=${metrics.equivalent ? 'yes' : 'no'}`)
    if (typeof metrics.timeoutCount === 'number') parts.push(`timeouts=${metrics.timeoutCount}`)
    if (typeof metrics.cancelCount === 'number') parts.push(`cancels=${metrics.cancelCount}`)
    if (typeof metrics.heapDeltaGcMb === 'number') parts.push(`heap_delta_gc=${metrics.heapDeltaGcMb.toFixed(3)}mb`)
    hooks.log(`[${hooks.runtimeName}] ${name}: ${parts.join(' ')}`)
  }
}

const withDefaults = (input) => {
  const hooks = {
    runtimeName: input.runtimeName ?? 'runtime',
    now: input.now ?? defaultNow,
    sleep: input.sleep ?? defaultSleep,
    log: input.log ?? (() => {}),
    forceGc: input.forceGc,
    getMemoryUsage: input.getMemoryUsage,
    limits: {
      singleRunMs: input.limits?.singleRunMs ?? 30_000,
      worstCaseMs: input.limits?.worstCaseMs ?? 60_000,
      worstCaseMemoryDeltaMb: input.limits?.worstCaseMemoryDeltaMb ?? null,
      enforceNonPositiveHeapDeltaAfterGc: input.limits?.enforceNonPositiveHeapDeltaAfterGc ?? true,
    },
    api: input.api,
  }
  return hooks
}

const eventClassCache = new WeakMap()

const getEventClasses = (BaseEvent) => {
  const cached = eventClassCache.get(BaseEvent)
  if (cached) return cached

  const classes = {
    PerfSimpleEvent: BaseEvent.extend('PerfSimpleEvent', {}),
    PerfTrimEvent: BaseEvent.extend('PerfTrimEvent', {}),
    PerfTrimEventEphemeral: BaseEvent.extend('PerfTrimEventEphemeral', {}),
    PerfRequestEvent: BaseEvent.extend('PerfRequestEvent', {}),
    PerfTrimEventOnOff: BaseEvent.extend('PerfTrimEventOnOff', {}),
    PerfFixedHandlersEvent: BaseEvent.extend('PerfFixedHandlersEvent', {}),
    PerfTrimEventFixedHandlers: BaseEvent.extend('PerfTrimEventFixedHandlers', {}),
    WCParent: BaseEvent.extend('WCParent', {}),
    WCChild: BaseEvent.extend('WCChild', {}),
    WCGrandchild: BaseEvent.extend('WCGrandchild', {}),
    WCTrimEvent: BaseEvent.extend('WCTrimEvent', {}),
    CleanupEqEvent: BaseEvent.extend('CleanupEqEvent', {}),
    CleanupEqTrimEvent: BaseEvent.extend('CleanupEqTrimEvent', {}),
    PerfWarmupEvent: BaseEvent.extend('PerfWarmupEvent', {}),
    PerfWarmupTrimEvent: BaseEvent.extend('PerfWarmupTrimEvent', {}),
  }
  eventClassCache.set(BaseEvent, classes)
  return classes
}

export const runPerf50kEvents = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const totalEvents = 50_000
  const batchSize = 512
  const { PerfSimpleEvent: SimpleEvent, PerfTrimEvent: TrimEvent } = getEventClasses(BaseEvent)
  const bus = new EventBus('PerfBus', { max_history_size: HISTORY_LIMIT_STREAM })

  let processedCount = 0
  const sampledEarlyEvents = []
  bus.on(SimpleEvent, () => {
    processedCount += 1
  })

  maybeForceGc(hooks)
  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()

  let dispatched = 0
  while (dispatched < totalEvents) {
    const pending = []
    const thisBatch = Math.min(batchSize, totalEvents - dispatched)
    for (let i = 0; i < thisBatch; i += 1) {
      const dispatchedEvent = bus.dispatch(SimpleEvent({}))
      pending.push(dispatchedEvent)
      if (sampledEarlyEvents.length < 64) {
        const original = dispatchedEvent._event_original ?? dispatchedEvent
        sampledEarlyEvents.push(original)
      }
      dispatched += 1
    }

    await Promise.all(pending.map((event) => event.done()))
    await bus.waitUntilIdle()
    if (dispatched % 2048 === 0) memory.sample()
  }

  const tDispatch = hooks.now()
  memory.sample()

  await trimBusHistoryToOneEvent(hooks, bus, TrimEvent)
  const tDone = hooks.now()
  memory.sample()
  const memDone = measureMemory(hooks)
  maybeForceGc(hooks)
  const memGc = measureMemory(hooks)

  const dispatchMs = tDispatch - t0
  const awaitMs = tDone - tDispatch
  const totalMs = tDone - t0
  const msPerEvent = totalMs / totalEvents
  const ramKbPerEvent = memory.peakRssKbPerEvent(totalEvents)

  assert(processedCount === totalEvents, `50k events processed ${processedCount}/${totalEvents}`)
  assert(totalMs < hooks.limits.singleRunMs, `50k events took ${Math.round(totalMs)}ms (limit ${hooks.limits.singleRunMs}ms)`)
  assert(
    bus.event_history.size <= bus.max_history_size,
    `50k events history exceeded limit: ${bus.event_history.size}/${bus.max_history_size}`
  )

  assert(sampledEarlyEvents.length > 0, 'expected sampled early events to be captured')

  let sampledEvictedCount = 0
  for (const event of sampledEarlyEvents) {
    const isStillInHistory = bus.event_history.has(event.event_id)
    assert(!isStillInHistory, `expected sampled early event to be evicted from history: ${event.event_id}`)
    sampledEvictedCount += 1
    assert(event.event_results.size === 0, `trimmed event still has event_results: ${event.event_id} (${event.event_results.size})`)
    assert(event.bus === undefined, `trimmed event still has bus reference: ${event.event_id}`)
  }
  assert(
    sampledEvictedCount === sampledEarlyEvents.length,
    `expected all sampled events to be evicted: ${sampledEvictedCount}/${sampledEarlyEvents.length}`
  )

  const result = {
    scenario: '50k events',
    totalEvents,
    totalMs,
    dispatchMs,
    awaitMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent,
    ramKbPerEventLabel: ramKbPerEvent === null ? 'n/a' : formatKbPerEvent(ramKbPerEvent),
    throughput: Math.round(totalEvents / (totalMs / 1000)),
    processedCount,
    sampledEvictedCount,
  }

  if (memory.baseline && memDone && memGc) {
    result.heapBeforeMb = Number(mb(memory.baseline.heapUsed))
    result.heapDoneMb = Number(mb(memDone.heapUsed))
    result.heapGcMb = Number(mb(memGc.heapUsed))
    result.rssBeforeMb = Number(mb(memory.baseline.rss))
    result.rssDoneMb = Number(mb(memDone.rss))
    result.rssPeakMb = Number(mb(memory.peak.rss))
  }

  bus.destroy()
  record(hooks, result.scenario, result)
  return result
}

export const runPerfEphemeralBuses = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const totalBuses = 500
  const eventsPerBus = 100
  const totalEvents = totalBuses * eventsPerBus
  const { PerfSimpleEvent: SimpleEvent, PerfTrimEventEphemeral: TrimEvent } = getEventClasses(BaseEvent)

  let processedCount = 0
  maybeForceGc(hooks)
  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()

  for (let b = 0; b < totalBuses; b += 1) {
    const bus = new EventBus(`ReqBus-${b}`, { max_history_size: HISTORY_LIMIT_EPHEMERAL_BUS })
    bus.on(SimpleEvent, () => {
      processedCount += 1
    })

    const pending = []
    for (let i = 0; i < eventsPerBus; i += 1) {
      pending.push(bus.dispatch(SimpleEvent({})))
    }

    await Promise.all(pending.map((event) => event.done()))
    await bus.waitUntilIdle()
    await trimBusHistoryToOneEvent(hooks, bus, TrimEvent)
    bus.destroy()
    if (b % 10 === 0) memory.sample()
  }

  const totalMs = hooks.now() - t0
  memory.sample()
  const msPerEvent = totalMs / totalEvents
  const ramKbPerEvent = memory.peakRssKbPerEvent(totalEvents)

  assert(processedCount === totalEvents, `500x100 buses processed ${processedCount}/${totalEvents}`)
  assert(totalMs < hooks.limits.singleRunMs, `500x100 buses took ${Math.round(totalMs)}ms (limit ${hooks.limits.singleRunMs}ms)`)
  assert(EventBus._all_instances.size === 0, `500x100 buses leaked instances: ${EventBus._all_instances.size}`)

  const result = {
    scenario: '500 buses x 100 events',
    totalEvents,
    totalMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent,
    ramKbPerEventLabel: ramKbPerEvent === null ? 'n/a' : formatKbPerEvent(ramKbPerEvent),
    throughput: Math.round(totalEvents / (totalMs / 1000)),
    processedCount,
  }
  record(hooks, result.scenario, result)
  return result
}

export const runPerfSingleEventManyFixedHandlers = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const totalEvents = 1
  const totalHandlers = 50_000
  const { PerfFixedHandlersEvent: FixedHandlersEvent, PerfTrimEventFixedHandlers: TrimEvent } = getEventClasses(BaseEvent)
  const bus = new EventBus('FixedHandlersBus', {
    max_history_size: HISTORY_LIMIT_FIXED_HANDLERS,
    event_handler_concurrency: 'parallel',
  })

  let processedCount = 0
  for (let i = 0; i < totalHandlers; i += 1) {
    bus.on(
      FixedHandlersEvent,
      () => {
        processedCount += 1
      },
      { id: `fixed-handler-${i}` }
    )
    if (i % 1000 === 0) {
      // Keep memory sampling overhead bounded during massive registration.
      measureMemory(hooks)
    }
  }

  maybeForceGc(hooks)
  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()

  const event = bus.dispatch(FixedHandlersEvent({}))
  await event.done()
  await bus.waitUntilIdle()

  const totalMs = hooks.now() - t0
  memory.sample()
  const msPerEvent = totalMs / totalEvents
  const ramKbPerEvent = memory.peakRssKbPerEvent(totalEvents)

  assert(processedCount === totalHandlers, `fixed-handlers processed ${processedCount}/${totalHandlers}`)
  assert(totalMs < hooks.limits.singleRunMs, `fixed-handlers took ${Math.round(totalMs)}ms (limit ${hooks.limits.singleRunMs}ms)`)
  assert(bus.handlers.size === totalHandlers, `fixed-handlers expected ${totalHandlers} registered handlers, got ${bus.handlers.size}`)

  await trimBusHistoryToOneEvent(hooks, bus, TrimEvent)
  bus.destroy()

  const result = {
    scenario: '1 event x 50k parallel handlers',
    totalEvents,
    totalMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent,
    ramKbPerEventLabel: ramKbPerEvent === null ? 'n/a' : formatKbPerEvent(ramKbPerEvent),
    throughput: Math.round(totalEvents / (totalMs / 1000)),
    processedCount,
    totalHandlers,
  }
  record(hooks, result.scenario, result)
  return result
}

export const runPerfOnOffChurn = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const { PerfRequestEvent: RequestEvent, PerfTrimEventOnOff: TrimEvent } = getEventClasses(BaseEvent)

  const totalEvents = 50_000
  const bus = new EventBus('OneOffHandlerBus', { max_history_size: HISTORY_LIMIT_ON_OFF })

  let processedCount = 0

  maybeForceGc(hooks)
  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()
  for (let i = 0; i < totalEvents; i += 1) {
    const oneOffHandler = () => {
      processedCount += 1
    }
    bus.on(RequestEvent, oneOffHandler)

    const event = RequestEvent({})
    const ev = bus.dispatch(event)
    await ev.done()

    bus.off(RequestEvent, oneOffHandler)
    if (i % 1000 === 0) memory.sample()
  }

  await bus.waitUntilIdle()
  const totalMs = hooks.now() - t0
  memory.sample()
  const msPerEvent = totalMs / totalEvents
  const ramKbPerEvent = memory.peakRssKbPerEvent(totalEvents)

  assert(processedCount === totalEvents, `50k one-off handlers processed ${processedCount}/${totalEvents}`)
  assert(totalMs < hooks.limits.singleRunMs, `50k on/off took ${Math.round(totalMs)}ms (limit ${hooks.limits.singleRunMs}ms)`)
  assert(bus.handlers.size === 0, `50k on/off leaked handlers: ${bus.handlers.size}`)

  await trimBusHistoryToOneEvent(hooks, bus, TrimEvent)
  bus.destroy()

  const result = {
    scenario: '50k one-off handlers over 50k events',
    totalEvents,
    totalMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent,
    ramKbPerEventLabel: ramKbPerEvent === null ? 'n/a' : formatKbPerEvent(ramKbPerEvent),
    throughput: Math.round(totalEvents / (totalMs / 1000)),
    processedCount,
  }
  record(hooks, result.scenario, result)
  return result
}

export const runPerfWorstCase = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus, EventHandlerTimeoutError, EventHandlerCancelledError } = hooks.api

  const { WCParent: ParentEvent, WCChild: ChildEvent, WCGrandchild: GrandchildEvent, WCTrimEvent: TrimEvent } = getEventClasses(BaseEvent)

  const totalIterations = 500
  const historyLimit = HISTORY_LIMIT_WORST_CASE
  const busA = new EventBus('WCA', { max_history_size: historyLimit })
  const busB = new EventBus('WCB', { max_history_size: historyLimit })
  const busC = new EventBus('WCC', { max_history_size: historyLimit })

  let parentHandledA = 0
  let parentHandledB = 0
  let childHandled = 0
  let grandchildHandled = 0
  let timeoutCount = 0
  let cancelCount = 0

  busB.on(ParentEvent, () => {
    parentHandledB += 1
  })

  busC.on(ChildEvent, async (event) => {
    childHandled += 1
    const gc = event.bus.emit(GrandchildEvent({}))
    busC.dispatch(gc)
    await gc.done()
  })

  busC.on(GrandchildEvent, async () => {
    grandchildHandled += 1
    // Deterministically slow path so child timeout iterations reliably trigger.
    await hooks.sleep(20)
  })

  maybeForceGc(hooks)
  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()

  for (let i = 0; i < totalIterations; i += 1) {
    const shouldTimeout = i % 5 === 0

    const ephemeralHandler = async (event) => {
      parentHandledA += 1
      const child = event.bus.emit(
        ChildEvent({
          event_timeout: shouldTimeout ? 0.005 : null,
        })
      )
      busC.dispatch(child)
      try {
        await child.done()
      } catch {
        // Timeouts are expected for timeout iterations.
      }
    }

    busA.on(ParentEvent, ephemeralHandler)
    const parent = ParentEvent({})
    const evA = busA.dispatch(parent)
    busB.dispatch(parent)
    await evA.done()
    busA.off(ParentEvent, ephemeralHandler)

    if (i % 10 === 0) {
      busA.find(ParentEvent, { future: 0.001 })
    }
    if (i % 5 === 0) memory.sample()
  }

  await busA.waitUntilIdle()
  await busB.waitUntilIdle()
  await busC.waitUntilIdle()
  memory.sample()

  for (const event of busC.event_history.values()) {
    for (const result of event.event_results.values()) {
      if (result.error instanceof EventHandlerTimeoutError) timeoutCount += 1
      if (result.error instanceof EventHandlerCancelledError) cancelCount += 1
    }
  }

  const totalMs = hooks.now() - t0
  const estimatedEvents = totalIterations * 3
  const msPerEvent = totalMs / estimatedEvents
  const ramKbPerEvent = memory.peakRssKbPerEvent(estimatedEvents)

  assert(parentHandledA === totalIterations, `worst-case parentA ${parentHandledA}/${totalIterations}`)
  assert(parentHandledB === totalIterations, `worst-case parentB ${parentHandledB}/${totalIterations}`)
  assert(busA.handlers.size === 0, `worst-case leaked busA handlers: ${busA.handlers.size}`)
  assert(busA.event_history.size <= historyLimit, `worst-case busA history ${busA.event_history.size}/${historyLimit}`)
  assert(busB.event_history.size <= historyLimit, `worst-case busB history ${busB.event_history.size}/${historyLimit}`)
  assert(busC.event_history.size <= historyLimit, `worst-case busC history ${busC.event_history.size}/${historyLimit}`)
  assert(totalMs < hooks.limits.worstCaseMs, `worst-case took ${Math.round(totalMs)}ms (limit ${hooks.limits.worstCaseMs}ms)`)

  await trimBusHistoryToOneEvent(hooks, busA, TrimEvent)
  await trimBusHistoryToOneEvent(hooks, busB, TrimEvent)
  await trimBusHistoryToOneEvent(hooks, busC, TrimEvent)
  busA.destroy()
  busB.destroy()
  busC.destroy()

  const result = {
    scenario: 'worst-case forwarding + timeouts',
    totalEvents: estimatedEvents,
    totalMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent,
    ramKbPerEventLabel: ramKbPerEvent === null ? 'n/a' : formatKbPerEvent(ramKbPerEvent),
    parentHandledA,
    parentHandledB,
    childHandled,
    grandchildHandled,
    timeoutCount,
    cancelCount,
  }
  record(hooks, result.scenario, result)
  assert(EventBus._all_instances.size === 0, `worst-case leaked instances: ${EventBus._all_instances.size}`)

  return result
}

export const runCleanupEquivalence = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const { CleanupEqEvent: CleanupEvent, CleanupEqTrimEvent: TrimEvent } = getEventClasses(BaseEvent)

  const busesPerMode = 80
  const eventsPerBus = 64
  const totalEvents = busesPerMode * eventsPerBus * 2
  const baselineRegistrySize = EventBus._all_instances.size

  maybeForceGc(hooks)
  const t0 = hooks.now()

  const runBurst = async (destroyMode) => {
    for (let i = 0; i < busesPerMode; i += 1) {
      let bus = new EventBus(`CleanupEq-${destroyMode ? 'destroy' : 'scope'}-${i}`, { max_history_size: HISTORY_LIMIT_EPHEMERAL_BUS })
      bus.on(CleanupEvent, () => {})

      const pending = []
      for (let e = 0; e < eventsPerBus; e += 1) {
        // Store completion promises (not event proxies) to avoid retaining bus-bound proxies across GC checks.
        pending.push(bus.dispatch(CleanupEvent({})).done().then(() => undefined))
      }
      await Promise.all(pending)
      pending.length = 0
      await bus.waitUntilIdle()
      await trimBusHistoryToOneEvent(hooks, bus, TrimEvent)

      if (destroyMode) {
        bus.destroy()
      }
      bus = null
    }
  }

  await runBurst(true)
  assert(
    EventBus._all_instances.size === baselineRegistrySize,
    `cleanup equivalence destroy branch leaked instances: ${EventBus._all_instances.size}/${baselineRegistrySize}`
  )

  await (async () => {
    await runBurst(false)
  })()

  const scopeCollectionAttempts = hooks.runtimeName === 'deno' ? 500 : 150
  const scopeCollected = await waitForRegistrySize(hooks, EventBus, baselineRegistrySize, scopeCollectionAttempts)
  assert(scopeCollected, `cleanup equivalence scope branch retained instances: ${EventBus._all_instances.size}/${baselineRegistrySize}`)

  const totalMs = hooks.now() - t0
  const msPerEvent = totalMs / totalEvents

  const result = {
    scenario: 'cleanup destroy vs scope equivalence',
    totalEvents,
    totalMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent: null,
    equivalent: scopeCollected,
  }
  record(hooks, result.scenario, result)
  return result
}

const runWithLeakCheck = async (input, scenarioFn) => {
  const hooks = withDefaults(input)
  const baselineHeapUsed = await measureStableHeapUsed(hooks, 'max', 12)
  const result = await scenarioFn(input)
  const heapDeltaGcMbRaw = await measureHeapDeltaAfterGc(hooks, baselineHeapUsed)
  const noiseFloorMb = heapDeltaNoiseFloorMb(hooks.runtimeName)
  const heapDeltaGcMb = heapDeltaGcMbRaw === null ? null : heapDeltaGcMbRaw - noiseFloorMb
  result.heapDeltaGcMbRaw = heapDeltaGcMbRaw
  result.heapDeltaGcMb = heapDeltaGcMb

  if (hooks.limits.enforceNonPositiveHeapDeltaAfterGc && typeof heapDeltaGcMb === 'number') {
    assert(heapDeltaGcMb <= 0, `${result.scenario} heap delta after GC is positive: ${heapDeltaGcMb.toFixed(3)}MB`)
  }
  if (
    result.scenario === 'worst-case forwarding + timeouts' &&
    hooks.limits.worstCaseMemoryDeltaMb !== null &&
    typeof heapDeltaGcMbRaw === 'number'
  ) {
    assert(
      heapDeltaGcMbRaw < hooks.limits.worstCaseMemoryDeltaMb,
      `worst-case memory delta after GC was ${heapDeltaGcMbRaw.toFixed(1)}MB (limit ${hooks.limits.worstCaseMemoryDeltaMb}MB)`
    )
  }

  if (typeof hooks.log === 'function' && typeof heapDeltaGcMb === 'number') {
    hooks.log(
      `[${hooks.runtimeName}] ${result.scenario} leak-check: heap_delta_gc=${heapDeltaGcMb.toFixed(3)}mb (raw=${heapDeltaGcMbRaw?.toFixed(3)}mb, noise_floor=${noiseFloorMb.toFixed(3)}mb)`
    )
  }

  return result
}

export const runAllPerfScenarios = async (input) => {
  await runWarmup(input)
  const results = []
  results.push(await runWithLeakCheck(input, runPerf50kEvents))
  results.push(await runWithLeakCheck(input, runPerfEphemeralBuses))
  results.push(await runWithLeakCheck(input, runPerfSingleEventManyFixedHandlers))
  results.push(await runWithLeakCheck(input, runPerfOnOffChurn))
  results.push(await runWithLeakCheck(input, runPerfWorstCase))
  results.push(await runWithLeakCheck(input, runCleanupEquivalence))
  return results
}
