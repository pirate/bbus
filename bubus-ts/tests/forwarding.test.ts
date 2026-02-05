import assert from "node:assert/strict";
import { test } from "node:test";

import { z } from "zod";

import { BaseEvent, EventBus } from "../src/index.js";

const PingEvent = BaseEvent.extend("PingEvent", { value: z.number() });

test("events forward between buses without duplication", async () => {
  const bus_a = new EventBus("BusA");
  const bus_b = new EventBus("BusB");
  const bus_c = new EventBus("BusC");

  const seen_a: string[] = [];
  const seen_b: string[] = [];
  const seen_c: string[] = [];

  bus_a.on(PingEvent, (event) => {
    seen_a.push(event.event_id);
  });

  bus_b.on(PingEvent, (event) => {
    seen_b.push(event.event_id);
  });

  bus_c.on(PingEvent, (event) => {
    seen_c.push(event.event_id);
  });

  bus_a.on("*", bus_b.dispatch);
  bus_b.on("*", bus_c.dispatch);

  const event = bus_a.dispatch(PingEvent({ value: 1 }));

  await bus_a.waitUntilIdle();
  await bus_b.waitUntilIdle();
  await bus_c.waitUntilIdle();

  assert.equal(seen_a.length, 1);
  assert.equal(seen_b.length, 1);
  assert.equal(seen_c.length, 1);

  assert.equal(seen_a[0], event.event_id);
  assert.equal(seen_b[0], event.event_id);
  assert.equal(seen_c[0], event.event_id);

  assert.deepEqual(event.event_path, ["BusA", "BusB", "BusC"]);
});

test("await event.done waits for handlers on forwarded buses", async () => {
  const bus_a = new EventBus("BusA");
  const bus_b = new EventBus("BusB");
  const bus_c = new EventBus("BusC");

  const completion_log: string[] = [];

  const delay = (ms: number): Promise<void> =>
    new Promise((resolve) => {
      setTimeout(resolve, ms);
    });

  bus_a.on(PingEvent, async () => {
    await delay(10);
    completion_log.push("A");
  });

  bus_b.on(PingEvent, async () => {
    await delay(30);
    completion_log.push("B");
  });

  bus_c.on(PingEvent, async () => {
    await delay(50);
    completion_log.push("C");
  });

  bus_a.on("*", bus_b.dispatch);
  bus_b.on("*", bus_c.dispatch);

  const event = bus_a.dispatch(PingEvent({ value: 2 }));

  await event.done();

  assert.deepEqual(completion_log.sort(), ["A", "B", "C"]);
  assert.equal(event.event_pending_buses, 0);
});

test("await event.done waits when forwarding handler is async-delayed", async () => {
  const bus_a = new EventBus("BusA");
  const bus_b = new EventBus("BusB");

  const delay = (ms: number): Promise<void> =>
    new Promise((resolve) => {
      setTimeout(resolve, ms);
    });

  let bus_a_done = false;
  let bus_b_done = false;

  bus_a.on(PingEvent, async () => {
    await delay(20);
    bus_a_done = true;
  });

  bus_b.on(PingEvent, async () => {
    await delay(10);
    bus_b_done = true;
  });

  bus_a.on("*", async (event) => {
    await delay(30);
    bus_b.dispatch(event);
  });

  const event = bus_a.dispatch(PingEvent({ value: 3 }));
  await event.done();

  assert.equal(bus_a_done, true);
  assert.equal(bus_b_done, true);
  assert.equal(event.event_pending_buses, 0);
  assert.deepEqual(event.event_path, ["BusA", "BusB"]);
});
