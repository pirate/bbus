import assert from "node:assert/strict";
import { test } from "node:test";

import { z } from "zod";

import { BaseEvent, EventBus } from "../src/index.js";

const ParentEvent = BaseEvent.extend("ParentEvent", {});
const ChildEvent = BaseEvent.extend("ChildEvent", {});
const UnrelatedEvent = BaseEvent.extend("UnrelatedEvent", {});
const ScreenshotEvent = BaseEvent.extend("ScreenshotEvent", { target_id: z.string() });

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

test("find past returns most recent completed event", async () => {
  const bus = new EventBus("FindPastBus");

  const first_event = bus.dispatch(ParentEvent({}));
  await first_event.done();
  await delay(20);
  const second_event = bus.dispatch(ParentEvent({}));
  await second_event.done();

  const found_event = await bus.find(ParentEvent, { past: true, future: false });
  assert.ok(found_event);
  assert.equal(found_event.event_id, second_event.event_id);
});

test("find past window filters by time", async () => {
  const bus = new EventBus("FindWindowBus");

  const old_event = bus.dispatch(ParentEvent({}));
  await old_event.done();
  await delay(120);
  const new_event = bus.dispatch(ParentEvent({}));
  await new_event.done();

  const found_event = await bus.find(ParentEvent, { past: 0.1, future: false });
  assert.ok(found_event);
  assert.equal(found_event.event_id, new_event.event_id);
});

test("find past returns null when all events are too old", async () => {
  const bus = new EventBus("FindTooOldBus");

  const old_event = bus.dispatch(ParentEvent({}));
  await old_event.done();
  await delay(120);

  const found_event = await bus.find(ParentEvent, { past: 0.05, future: false });
  assert.equal(found_event, null);
});

test("find future waits for event", async () => {
  const bus = new EventBus("FindFutureBus");

  const find_promise = bus.find(ParentEvent, { past: false, future: 0.5 });

  setTimeout(() => {
    bus.dispatch(ParentEvent({}));
  }, 50);

  const found_event = await find_promise;
  assert.ok(found_event);
  assert.equal(found_event.event_type, "ParentEvent");
});

test("find future times out when no event arrives", async () => {
  const bus = new EventBus("FindFutureTimeoutBus");

  const found_event = await bus.find(ParentEvent, { past: false, future: 0.05 });
  assert.equal(found_event, null);
});

test("find respects where filter", async () => {
  const bus = new EventBus("FindWhereBus");

  const event_a = bus.dispatch(ScreenshotEvent({ target_id: "tab-a" }));
  const event_b = bus.dispatch(ScreenshotEvent({ target_id: "tab-b" }));
  await event_a.done();
  await event_b.done();

  const found_event = await bus.find(
    ScreenshotEvent,
    (event) => event.target_id === "tab-b",
    { past: true, future: false }
  );

  assert.ok(found_event);
  assert.equal(found_event.event_id, event_b.event_id);
});

test("find child_of returns child event", async () => {
  const bus = new EventBus("FindChildBus");

  bus.on(ParentEvent, (event) => {
    event.bus?.emit(ChildEvent({}));
  });

  const parent_event = bus.dispatch(ParentEvent({}));
  await bus.waitUntilIdle();

  const child_event = await bus.find(ChildEvent, {
    past: true,
    future: false,
    child_of: parent_event
  });

  assert.ok(child_event);
  assert.equal(child_event.event_parent_id, parent_event.event_id);
});

test("find child_of returns null for non-child", async () => {
  const bus = new EventBus("FindNonChildBus");

  const parent_event = bus.dispatch(ParentEvent({}));
  const unrelated_event = bus.dispatch(UnrelatedEvent({}));
  await parent_event.done();
  await unrelated_event.done();

  const found_event = await bus.find(UnrelatedEvent, {
    past: true,
    future: false,
    child_of: parent_event
  });

  assert.equal(found_event, null);
});
