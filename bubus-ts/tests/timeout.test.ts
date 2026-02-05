import assert from "node:assert/strict";
import { test } from "node:test";

import { BaseEvent, EventBus, EventHandlerTimeoutError } from "../src/index.js";

const TimeoutEvent = BaseEvent.extend("TimeoutEvent", {});

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

test("handler timeout marks EventResult as error", async () => {
  const bus = new EventBus("TimeoutBus");

  bus.on(TimeoutEvent, async () => {
    await delay(50);
    return "slow";
  });

  const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.01 }));
  await event.done();

  const result = Array.from(event.event_results.values())[0];
  assert.equal(result.status, "error");
  assert.ok(result.error instanceof EventHandlerTimeoutError);
});

test("handler completes within timeout", async () => {
  const bus = new EventBus("TimeoutOkBus");

  bus.on(TimeoutEvent, async () => {
    await delay(5);
    return "fast";
  });

  const event = bus.dispatch(TimeoutEvent({ event_timeout: 0.5 }));
  await event.done();

  const result = Array.from(event.event_results.values())[0];
  assert.equal(result.status, "completed");
  assert.equal(result.result, "fast");
});
