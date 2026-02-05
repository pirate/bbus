import assert from "node:assert/strict";
import { test } from "node:test";

import { z } from "zod";

import { BaseEvent, EventBus } from "../src/index.js";

const OrderEvent = BaseEvent.extend("OrderEvent", { order: z.number() });

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

test("events are processed in FIFO order", async () => {
  const bus = new EventBus("FifoBus");

  const processed_orders: number[] = [];
  const handler_start_times: number[] = [];

  bus.on(OrderEvent, async (event) => {
    handler_start_times.push(Date.now());
    if (event.order % 2 === 0) {
      await delay(30);
    } else {
      await delay(5);
    }
    processed_orders.push(event.order);
  });

  for (let i = 0; i < 10; i += 1) {
    bus.dispatch(OrderEvent({ order: i }));
  }

  await bus.waitUntilIdle();

  assert.deepEqual(processed_orders, Array.from({ length: 10 }, (_, i) => i));
  for (let i = 1; i < handler_start_times.length; i += 1) {
    assert.ok(handler_start_times[i] >= handler_start_times[i - 1]);
  }
});
