import assert from "node:assert/strict";
import { test } from "node:test";

import { z } from "zod";

import { BaseEvent, EventBus } from "../src/index.js";

const StringResultEvent = BaseEvent.extend(
  "StringResultEvent",
  {},
  { event_result_schema: z.string(), event_result_type: "string" }
);

const ObjectResultEvent = BaseEvent.extend(
  "ObjectResultEvent",
  {},
  { event_result_schema: z.object({ value: z.string(), count: z.number() }) }
);

const NoResultSchemaEvent = BaseEvent.extend("NoResultSchemaEvent", {});

test("event results capture handler return values", async () => {
  const bus = new EventBus("ResultCaptureBus");

  bus.on(StringResultEvent, () => "ok");

  const event = bus.dispatch(StringResultEvent({}));
  await event.done();

  assert.equal(event.event_results.size, 1);
  const result = Array.from(event.event_results.values())[0];
  assert.equal(result.status, "completed");
  assert.equal(result.result, "ok");
});

test("event_result_schema validates handler results", async () => {
  const bus = new EventBus("ResultSchemaBus");

  bus.on(ObjectResultEvent, () => ({ value: "hello", count: 2 }));

  const event = bus.dispatch(ObjectResultEvent({}));
  await event.done();

  const result = Array.from(event.event_results.values())[0];
  assert.equal(result.status, "completed");
  assert.deepEqual(result.result, { value: "hello", count: 2 });
});

test("invalid result marks handler error", async () => {
  const bus = new EventBus("ResultSchemaErrorBus");

  bus.on(ObjectResultEvent, () => ({ value: "bad", count: "nope" } as unknown));

  const event = bus.dispatch(ObjectResultEvent({}));
  await event.done();

  const result = Array.from(event.event_results.values())[0];
  assert.equal(result.status, "error");
  assert.ok(result.error instanceof Error);
});

test("event with no result schema stores raw values", async () => {
  const bus = new EventBus("NoSchemaBus");

  bus.on(NoResultSchemaEvent, () => ({ raw: true }));

  const event = bus.dispatch(NoResultSchemaEvent({}));
  await event.done();

  const result = Array.from(event.event_results.values())[0];
  assert.equal(result.status, "completed");
  assert.deepEqual(result.result, { raw: true });
});
