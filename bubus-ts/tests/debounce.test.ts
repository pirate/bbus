import assert from "node:assert/strict";
import { test } from "node:test";

import { z } from "zod";

import { BaseEvent, EventBus } from "../src/index.js";

const ParentEvent = BaseEvent.extend("ParentEvent", {});

const ScreenshotEvent = BaseEvent.extend("ScreenshotEvent", { target_id: z.string() });

const SyncEvent = BaseEvent.extend("SyncEvent", {});

test("simple debounce uses recent history or dispatches new", async () => {
  const bus = new EventBus("DebounceBus");

  const parent_event = bus.dispatch(ParentEvent({}));
  await parent_event.done();

  const child_event = parent_event.bus?.emit(ScreenshotEvent({ target_id: "tab-1" }));
  assert.ok(child_event);
  await child_event.done();

  const reused_event =
    (await bus.find(ScreenshotEvent, {
      past: 10,
      future: false,
      child_of: parent_event
    })) ?? (await bus.dispatch(ScreenshotEvent({ target_id: "fallback" })).done());

  assert.equal(reused_event.event_id, child_event.event_id);
  assert.equal(reused_event.event_parent_id, parent_event.event_id);
});

test("advanced debounce prefers history, then waits for future, then dispatches", async () => {
  const bus = new EventBus("AdvancedDebounceBus");

  const pending_event = bus.find(SyncEvent, { past: false, future: 0.5 });

  setTimeout(() => {
    bus.dispatch(SyncEvent({}));
  }, 50);

  const resolved_event =
    (await bus.find(SyncEvent, { past: true, future: false })) ??
    (await pending_event) ??
    (await bus.dispatch(SyncEvent({})).done());

  assert.ok(resolved_event);
  assert.equal(resolved_event.event_type, "SyncEvent");
});
