import { z } from "zod";

import { BaseEvent, EventBus } from "../src/index.js";

const RootEvent = BaseEvent.extend(
  "RootEvent",
  { url: z.string() },
  { event_result_schema: z.string(), event_result_type: "string" }
);

const ChildEvent = BaseEvent.extend(
  "ChildEvent",
  { tab_id: z.string() },
  { event_result_schema: z.string(), event_result_type: "string" }
);

const GrandchildEvent = BaseEvent.extend(
  "GrandchildEvent",
  { status: z.string() },
  { event_result_schema: z.string(), event_result_type: "string" }
);

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

async function main(): Promise<void> {
  const bus_a = new EventBus("BusA");
  const bus_b = new EventBus("BusB");

  async function forward_to_bus_b(event: InstanceType<typeof RootEvent>): Promise<string> {
    await delay(20);
    bus_b.dispatch(event);
    return "forwarded_to_bus_b";
  }

  bus_a.on("*", forward_to_bus_b);

  async function root_fast_handler(event: InstanceType<typeof RootEvent>): Promise<string> {
    await delay(10);
    const child = event.bus?.emit(
      ChildEvent({ tab_id: "tab-123", event_timeout: 0.05 })
    );
    if (child) {
      await child.done();
    }
    return "root_fast_handler_ok";
  }

  async function root_slow_handler(): Promise<string> {
    await delay(120);
    return "root_slow_handler_timeout";
  }

  bus_a.on(RootEvent, root_fast_handler);
  bus_a.on(RootEvent, root_slow_handler);

  async function child_slow_handler(_event: InstanceType<typeof ChildEvent>): Promise<string> {
    await delay(200);
    return "child_slow_handler_done";
  }

  async function child_fast_handler(event: InstanceType<typeof ChildEvent>): Promise<string> {
    await delay(10);
    event.bus?.emit(
      GrandchildEvent({ status: "ok", event_timeout: 0.05 })
    );
    return "child_handler_ok";
  }

  async function grandchild_fast_handler(): Promise<string> {
    await delay(5);
    return "grandchild_fast_handler_ok";
  }

  async function grandchild_slow_handler(): Promise<string> {
    await delay(80);
    return "grandchild_slow_handler_timeout";
  }

  bus_b.on(ChildEvent, child_slow_handler);
  bus_b.on(ChildEvent, child_fast_handler);
  bus_b.on(GrandchildEvent, grandchild_fast_handler);
  bus_b.on(GrandchildEvent, grandchild_slow_handler);

  const root_event = bus_a.dispatch(
    RootEvent({ url: "https://example.com", event_timeout: 0.05 })
  );

  await root_event.done();

  console.log("\n=== BusA logTree ===");
  console.log(bus_a.logTree());

  console.log("\n=== BusB logTree ===");
  console.log(bus_b.logTree());
}

await main();
