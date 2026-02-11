# `bubus`: 📢 Production-ready multi-language event bus

<img width="200" alt="image" src="https://github.com/user-attachments/assets/b3525c24-51ba-496c-b327-ccdfe46a7362" align="right" />

[![DeepWiki: Python](https://img.shields.io/badge/DeepWiki-bbus%2FPython-yellow.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACwAAAAyCAYAAAAnWDnqAAAAAXNSR0IArs4c6QAAA05JREFUaEPtmUtyEzEQhtWTQyQLHNak2AB7ZnyXZMEjXMGeK/AIi+QuHrMnbChYY7MIh8g01fJoopFb0uhhEqqcbWTp06/uv1saEDv4O3n3dV60RfP947Mm9/SQc0ICFQgzfc4CYZoTPAswgSJCCUJUnAAoRHOAUOcATwbmVLWdGoH//PB8mnKqScAhsD0kYP3j/Yt5LPQe2KvcXmGvRHcDnpxfL2zOYJ1mFwrryWTz0advv1Ut4CJgf5uhDuDj5eUcAUoahrdY/56ebRWeraTjMt/00Sh3UDtjgHtQNHwcRGOC98BJEAEymycmYcWwOprTgcB6VZ5JK5TAJ+fXGLBm3FDAmn6oPPjR4rKCAoJCal2eAiQp2x0vxTPB3ALO2CRkwmDy5WohzBDwSEFKRwPbknEggCPB/imwrycgxX2NzoMCHhPkDwqYMr9tRcP5qNrMZHkVnOjRMWwLCcr8ohBVb1OMjxLwGCvjTikrsBOiA6fNyCrm8V1rP93iVPpwaE+gO0SsWmPiXB+jikdf6SizrT5qKasx5j8ABbHpFTx+vFXp9EnYQmLx02h1QTTrl6eDqxLnGjporxl3NL3agEvXdT0WmEost648sQOYAeJS9Q7bfUVoMGnjo4AZdUMQku50McDcMWcBPvr0SzbTAFDfvJqwLzgxwATnCgnp4wDl6Aa+Ax283gghmj+vj7feE2KBBRMW3FzOpLOADl0Isb5587h/U4gGvkt5v60Z1VLG8BhYjbzRwyQZemwAd6cCR5/XFWLYZRIMpX39AR0tjaGGiGzLVyhse5C9RKC6ai42ppWPKiBagOvaYk8lO7DajerabOZP46Lby5wKjw1HCRx7p9sVMOWGzb/vA1hwiWc6jm3MvQDTogQkiqIhJV0nBQBTU+3okKCFDy9WwferkHjtxib7t3xIUQtHxnIwtx4mpg26/HfwVNVDb4oI9RHmx5WGelRVlrtiw43zboCLaxv46AZeB3IlTkwouebTr1y2NjSpHz68WNFjHvupy3q8TFn3Hos2IAk4Ju5dCo8B3wP7VPr/FGaKiG+T+v+TQqIrOqMTL1VdWV1DdmcbO8KXBz6esmYWYKPwDL5b5FA1a0hwapHiom0r/cKaoqr+27/XcrS5UwSMbQAAAABJRU5ErkJggg==)](https://deepwiki.com/pirate/bbus) ![PyPI - Version](https://img.shields.io/pypi/v/bubus) ![GitHub License](https://img.shields.io/github/license/pirate/bbus) ![GitHub last commit](https://img.shields.io/github/last-commit/pirate/bbus)

[![DeepWiki: TS](https://img.shields.io/badge/DeepWiki-bbus%2FTypescript-blue.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACwAAAAyCAYAAAAnWDnqAAAAAXNSR0IArs4c6QAAA05JREFUaEPtmUtyEzEQhtWTQyQLHNak2AB7ZnyXZMEjXMGeK/AIi+QuHrMnbChYY7MIh8g01fJoopFb0uhhEqqcbWTp06/uv1saEDv4O3n3dV60RfP947Mm9/SQc0ICFQgzfc4CYZoTPAswgSJCCUJUnAAoRHOAUOcATwbmVLWdGoH//PB8mnKqScAhsD0kYP3j/Yt5LPQe2KvcXmGvRHcDnpxfL2zOYJ1mFwrryWTz0advv1Ut4CJgf5uhDuDj5eUcAUoahrdY/56ebRWeraTjMt/00Sh3UDtjgHtQNHwcRGOC98BJEAEymycmYcWwOprTgcB6VZ5JK5TAJ+fXGLBm3FDAmn6oPPjR4rKCAoJCal2eAiQp2x0vxTPB3ALO2CRkwmDy5WohzBDwSEFKRwPbknEggCPB/imwrycgxX2NzoMCHhPkDwqYMr9tRcP5qNrMZHkVnOjRMWwLCcr8ohBVb1OMjxLwGCvjTikrsBOiA6fNyCrm8V1rP93iVPpwaE+gO0SsWmPiXB+jikdf6SizrT5qKasx5j8ABbHpFTx+vFXp9EnYQmLx02h1QTTrl6eDqxLnGjporxl3NL3agEvXdT0WmEost648sQOYAeJS9Q7bfUVoMGnjo4AZdUMQku50McDcMWcBPvr0SzbTAFDfvJqwLzgxwATnCgnp4wDl6Aa+Ax283gghmj+vj7feE2KBBRMW3FzOpLOADl0Isb5587h/U4gGvkt5v60Z1VLG8BhYjbzRwyQZemwAd6cCR5/XFWLYZRIMpX39AR0tjaGGiGzLVyhse5C9RKC6ai42ppWPKiBagOvaYk8lO7DajerabOZP46Lby5wKjw1HCRx7p9sVMOWGzb/vA1hwiWc6jm3MvQDTogQkiqIhJV0nBQBTU+3okKCFDy9WwferkHjtxib7t3xIUQtHxnIwtx4mpg26/HfwVNVDb4oI9RHmx5WGelRVlrtiw43zboCLaxv46AZeB3IlTkwouebTr1y2NjSpHz68WNFjHvupy3q8TFn3Hos2IAk4Ju5dCo8B3wP7VPr/FGaKiG+T+v+TQqIrOqMTL1VdWV1DdmcbO8KXBz6esmYWYKPwDL5b5FA1a0hwapHiom0r/cKaoqr+27/XcrS5UwSMbQAAAABJRU5ErkJggg==)](https://deepwiki.com/pirate/bbus/3-typescript-implementation) ![NPM Version](https://img.shields.io/npm/v/bubus) 

Bubus is an in-memory event bus library for async Python and TS (node/browser).

It's designed for quickly building resilient, predictable, complex event-driven apps.

It "just works" with an intuitive, but powerful event JSON format + dispatch API that's consistent across both languages and scales consistently from one even up to millions:
```python
bus.on(SomeEvent, some_function)
bus.emit(SomeEvent({some_data: 132}))
```

It's async native, has proper automatic nested event tracking, and powerful concurrency control options. The API is inspired by `EventEmitter` or [`emittery`](https://github.com/sindresorhus/emittery) in JS, but it takes it a step further:

- nice Pydantic / Zod schemas for events that can be exchanged between both languages
- automatic UUIDv7s and monotonic nanosecond timestamps for ordering events globally
- built in locking options to force strict global FIFO procesing or fully parallel processing

---

♾️ It's inspired by the simplicity of async and events in `JS` but with baked-in features that allow to eliminate most of the tedious repetitive complexity in event-driven codebases:

- correct timeout enforcement across multiple levels of events, if a parent times out it correctly aborts all child event processing
- ability to strongly type hint and enforce the return type of event handlers at compile-time
- ability to queue events on the bus, or inline await them for immediate execution like a normal function call
- handles thousands of events/sec/core in both languages; see the runtime matrix below for current measured numbers

<br/>

## 🏃 Runtime (Python)

Performance matrix measured locally on **February 11, 2026** with:

- `uv run python tests/performance_runtime.py --json`

| Runtime | 1 bus x 50k events x 1 handler | 500 busses x 100 events x 1 handler | 1 bus x 1 event x 50k parallel handlers | 1 bus x 50k events x 50k one-off handlers | Worst case (N busses x N events x N handlers) |
| ------------------ | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ |
| Python | `0.239ms/event`, `8.024kb/event` | `0.259ms/event`, `0.148kb/event` | `0.077ms/handler`, `7.785kb/handler` | `0.310ms/event`, `0.025kb/event` | `0.694ms/event`, `2.464kb/event` |

Notes:

- These runs use default bus setup (no special tuning knobs like custom history limits).
- `1 bus x 50k events x 1 handler` dispatches all 50k events in one go (no manual batching).
- `kb/event` and `kb/handler` are peak RSS deltas normalized per work unit for each scenario.
- CPU totals are also collected by the harness (see `cpu_ms` / `cpu_ms_per_event` in JSON output) so wall-clock latency is not interpreted as pure CPU cost.

<br/>

## 🔢 Quickstart

Install bubus and get started with a simple event-driven application:

```bash
pip install bubus      # see ./bubus-ts/README.md for JS instructions
```

```python
import asyncio
from bubus import EventBus, BaseEvent
from your_auth_events import AuthRequestEvent, AuthResponseEvent

class UserLoginEvent(BaseEvent[str]):
    username: str
    is_admin: bool

async def handle_login(event: UserLoginEvent) -> str:
    auth_request = await event.event_bus.dispatch(AuthRequestEvent(...))  # nested events supported
    auth_response = await event.event_bus.find(AuthResponseEvent, child_of=auth_request, future=30)
    return f"User {event.username} logged in admin={event.is_admin} with API response: {await auth_response.event_result()}"

bus = EventBus()
bus.on(UserLoginEvent, handle_login)
bus.on(AuthRequestEvent, AuthAPI.post)

event = bus.dispatch(UserLoginEvent(username="alice", is_admin=True))
print(await event.event_result())
# User alice logged in admin=True with API response: {...}
```

<br/>

---

<br/>

## ✨ Features

<br/>

### 🔎 Event Pattern Matching

Subscribe to events using multiple patterns:

```python
# By event model class (recommended for best type hinting)
bus.on(UserActionEvent, handler)

# By event type string
bus.on('UserActionEvent', handler)

# Wildcard - handle all events
bus.on('*', universal_handler)
```

<br/>

### 🔀 Async and Sync Handler Support

Register both synchronous and asynchronous handlers for maximum flexibility:

```python
# Async handler
async def async_handler(event: SomeEvent) -> str:
    await asyncio.sleep(0.1)  # Simulate async work
    return "async result"

# Sync handler
def sync_handler(event: SomeEvent) -> str:
    return "sync result"

bus.on(SomeEvent, async_handler)
bus.on(SomeEvent, sync_handler)
```

Handlers can also be defined under classes for easier organization:

```python
class SomeService:
    some_value = 'this works'

    async def handlers_can_be_methods(self, event: SomeEvent) -> str:
        return self.some_value
    
    @classmethod
    async def handler_can_be_classmethods(cls, event: SomeEvent) -> str:
        return cls.some_value

    @staticmethod
    async def handlers_can_be_staticmethods(event: SomeEvent) -> str:
        return 'this works too'

# All usage patterns behave the same:
bus.on(SomeEvent, SomeService().handlers_can_be_methods)
bus.on(SomeEvent, SomeService.handler_can_be_classmethods)
bus.on(SomeEvent, SomeService.handlers_can_be_staticmethods)
```

<br/>


### 🔠 Type-Safe Events with Pydantic

Define events as Pydantic models with full type checking and validation:

```python
from typing import Any
from bubus import BaseEvent

class OrderCreatedEvent(BaseEvent):
    order_id: str
    customer_id: str
    total_amount: float
    items: list[dict[str, Any]]

# Events are automatically validated
event = OrderCreatedEvent(
    order_id="ORD-123",
    customer_id="CUST-456", 
    total_amount=99.99,
    items=[{"sku": "ITEM-1", "quantity": 2}]
)
```

> [!TIP]
> You can also enforce the types of [event handler return values](#-event-handler-return-values).

<br/>



### ⏩ Forward `Events` Between `EventBus`s 

You can define separate `EventBus` instances in different "microservices" to separate different areas of concern.
`EventBus`s can be set up to forward events between each other (with automatic loop prevention):

```python
# Create a hierarchy of buses
main_bus = EventBus(name='MainBus')
auth_bus = EventBus(name='AuthBus')
data_bus = EventBus(name='DataBus')

# Share all or specific events between buses
main_bus.on('*', auth_bus.dispatch)  # if main bus gets LoginEvent, will forward to AuthBus
auth_bus.on('*', data_bus.dispatch)  # auth bus will forward everything to DataBus
data_bus.on('*', main_bus.dispatch)  # don't worry! event will only be processed once by each, no infinite loop occurs

# Events flow through the hierarchy with tracking
event = main_bus.dispatch(LoginEvent())
await event
print(event.event_path)  # ['MainBus', 'AuthBus', 'DataBus']  # list of buses that have already procssed the event
```

<br/>

### Bridges

Bridges are optional extra connectors provided that allow you to send/receive events from an external service, and you do not need to use a bridge to use bubus since it's normally purely in-memory. These are just simple helpers to forward bubus events JSON to storage engines / other processes / other machines; they prevent loops automatically, but beyond that it's only basic forwarding with no handler pickling or anything fancy.

Bridges all expose a very simple bus-like API with only `.emit()` and `.on()`.

**Example usage: link a bus to a redis pub/sub channel**
```python
bridge = RedisEventBridge('redis://redis@localhost:6379')

bus.on('*', bridge.emit)  # listen for all events on bus and send them to redis channel
bridge.on('*', bus.emit)  # listen for new events in redis channel and dispatch them to our bus
```

- `SocketEventBridge('/tmp/bubus_events.sock')`
- `HTTPEventBridge(send_to='https://127.0.0.1:8001/bubus_events', listen_on='http://0.0.0.0:8002/bubus_events')`
- `JSONLEventBridge('/tmp/bubus_events.jsonl')`
- `SQLiteEventBridge('/tmp/bubus_events.sqlite3')`
- `PostgresEventBridge('postgresql://user:pass@localhost:5432/dbname/bubus_events')`
- `RedisEventBridge('redis://user:pass@localhost:6379/1/bubus_events')`
- `NATSEventBridge('nats://localhost:4222', 'bubus_events')`

<br/>

### 🔱 Event Results Aggregation

Collect and aggregate results from multiple handlers:

```python
async def load_user_config(event: GetConfigEvent) -> dict[str, Any]:
    return {"debug": True, "port": 8080}

async def load_system_config(event: GetConfigEvent) -> dict[str, Any]:
    return {"debug": False, "timeout": 30}

bus.on(GetConfigEvent, load_user_config)
bus.on(GetConfigEvent, load_system_config)

# Get a merger of all dict results
# (conflicting keys raise ValueError unless raise_if_conflicts=False)
event = await bus.dispatch(GetConfigEvent())
config = await event.event_results_flat_dict(raise_if_conflicts=False)
# {'debug': False, 'port': 8080, 'timeout': 30}

# Or get individual results
await event.event_results_by_handler_id()
await event.event_results_list()
```

<br/>

### 🚦 FIFO Event Processing

Events are processed in strict FIFO order, maintaining consistency:

```python
# Events are processed in the order they were dispatched
for i in range(10):
    bus.dispatch(ProcessTaskEvent(task_id=i))

# Even with async handlers, order is preserved
await bus.wait_until_idle(timeout=30.0)
```

If a handler dispatches and awaits any child events during execution, those events will jump the FIFO queue and be processed immediately:
```python
def child_handler(event: SomeOtherEvent) -> str:
    return 'xzy123'

def main_handler(event: MainEvent) -> str:
    # enqueue event for processing after main_handler exits
    child_event = bus.dispatch(SomeOtherEvent())
    
    # can also await child events to process immediately instead of adding to FIFO queue
    completed_child_event = await child_event
    return f'result from awaiting child event: {await completed_child_event.event_result()}'  # 'xyz123'

bus.on(SomeOtherEvent, child_handler)
bus.on(MainEvent, main_handler)

await bus.dispatch(MainEvent()).event_result()
# result from awaiting child event: xyz123
```

<br/>

### 🪆 Dispatch Nested Child Events From Handlers

Automatically track event relationships and causality tree:

```python
async def parent_handler(event: BaseEvent):
    # handlers can emit more events to be processed asynchronously after this handler completes
    child = ChildEvent()
    child_event_async = event.event_bus.dispatch(child)   # equivalent to bus.dispatch(...)
    assert child.event_status != 'completed'
    assert child_event_async.event_parent_id == event.event_id
    await child_event_async

    # or you can dispatch an event and block until it finishes processing by awaiting the event
    # this recursively waits for all handlers, including if event is forwarded to other buses
    # (note: awaiting an event from inside a handler jumps the FIFO queue and will process it immediately, before any other pending events)
    child_event_sync = await bus.dispatch(ChildEvent())
    # ChildEvent handlers run immediately
    assert child_event_sync.event_status == 'completed'

    # in all cases, parent-child relationships are automagically tracked
    assert child_event_sync.event_parent_id == event.event_id

async def run_main():
    bus.on(ChildEvent, child_handler)
    bus.on(ParentEvent, parent_handler)

    parent_event = bus.dispatch(ParentEvent())
    print(parent_event.event_children)           # show all the child events emitted during handling of an event
    await parent_event
    print(bus.log_tree())
    await bus.stop()

if __name__ == '__main__':
    asyncio.run(run_main())
```

<img width="100%" alt="show the whole tree of events at any time using the logging helpers" src="https://github.com/user-attachments/assets/f94684a6-7694-4066-b948-46925f47b56c" /><br/>
<img width="100%" alt="intelligent timeout handling to differentiate handler that timed out from handler that was interrupted" src="https://github.com/user-attachments/assets/8da341fd-6c26-4c68-8fec-aef1ca55c189" />


<br/><br/>

### 🔎 Find Events in History or Wait for Future Events

The `find()` method provides a unified way to search past event history and/or wait for future events. It's the recommended approach for most event lookup scenarios.

The `past` and `future` parameters accept either `bool` or `float` values:

| Value | `past` meaning | `future` meaning |
|-------|----------------|------------------|
| `True` | Search all history | Wait forever |
| `False` | Skip history search | Don't wait |
| `5.0` | Search last 5 seconds | Wait up to 5 seconds |

```python
# Search all history, wait up to 5s for future
event = await bus.find(ResponseEvent, past=True, future=5)

# Search last 5s of history, wait forever
event = await bus.find(ResponseEvent, past=5, future=True)

# Search last 5s of history, wait up to 5s
event = await bus.find(ResponseEvent, past=5, future=5)

# Search all history only, don't wait (instant)
event = await bus.find(ResponseEvent, past=True, future=False)

# Wait up to 5s for future only (like expect())
event = await bus.find(ResponseEvent, past=False, future=5)

# With custom filter
event = await bus.find(ResponseEvent, where=lambda e: e.request_id == my_id, future=5)
```

#### Finding Child Events

When you dispatch an event that triggers child events, use `child_of` to find specific descendants:

```python
# Dispatch a parent event that triggers child events
nav_event = await bus.dispatch(NavigateToUrlEvent(url="https://example.com"))

# Find a child event (already fired while NavigateToUrlEvent was being handled)
new_tab = await bus.find(TabCreatedEvent, child_of=nav_event, past=5)
if new_tab:
    print(f"New tab created: {new_tab.tab_id}")
```

This solves race conditions where child events fire before you start waiting for them.

> [!IMPORTANT]
> `find()` resolves when the event is first *dispatched* to the `EventBus`, not when it completes. Use `await event` to wait for handlers to finish.
> If no match is found (or future timeout elapses), `find()` returns `None`.

<br/>

### 🔁 Event Debouncing

Avoid re-running expensive work by reusing recent events. The `find()` method makes debouncing simple:

```python
# Simple debouncing: reuse event from last 10 seconds, or dispatch new
event = (
    bus.find(ScreenshotEvent, past=10, future=False)  # Check last 10s of history (instant)
    or await bus.dispatch(ScreenshotEvent())
)

# Advanced: check history, wait briefly for new event to appear, fallback to dispatch new event
event = (
    await bus.find(SyncEvent, past=True, future=False)   # Check all history (instant)
    or await bus.find(SyncEvent, past=False, future=5)   # Wait up to 5s for in-flight
    or await bus.dispatch(SyncEvent())                   # Fallback: dispatch new
)
```

<br/>

### 🎯 Event Handler Return Values

There are two ways to get return values from event handlers:

**1. Have handlers return their values directly, which puts them in `event.event_results`:**

```python
class DoSomeMathEvent(BaseEvent[int]):  # BaseEvent[int] = expect int returned from all event handlers
    a: int
    b: int

def do_some_math(event: DoSomeMathEvent) -> int:                                                                                                                        
    return event.a + event.b

event_bus.on(DoSomeMathEvent, do_some_math)
print(await event_bus.dispatch(DoSomeMathEvent(a=100, b=120)).event_result())
# 220
```

You can use these helpers to interact with the results returned by handlers:
- `BaseEvent.event_result()`
- `BaseEvent.event_results_list()`, `BaseEvent.event_results_filtered()`
- `BaseEvent.event_results_by_handler_id()`, `BaseEvent.event_results_by_handler_name()`
- `BaseEvent.event_results_flat_list()`, `BaseEvent.event_results_flat_dict()`

**2. Have the handler do the work, then dispatch another event containing the result value, which other code can expect:**

```python
def do_some_math(event: DoSomeMathEvent[int]) -> int:
    result = event.a + event.b
    event.event_bus.dispatch(MathCompleteEvent(final_sum=result))

event_bus.on(DoSomeMathEvent, do_some_math)
await event_bus.dispatch(DoSomeMathEvent(a=100, b=120))
result_event = await event_bus.expect(MathCompleteEvent)
print(result_event.final_sum)
# 220
```

#### Annotating Event Handler Return Value Types

Bubus supports optional strict typing for Event handler return values using a generic parameter passed to `BaseEvent[ReturnTypeHere]`.
For example if you use `BaseEvent[str]`, bubus would enforce that all handler functions must return `str | None` at compile-time via IDE/`mypy`/`pyright`/`ty` type hints, and at runtime when each handler finishes.

```python
class ScreenshotEvent(BaseEvent[bytes]):  # BaseEvent[bytes] will enforce that handlers can only return bytes
    width: int
    height: int

async def on_ScreenshotEvent(event: ScreenshotEvent) -> bytes:
    return b'someimagebytes...'  # ✅ IDE type-hints & runtime both enforce return type matches expected: bytes
    return 123                   # ❌ will show mypy/pyright issue + raise TypeError if the wrong type is returned

event_bus.on(ScreenshotEvent, on_ScreenshotEvent)

# Handler return values are automatically validated against the bytes type
returned_bytes = await event_bus.dispatch(ScreenshotEvent(...)).event_result()
assert isinstance(returned_bytes, bytes)
```

**Important:** The validation uses Pydantic's `TypeAdapter`, which validates but does not coerce types. Handlers must return the exact type specified or `None`:

```python
class StringEvent(BaseEvent[str]):
    pass

# ✅ This works - returns the expected str type
def good_handler(event: StringEvent) -> str:
    return "hello"

# ❌ This fails validation - returns int instead of str
def bad_handler(event: StringEvent) -> str:
    return 42  # ValidationError: expected str, got int
```

This also works with complex types and Pydantic models:

```python
class EmailMessage(BaseModel):
    subject: str
    content_len: int
    email_from: str

class FetchInboxEvent(BaseEvent[list[EmailMessage]]):
    account_id: UUID
    auth_key: str

async def fetch_from_gmail(event: FetchInboxEvent) -> list[EmailMessage]:
    return [EmailMessage(subject=msg.subj, ...) for msg in GmailAPI.get_msgs(event.account_id, ...)]

event_bus.on(FetchInboxEvent, fetch_from_gmail)

# Return values are automatically validated as list[EmailMessage]
email_list = await event_bus.dispatch(FetchInboxEvent(account_id='124', ...)).event_result()
```

<br/>

### 🧵 ContextVar Propagation

ContextVars set before `dispatch()` are automatically propagated to event handlers. This is essential for request-scoped context like request IDs, user sessions, or tracing spans:

```python
from contextvars import ContextVar

# Define your context variables
request_id: ContextVar[str] = ContextVar('request_id', default='<unset>')
user_id: ContextVar[str] = ContextVar('user_id', default='<unset>')

async def handler(event: MyEvent) -> str:
    # Handler sees the context values that were set before dispatch()
    print(f"Request: {request_id.get()}, User: {user_id.get()}")
    return "done"

bus.on(MyEvent, handler)

# Set context before dispatch (e.g., in FastAPI middleware)
request_id.set('req-12345')
user_id.set('user-abc')

# Handler will see request_id='req-12345' and user_id='user-abc'
await bus.dispatch(MyEvent())
```

**Context propagates through nested handlers:**

```python
async def parent_handler(event: ParentEvent) -> str:
    # Context is captured at dispatch time
    print(f"Parent sees: {request_id.get()}")  # 'req-12345'

    # Child events inherit the same context
    await bus.dispatch(ChildEvent())
    return "parent_done"

async def child_handler(event: ChildEvent) -> str:
    # Child also sees the original dispatch context
    print(f"Child sees: {request_id.get()}")  # 'req-12345'
    return "child_done"
```

**Context isolation between dispatches:**

Each dispatch captures its own context snapshot. Concurrent dispatches with different context values are properly isolated:

```python
request_id.set('req-A')
event_a = bus.dispatch(MyEvent())  # Handler A sees 'req-A'

request_id.set('req-B')
event_b = bus.dispatch(MyEvent())  # Handler B sees 'req-B'

await event_a  # Still sees 'req-A'
await event_b  # Still sees 'req-B'
```

> [!NOTE]
> Context is captured at `dispatch()` time, not when the handler executes. This ensures handlers see the context from the call site, even if the event is processed later from a queue.

<br/>

### 🧹 Memory Management

EventBus includes automatic memory management to prevent unbounded growth in long-running applications:

```python
# Create a bus with memory limits (default: 50 events)
bus = EventBus(max_history_size=100)  # Keep max 100 events in history

# Or disable memory limits for unlimited history
bus = EventBus(max_history_size=None)

# Or keep only in-flight events in history (drop each event as soon as it completes)
bus = EventBus(max_history_size=0)

# Or reject new dispatches when history is full (instead of dropping old history)
bus = EventBus(max_history_size=100, max_history_drop=False)
```

**Automatic Cleanup:**
- When `max_history_size` is set and `max_history_drop=True` (default), EventBus removes old events when the limit is exceeded
- If `max_history_size=0`, history keeps only pending/started events and drops each event immediately after completion
- If `max_history_drop=True`, the bus may drop oldest history entries even if they are uncompleted events
- Completed events are removed first (oldest first), then started events, then pending events
- This ensures active events are preserved while cleaning up old completed events

**Manual Memory Management:**
```python
# For request-scoped buses (e.g. web servers), clear all memory after each request
try:
    event_service = EventService()  # Creates internal EventBus
    await event_service.process_request()
finally:
    # Clear all event history and remove from global tracking
    await event_service.eventbus.stop(clear=True)
```

**Memory Monitoring:**
- EventBus automatically monitors total memory usage across all instances
- Warnings are logged when total memory exceeds 50MB
- Use `bus.stop(clear=True)` to completely free memory for unused buses
- To avoid memory leaks from big events, the default limits are intentionally kept low. events are normally processed as they come in, and there is rarely a need to keep every event in memory longer after its complete. long-term storage should be accomplished using other mechanisms, like the WAL

<br/>

### ⛓️ Parallel Handler Execution

> [!CAUTION]
> **Not Recommended.** Only for advanced users willing to implement their own concurrency control.

Enable parallel processing of handlers for better performance.  
The harsh tradeoff is less deterministic ordering as handler execution order will not be guaranteed when run in parallel. 
(It's very hard to write non-flaky/reliable applications when handler execution order is not guaranteed.)

```python
# Create bus with parallel handler execution
bus = EventBus(parallel_handlers=True)

# Multiple handlers run concurrently for each event
bus.on('DataEvent', slow_handler_1)  # Takes 1 second
bus.on('DataEvent', slow_handler_2)  # Takes 1 second

start = time.time()
await bus.dispatch(DataEvent())
# Total time: ~1 second (not 2)
```

<br/>

### 📝 Write-Ahead Logging

Persist events automatically to a `jsonl` file for future replay and debugging:

```python
from pathlib import Path

from bubus import EventBus, SQLiteHistoryMirrorMiddleware
from bubus.middlewares import LoggerEventBusMiddleware, WALEventBusMiddleware

# Enable WAL event log persistence (optional)
bus = EventBus(
    name='MyBus',
    middlewares=[
        SQLiteHistoryMirrorMiddleware('./events.sqlite'),
        WALEventBusMiddleware('./events.jsonl'),
        LoggerEventBusMiddleware('./events.log'),
    ],
)

# LoggerEventBusMiddleware defaults to stdout-only logging if no file path is provided

# All completed events are automatically appended as JSON lines to the end
await bus.dispatch(SecondEventAbc(some_key="banana"))
```

`./events.jsonl`:
```json
{"event_type": "FirstEventXyz", "event_created_at": "2025-07-10T20:39:56.462000+00:00", "some_key": "some_val", ...}
{"event_type": "SecondEventAbc", ..., "some_key": "banana"}
...
```

<br/>

---
---

<br/>

## 📚 API Documentation

### `EventBus`

The main event bus class that manages event processing and handler execution.

```python
EventBus(
    name: str | None = None,
    parallel_handlers: bool = False,
    max_history_size: int | None = 50,
    max_history_drop: bool = True,
    middlewares: Sequence[EventBusMiddleware | type[EventBusMiddleware]] | None = None,
)
```

**Parameters:**

- `name`: Optional unique name for the bus (auto-generated if not provided)
- `parallel_handlers`: If `True`, handlers run concurrently for each event, otherwise serially if `False` (the default)
- `max_history_size`: Maximum number of events to keep in history (default: 50, `None` = unlimited, `0` = keep only in-flight events and drop completed events immediately)
- `max_history_drop`: If `True` (default), drop oldest history entries when full (even uncompleted events). If `False`, reject new dispatches once history reaches `max_history_size` (except when `max_history_size=0`, which never rejects on history size)
- `middlewares`: Optional list of `EventBusMiddleware` subclasses or instances that hook into handler execution for analytics, logging, retries, etc.

Handler middlewares subclass `EventBusMiddleware` and override whichever lifecycle hooks they need:

```python
from bubus.middlewares import EventBusMiddleware

class AnalyticsMiddleware(EventBusMiddleware):
    async def process_handler_start(self, eventbus, event, event_result):
        await analytics_bus.dispatch(HandlerStartedAnalyticsEvent(event_id=event_result.event_id))

    async def process_handler_end(self, eventbus, event, event_result):
        await analytics_bus.dispatch(HandlerCompletedAnalyticsEvent(event_id=event_result.event_id))

    async def process_handler_exception(self, eventbus, event, event_result, error):
        await analytics_bus.dispatch(HandlerCompletedAnalyticsEvent(event_id=event_result.event_id, error=error))
```

Middlewares can observe or mutate the `EventResult` at each step, dispatch additional events, or trigger other side effects (metrics, retries, auth checks, etc.).

Pair that with the built-in `SQLiteHistoryMirrorMiddleware` to mirror every event and handler transition into append-only `events_log` and `event_results_log` tables, making it easy to inspect or audit the bus state:

```python
from bubus import EventBus, SQLiteHistoryMirrorMiddleware

bus = EventBus(middlewares=[SQLiteHistoryMirrorMiddleware('./events.sqlite')])
```
#### `EventBus` Properties

- `name`: The bus identifier
- `id`: Unique UUID7 for this bus instance
- `event_history`: Dict of all events the bus has seen by event_id (limited by `max_history_size`)
- `events_pending`: List of events waiting to be processed
- `events_started`: List of events currently being processed
- `events_completed`: List of completed events
- `all_instances`: Class-level WeakSet tracking all active EventBus instances (for memory monitoring)


#### `EventBus` Methods

##### `on(event_type: str | Type[BaseEvent], handler: Callable)`

Subscribe a handler to events matching a specific event type or `'*'` for all events.

```python
bus.on('UserEvent', handler_func)  # By event type string
bus.on(UserEvent, handler_func)    # By event class
bus.on('*', handler_func)          # Wildcard - all events
```

##### `dispatch(event: BaseEvent) -> BaseEvent`

Enqueue an event for processing and return the pending `Event` immediately (synchronous).

```python
event = bus.dispatch(MyEvent(data="test"))
result = await event  # await the pending Event to get the completed Event
```

**Note:** Queueing is unbounded. History pressure is controlled by `max_history_size` + `max_history_drop`:
- `max_history_drop=True`: absorb new events and trim old history entries (even uncompleted events).
- `max_history_drop=False`: raise `RuntimeError` when history is full.
- `max_history_size=0`: keep pending/in-flight events only; completed events are immediately removed from history.

##### `query(event_type: str | Type[BaseEvent], *, include: Callable[[BaseEvent], bool] | None=None, exclude: Callable[[BaseEvent], bool] | None=None, since: timedelta | float | int | None=None) -> BaseEvent | None`

Return the most recently completed event in history that matches the type and optional predicates. Returns `None` if nothing qualifies.

```python
recent_sync = await bus.query(
    SyncEvent,
    since=timedelta(seconds=30),
    include=lambda e: e.account_id == account_id,
)

if recent_sync is not None:
    print('We already synced recently, skipping')
```

##### `find(event_type: str | Type[BaseEvent], *, where: Callable[[BaseEvent], bool]=None, child_of: BaseEvent | None=None, past: bool | float=True, future: bool | float=True) -> BaseEvent | None`

Find an event matching criteria in history and/or future. This is the recommended unified method for event lookup.

**Parameters:**

- `event_type`: The event type string or model class to find
- `where`: Predicate function for filtering (default: matches all)
- `child_of`: Only match events that are descendants of this parent event
- `past`: Controls history search behavior (default: `True`)
  - `True`: search all history
  - `False`: skip history search
  - `float`: search events from last N seconds only
- `future`: Controls future wait behavior (default: `True`)
  - `True`: wait forever for matching event
  - `False`: don't wait for future events
  - `float`: wait up to N seconds for matching event

```python
# Search all history, wait up to 5s for future
event = await bus.find(ResponseEvent, past=True, future=5)

# Search last 5s of history, wait forever
event = await bus.find(ResponseEvent, past=5, future=True)

# Search last 5s of history, wait up to 5s
event = await bus.find(ResponseEvent, past=5, future=5)

# Search all history only, don't wait (instant)
event = await bus.find(ResponseEvent, past=True, future=False)

# Wait up to 5s for future only (ignore history)
event = await bus.find(ResponseEvent, past=False, future=5)

# Find child of a specific parent event
child = await bus.find(ChildEvent, child_of=parent_event, future=5)

# With custom filter
event = await bus.find(ResponseEvent, where=lambda e: e.status == 'success', future=5)
```

##### `expect(event_type: str | Type[BaseEvent], *, include: Callable=None, exclude: Callable=None, timeout: float | None=None, past: bool | float=False, child_of: BaseEvent | None=None) -> BaseEvent | None`

Wait for a specific event to occur. This is a backwards-compatible wrapper around `find()`.

**Parameters:**

- `event_type`: The event type string or model class to wait for
- `include`: Filter function that must return `True` for the event to match
- `exclude`: Filter function that must return `False` for the event to match
- `timeout`: Maximum time to wait in seconds (None = wait forever). Maps to `future` parameter of `find()`.
- `past`: Controls history search behavior (default: `False`)
  - `True`: search all history first
  - `False`: skip history search
  - `float`: search events from last N seconds
- `child_of`: Only match events that are descendants of this parent event

```python
# Wait for any UserEvent
event = await bus.expect('UserEvent', timeout=30)

# Wait with custom filter
event = await bus.expect(
    'UserEvent',
    include=lambda e: e.user_id == 'specific_user',
    timeout=30,
)

# Search history first, then wait
event = await bus.expect('UserEvent', past=True, timeout=30)

# Search last 10 seconds of history, then wait
event = await bus.expect('UserEvent', past=10, timeout=30)

# Find child event
child = await bus.expect(ChildEvent, child_of=parent_event, timeout=5)

if event is None:
    print('No matching event arrived within 30 seconds')
```

##### `event_is_child_of(event: BaseEvent, ancestor: BaseEvent) -> bool`

Check if event is a descendant of ancestor (child, grandchild, etc.).

```python
if bus.event_is_child_of(child_event, parent_event):
    print("child_event is a descendant of parent_event")
```

##### `event_is_parent_of(event: BaseEvent, descendant: BaseEvent) -> bool`

Check if event is an ancestor of descendant (parent, grandparent, etc.).

```python
if bus.event_is_parent_of(parent_event, child_event):
    print("parent_event is an ancestor of child_event")
```

##### `wait_until_idle(timeout: float | None=None)`

Wait until all events are processed and the bus is idle.

```python
await bus.wait_until_idle()             # wait indefinitely until EventBus has finished processing all events

await bus.wait_until_idle(timeout=5.0)  # wait up to 5 seconds
```

##### `stop(timeout: float | None=None, clear: bool=False)`

Stop the event bus, optionally waiting for pending events and clearing memory.

```python
await bus.stop(timeout=1.0)  # Graceful stop, wait up to 1sec for pending and active events to finish processing
await bus.stop()             # Immediate shutdown, aborts all pending and actively processing events
await bus.stop(clear=True)   # Stop and clear all event history and handlers to free memory
```

---

### `BaseEvent`

Base class for all events. Subclass `BaseEvent` to define your own events.

Make sure none of your own event data fields start with `event_` or `model_` to avoid clashing with `BaseEvent` or `pydantic` builtin attrs.

#### `BaseEvent` Fields

```python
T_EventResultType = TypeVar('T_EventResultType', bound=Any, default=None)

class BaseEvent(BaseModel, Generic[T_EventResultType]):
    # Framework-managed fields
    event_type: str              # Defaults to class name
    event_version: str           # Defaults to '0.0.1' (override per class/instance for event payload versioning)
    event_id: str                # Unique UUID7 identifier, auto-generated if not provided
    event_timeout: float = 300.0 # Maximum execution in seconds for each handler
    event_schema: str            # Module.Class@version (auto-set based on class & LIBRARY_VERSION env var)
    event_parent_id: str         # Parent event ID (auto-set)
    event_path: list[str]        # List of bus names traversed (auto-set)
    event_created_at: datetime   # When event was created, auto-generated
    event_results: dict[str, EventResult]   # Handler results
    event_result_type: type[T_EventResultType] | None  # Auto-detected from Generic[T] parameter
    
    # Data fields
    # ... subclass BaseEvent to add your own event data fields here ...
    # some_key: str
    # some_other_key: dict[str, int]
    # ...
```

`event.event_results` contains a dict of pending `EventResult` objects that will be completed once handlers finish executing.


#### `BaseEvent` Properties

- `event_status`: `Literal['pending', 'started', 'completed']` Event status
- `event_started_at`: `datetime` When first handler started processing
- `event_completed_at`: `datetime` When all handlers completed processing
- `event_children`: `list[BaseEvent]` Get any child events emitted during handling of this event
- `event_bus`: `EventBus` Shortcut to get the bus currently processing this event
- `event_result_type`: `type[Any] | None` Expected handler return type (auto-detected from `BaseEvent[T]` generic parameter)

#### `BaseEvent` Methods

##### `await event`

Await the `Event` object directly to get the completed `Event` object once all handlers have finished executing.

```python
event = bus.dispatch(MyEvent())
completed_event = await event

raw_result_values = [(await event_result) for event_result in completed_event.event_results.values()]
# equivalent to: completed_event.event_results_list()  (see below)
```

##### `reset() -> Self`

Return a fresh event copy with runtime processing state reset back to pending.

- Intended for re-dispatching an already-seen event payload (for example after crossing a bridge boundary).
- The original event object is unchanged.
- Runtime completion state is cleared (`event_results`, completion signal/flags, processed timestamp, dispatch context).

##### `event_result(timeout: float | None=None, include: EventResultFilter=None, raise_if_any: bool=True, raise_if_none: bool=True) -> Any`

Utility method helper to execute all the handlers and return the first handler's raw result value.

**Parameters:**

- `timeout`: Maximum time to wait for handlers to complete (None = use default event timeout)
- `include`: Filter function to include only specific results (default: only non-None, non-exception results)
- `raise_if_any`: If `True`, raise exception if any handler raises any `Exception` (`default: True`)
- `raise_if_none`: If `True`, raise exception if results are empty / all results are `None` or `Exception` (`default: True`)

```python
# by default it returns the first successful non-None result value
result = await event.event_result()

# Get result from first handler that returns a string
valid_result = await event.event_result(include=lambda r: isinstance(r.result, str) and len(r.result) > 100)

# Get result but don't raise exceptions or error for 0 results, just return None
result_or_none = await event.event_result(raise_if_any=False, raise_if_none=False)
```

##### `event_results_by_handler_id(timeout: float | None=None, include: EventResultFilter=None, raise_if_any: bool=True, raise_if_none: bool=True) -> dict`

Utility method helper to get all raw result values organized by `{handler_id: result_value}`.

**Parameters:**

- `timeout`: Maximum time to wait for handlers to complete (None = use default event timeout)
- `include`: Filter function to include only specific results (default: only non-None, non-exception results)
- `raise_if_any`: If `True`, raise exception if any handler raises any `Exception` (`default: True`)
- `raise_if_none`: If `True`, raise exception if results are empty / all results are `None` or `Exception` (`default: True`)

```python
# by default it returns all successful non-None result values
results = await event.event_results_by_handler_id()
# {'handler_id_1': result1, 'handler_id_2': result2}

# Only include results from handlers that returned integers
int_results = await event.event_results_by_handler_id(include=lambda r: isinstance(r.result, int))

# Get all results including errors and None values
all_results = await event.event_results_by_handler_id(raise_if_any=False, raise_if_none=False)
```

##### `event_results_list(timeout: float | None=None, include: EventResultFilter=None, raise_if_any: bool=True, raise_if_none: bool=True) -> list[Any]`

Utility method helper to get all raw result values in a list.

**Parameters:**

- `timeout`: Maximum time to wait for handlers to complete (None = use default event timeout)
- `include`: Filter function to include only specific results (default: only non-None, non-exception results)
- `raise_if_any`: If `True`, raise exception if any handler raises any `Exception` (`default: True`)
- `raise_if_none`: If `True`, raise exception if results are empty / all results are `None` or `Exception` (`default: True`)

```python
# by default it returns all successful non-None result values
results = await event.event_results_list()
# [result1, result2]

# Only include results that are strings longer than 10 characters
filtered_results = await event.event_results_list(include=lambda r: isinstance(r.result, str) and len(r.result) > 10)

# Get all results without raising on errors
all_results = await event.event_results_list(raise_if_any=False, raise_if_none=False)
```

##### `event_results_flat_dict(timeout: float | None=None, include: EventResultFilter=None, raise_if_any: bool=True, raise_if_none: bool=False, raise_if_conflicts: bool=True) -> dict`

Utility method helper to merge all raw result values that are `dict`s into a single flat `dict`.

**Parameters:**

- `timeout`: Maximum time to wait for handlers to complete (None = use default event timeout)
- `include`: Filter function to include only specific results (default: only non-None, non-exception results)
- `raise_if_any`: If `True`, raise exception if any handler raises any `Exception` (`default: True`)
- `raise_if_none`: If `True`, raise exception if results are empty / all results are `None` or `Exception` (`default: False`)
- `raise_if_conflicts`: If `True`, raise exception if dict keys conflict between handlers (`default: True`)

```python
# by default it merges all successful dict results
results = await event.event_results_flat_dict()
# {'key1': 'value1', 'key2': 'value2'}

# Merge only dicts with specific keys
config_dicts = await event.event_results_flat_dict(include=lambda r: isinstance(r.result, dict) and 'config' in r.result)

# Allow conflicts, last handler wins
merged = await event.event_results_flat_dict(raise_if_conflicts=False)
```

##### `event_results_flat_list(timeout: float | None=None, include: EventResultFilter=None, raise_if_any: bool=True, raise_if_none: bool=True) -> list`

Utility method helper to merge all raw result values that are `list`s into a single flat `list`.

**Parameters:**

- `timeout`: Maximum time to wait for handlers to complete (None = use default event timeout)
- `include`: Filter function to include only specific results (default: only non-None, non-exception results)
- `raise_if_any`: If `True`, raise exception if any handler raises any `Exception` (`default: True`)
- `raise_if_none`: If `True`, raise exception if results are empty / all results are `None` or `Exception` (`default: True`)

```python
# by default it merges all successful list results
results = await event.event_results_flat_list()
# ['item1', 'item2', 'item3']

# Merge only lists with more than 2 items
long_lists = await event.event_results_flat_list(include=lambda r: isinstance(r.result, list) and len(r.result) > 2)

# Get all list results without raising on errors
all_items = await event.event_results_flat_list(raise_if_any=False, raise_if_none=False)
```

##### `event_create_pending_results(handlers: dict[str, EventHandler], eventbus: EventBus | None = None, timeout: float | None = None) -> dict[str, EventResult]`

Create (or reset) the `EventResult` placeholders for the provided handlers. The `EventBus` uses this internally before it begins executing handlers so that the event's state is immediately visible. Advanced users can call it when coordinating handler execution manually.

```python
applicable_handlers = bus._get_applicable_handlers(event)  # internal helper shown for illustration
pending_results = event.event_create_pending_results(applicable_handlers, eventbus=bus)

assert all(result.status == 'pending' for result in pending_results.values())
```

##### `event_bus` (property)

Shortcut to get the `EventBus` that is currently processing this event. Can be used to avoid having to pass an `EventBus` instance to your handlers.

```python
bus = EventBus()

async def some_handler(event: MyEvent):
    # You can always dispatch directly to any bus you have a reference to
    child_event = bus.dispatch(ChildEvent())
    
    # OR use the event.event_bus shortcut to get the current bus:
    child_event = await event.event_bus.dispatch(ChildEvent())
```


---

### `EventResult`

The placeholder object that represents the pending result from a single handler executing an event.  
`Event.event_results` contains a `dict[PythonIdStr, EventResult]` in the shape of `{handler_id: EventResult()}`.

You generally won't interact with this class directly—the bus instantiates and updates it for you—but its API is documented here for advanced integrations and custom dispatch loops.

#### `EventResult` Fields

```python
class EventResult(BaseModel):
    id: str                    # Unique identifier
    handler_id: str           # Handler function ID
    handler_name: str         # Handler function name
    eventbus_id: str          # Bus that executed this handler
    eventbus_name: str        # Bus name
    
    status: str               # 'pending', 'started', 'completed', 'error'
    result: Any               # Handler return value
    error: BaseException | None  # Captured exception if the handler failed
    
    started_at: datetime      # When handler started
    completed_at: datetime    # When handler completed
    timeout: float            # Handler timeout in seconds
    event_children: list[BaseEvent] # child events emitted during handler execution
```

#### `EventResult` Methods

##### `await result`

Await the `EventResult` object directly to get the raw result value.

```python
handler_result = event.event_results['handler_id']
value = await handler_result  # Returns result or raises an exception if handler hits an error
```

- `execute(event, handler, *, eventbus, timeout, enter_handler_context, exit_handler_context, format_exception_for_log)`  
  Low-level helper that runs the handler, updates timing/status fields, captures errors, and notifies its completion signal. `EventBus.execute_handler()` delegates to this; you generally only need it when building a custom bus or integrating the event system into another dispatcher.

---

## 🧵 Advanced Concurrency Control

### `@retry` Decorator

The `@retry` decorator provides automatic retry functionality with built-in concurrency control for any function, including event handlers. This is particularly useful when handlers interact with external services that may temporarily fail.

```python
from bubus import EventBus, BaseEvent
from bubus.helpers import retry

bus = EventBus()

class FetchDataEvent(BaseEvent):
    url: str

@retry(
    wait=2,                     # Wait 2 seconds between retries
    retries=3,                  # Retry up to 3 times after initial failure
    timeout=5,                  # Each attempt times out after 5 seconds
    semaphore_limit=5,          # Max 5 concurrent executions
    backoff_factor=1.5,         # Exponential backoff: 2s, 3s, 4.5s
    retry_on=(TimeoutError, ConnectionError)  # Only retry on specific exceptions
)
async def fetch_with_retry(event: FetchDataEvent):
    # This handler will automatically retry on network failures
    async with aiohttp.ClientSession() as session:
        async with session.get(event.url) as response:
            return await response.json()

bus.on(FetchDataEvent, fetch_with_retry)
```

#### Retry Parameters

- **`timeout`**: Maximum amount of time function is allowed to take per attempt, in seconds (default: 5)
- **`retries`**: Number of additional retry attempts if function raises an exception (default: 3)
- **`retry_on`**: Tuple of exception types to retry on (default: `None` = retry on any `Exception`)
- **`wait`**: Base seconds to wait between retries (default: 3)
- **`backoff_factor`**: Multiplier for wait time after each retry (default: 1.0)
- **`semaphore_limit`**: Maximum number of concurrent calls that can run at the same time
- **`semaphore_scope`**: Scope for the semaphore: `class`, `self`, `global`, or `multiprocess`
- **`semaphore_timeout`**: Maximum time to wait for a semaphore slot before proceeding or failing
- **`semaphore_lax`**: Continue anyway if semaphore fails to be acquired in within the given time
- **`semaphore_name`**: Unique semaphore name to allow sharing a semaphore between functions

#### Semaphore Options

Control concurrency with built-in semaphore support:

```python
# Global semaphore - all calls share one limit
@retry(semaphore_limit=3, semaphore_scope='global')
async def global_limited_handler(event): ...

# Per-class semaphore - all instances of a class share one limit
class MyService:
    @retry(semaphore_limit=2, semaphore_scope='class')
    async def class_limited_handler(self, event): ...

# Per-instance semaphore - each instance gets its own limit
class MyService:
    @retry(semaphore_limit=1, semaphore_scope='self')
    async def instance_limited_handler(self, event): ...

# Cross-process semaphore - all processes share one limit
@retry(semaphore_limit=5, semaphore_scope='multiprocess')
async def process_limited_handler(event): ...
```

#### Advanced Example

```python
import logging

# Configure logging to see retry attempts
logging.basicConfig(level=logging.INFO)

class DatabaseEvent(BaseEvent):
    query: str

class DatabaseService:
    @retry(
        wait=1,
        retries=5,
        timeout=10,
        semaphore_limit=10,          # Max 10 concurrent DB operations
        semaphore_scope='class',     # Shared across all instances
        semaphore_timeout=30,        # Wait up to 30s for semaphore
        semaphore_lax=False,         # Fail if can't acquire semaphore
        backoff_factor=2.0,          # Exponential backoff: 1s, 2s, 4s, 8s, 16s
        retry_on=(ConnectionError, TimeoutError)
    )
    async def execute_query(self, event: DatabaseEvent):
        # Automatically retries on connection failures
        # Limited to 10 concurrent operations across all instances
        result = await self.db.execute(event.query)
        return result

# Register the handler
db_service = DatabaseService()
bus.on(DatabaseEvent, db_service.execute_query)
```

<br/>

---
---

<br/>

## 👾 Development

Set up the python development environment using `uv`:

```bash
git clone https://github.com/browser-use/bubus && cd bubus

# Create virtual environment with Python 3.12
uv venv --python 3.12

# Activate virtual environment (varies by OS)
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate  # On Windows

# Install dependencies
uv sync --dev --all-extras
```

```bash
# Run linter & type checker
uv run ruff check --fix
uv run ruff format
uv run pyright

# Run all tests
uv run pytest -vxs --full-trace tests/

# Run specific test file
uv run pytest tests/test_eventbus.py

# Run Python perf suite
uv run perf

# Run the entire lint+test+examples+perf suite for both python and ts
./test.sh
```

> For Bubus-TS development see the `bubus-ts/README.md` `# Development` section.

## 🔗 Inspiration

- https://www.cosmicpython.com/book/chapter_08_events_and_message_bus.html#message_bus_diagram ⭐️
- https://developer.mozilla.org/en-US/docs/Web/API/EventTarget ⭐️
- https://github.com/sindresorhus/emittery ⭐️ (equivalent for JS), https://github.com/EventEmitter2/EventEmitter2, https://github.com/vitaly-t/sub-events
- https://github.com/pytest-dev/pluggy ⭐️
- https://github.com/teamhide/fastapi-event ⭐️
- https://github.com/ethereum/lahja ⭐️
- https://github.com/enricostara/eventure ⭐️
- https://github.com/akhundMurad/diator ⭐️
- https://github.com/n89nanda/pyeventbus
- https://github.com/iunary/aioemit
- https://github.com/dboslee/evently
- https://github.com/faust-streaming/faust
- https://github.com/ArcletProject/Letoderea
- https://github.com/seanpar203/event-bus
- https://github.com/n89nanda/pyeventbus
- https://github.com/nicolaszein/py-async-bus
- https://github.com/AngusWG/simple-event-bus
- https://www.joeltok.com/posts/2021-03-building-an-event-bus-in-python/

---


> [🧠 DeepWiki Docs](https://deepwiki.com/browser-use/bubus)  
> <img width="400" alt="image" src="https://github.com/user-attachments/assets/cedb5a2e-0643-4240-9a3d-5f27cb8b5741" /><img width="400" alt="image" src="https://github.com/user-attachments/assets/3ee0ee8c-8322-449f-979b-5c99ba6bd960" />



## 🏛️ License

This project is licensed under the MIT License. For more information, see the main browser-use repository: https://github.com/browser-use/browser-use
