# TODO: TypeScript Unified Runtime Refactor (Middleware + Context-Manager Style)

This plan merges:
- middleware support work (`TODO_middleware.md`)
- context/lock/timeout architecture work (`TODO_context_manager_refactor.md`)

into one ordered TS implementation plan.
This file supersedes those two TODOs for TypeScript implementation planning.

## Semantic Decisions (Already Fixed)

1. `event_timeout` is a hard cap across all handlers for an event.
2. `event_handler_timeout` defaults to `event_timeout` when unset.
3. Queue-jump path remains lock-driven; no enum-only shortcut logic.
4. `first()` winner in TS is first non-`undefined`, non-error, non-`BaseEvent` result (`null` is valid).
5. Core handler error taxonomy must include:
- `EventHandlerCancelledError`
- `EventHandlerResultSchemaError`
- `EventHandlerTimeoutError`
- `EventHandlerAbortedError`
6. Event status lifecycle remains `pending | started | completed` (errors live on `EventResult`, not `Event`).

## Constraints (Locked In)

1. `middlewares` must accept either classes (constructors) or instances.
2. Class entries in `middlewares` are auto-instantiated.
3. Hook execution is sequential in registration order.
4. Middleware throw behavior is undefined (no suppression/guarding required).
5. Hook re-fires are acceptable (keep implementation simple).
6. Per-bus hook emission is correct (different buses can have different middleware stacks).
7. `markCancelled` must never produce `completed -> started`.
8. Docs updates are out of scope for this pass.

## Target Architecture

### Core execution ownership

- `EventBus` owns orchestration.
- `BaseEvent`/`EventResult` only perform local state mutation + upward notifications.
- Middleware invocation is centralized in `EventBus`.

### Context-manager style in TS

Use scoped wrapper functions (functional context-manager style):

- `withEventLock(event, fn)`
- `withHandlerLock(event, result, fn)`
- `withHandlerDispatchContext(event, fn)` (or equivalent wrapper around existing ALS restore)
- `withTimeout(timeoutSecs, fn)` for event-level and handler-level wrapping
- `withSlowMonitor(start/stop monitor, fn)`

These wrappers should compose, and keep the current semaphore/queue-jump behavior.

## Verified Runtime Trace (Implementation Anchors)

Use these concrete call paths to keep changes behavior-safe while refactoring:

1. Dispatch + context capture:
- `EventBus.dispatch(...)` captures async context via `captureAsyncContext()`.
- Queue insert + runloop kick happen here; this is the correct `event: pending` emission anchor.

2. Runloop + event processing:
- `EventBus.runloop(...)` dequeues and resolves semaphore policy through `LockManager.getSemaphoreForEvent(...)`.
- `EventBus.processEvent(...)` is the bus-local orchestration seam for `event: started/completed`, pending result creation, and future event-level hard timeout wrapper.

3. Handler execution stack:
- `BaseEvent.createPendingHandlerResults(...)` creates result records; this is the correct `event_result: pending` emission anchor.
- `BaseEvent.processEvent(...)` drives all/first semantics.
- `EventResult.runHandler(...)` already centralizes handler lock acquisition, timeout race, slow warnings, abort signaling, and terminal result marking.

4. Queue-jump/cross-bus path:
- `BaseEvent.done()/immediate()` route through `EventBus.processEventImmediately(...)`.
- `EventBus.processEventImmediatelyAcrossBuses(...)` and `HandlerLock.runQueueJump(...)` coordinate pause/release and semaphore bypass where required.
- Any lock abstraction must preserve this exact lifecycle.

5. Status mutation anchors:
- Event transitions live in `BaseEvent.markStarted/markCompleted/markCancelled`.
- Result transitions live in `EventResult.markStarted/markCompleted/markError`.
- Middleware integration should hook at these transition anchors via bus notifiers, not by duplicating status logic in middleware code.

### Compact Line-Anchored Call Graph

- `EventBus.dispatch(...)` enqueue/context capture: `bubus-ts/src/event_bus.ts:450`, `bubus-ts/src/event_bus.ts:459`.
- `EventBus.runloop(...)` dequeue/event semaphore path: `bubus-ts/src/event_bus.ts:865`.
- `EventBus.processEvent(...)` bus-local orchestration seam: `bubus-ts/src/event_bus.ts:687`.
- `BaseEvent.createPendingHandlerResults(...)` pending result materialization: `bubus-ts/src/base_event.ts:373`.
- `BaseEvent.processEvent(...)` all/first fanout and completion policy: `bubus-ts/src/base_event.ts:394`.
- `EventResult.runHandler(...)` handler timeout/slow/error core: `bubus-ts/src/event_result.ts:245`.
- Queue-jump entry/coordination:
  - `BaseEvent.done()/immediate()`: `bubus-ts/src/base_event.ts:655`.
  - `EventBus.processEventImmediatelyAcrossBuses(...)`: `bubus-ts/src/event_bus.ts:787`.
  - `HandlerLock.runQueueJump(...)`: `bubus-ts/src/lock_manager.ts:131`.

### Scope-Nesting Rationale

- Preferred execution layering for both event and handler scopes is:
  - lock -> error save/normalization -> hard timeout -> slow monitor -> body.
- Timeout scope wraps slow monitor so timeout cancellation naturally tears down monitoring.
- Error wrapper stays outside timeout/monitor scopes so timeout and abort errors are normalized consistently into agreed taxonomy.

## Sequencing Rationale (Why This Order)

1. Middleware surface and hook runner land before lock/timeout refactors so lifecycle observability is stable while internals move.
2. Context-manager wrappers are introduced as thin seams first (no semantic changes), then hot paths are migrated behind them.
3. Event-level hard timeout is delayed until wrappers exist so timeout finalization can reuse lock/abort/release mechanics instead of duplicating them.
4. `first()` and taxonomy alignment are late-phase normalization work once timeout + lifecycle behavior is stable.
5. Built-in middleware parity is explicitly follow-on to avoid coupling core runtime correctness with additional feature surface.

## Ordered Implementation Plan

## Phase 0: Baseline + safety net

1. Run and snapshot current TS tests that cover locking/timeout/first/context:
- `bubus-ts/tests/locking.test.ts`
- `bubus-ts/tests/timeout.test.ts`
- `bubus-ts/tests/first.test.ts`
- `bubus-ts/tests/context_propagation.test.ts`
- `bubus-ts/tests/forwarding.test.ts`

2. Add focused middleware test file skeleton (failing tests allowed initially):
- `bubus-ts/tests/middleware.test.ts`

## Phase 1: Public middleware surface

Files:
- `bubus-ts/src/middlewares.ts` (new)
- `bubus-ts/src/event_bus.ts`
- `bubus-ts/src/index.ts`
- `bubus-ts/src/types.ts` (only if minor export/type wiring needed)

Changes:
1. Add `EventBusMiddleware` interface.
2. Add `EventBusMiddlewareCtor = new () => EventBusMiddleware`.
3. Re-export `EventStatus` from existing `types.ts` (do not duplicate type definition).
4. Extend `EventBusOptions` with:
- `middlewares?: Array<EventBusMiddleware | EventBusMiddlewareCtor>`
5. Constructor normalization:
- class -> `new Class()`
- instance -> use directly
6. Store normalized middlewares on bus runtime state.
7. Keep middleware types runtime-only (no behavior in this phase).

## Phase 2: Internal middleware hook runner

Files:
- `bubus-ts/src/event_bus.ts`

Changes:
1. Add internal notifiers:
- `_on_event_change(event, status)`
- `_on_event_result_change(event, result, status)`
- `_on_handler_change(handler, registered)`
2. Add `runMiddlewareHook(...)`:
- iterate middlewares in order
- `await` each hook sequentially
- do not catch middleware errors
3. Add `scheduleMicrotask(fn)` helper:
- `queueMicrotask` if available
- fallback `Promise.resolve().then(...)`
4. Use `scheduleMicrotask` in `startRunloop` for runtime portability.
5. Keep middleware scheduling and execution outside lock acquisition where possible.

## Phase 3: Hook integration into lifecycle (simple semantics)

Files:
- `bubus-ts/src/event_bus.ts`
- `bubus-ts/src/base_event.ts`
- `bubus-ts/src/event_result.ts`

Changes:
1. Event hooks:
- `pending`: emit from `dispatch()` after enqueue, before runloop kick.
- `started`: emit when event transitions pending -> started.
- `completed`: emit when event transitions to completed.

2. Event-result hooks:
- `pending`: emit right after `createPendingHandlerResults(...)` in bus orchestration.
- `started`: emit on `EventResult.markStarted()` transition.
- `completed`: emit on `EventResult.markCompleted()` and `EventResult.markError()`.

3. Handler registration hooks:
- emit `on_handler_change(..., true)` after `on(...)`.
- emit `on_handler_change(..., false)` after removal in `off(...)`.
- use fire-and-forget microtask scheduling.

Notes:
- Re-fires are acceptable; do not add dedupe complexity.
- Keep ordering monotonic per call site.
- For sync mutation paths (`markCancelled`, `markCompleted`, `markError`) emit async hooks via microtask helper to avoid changing sync signatures.

## Phase 4: Context-manager style execution seams

Files:
- `bubus-ts/src/lock_manager.ts`
- `bubus-ts/src/event_bus.ts`
- `bubus-ts/src/event_result.ts`
- `bubus-ts/src/async_context.ts` (if helper extraction needed)

Changes:
1. Introduce lock-policy wrapper methods around existing semaphores/locks:
- `withEventLock(...)`
- `withHandlerLock(...)`
2. Introduce one composable dispatch-context wrapper around existing ALS restore path.
3. Migrate hot paths to wrappers without changing behavior:
- `EventBus.processEvent(...)`
- `EventResult.runHandler()`
- queue-jump path (`processEventImmediately...`) must keep current pause/release mechanics.
4. Keep identity/state access for lock checks used by queue-jump slow/active paths.

## Phase 5: Event-level hard timeout integration

Files:
- `bubus-ts/src/event_bus.ts`
- `bubus-ts/src/base_event.ts`
- `bubus-ts/src/event_result.ts`

Changes:
1. Add event-level hard timeout wrapper in `EventBus.processEvent(...)` around bus-local handler execution scope.
2. Ensure middleware latency does not weaken timeout semantics:
- timeout applies to handler execution scope, not middleware hook runtime.
3. Timeout finalizer rules (bus-local):
- started handlers -> `EventHandlerAbortedError` + abort signal
- pending handlers -> `EventHandlerCancelledError`
4. Release active execution state in finalizer:
- `_lock?.exitHandlerRun()`
- `releaseQueueJumpPauses()`
- `signalAbort(aborted_error)` for started handlers
5. Do not wait for in-flight promises after timeout finalization (detach/suppress late completions).
6. Keep terminal guards so late completions cannot overwrite finalized status.
7. Ensure hook ordering remains monotonic (`started -> completed`, never reverse).
8. Reuse descendant cancellation logic for pending child/downstream work.
9. Keep queue-jump lock safety and semaphore bypass behavior unchanged.

## Phase 6: `first()` winner semantics alignment (TS side)

Files:
- `bubus-ts/src/base_event.ts`
- `bubus-ts/src/event_result.ts`
- `bubus-ts/tests/first.test.ts`

Rules:
1. Winner must be:
- non-error
- non-`BaseEvent`
- non-`undefined`
2. `null` is valid winner.
3. Cancellation of non-winners stays bus-local and consistent with existing behavior.
4. Align tests to explicitly assert `null` can win while `undefined` cannot.

## Phase 7: Error taxonomy alignment (TS side)

Files:
- `bubus-ts/src/event_handler.ts`
- `bubus-ts/src/event_result.ts`
- `bubus-ts/tests/error_handling.test.ts`
- `bubus-ts/tests/timeout.test.ts`

Changes:
1. Ensure all timeout/cancel/abort/result-schema terminal paths use the four agreed classes.
2. Keep retry-internal error types allowed, but normalize surfaced handler terminal states to agreed taxonomy.
3. Validate cause/event_result metadata remains attached for middleware/diagnostics.

## Phase 8: Verification + cleanup

1. Run full targeted suite:
```bash
cd bubus-ts
pnpm test tests/middleware.test.ts
pnpm test tests/locking.test.ts
pnpm test tests/timeout.test.ts
pnpm test tests/first.test.ts
pnpm test tests/error_handling.test.ts
pnpm test tests/context_propagation.test.ts
pnpm test tests/forwarding.test.ts
```

2. Add/adjust tests for:
- ctor + instance middleware auto-init
- hook sequencing
- per-bus hooks on forwarded events
- no-handler event lifecycle hooks
- cancellation path ordering (no `completed -> started`)
- hard event-timeout immediate finalization (no wait on inflight handlers)
- taxonomy assertions for cancelled/aborted/timeout/result-schema

3. Remove temporary compatibility shims if any were introduced during migration.

## Phase 9: Built-in middleware parity (follow-on)

Files:
- `bubus-ts/src/middlewares.ts`
- `bubus-ts/src/index.ts`
- `bubus-ts/tests/*` (targeted parity tests)

Add TS equivalents of:
- `AutoErrorEventMiddleware`
- `AutoReturnEventMiddleware`
- `AutoHandlerChangeEventMiddleware`
- `LoggerEventBusMiddleware`
- `WALEventBusMiddleware`
- `SQLiteHistoryMirrorMiddleware`

Important UI compatibility note:
- `SQLiteHistoryMirrorMiddleware` table/schema writes must stay compatible with reads in `ui/db.py`.

## Done Criteria

1. Middleware API is public and stable.
2. Core lifecycle hooks are emitted in expected order.
3. Lock/context/timeout wrappers are centralized and composable.
4. Queue-jump behavior remains correct.
5. `first()` winner semantics and error taxonomy match locked decisions.
6. Core unified-runtime targeted tests pass.
7. Built-in middleware parity is either implemented (Phase 9 done) or explicitly deferred as follow-on.
