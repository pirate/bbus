import asyncio
import gc
import os
import time
from typing import Any

import psutil
import pytest

from bubus import BaseEvent, EventBus


def get_memory_usage_mb():
    """Get current process memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


class SimpleEvent(BaseEvent):
    """Simple event without Generic for performance testing"""

    pass


@pytest.mark.asyncio
async def test_20k_events_with_memory_control():
    """Test processing 20k events with no memory leaks"""

    # Record initial memory
    gc.collect()
    initial_memory = get_memory_usage_mb()
    print(f'\nInitial memory: {initial_memory:.1f} MB')

    # Create EventBus with proper limits (now default)
    bus = EventBus(name='ManyEvents', middlewares=[])

    print('EventBus settings:')
    print(f'  max_history_size: {bus.max_history_size}')
    print(f'  queue maxsize: {bus.event_queue.maxsize if bus.event_queue else "not created"}')
    print('Starting event dispatch...')

    processed_count = 0

    async def handler(event: SimpleEvent) -> None:
        nonlocal processed_count
        processed_count += 1

    bus.on('SimpleEvent', handler)

    total_events = 20_000  # Reduced for faster tests

    start_time = time.time()
    memory_samples: list[float] = []
    max_memory = initial_memory

    # Dispatch all events as fast as possible
    dispatched = 0
    pending_events: list[BaseEvent[Any]] = []

    while dispatched < total_events:
        try:
            event = bus.dispatch(SimpleEvent())
            pending_events.append(event)
            dispatched += 1
            if dispatched <= 5:
                print(f'Dispatched event {dispatched}')
        except RuntimeError as e:
            if 'EventBus at capacity' in str(e):
                # Queue is full, complete the oldest pending events to make room
                # Complete first 10 events to free up space
                if pending_events:
                    to_complete = pending_events[:10]
                    await asyncio.gather(*to_complete)
                    pending_events = pending_events[10:]
            else:
                raise

        # Sample memory every 10k events
        if dispatched % 10_000 == 0 and dispatched > 0:
            gc.collect()
            current_memory = get_memory_usage_mb()
            memory_samples.append(current_memory)
            max_memory = max(max_memory, current_memory)
            elapsed = time.time() - start_time
            rate = dispatched / elapsed
            print(
                f'Progress: {dispatched:,} events, '
                f'Memory: {current_memory:.1f} MB (+{current_memory - initial_memory:.1f} MB), '
                f'History: {len(bus.event_history)}, '
                f'Rate: {rate:.0f} events/sec'
            )

    # Wait for all remaining events to complete
    if pending_events:
        await asyncio.gather(*pending_events)

    # Final wait
    await bus.wait_until_idle()

    duration = time.time() - start_time

    # Final memory check
    gc.collect()
    final_memory = get_memory_usage_mb()
    memory_growth = final_memory - initial_memory
    peak_growth = max_memory - initial_memory

    print('\nFinal Results:')
    print(f'Processed: {processed_count:,} events')
    print(f'Duration: {duration:.2f} seconds')
    print(f'Rate: {processed_count / duration:,.0f} events/sec')
    print(f'Initial memory: {initial_memory:.1f} MB')
    print(f'Peak memory: {max_memory:.1f} MB (+{peak_growth:.1f} MB)')
    print(f'Final memory: {final_memory:.1f} MB (+{memory_growth:.1f} MB)')

    # Debug: Check if event loop is still processing
    print(f'DEBUG: Bus is running: {bus._is_running}')  # type: ignore
    print(f'DEBUG: Runloop task: {bus._runloop_task}')  # type: ignore
    if bus._runloop_task:  # type: ignore
        print(f'DEBUG: Runloop task done: {bus._runloop_task.done()}')  # type: ignore

    # Safely get event history size without iterating
    try:
        history_size = len(bus.event_history)
        print(f'Event history size: {history_size} (capped at {bus.max_history_size})')
    except Exception as e:
        print(f'ERROR getting event history size: {type(e).__name__}: {e}')

    # Verify results
    print('DEBUG: About to check processed_count assertion...')
    assert processed_count == total_events, f'Only processed {processed_count} of {total_events}'
    print('DEBUG: About to check duration assertion...')
    assert duration < 120, f'Took {duration:.2f}s, should be < 120s'  # Allow more time for CI

    # Check memory usage stayed reasonable
    print('DEBUG: About to check memory assertion...')
    assert peak_growth < 100, f'Memory grew by {peak_growth:.1f} MB at peak, indicates memory leak'

    # Check event history is properly limited
    print('DEBUG: About to check history size assertions...')
    assert bus.max_history_size is not None
    assert len(bus.event_history) <= bus.max_history_size, (
        f'Event history has {len(bus.event_history)} events, should be <= {bus.max_history_size}'
    )

    # Explicitly clean up the bus to prevent hanging
    print('\nCleaning up EventBus...')
    print(f'Before stop - Running: {bus._is_running}')  # type: ignore
    print(f'Before stop - Runloop task: {bus._runloop_task}')  # type: ignore
    if bus._runloop_task:  # type: ignore
        print(f'  - Done: {bus._runloop_task.done()}')  # type: ignore
        print(f'  - Cancelled: {bus._runloop_task.cancelled()}')  # type: ignore

    await bus.stop(timeout=0, clear=True)
    print('EventBus stopped successfully')


@pytest.mark.asyncio
async def test_hard_limit_enforcement():
    """Test that hard limit of 100 pending events is enforced"""
    bus = EventBus(name='HardLimitTest', middlewares=[])

    try:
        # Create a slow handler to keep events pending
        async def slow_handler(event: SimpleEvent) -> None:
            await asyncio.sleep(0.5)  # Reduced from 10s to 0.5s

        bus.on('SimpleEvent', slow_handler)

        # Try to dispatch more than the pending limit
        events_dispatched = 0
        errors = 0

        for _ in range(200):
            try:
                bus.dispatch(SimpleEvent())
                events_dispatched += 1
            except RuntimeError as e:
                if 'EventBus at capacity' in str(e):
                    errors += 1
                else:
                    raise

        print(f'\nDispatched {events_dispatched} events')
        print(f'Hit capacity error {errors} times')

        # Should hit the limit
        assert bus.max_history_size is not None
        assert events_dispatched <= bus.max_history_size
        assert errors > 0

    finally:
        # Properly stop the bus to clean up pending tasks
        await bus.stop(timeout=0, clear=True)  # Don't wait, just force cleanup


@pytest.mark.asyncio
async def test_cleanup_prioritizes_pending():
    """Test that cleanup keeps pending events and removes completed ones"""
    bus = EventBus(name='CleanupTest', max_history_size=10, middlewares=[])

    try:
        # Process some events to completion
        completed_events: list[BaseEvent[Any]] = []
        for _ in range(5):
            event = bus.dispatch(BaseEvent(event_type='QuickEvent'))
            completed_events.append(event)

        await asyncio.gather(*completed_events)

        # Add pending events with slow handler (reduced sleep time)
        async def slow_handler(event: BaseEvent) -> None:
            if event.event_type == 'SlowEvent':
                await asyncio.sleep(0.5)  # Reduced from 10s to 0.5s

        bus.on('*', slow_handler)

        pending_events: list[BaseEvent[Any]] = []
        for _ in range(10):
            event = bus.dispatch(BaseEvent(event_type='SlowEvent'))
            pending_events.append(event)

        # Give them time to start
        await asyncio.sleep(0.1)

        # Check history - should prioritize keeping pending events
        history_types: dict[str, int] = {}
        for event in bus.event_history.values():
            status = event.event_status
            history_types[status] = history_types.get(status, 0) + 1

        print('\nHistory after cleanup:')
        print(f'  Total: {len(bus.event_history)} (max: {bus.max_history_size})')
        print(f'  By status: {history_types}')

        # Should have removed completed events to make room for pending
        assert bus.max_history_size is not None
        assert len(bus.event_history) <= bus.max_history_size * 1.2  # allow for some overhead to avoid frequent gc pausing
        assert history_types.get('pending', 0) + history_types.get('started', 0) >= 5

    finally:
        # Properly stop the bus to clean up pending tasks
        await bus.stop(timeout=0, clear=True)  # Don't wait, just force cleanup


@pytest.mark.asyncio
async def test_ephemeral_buses_with_forwarding_churn():
    """
    Closest Python equivalent to request-scoped bus churn:
    create short-lived buses, forward between them, process events, then clear.
    """
    total_bus_pairs = 60
    events_per_pair = 20
    total_events = total_bus_pairs * events_per_pair
    initial_instances = len(EventBus.all_instances)

    handled_a = 0
    handled_b = 0

    start = time.time()

    for idx in range(total_bus_pairs):
        bus_a = EventBus(name=f'EphemeralA_{idx}_{os.getpid()}', middlewares=[])
        bus_b = EventBus(name=f'EphemeralB_{idx}_{os.getpid()}', middlewares=[])

        async def handler_a(event: SimpleEvent) -> None:
            nonlocal handled_a
            handled_a += 1

        async def handler_b(event: SimpleEvent) -> None:
            nonlocal handled_b
            handled_b += 1

        bus_a.on(SimpleEvent, handler_a)
        bus_b.on(SimpleEvent, handler_b)
        bus_a.on('*', bus_b.dispatch)

        try:
            pending = [bus_a.dispatch(SimpleEvent()) for _ in range(events_per_pair)]
            await asyncio.gather(*pending)
            await bus_a.wait_until_idle()
            await bus_b.wait_until_idle()

            assert bus_a.max_history_size is None or len(bus_a.event_history) <= bus_a.max_history_size
            assert bus_b.max_history_size is None or len(bus_b.event_history) <= bus_b.max_history_size
        finally:
            await bus_a.stop(timeout=0, clear=True)
            await bus_b.stop(timeout=0, clear=True)

    duration = time.time() - start
    gc.collect()

    assert handled_a == total_events
    assert handled_b == total_events
    assert len(EventBus.all_instances) <= initial_instances
    assert duration < 60, f'Ephemeral bus churn took too long: {duration:.2f}s'


@pytest.mark.asyncio
async def test_forwarding_queue_jump_timeout_mix_stays_stable():
    """
    Stress a mixed path in Python:
    parent handler awaits forwarded child events, with intermittent child timeouts.
    """
    class MixedParentEvent(BaseEvent):
        iteration: int = 0
        event_timeout: float | None = 0.2

    class MixedChildEvent(BaseEvent):
        iteration: int = 0
        event_timeout: float | None = 0.05

    history_limit = 500
    total_iterations = 300

    bus_a = EventBus(name='MixedPathA', max_history_size=history_limit, middlewares=[])
    bus_b = EventBus(name='MixedPathB', max_history_size=history_limit, middlewares=[])

    parent_handled = 0
    child_handled = 0
    child_events: list[MixedChildEvent] = []

    async def child_handler(event: MixedChildEvent) -> str:
        nonlocal child_handled
        child_handled += 1
        if event.iteration % 7 == 0:
            await asyncio.sleep(0.01)
        else:
            await asyncio.sleep(0.0005)
        return 'child_done'

    async def parent_handler(event: MixedParentEvent) -> str:
        nonlocal parent_handled
        parent_handled += 1

        child_timeout = 0.001 if event.iteration % 7 == 0 else 0.05
        child = bus_a.dispatch(MixedChildEvent(iteration=event.iteration, event_timeout=child_timeout))
        bus_b.dispatch(child)
        child_events.append(child)
        await child
        return 'parent_done'

    bus_a.on(MixedParentEvent, parent_handler)
    bus_b.on(MixedChildEvent, child_handler)

    start = time.time()
    try:
        for i in range(total_iterations):
            await bus_a.dispatch(MixedParentEvent(iteration=i))

        await bus_a.wait_until_idle()
        await bus_b.wait_until_idle()
    finally:
        await bus_a.stop(timeout=0, clear=True)
        await bus_b.stop(timeout=0, clear=True)

    duration = time.time() - start

    assert parent_handled == total_iterations
    assert child_handled == total_iterations
    timeout_count = sum(
        1
        for child in child_events
        if any(isinstance(result.error, TimeoutError) for result in child.event_results.values())
    )
    assert timeout_count > 0
    assert len(bus_a.event_history) <= history_limit
    assert len(bus_b.event_history) <= history_limit
    assert duration < 60, f'Mixed forwarding/queue-jump/timeout path took too long: {duration:.2f}s'


@pytest.mark.asyncio
async def test_history_bound_is_strict_after_idle():
    """After steady-state processing, history should stay within max_history_size."""
    bus = EventBus(name='StrictHistoryBound', max_history_size=25, middlewares=[])

    async def handler(event: SimpleEvent) -> None:
        return None

    bus.on(SimpleEvent, handler)

    try:
        for _ in range(200):
            await bus.dispatch(SimpleEvent())

        await bus.wait_until_idle()
        assert len(bus.event_history) <= 25
    finally:
        await bus.stop(timeout=0, clear=True)


@pytest.mark.asyncio
async def test_basic_throughput_floor_regression_guard():
    """
    Throughput regression guard (Python-specific floor).
    Keeps threshold conservative to avoid CI flakiness while still catching
    severe slowdowns.
    """
    bus = EventBus(name='ThroughputFloor', middlewares=[])

    processed = 0

    async def handler(event: SimpleEvent) -> None:
        nonlocal processed
        processed += 1

    bus.on(SimpleEvent, handler)

    total_events = 5_000
    batch_size = 50
    pending: list[BaseEvent[Any]] = []

    start = time.time()
    try:
        for _ in range(total_events):
            pending.append(bus.dispatch(SimpleEvent()))
            if len(pending) >= batch_size:
                await asyncio.gather(*pending)
                pending.clear()

        if pending:
            await asyncio.gather(*pending)

        await bus.wait_until_idle()
    finally:
        await bus.stop(timeout=0, clear=True)

    duration = time.time() - start
    rate = total_events / duration

    assert processed == total_events
    assert rate >= 600, f'Throughput regression: {rate:.0f} events/sec (expected >= 600 events/sec)'
