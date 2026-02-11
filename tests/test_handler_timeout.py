"""Test for per-handler timeout enforcement matching the exact scenario from the issue"""

import asyncio

import pytest

from bubus import BaseEvent, EventBus


# Event definitions
class TopmostEvent(BaseEvent[str]):
    """Event for navigating to a URL"""

    url: str = 'https://example.com'

    event_timeout: float | None = 5.0


class ChildEvent(BaseEvent[str]):
    """Event for tab creation"""

    tab_id: str = 'tab-123'

    event_timeout: float | None = 2


class GrandchildEvent(BaseEvent[str]):
    """Event for navigation completion"""

    success: bool = True

    event_timeout: float | None = 1


# Watchdog classes
class HandlerClass1:
    async def on_TopmostEvent(self, event: TopmostEvent) -> str:
        """Completes quickly - 1 second"""
        await asyncio.sleep(0.1)
        return 'HandlerClass1.on_TopmostEvent completed after 0.1s'

    async def on_GrandchildEvent(self, event: GrandchildEvent) -> str:
        """Starts but gets interrupted after 1 second by parent timeout"""
        await asyncio.sleep(5)  # Would take 5 seconds but will be interrupted
        return 'HandlerClass1.on_GrandchildEvent completed after 5s'


class HandlerClass2:
    async def on_GrandchildEvent(self, event: GrandchildEvent) -> str:
        """Completes instantly"""
        # No sleep - completes immediately
        return 'HandlerClass2.on_GrandchildEvent completed immediately'


class HandlerClass3:
    async def on_GrandchildEvent(self, event: GrandchildEvent) -> str:
        """Never gets to run - pending when timeout occurs"""
        await asyncio.sleep(0.2)
        return 'HandlerClass3.on_GrandchildEvent completed after 0.2s'


class HandlerClass4:
    async def on_GrandchildEvent(self, event: GrandchildEvent) -> str:
        """Never gets to run - pending when timeout occurs"""
        await asyncio.sleep(0.1)
        return 'HandlerClass4.on_GrandchildEvent completed after 0.1s'


class MainClass0:
    def __init__(self, bus: EventBus):
        self.bus = bus

    async def on_TopmostEvent(self, event: TopmostEvent) -> str:
        """Takes 11 seconds total - dispatches ChildEvent"""
        # Do some work
        await asyncio.sleep(1)

        # Dispatch and wait for ChildEvent
        child_event = self.bus.dispatch(ChildEvent())
        try:
            await child_event  # This will timeout after 10s
        except Exception as e:
            print(f'DEBUG: Parent caught child error: {type(e).__name__}: {e}')

            import threading

            all_tasks = asyncio.all_tasks()
            print(f'\nOutstanding asyncio tasks ({len(all_tasks)}):')
            for task in all_tasks:
                print(f'  - {task.get_name()}: {task._state} - {task.get_coro()}')

            # List all threads
            all_threads = threading.enumerate()
            print(f'\nActive threads ({len(all_threads)}):')
            for thread in all_threads:
                print(f'  - {thread.name}: {thread.is_alive()}')

            raise

        # Would continue but won't get here due to timeout
        return 'MainClass0.on_TopmostEvent completed after all child events'

    async def on_ChildEvent(self, event: ChildEvent) -> str:
        """Takes 10 seconds - will timeout, dispatches GrandchildEvent"""
        # Dispatch GrandchildEvent immediately
        grandchild_event = self.bus.dispatch(GrandchildEvent())

        # Wait for GrandchildEvent to complete
        # This will take 9s (MainClass0) + 0s (AboutBlank) + partial HandlerClass1 time
        # Since handlers run serially and we have a 10s timeout, we'll timeout while
        # HandlerClass1 is still running (after about 1s of its 5s execution)
        await grandchild_event  # .event_result(raise_if_any=False, raise_if_none=True, timeout=15)

        # Would continue but we timeout first
        return 'MainClass0.on_ChildEvent completed after GrandchildEvent() finished processing'

    async def on_GrandchildEvent(self, event: GrandchildEvent) -> str:
        """Completes in 5 seconds"""
        # print('GRANDCHILD EVENT HANDLING STARTED')
        await asyncio.sleep(2)
        return 'MainClass0.on_GrandchildEvent completed after 2s'


@pytest.mark.asyncio
async def test_nested_timeout_scenario_from_issue():
    """Test the exact timeout scenario described in the issue

    This tests:
    1. TopmostEvent with 30s timeout dispatches ChildEvent
    2. ChildEvent with 10s timeout times out after 10s
    3. GrandchildEvent is dispatched from ChildEvent handler
    4. Some handlers complete, some are interrupted, some never run
    5. The timeout tree logging shows the complete hierarchy
    """
    # Create single event bus
    bus = EventBus(name='MainClass0EventBus')

    # Create instances
    handlerclass1 = HandlerClass1()
    handlerclass2 = HandlerClass2()
    handlerclass3 = HandlerClass3()
    handlerclass4 = HandlerClass4()
    mainclass0 = MainClass0(bus)

    # Register handlers for TopmostEvent
    bus.on('TopmostEvent', handlerclass1.on_TopmostEvent)
    bus.on('TopmostEvent', mainclass0.on_TopmostEvent)

    # Register handlers for ChildEvent
    bus.on('ChildEvent', mainclass0.on_ChildEvent)

    # Register handlers for GrandchildEvent (order matters for the test)
    bus.on('GrandchildEvent', mainclass0.on_GrandchildEvent)
    bus.on('GrandchildEvent', handlerclass2.on_GrandchildEvent)
    bus.on('GrandchildEvent', handlerclass1.on_GrandchildEvent)
    bus.on('GrandchildEvent', handlerclass3.on_GrandchildEvent)
    bus.on('GrandchildEvent', handlerclass4.on_GrandchildEvent)

    # Dispatch the root event
    navigate_event = bus.dispatch(TopmostEvent())

    # Wait for it to complete (will fail due to timeout)
    # with pytest.raises((RuntimeError, TimeoutError)) as exc_info:
    try:
        await (
            navigate_event
        )  # .event_result(raise_if_any=True, raise_if_none=True, timeout=20)  # The event should complete with an error
    except Exception as e:
        print(f'Exception caught: {type(e).__name__}: {e}')
        raise

    # import ipdb; ipdb.set_trace()

    # print('-----------------------------------------------------')
    # print(f"Exception caught: {type(exc_info.value).__name__}: {exc_info.value}")
    # # assert 'ChildEvent' in str(exc_info.value) or 'ChildEvent' in str(exc_info.value)

    await bus.stop(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_handler_timeout_marks_error_and_other_handlers_still_complete():
    """Focused timeout behavior: one handler times out, another still completes."""
    bus = EventBus(name='TimeoutFocusedBus')

    class TimeoutFocusedEvent(BaseEvent[str]):
        event_timeout: float | None = 0.01

    execution_order: list[str] = []

    async def slow_handler(event: TimeoutFocusedEvent) -> str:
        execution_order.append('slow_start')
        await asyncio.sleep(0.05)
        execution_order.append('slow_end')
        return 'slow'

    async def fast_handler(event: TimeoutFocusedEvent) -> str:
        execution_order.append('fast_start')
        return 'fast'

    bus.on(TimeoutFocusedEvent, slow_handler)
    bus.on(TimeoutFocusedEvent, fast_handler)

    try:
        event = await bus.dispatch(TimeoutFocusedEvent())
        await bus.wait_until_idle()

        slow_result = next((r for r in event.event_results.values() if r.handler_name.endswith('slow_handler')), None)
        fast_result = next((r for r in event.event_results.values() if r.handler_name.endswith('fast_handler')), None)

        assert slow_result is not None
        assert slow_result.status == 'error'
        assert isinstance(slow_result.error, TimeoutError)

        assert fast_result is not None
        assert fast_result.status == 'completed'
        assert fast_result.result == 'fast'
        assert 'fast_start' in execution_order
    finally:
        await bus.stop(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_multi_bus_timeout_is_recorded_on_target_bus():
    """Closest Python equivalent: same event dispatched to two buses, timeout on target bus is captured."""
    bus_a = EventBus(name='MultiTimeoutA')
    bus_b = EventBus(name='MultiTimeoutB')

    class MultiBusTimeoutEvent(BaseEvent[str]):
        event_timeout: float | None = 0.01

    async def slow_target_handler(event: MultiBusTimeoutEvent) -> str:
        await asyncio.sleep(0.05)
        return 'slow'

    bus_b.on(MultiBusTimeoutEvent, slow_target_handler)

    try:
        event = MultiBusTimeoutEvent()
        bus_a.dispatch(event)
        bus_b.dispatch(event)
        await bus_b.wait_until_idle()

        bus_b_result = next((r for r in event.event_results.values() if r.eventbus_name == bus_b.name), None)
        assert bus_b_result is not None
        assert bus_b_result.status == 'error'
        assert isinstance(bus_b_result.error, TimeoutError)
        assert event.event_path == [bus_a.label, bus_b.label]
    finally:
        await bus_a.stop(clear=True, timeout=0)
        await bus_b.stop(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_followup_event_runs_after_parent_timeout_in_queue_jump_path():
    """
    Regression guard: timeout in a handler that awaited a child event should not
    stall subsequent events on the same bus.
    """
    bus = EventBus(name='TimeoutQueueJumpFollowupBus')

    class ParentEvent(BaseEvent[str]):
        event_timeout: float | None = 0.02

    class ChildEvent(BaseEvent[str]):
        event_timeout: float | None = 0.2

    class TailEvent(BaseEvent[str]):
        event_timeout: float | None = 0.2

    tail_runs = 0

    async def child_handler(event: ChildEvent) -> str:
        await asyncio.sleep(0.001)
        return 'child_done'

    async def parent_handler(event: ParentEvent) -> str:
        child = bus.dispatch(ChildEvent())
        await child
        await asyncio.sleep(0.05)  # Exceeds parent timeout
        return 'parent_done'

    async def tail_handler(event: TailEvent) -> str:
        nonlocal tail_runs
        tail_runs += 1
        return 'tail_done'

    bus.on(ParentEvent, parent_handler)
    bus.on(ChildEvent, child_handler)
    bus.on(TailEvent, tail_handler)

    try:
        parent = await bus.dispatch(ParentEvent())
        await bus.wait_until_idle()

        parent_result = next(iter(parent.event_results.values()))
        assert parent_result.status == 'error'
        assert isinstance(parent_result.error, TimeoutError)

        tail = bus.dispatch(TailEvent())
        completed_tail = await asyncio.wait_for(tail, timeout=1.0)
        assert completed_tail.event_status == 'completed'
        assert tail_runs == 1
    finally:
        await bus.stop(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_forwarded_timeout_path_does_not_stall_followup_events():
    """
    Regression guard: if a forwarded awaited child times out, subsequent events
    should still run on both source and target buses.
    """
    bus_a = EventBus(name='TimeoutForwardA')
    bus_b = EventBus(name='TimeoutForwardB')

    class ParentEvent(BaseEvent[str]):
        event_timeout: float | None = 0.02

    class ChildEvent(BaseEvent[str]):
        event_timeout: float | None = 0.01

    class TailEvent(BaseEvent[str]):
        event_timeout: float | None = 0.2

    bus_a_tail_runs = 0
    bus_b_tail_runs = 0
    child_ref: ChildEvent | None = None

    async def parent_handler(event: ParentEvent) -> str:
        nonlocal child_ref
        child = bus_a.dispatch(ChildEvent())
        child_ref = child
        await child
        return 'parent_done'

    async def slow_child_handler(event: ChildEvent) -> str:
        await asyncio.sleep(0.05)  # Guaranteed timeout on child.
        return 'child_done'

    async def tail_handler_a(event: TailEvent) -> str:
        nonlocal bus_a_tail_runs
        bus_a_tail_runs += 1
        return 'tail_a'

    async def tail_handler_b(event: TailEvent) -> str:
        nonlocal bus_b_tail_runs
        bus_b_tail_runs += 1
        return 'tail_b'

    bus_a.on(ParentEvent, parent_handler)
    bus_a.on(TailEvent, tail_handler_a)
    bus_a.on('*', bus_b.dispatch)
    bus_b.on(ChildEvent, slow_child_handler)
    bus_b.on(TailEvent, tail_handler_b)

    try:
        parent = await bus_a.dispatch(ParentEvent())
        await bus_a.wait_until_idle()
        await bus_b.wait_until_idle()

        parent_result = next(iter(parent.event_results.values()))
        assert parent_result.status == 'completed'

        assert child_ref is not None
        assert any(isinstance(result.error, TimeoutError) for result in child_ref.event_results.values()), child_ref.event_results

        # Lock/queue state should remain healthy after timeout.
        tail = bus_a.dispatch(TailEvent())
        completed_tail = await asyncio.wait_for(tail, timeout=1.0)
        await bus_a.wait_until_idle()
        await bus_b.wait_until_idle()

        assert completed_tail.event_status == 'completed'
        assert bus_a_tail_runs == 1
        assert bus_b_tail_runs == 1
    finally:
        await bus_a.stop(clear=True, timeout=0)
        await bus_b.stop(clear=True, timeout=0)
