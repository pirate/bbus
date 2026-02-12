import asyncio

from bubus import BaseEvent, EventBus


class CompletionEvent(BaseEvent[str]):
    pass


class IntCompletionEvent(BaseEvent[int]):
    pass


async def test_event_handler_completion_bus_default_first_serial() -> None:
    bus = EventBus(name='CompletionDefaultFirstBus', event_handler_concurrency='serial', event_handler_completion='first')
    second_handler_called = False

    async def first_handler(_event: CompletionEvent) -> str:
        return 'first'

    async def second_handler(_event: CompletionEvent) -> str:
        nonlocal second_handler_called
        second_handler_called = True
        return 'second'

    bus.on(CompletionEvent, first_handler)
    bus.on(CompletionEvent, second_handler)

    try:
        event = bus.dispatch(CompletionEvent())
        assert event.event_handler_completion == 'first'

        await event
        assert second_handler_called is False

        result = await event.event_result(raise_if_any=False, raise_if_none=False)
        assert result == 'first'

        first_result = next(result for result in event.event_results.values() if result.handler_name.endswith('first_handler'))
        second_result = next(result for result in event.event_results.values() if result.handler_name.endswith('second_handler'))
        assert first_result.status == 'completed'
        assert second_result.status == 'error'
        assert isinstance(second_result.error, asyncio.CancelledError)
    finally:
        await bus.stop()


async def test_event_handler_completion_explicit_override_beats_bus_default() -> None:
    bus = EventBus(name='CompletionOverrideBus', event_handler_concurrency='serial', event_handler_completion='first')
    second_handler_called = False

    async def first_handler(_event: CompletionEvent) -> str:
        return 'first'

    async def second_handler(_event: CompletionEvent) -> str:
        nonlocal second_handler_called
        second_handler_called = True
        return 'second'

    bus.on(CompletionEvent, first_handler)
    bus.on(CompletionEvent, second_handler)

    try:
        event = bus.dispatch(CompletionEvent(event_handler_completion='all'))
        assert event.event_handler_completion == 'all'
        await event
        assert second_handler_called is True
    finally:
        await bus.stop()


async def test_event_parallel_first_races_and_cancels_non_winners() -> None:
    bus = EventBus(name='CompletionParallelFirstBus', event_handler_concurrency='serial', event_handler_completion='all')
    slow_started = False

    async def slow_handler_started(_event: CompletionEvent) -> str:
        nonlocal slow_started
        slow_started = True
        await asyncio.sleep(0.5)
        return 'slow-started'

    async def fast_winner(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.01)
        return 'winner'

    async def slow_handler_pending_or_started(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.5)
        return 'slow-other'

    bus.on(CompletionEvent, slow_handler_started)
    bus.on(CompletionEvent, fast_winner)
    bus.on(CompletionEvent, slow_handler_pending_or_started)

    try:
        event = bus.dispatch(CompletionEvent(event_handler_concurrency='parallel', event_handler_completion='first'))
        assert event.event_handler_concurrency == 'parallel'
        assert event.event_handler_completion == 'first'

        started = asyncio.get_running_loop().time()
        await event
        elapsed = asyncio.get_running_loop().time() - started
        assert elapsed < 0.2
        assert slow_started is True

        winner_result = next(result for result in event.event_results.values() if result.handler_name.endswith('fast_winner'))
        assert winner_result.status == 'completed'
        assert winner_result.error is None
        assert winner_result.result == 'winner'

        loser_results = [result for result in event.event_results.values() if not result.handler_name.endswith('fast_winner')]
        assert len(loser_results) == 2
        assert all(result.status == 'error' for result in loser_results)
        assert all(isinstance(result.error, asyncio.CancelledError) for result in loser_results)

        resolved = await event.event_result(raise_if_any=False, raise_if_none=True)
        assert resolved == 'winner'
    finally:
        await bus.stop()


async def test_event_first_shortcut_sets_mode_and_cancels_parallel_losers() -> None:
    bus = EventBus(name='CompletionFirstShortcutBus', event_handler_concurrency='parallel', event_handler_completion='all')
    slow_handler_completed = False

    async def fast_handler(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.01)
        return 'fast'

    async def slow_handler(_event: CompletionEvent) -> str:
        nonlocal slow_handler_completed
        await asyncio.sleep(0.5)
        slow_handler_completed = True
        return 'slow'

    bus.on(CompletionEvent, fast_handler)
    bus.on(CompletionEvent, slow_handler)

    try:
        event = bus.dispatch(CompletionEvent())
        assert event.event_handler_completion == 'all'

        first_value = await event.first()

        assert first_value == 'fast'
        assert event.event_handler_completion == 'first'
        assert slow_handler_completed is False

        error_results = [result for result in event.event_results.values() if result.status == 'error']
        assert error_results
        assert any(isinstance(result.error, asyncio.CancelledError) for result in error_results)
    finally:
        await bus.stop()


async def test_event_first_preserves_falsy_results() -> None:
    bus = EventBus(name='CompletionFalsyBus', event_handler_concurrency='serial', event_handler_completion='all')
    second_handler_called = False

    async def zero_handler(_event: IntCompletionEvent) -> int:
        return 0

    async def second_handler(_event: IntCompletionEvent) -> int:
        nonlocal second_handler_called
        second_handler_called = True
        return 99

    bus.on(IntCompletionEvent, zero_handler)
    bus.on(IntCompletionEvent, second_handler)

    try:
        event = bus.dispatch(IntCompletionEvent())
        result = await event.first()
        assert result == 0
        assert second_handler_called is False
    finally:
        await bus.stop()


async def test_event_first_returns_none_when_all_handlers_fail() -> None:
    bus = EventBus(name='CompletionAllFailBus', event_handler_concurrency='parallel')

    async def fail_fast(_event: CompletionEvent) -> str:
        raise RuntimeError('boom1')

    async def fail_slow(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.01)
        raise RuntimeError('boom2')

    bus.on(CompletionEvent, fail_fast)
    bus.on(CompletionEvent, fail_slow)

    try:
        event = bus.dispatch(CompletionEvent())
        result = await event.first()
        assert result is None
    finally:
        await bus.stop()
