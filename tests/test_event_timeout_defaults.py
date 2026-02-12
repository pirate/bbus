import asyncio
import logging

import pytest

from bubus import BaseEvent, EventBus


class TimeoutDefaultsEvent(BaseEvent[str]):
    pass


@pytest.mark.asyncio
async def test_dispatch_copies_bus_timeout_defaults_to_event_fields() -> None:
    bus = EventBus(
        name='TimeoutDefaultsCopyBus',
        event_timeout=12.0,
        event_slow_timeout=34.0,
        event_handler_slow_timeout=56.0,
    )

    async def handler(_event: TimeoutDefaultsEvent) -> str:
        return 'ok'

    bus.on(TimeoutDefaultsEvent, handler)

    try:
        event = bus.dispatch(TimeoutDefaultsEvent())
        assert event.event_timeout == 12.0
        assert event.event_handler_timeout is None
        assert event.event_handler_slow_timeout == 56.0
        assert getattr(event, 'event_slow_timeout', None) == 34.0
        await event
    finally:
        await bus.stop()


@pytest.mark.asyncio
async def test_handler_timeout_resolution_matches_ts_precedence() -> None:
    bus = EventBus(name='TimeoutPrecedenceBus', event_timeout=0.2)

    async def default_handler(_event: TimeoutDefaultsEvent) -> str:
        await asyncio.sleep(0.001)
        return 'default'

    async def overridden_handler(_event: TimeoutDefaultsEvent) -> str:
        await asyncio.sleep(0.001)
        return 'override'

    bus.on(TimeoutDefaultsEvent, default_handler)
    overridden_entry = bus.on(TimeoutDefaultsEvent, overridden_handler)
    overridden_entry.handler_timeout = 0.12

    try:
        event = await bus.dispatch(TimeoutDefaultsEvent(event_timeout=0.2, event_handler_timeout=0.05))

        default_result = next(
            result for result in event.event_results.values() if result.handler_name.endswith('default_handler')
        )
        overridden_result = next(
            result for result in event.event_results.values() if result.handler_name.endswith('overridden_handler')
        )

        assert default_result.timeout is not None and abs(default_result.timeout - 0.05) < 1e-9
        assert overridden_result.timeout is not None and abs(overridden_result.timeout - 0.12) < 1e-9

        tighter_event_timeout = await bus.dispatch(TimeoutDefaultsEvent(event_timeout=0.08, event_handler_timeout=0.2))
        tighter_default = next(
            result for result in tighter_event_timeout.event_results.values() if result.handler_name.endswith('default_handler')
        )
        tighter_overridden = next(
            result
            for result in tighter_event_timeout.event_results.values()
            if result.handler_name.endswith('overridden_handler')
        )

        assert tighter_default.timeout is not None and abs(tighter_default.timeout - 0.08) < 1e-9
        assert tighter_overridden.timeout is not None and abs(tighter_overridden.timeout - 0.08) < 1e-9
    finally:
        await bus.stop()


@pytest.mark.asyncio
async def test_event_handler_detect_file_paths_toggle() -> None:
    bus = EventBus(name='NoDetectPathsBus', event_handler_detect_file_paths=False)

    async def handler(_event: TimeoutDefaultsEvent) -> str:
        return 'ok'

    try:
        entry = bus.on(TimeoutDefaultsEvent, handler)
        assert entry.handler_file_path is None
    finally:
        await bus.stop()


@pytest.mark.asyncio
async def test_handler_slow_warning_uses_event_handler_slow_timeout(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger='bubus')
    bus = EventBus(
        name='SlowHandlerWarnBus',
        event_timeout=0.5,
        event_slow_timeout=None,
        event_handler_slow_timeout=0.01,
    )

    async def slow_handler(_event: TimeoutDefaultsEvent) -> str:
        await asyncio.sleep(0.03)
        return 'ok'

    bus.on(TimeoutDefaultsEvent, slow_handler)

    try:
        await bus.dispatch(TimeoutDefaultsEvent())
        assert any('Slow event handler:' in record.message for record in caplog.records)
    finally:
        await bus.stop()


@pytest.mark.asyncio
async def test_event_slow_warning_uses_event_slow_timeout(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger='bubus')
    bus = EventBus(
        name='SlowEventWarnBus',
        event_timeout=0.5,
        event_slow_timeout=0.01,
        event_handler_slow_timeout=None,
    )

    async def slow_event_handler(_event: TimeoutDefaultsEvent) -> str:
        await asyncio.sleep(0.03)
        return 'ok'

    bus.on(TimeoutDefaultsEvent, slow_event_handler)

    try:
        await bus.dispatch(TimeoutDefaultsEvent())
        assert any('Slow event processing:' in record.message for record in caplog.records)
    finally:
        await bus.stop()
