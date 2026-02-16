import inspect
from typing import Any

import pytest

from bubus.base_event import BaseEvent
from bubus.event_bus import EventBus
from bubus.event_handler import EventHandler


@pytest.mark.asyncio
async def test_on_stores_eventhandler_entry_and_index() -> None:
    bus = EventBus(name='RegistryBus')

    def handler(event: BaseEvent[Any]) -> str:
        return event.event_type

    entry = bus.on('RegistryEvent', handler)

    assert isinstance(entry, EventHandler)
    assert entry.id is not None
    assert entry.id in bus.handlers
    assert bus.handlers[entry.id] is entry
    assert 'RegistryEvent' in bus.handlers_by_key
    assert entry.id in bus.handlers_by_key['RegistryEvent']

    dispatched = bus.dispatch(BaseEvent(event_type='RegistryEvent'))
    completed = await dispatched
    assert entry.id in completed.event_results
    assert completed.event_results[entry.id].handler.id == entry.id

    await bus.stop(clear=True)


@pytest.mark.asyncio
async def test_off_removes_by_callable_id_entry_or_all() -> None:
    bus = EventBus(name='RegistryOffBus')

    def handler_a(event: BaseEvent[Any]) -> None:
        return None

    def handler_b(event: BaseEvent[Any]) -> None:
        return None

    def handler_c(event: BaseEvent[Any]) -> None:
        return None

    entry_a = bus.on('RegistryEvent', handler_a)
    entry_b = bus.on('RegistryEvent', handler_b)
    entry_c = bus.on('RegistryEvent', handler_c)
    assert entry_a.id and entry_b.id and entry_c.id

    bus.off('RegistryEvent', handler_a)
    assert entry_a.id not in bus.handlers
    assert entry_a.id not in bus.handlers_by_key['RegistryEvent']
    assert entry_b.id in bus.handlers

    bus.off('RegistryEvent', entry_b.id)
    assert entry_b.id not in bus.handlers
    assert entry_b.id not in bus.handlers_by_key['RegistryEvent']
    assert entry_c.id in bus.handlers

    bus.off('RegistryEvent', entry_c)
    assert entry_c.id not in bus.handlers
    assert 'RegistryEvent' not in bus.handlers_by_key

    bus.on('RegistryEvent', handler_a)
    bus.on('RegistryEvent', handler_b)
    bus.off('RegistryEvent')
    assert 'RegistryEvent' not in bus.handlers_by_key
    assert all(entry.event_pattern != 'RegistryEvent' for entry in bus.handlers.values())

    await bus.stop(clear=True)


@pytest.mark.asyncio
async def test_on_normalizes_sync_handler_to_async_callable() -> None:
    bus = EventBus(name='RegistryNormalizeBus')

    class RegistryNormalizeEvent(BaseEvent[str]):
        event_timeout: float | None = 0.2

    calls: list[str] = []

    def sync_handler(event: RegistryNormalizeEvent) -> str:
        calls.append(event.event_id)
        return 'normalized'

    entry = bus.on(RegistryNormalizeEvent, sync_handler)

    assert entry.handler is sync_handler
    assert entry.handler_async is not None
    assert inspect.iscoroutinefunction(entry.handler_async)
    assert entry.handler_name.endswith('sync_handler')

    direct_result = await entry.handler_async(RegistryNormalizeEvent())
    assert direct_result == 'normalized'

    dispatched = bus.dispatch(RegistryNormalizeEvent())
    completed = await dispatched
    result = completed.event_results[entry.id]

    assert result.status == 'completed'
    assert result.result == 'normalized'
    assert len(calls) == 2

    await bus.stop(clear=True)
