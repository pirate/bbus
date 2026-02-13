from datetime import UTC, datetime
from typing import Any, cast
from uuid import NAMESPACE_DNS, uuid4, uuid5

import pytest

from bubus.event_bus import EventBus
from bubus.models import BaseEvent, EventHandler, EventHandlerCallable, EventResult


class _StubEvent:
    """Minimal event-like object used to verify EventResult independence."""

    def __init__(self):
        self.event_id = 'stub-event'
        self.event_children: list[BaseEvent | _StubEvent] = []
        self.event_result_type = str
        self.event_timeout = 0.5
        self.event_completed_at = None
        self.event_results: dict[str, EventResult] = {}
        self._cancelled_due_to_error: BaseException | None = None

    def event_cancel_pending_child_processing(self, error: BaseException) -> None:
        self._cancelled_due_to_error = error


@pytest.mark.asyncio
async def test_event_result_execute_without_base_event() -> None:
    """EventResult should execute without requiring a real BaseEvent or EventBus."""

    stub_event = _StubEvent()

    async def handler(event: _StubEvent) -> str:
        return 'ok'

    handler_entry = EventHandler.from_callable(
        handler=cast(EventHandlerCallable, handler),
        event_pattern='StubEvent',
        eventbus_name='Standalone',
        eventbus_id='standalone-1',
    )

    event_result = EventResult(
        event_id=str(uuid4()),
        handler=handler_entry,
        timeout=stub_event.event_timeout,
        result_type=str,
    )

    test_bus = EventBus(name='StandaloneTest1')
    result_value = await event_result.execute(
        cast(BaseEvent[Any], stub_event),
        eventbus=test_bus,
        timeout=stub_event.event_timeout,
    )

    assert result_value == 'ok'
    assert event_result.status == 'completed'
    assert event_result.result == 'ok'
    assert stub_event.__dict__.get('_cancelled_due_to_error') is None
    await test_bus.stop()


class StandaloneEvent(BaseEvent[str]):
    data: str


@pytest.mark.asyncio
async def test_event_and_result_without_eventbus() -> None:
    """Verify BaseEvent + EventResult work without instantiating an EventBus."""

    event = StandaloneEvent(data='message')

    def handler(evt: StandaloneEvent) -> str:
        return evt.data.upper()

    handler_entry = EventHandler.from_callable(
        handler=cast(EventHandlerCallable, handler),
        event_pattern='StandaloneEvent',
        eventbus_name='EventBus',
        eventbus_id='00000000-0000-0000-0000-000000000000',
    )
    assert handler_entry.id is not None
    handler_id = handler_entry.id
    pending_results = event.event_create_pending_results({handler_id: handler_entry})
    event_result = pending_results[handler_id]

    test_bus = EventBus(name='StandaloneTest2')
    value = await event_result.execute(
        event,
        eventbus=test_bus,
        timeout=event.event_timeout,
    )

    assert value == 'MESSAGE'
    assert event_result.status == 'completed'
    assert event.event_results[handler_id] is event_result

    event.event_mark_complete_if_all_handlers_completed()
    assert event.event_completed_at is not None
    await test_bus.stop()


def test_event_handler_model_is_serializable() -> None:
    """EventHandler is a Pydantic model and can round-trip serialized metadata."""

    def handler(event: StandaloneEvent) -> str:
        return event.data

    entry = EventHandler.from_callable(
        handler=cast(EventHandlerCallable, handler),
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
    )

    dumped = entry.model_dump(mode='json')
    assert dumped['event_pattern'] == 'StandaloneEvent'
    assert dumped['eventbus_name'] == 'StandaloneBus'
    assert dumped.get('handler') is None

    loaded = EventHandler.model_validate(dumped)
    assert loaded.id == entry.id
    assert loaded.event_pattern == entry.event_pattern
    assert loaded.handler is None


def test_event_handler_id_matches_ts_uuidv5_algorithm() -> None:
    registered_at = datetime(2025, 1, 2, 3, 4, 5, 678901, tzinfo=UTC)
    entry = EventHandler(
        handler_name='pkg.module.handler',
        handler_file_path='~/project/app.py:123',
        handler_registered_at=registered_at,
        handler_registered_ts=1735787045678901000,
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
    )

    namespace = uuid5(NAMESPACE_DNS, 'bubus-handler')
    expected_seed = (
        '018f8e40-1234-7000-8000-000000001234|pkg.module.handler|~/project/app.py:123|'
        '2025-01-02T03:04:05.678Z|1735787045678901000|StandaloneEvent'
    )
    expected_id = str(uuid5(namespace, expected_seed))

    assert entry.compute_handler_id() == expected_id
    assert entry.id == expected_id


def test_event_handler_model_detects_handler_file_path() -> None:
    def handler(event: StandaloneEvent) -> str:
        return event.data

    entry = EventHandler.from_callable(
        handler=cast(EventHandlerCallable, handler),
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
    )

    assert entry.handler_file_path is not None
    expected_suffix = f'test_event_result_standalone.py:{handler.__code__.co_firstlineno}'
    assert entry.handler_file_path.endswith(expected_suffix)


def test_event_result_serializes_handler_metadata_and_derived_fields() -> None:
    """EventResult stores handler metadata and derives convenience fields from it."""

    def handler(event: StandaloneEvent) -> str:
        return event.data

    entry = EventHandler.from_callable(
        handler=cast(EventHandlerCallable, handler),
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
    )

    result = EventResult(
        event_id=str(uuid4()),
        handler=entry,
    )
    payload = result.model_dump(mode='json')

    assert payload['handler']['id'] == entry.id
    assert payload['handler']['handler_name'] == entry.handler_name
    assert 'result_type' not in payload
    assert payload['handler_id'] == entry.id
    assert payload['handler_name'] == entry.handler_name
    assert payload['eventbus_id'] == entry.eventbus_id
    assert payload['eventbus_name'] == entry.eventbus_name
