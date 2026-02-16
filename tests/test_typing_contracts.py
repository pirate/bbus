"""Static typing contracts for the event execution pipeline.

This module is never imported by runtime code. It exists so strict type checks
(`pyright`, `ty`) fail if the end-to-end event handler pipeline is weakened.
"""

from typing import Any, AsyncContextManager, ContextManager, assert_type

from pydantic import BaseModel

from bubus.base_event import BaseEvent, EventResult
from bubus.event_bus import EventBus
from bubus.event_handler import EventHandler


class TypeContractResult(BaseModel):
    message: str


class TypeContractEvent(BaseEvent[TypeContractResult]):
    pass


async def _contract_handler(event: TypeContractEvent) -> TypeContractResult:
    return TypeContractResult(message=event.event_type)


def _build_contract_handler(bus: EventBus) -> EventHandler:
    return EventHandler.from_callable(
        handler=_contract_handler,
        event_pattern='TypeContractEvent',
        eventbus_name=bus.name,
        eventbus_id=bus.id,
    )


def _assert_pending_result_types(event: TypeContractEvent, bus: EventBus, handler_entry: EventHandler) -> None:
    pending_results = event._create_pending_handler_results(
        {handler_entry.id: handler_entry},
        eventbus=bus,
        timeout=event.event_timeout,
    )
    for pending_result in pending_results.values():
        assert_type(pending_result, EventResult[TypeContractResult])


async def _assert_pipeline_types(bus: EventBus, event: TypeContractEvent) -> None:
    handler_entry = _build_contract_handler(bus)
    _assert_pending_result_types(event, bus, handler_entry)

    dispatched_event = bus.emit(event)
    assert_type(dispatched_event, TypeContractEvent)

    emitted_event = bus.emit(event)
    assert_type(emitted_event, TypeContractEvent)

    handler_result = event.event_results[handler_entry.id]
    assert_type(handler_result, EventResult[TypeContractResult])

    handler_lock_scope: AsyncContextManager[None] = bus.locks._run_with_handler_lock(bus, event, handler_result)
    async with handler_lock_scope:
        pass

    handler_context_scope: ContextManager[None] = bus._run_with_handler_execution_context(event, handler_entry.id)
    with handler_context_scope:
        pass

    bus_run_value = await bus.run_handler(event, handler_entry, timeout=event.event_timeout)
    assert_type(bus_run_value, TypeContractResult | BaseEvent[Any] | None)

    result_run_value = await handler_result.run_handler(event, eventbus=bus, timeout=event.event_timeout)
    assert_type(result_run_value, TypeContractResult | BaseEvent[Any] | None)

    assert_type(handler_result.result, TypeContractResult | BaseEvent[Any] | None)


def test_typing_contracts_module_loads() -> None:
    """Runtime no-op so this file is a valid pytest module."""
    assert callable(_assert_pipeline_types)


# Consolidated from tests/test_handler_registration_typing.py

"""Static typing contracts for EventBus.on overload behavior.

This file is for static type checking only (pyright/ty), not runtime pytest execution.
"""

# pyright: strict, reportUnnecessaryTypeIgnoreComment=true, reportPrivateUsage=false

from typing import TYPE_CHECKING

from bubus.base_event import BaseEvent
from bubus.event_bus import EventBus
from bubus.event_handler import EventHandler


class _SomeEventClass(BaseEvent[str]):
    pass


class _OtherEventClass(BaseEvent[str]):
    pass


class _EventTypeA(BaseEvent[int]):
    field_a: int = 1234


class _EventTypeB(BaseEvent[int]):
    field_b: int = 5678


class _EventTypeSubclassOfA(_EventTypeA):
    field_sub: float = 123.123


def _some_handler(event: _SomeEventClass) -> str:
    return 'ok'


def _base_handler(event: BaseEvent[Any]) -> str:
    return 'ok'


def _other_handler(event: _OtherEventClass) -> str:
    return 'ok'


def _handler_for_a(event: _EventTypeA) -> int:
    return event.field_a


def _handler_for_specific_subclass(event: _EventTypeSubclassOfA) -> int:
    return int(event.field_sub)


if TYPE_CHECKING:
    _bus = EventBus()

    # Class pattern should preserve strict subclass typing.
    _class_entry = _bus.on(_SomeEventClass, _some_handler)
    assert_type(_class_entry, EventHandler)

    # String pattern is intentionally looser: BaseEvent handlers and subclass handlers are both accepted.
    _string_base_entry = _bus.on('SomeEventClass', _base_handler)
    assert_type(_string_base_entry, EventHandler)
    _string_subclass_entry = _bus.on('SomeEventClass', _some_handler)
    assert_type(_string_subclass_entry, EventHandler)

    # Expected static type errors:
    # 1) class pattern should reject a mismatched event subclass handler
    _bus.on(_SomeEventClass, _other_handler)  # pyright: ignore[reportCallIssue, reportArgumentType]  # ty: ignore[no-matching-overload]

    # Variance contracts for class patterns:
    # 2) unrelated class pattern should reject handler expecting a different event class
    _bus.on(_EventTypeB, _handler_for_a)  # pyright: ignore[reportCallIssue, reportArgumentType]  # ty: ignore[no-matching-overload]
    # 3) subclass pattern accepts base-class handler (contravariant safe)
    _subclass_ok = _bus.on(_EventTypeSubclassOfA, _handler_for_a)
    assert_type(_subclass_ok, EventHandler)
    # 4) base-class pattern rejects subclass-only handler
    _bus.on(_EventTypeA, _handler_for_specific_subclass)  # pyright: ignore[reportCallIssue, reportArgumentType]  # ty: ignore[no-matching-overload]
