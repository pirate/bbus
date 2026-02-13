"""Static typing contracts for EventBus.on overload behavior.

This file is for static type checking only (pyright/ty), not runtime pytest execution.
"""

# pyright: strict, reportUnnecessaryTypeIgnoreComment=true

from typing import TYPE_CHECKING, Any, assert_type

from bubus.event_bus import EventBus
from bubus.event_handler import EventHandler
from bubus.base_event import BaseEvent


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
