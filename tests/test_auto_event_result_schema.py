"""Test automatic event_result_type extraction from Generic type parameters."""

from typing import Any

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

from bubus.models import BaseEvent, _extract_basemodel_generic_arg


class UserData(BaseModel):
    name: str
    age: int


class TaskResult(BaseModel):
    task_id: str
    status: str


class ModuleLevelResult(BaseModel):
    """Module-level result type for testing auto-detection."""

    result_id: str
    data: dict[str, Any]
    success: bool


class NestedModuleResult(BaseModel):
    """Another module-level type for testing complex generics."""

    items: list[str]
    metadata: dict[str, int]


class EmailMessage(BaseModel):
    """Module-level type for testing _extract_basemodel_generic_arg."""

    subject: str
    body: str
    recipients: list[str]


def test_builtin_types_auto_extraction():
    """Built-in Generic[T] values populate result schema."""

    class StringEvent(BaseEvent[str]):
        message: str = 'Hello'

    class IntEvent(BaseEvent[int]):
        number: int = 42

    class FloatEvent(BaseEvent[float]):
        value: float = 3.14

    string_event = StringEvent()
    int_event = IntEvent()
    float_event = FloatEvent()

    assert string_event.event_result_type is str
    assert int_event.event_result_type is int
    assert float_event.event_result_type is float


def test_custom_pydantic_models_auto_extraction():
    """Custom Pydantic result schemas are extracted from Generic[T]."""

    class UserEvent(BaseEvent[UserData]):
        user_id: str = 'user123'

    class TaskEvent(BaseEvent[TaskResult]):
        batch_id: str = 'batch456'

    user_event = UserEvent()
    task_event = TaskEvent()

    assert user_event.event_result_type is UserData
    assert task_event.event_result_type is TaskResult


def test_complex_generic_types_auto_extraction():
    """Complex Generic[T] values are extracted."""

    class ListEvent(BaseEvent[list[str]]):
        pass

    class DictEvent(BaseEvent[dict[str, int]]):
        pass

    class SetEvent(BaseEvent[set[int]]):
        pass

    list_event = ListEvent()
    dict_event = DictEvent()
    set_event = SetEvent()

    assert list_event.event_result_type == list[str]
    assert dict_event.event_result_type == dict[str, int]
    assert set_event.event_result_type == set[int]


def test_complex_generic_with_custom_types():
    """Test complex generics containing custom types."""

    class TaskListEvent(BaseEvent[list[TaskResult]]):
        batch_id: str = 'batch456'

    task_list_event = TaskListEvent()

    assert task_list_event.event_result_type == list[TaskResult]


@pytest.mark.parametrize(
    ('json_schema', 'expected_schema'),
    [
        ({'type': 'string'}, str),
        ({'type': 'number'}, float),
        ({'type': 'boolean'}, bool),
    ],
)
def test_json_schema_primitive_deserialization(json_schema: dict[str, str], expected_schema: Any):
    """Primitive JSON Schema payloads reconstruct to Python runtime types."""
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    assert event.event_result_type is expected_schema
    serialized_schema = event.model_dump(mode='json')['event_result_type']
    assert isinstance(serialized_schema, dict)
    assert serialized_schema.get('type') == json_schema['type']


def test_no_generic_parameter():
    """Test that events without generic parameters don't get auto-set types."""

    class PlainEvent(BaseEvent):
        message: str = 'plain'

    plain_event = PlainEvent()

    # Should remain None since no schema was provided
    assert plain_event.event_result_type is None


def test_none_generic_parameter():
    """Test that BaseEvent[None] results in None type."""

    class NoneEvent(BaseEvent[None]):
        message: str = 'none'

    none_event = NoneEvent()

    # Should remain unset
    assert none_event.event_result_type is None


def test_nested_inheritance():
    """Test that generic type extraction works with nested inheritance."""

    class BaseUserEvent(BaseEvent[UserData]):
        pass

    class SpecificUserEvent(BaseUserEvent):
        specific_field: str = 'specific'

    specific_event = SpecificUserEvent()

    # Should inherit schema/type metadata from parent generic.
    assert specific_event.event_result_type is UserData


def test_module_level_types_auto_extraction():
    """Test that module-level schemas are automatically detected."""

    class ModuleEvent(BaseEvent[ModuleLevelResult]):
        operation: str = 'test_op'

    class NestedModuleEvent(BaseEvent[NestedModuleResult]):
        batch_id: str = 'batch123'

    module_event = ModuleEvent()
    nested_event = NestedModuleEvent()

    # Should auto-detect module-level schemas.
    assert module_event.event_result_type is ModuleLevelResult
    assert nested_event.event_result_type is NestedModuleResult


def test_complex_module_level_generics():
    """Test complex generics with module-level types are auto-detected."""

    class ListModuleEvent(BaseEvent[list[ModuleLevelResult]]):
        batch_size: int = 10

    class DictModuleEvent(BaseEvent[dict[str, NestedModuleResult]]):
        mapping_type: str = 'result_map'

    list_event = ListModuleEvent()
    dict_event = DictModuleEvent()

    # Should auto-detect complex schemas.
    assert list_event.event_result_type == list[ModuleLevelResult]
    assert dict_event.event_result_type == dict[str, NestedModuleResult]


async def test_module_level_runtime_enforcement():
    """Test that module-level auto-detected types are enforced at runtime."""
    from bubus import EventBus

    class RuntimeEvent(BaseEvent[ModuleLevelResult]):
        operation: str = 'runtime_test'

    # Verify auto-detection worked
    test_event = RuntimeEvent()
    assert test_event.event_result_type is ModuleLevelResult, f'Auto-detection failed: got {test_event.event_result_type}'

    bus = EventBus(name='runtime_test_bus')

    def correct_handler(event: RuntimeEvent):
        # Return dict that matches ModuleLevelResult schema
        return {'result_id': 'test123', 'data': {'key': 'value'}, 'success': True}

    def incorrect_handler(event: RuntimeEvent):
        # Return something that doesn't match ModuleLevelResult
        return {'wrong': 'format'}

    # Test correct handler
    bus.on('RuntimeEvent', correct_handler)

    event1 = RuntimeEvent()
    await bus.dispatch(event1)
    result1 = await event1.event_result()

    # Should be cast to ModuleLevelResult
    assert isinstance(result1, ModuleLevelResult)
    assert result1.result_id == 'test123'
    assert result1.data == {'key': 'value'}
    assert result1.success is True

    # Test incorrect handler
    bus.handlers.clear()  # Clear previous handler
    bus.on('RuntimeEvent', incorrect_handler)

    event2 = RuntimeEvent()
    await bus.dispatch(event2)

    # Should get an error due to validation failure
    handler_id = list(event2.event_results.keys())[0]
    event_result = event2.event_results[handler_id]

    assert event_result.status == 'error'
    assert isinstance(event_result.error, Exception)

    await bus.stop(clear=True)


def test_extract_basemodel_generic_arg_basic():
    """Test _extract_basemodel_generic_arg with basic types."""

    # Test BaseEvent[int]
    class IntResultEvent(BaseEvent[int]):
        pass

    result = _extract_basemodel_generic_arg(IntResultEvent)
    assert result is int


def test_extract_basemodel_generic_arg_dict():
    """Test _extract_basemodel_generic_arg with dict types."""

    # Test BaseEvent[dict[str, int]]
    class DictIntEvent(BaseEvent[dict[str, int]]):
        pass

    result = _extract_basemodel_generic_arg(DictIntEvent)
    assert result == dict[str, int]


def test_extract_basemodel_generic_arg_dict_with_module_type():
    """Test _extract_basemodel_generic_arg with dict containing module-level type."""

    # Test BaseEvent[dict[str, EmailMessage]]
    class DictEmailEvent(BaseEvent[dict[str, EmailMessage]]):
        pass

    result = _extract_basemodel_generic_arg(DictEmailEvent)
    assert result == dict[str, EmailMessage]


def test_extract_basemodel_generic_arg_dict_with_local_type():
    """Test _extract_basemodel_generic_arg with dict containing locally defined type."""

    # Define local type
    class EmailAttachment(BaseModel):
        filename: str
        content: bytes
        mime_type: str

    # Test BaseEvent[dict[str, EmailAttachment]]
    class DictAttachmentEvent(BaseEvent[dict[str, EmailAttachment]]):
        pass

    result = _extract_basemodel_generic_arg(DictAttachmentEvent)
    assert result == dict[str, EmailAttachment]


def test_extract_basemodel_generic_arg_no_generic():
    """Test _extract_basemodel_generic_arg with BaseEvent (no generic parameter)."""

    # Test BaseEvent without generic parameter
    class PlainEvent(BaseEvent):
        pass

    result = _extract_basemodel_generic_arg(PlainEvent)
    assert result is None


def test_type_adapter_validation():
    """Test that TypeAdapter can validate extracted types properly."""

    # Test dict[str, int] validation
    class DictIntEvent(BaseEvent[dict[str, int]]):
        pass

    extracted_type = _extract_basemodel_generic_arg(DictIntEvent)
    adapter = TypeAdapter(extracted_type)

    # Valid data should work
    valid_data = {'abc': 123, 'def': 456}
    result = adapter.validate_python(valid_data)
    assert result == valid_data

    # Invalid data should raise ValidationError
    invalid_data = {'abc': 'badvalue'}
    with pytest.raises(ValidationError) as exc_info:
        adapter.validate_python(invalid_data)

    # Check that the error is about the wrong type
    errors = exc_info.value.errors()
    assert len(errors) > 0
    assert any('int' in str(error) for error in errors)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
