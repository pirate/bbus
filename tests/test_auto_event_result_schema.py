"""Test automatic event_result_type extraction from Generic type parameters."""

from dataclasses import dataclass
from typing import Any, TypedDict

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

from bubus.helpers import extract_basemodel_generic_arg
from bubus.models import BaseEvent


def _to_plain(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return {key: _to_plain(item) for key, item in value.model_dump().items()}
    if isinstance(value, list):
        return [_to_plain(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_to_plain(item) for item in value)
    if isinstance(value, dict):
        return {key: _to_plain(item) for key, item in value.items()}
    return value


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
    """Module-level type for testing extract_basemodel_generic_arg."""

    subject: str
    body: str
    recipients: list[str]


class ProfileResult(TypedDict):
    user_id: str
    active: bool
    score: int


class OptionalProfileResult(TypedDict, total=False):
    nickname: str
    age: int


@dataclass
class DataClassResult:
    task_id: str
    priority: int


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
        ({'type': 'integer'}, int),
        ({'type': 'boolean'}, bool),
        ({'type': 'null'}, type(None)),
    ],
)
def test_json_schema_primitive_deserialization(json_schema: dict[str, str], expected_schema: Any):
    """Primitive JSON Schema payloads reconstruct to Python runtime types."""
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    assert event.event_result_type is expected_schema
    serialized_schema = event.model_dump(mode='json')['event_result_type']
    assert isinstance(serialized_schema, dict)
    assert serialized_schema.get('type') == json_schema['type']


def test_json_schema_list_of_models_deserialization():
    """Array schemas with $defs/$ref rehydrate into list[BaseModel]-compatible validators."""
    json_schema = TypeAdapter(list[UserData]).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    adapter = TypeAdapter(event.event_result_type)
    validated = adapter.validate_python([{'name': 'alice', 'age': 33}])
    assert isinstance(validated, list)
    assert len(validated) == 1
    assert isinstance(validated[0], BaseModel)
    assert validated[0].model_dump() == {'name': 'alice', 'age': 33}

    serialized_schema = event.model_dump(mode='json')['event_result_type']
    assert isinstance(serialized_schema, dict)
    assert serialized_schema.get('type') == 'array'
    assert '$defs' in serialized_schema


def test_json_schema_nested_object_collection_deserialization():
    """Nested dict[str, list[BaseModel]] schemas rehydrate into fully typed validators."""
    json_schema = TypeAdapter(dict[str, list[TaskResult]]).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    adapter = TypeAdapter(event.event_result_type)
    validated = adapter.validate_python({'batch_a': [{'task_id': 't1', 'status': 'ok'}]})
    assert isinstance(validated, dict)
    assert isinstance(validated['batch_a'], list)
    assert isinstance(validated['batch_a'][0], BaseModel)
    assert validated['batch_a'][0].model_dump() == {'task_id': 't1', 'status': 'ok'}

    serialized_schema = event.model_dump(mode='json')['event_result_type']
    assert isinstance(serialized_schema, dict)
    assert serialized_schema.get('type') == 'object'
    assert '$defs' in serialized_schema


@pytest.mark.parametrize(
    ('shape', 'payload'),
    [
        (list[str], ['a', 'b']),
        (tuple[str, int], ['a', 7]),
        (dict[str, list[int]], {'scores': [1, 2, 3]}),
        (list[tuple[str, int]], [['x', 1], ['y', 2]]),
        (list[UserData], [{'name': 'alice', 'age': 33}]),
        (dict[str, list[TaskResult]], {'batch_a': [{'task_id': 't1', 'status': 'ok'}]}),
    ],
)
def test_json_schema_top_level_shape_deserialization_matrix(shape: Any, payload: Any):
    """Top-level collection shapes rehydrate into equivalent runtime validators."""
    json_schema = TypeAdapter(shape).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    hydrated_adapter = TypeAdapter(event.event_result_type)
    expected_adapter = TypeAdapter(shape)

    hydrated_value = hydrated_adapter.validate_python(payload)
    expected_value = expected_adapter.validate_python(payload)
    assert _to_plain(hydrated_value) == _to_plain(expected_value)

    serialized_schema = event.model_dump(mode='json')['event_result_type']
    assert isinstance(serialized_schema, dict)
    assert '$schema' in serialized_schema


def test_json_schema_typed_dict_rehydrates_to_pydantic_model():
    """TypedDict schemas rehydrate into dynamic pydantic models."""
    json_schema = TypeAdapter(ProfileResult).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    assert isinstance(event.event_result_type, type)
    assert issubclass(event.event_result_type, BaseModel)

    adapter = TypeAdapter(event.event_result_type)
    validated = adapter.validate_python({'user_id': 'u1', 'active': True, 'score': 9})
    assert isinstance(validated, BaseModel)
    assert validated.model_dump() == {'user_id': 'u1', 'active': True, 'score': 9}


def test_json_schema_optional_typed_dict_is_lax_on_missing_fields():
    """Non-required TypedDict fields should not fail hydration-time validation."""
    json_schema = TypeAdapter(OptionalProfileResult).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    adapter = TypeAdapter(event.event_result_type)
    empty_validated = adapter.validate_python({})
    assert isinstance(empty_validated, BaseModel)

    partial_validated = adapter.validate_python({'nickname': 'squash'})
    assert isinstance(partial_validated, BaseModel)
    assert partial_validated.model_dump(exclude_none=True) == {'nickname': 'squash'}


def test_json_schema_dataclass_rehydrates_to_pydantic_model():
    """Dataclass schemas rehydrate into dynamic pydantic models."""
    json_schema = TypeAdapter(DataClassResult).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    adapter = TypeAdapter(event.event_result_type)
    validated = adapter.validate_python({'task_id': 'task-1', 'priority': 2})
    assert isinstance(validated, BaseModel)
    assert validated.model_dump() == {'task_id': 'task-1', 'priority': 2}


def test_json_schema_list_of_dataclass_rehydrates_to_list_of_models():
    """Nested dataclass objects inside collections should rehydrate cleanly."""
    json_schema = TypeAdapter(list[DataClassResult]).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    adapter = TypeAdapter(event.event_result_type)
    validated = adapter.validate_python([{'task_id': 'task-2', 'priority': 5}])
    assert isinstance(validated, list)
    assert isinstance(validated[0], BaseModel)
    assert validated[0].model_dump() == {'task_id': 'task-2', 'priority': 5}


async def test_json_schema_nested_object_and_array_runtime_enforcement():
    """Nested object/array schemas reconstructed from JSON enforce handler return values."""
    from bubus import EventBus

    nested_schema = {
        'type': 'object',
        'properties': {
            'items': {'type': 'array', 'items': {'type': 'integer'}},
            'meta': {'type': 'object', 'additionalProperties': {'type': 'boolean'}},
        },
        'required': ['items', 'meta'],
    }

    bus = EventBus(name='nested_schema_runtime_bus')

    async def valid_handler(event: BaseEvent[Any]) -> dict[str, Any]:
        return {'items': [1, 2, 3], 'meta': {'ok': True, 'cached': False}}

    bus.on('NestedSchemaEvent', valid_handler)

    valid_event = BaseEvent[Any].model_validate({'event_type': 'NestedSchemaEvent', 'event_result_type': nested_schema})
    await bus.dispatch(valid_event)
    valid_result = next(iter(valid_event.event_results.values()))
    assert valid_result.status == 'completed'
    assert valid_result.error is None
    assert isinstance(valid_result.result, BaseModel)
    assert valid_result.result.model_dump() == {'items': [1, 2, 3], 'meta': {'ok': True, 'cached': False}}

    bus.handlers.clear()

    async def invalid_handler(event: BaseEvent[Any]) -> dict[str, Any]:
        return {'items': ['not-an-int'], 'meta': {'ok': 'yes'}}

    bus.on('NestedSchemaEvent', invalid_handler)
    invalid_event = BaseEvent[Any].model_validate({'event_type': 'NestedSchemaEvent', 'event_result_type': nested_schema})
    await bus.dispatch(invalid_event)
    invalid_result = next(iter(invalid_event.event_results.values()))
    assert invalid_result.status == 'error'
    assert invalid_result.error is not None

    await bus.stop(clear=True)


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
    """Test extract_basemodel_generic_arg with basic types."""

    # Test BaseEvent[int]
    class IntResultEvent(BaseEvent[int]):
        pass

    result = extract_basemodel_generic_arg(IntResultEvent)
    assert result is int


def test_extract_basemodel_generic_arg_dict():
    """Test extract_basemodel_generic_arg with dict types."""

    # Test BaseEvent[dict[str, int]]
    class DictIntEvent(BaseEvent[dict[str, int]]):
        pass

    result = extract_basemodel_generic_arg(DictIntEvent)
    assert result == dict[str, int]


def test_extract_basemodel_generic_arg_dict_with_module_type():
    """Test extract_basemodel_generic_arg with dict containing module-level type."""

    # Test BaseEvent[dict[str, EmailMessage]]
    class DictEmailEvent(BaseEvent[dict[str, EmailMessage]]):
        pass

    result = extract_basemodel_generic_arg(DictEmailEvent)
    assert result == dict[str, EmailMessage]


def test_extract_basemodel_generic_arg_dict_with_local_type():
    """Test extract_basemodel_generic_arg with dict containing locally defined type."""

    # Define local type
    class EmailAttachment(BaseModel):
        filename: str
        content: bytes
        mime_type: str

    # Test BaseEvent[dict[str, EmailAttachment]]
    class DictAttachmentEvent(BaseEvent[dict[str, EmailAttachment]]):
        pass

    result = extract_basemodel_generic_arg(DictAttachmentEvent)
    assert result == dict[str, EmailAttachment]


def test_extract_basemodel_generic_arg_no_generic():
    """Test extract_basemodel_generic_arg with BaseEvent (no generic parameter)."""

    # Test BaseEvent without generic parameter
    class PlainEvent(BaseEvent):
        pass

    result = extract_basemodel_generic_arg(PlainEvent)
    assert result is None


def test_type_adapter_validation():
    """Test that TypeAdapter can validate extracted types properly."""

    # Test dict[str, int] validation
    class DictIntEvent(BaseEvent[dict[str, int]]):
        pass

    extracted_type = extract_basemodel_generic_arg(DictIntEvent)
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
