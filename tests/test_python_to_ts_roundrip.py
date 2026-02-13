import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from types import NoneType
from typing import Any

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError
from typing_extensions import TypedDict

from bubus import BaseEvent, EventBus


class ScreenshotRegion(BaseModel):
    id: str
    label: str
    score: float
    visible: bool


class ScreenshotResult(BaseModel):
    image_url: str
    width: int
    height: int
    tags: list[str]
    is_animated: bool
    confidence_scores: list[float]
    metadata: dict[str, float]
    regions: list[ScreenshotRegion]


class PyTsTypedDictResult(TypedDict):
    name: str
    active: bool
    count: int


@dataclass(slots=True)
class PyTsDataclassResult:
    name: str
    score: float
    tags: list[str]


@dataclass(slots=True)
class RoundtripCase:
    event: BaseEvent[Any]
    valid_results: list[Any]
    invalid_results: list[Any]


class PyTsIntResultEvent(BaseEvent[int]):
    value: int
    label: str


class PyTsFloatResultEvent(BaseEvent[float]):
    marker: str


class PyTsStringResultEvent(BaseEvent[str]):
    marker: str


class PyTsBoolResultEvent(BaseEvent[bool]):
    marker: str


class PyTsNullResultEvent(BaseEvent[NoneType]):
    marker: str


class PyTsStringListResultEvent(BaseEvent[list[str]]):
    marker: str


class PyTsDictResultEvent(BaseEvent[dict[str, int]]):
    marker: str


class PyTsNestedMapResultEvent(BaseEvent[dict[str, list[int]]]):
    marker: str


class PyTsTypedDictResultEvent(BaseEvent[PyTsTypedDictResult]):
    marker: str


class PyTsDataclassResultEvent(BaseEvent[PyTsDataclassResult]):
    marker: str


class PyTsScreenshotEvent(BaseEvent[ScreenshotResult]):
    target_id: str
    quality: str


def _value_repr(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True)
    except TypeError:
        return repr(value)


def _accepts_result_type(result_type: Any, value: Any) -> bool:
    try:
        TypeAdapter(result_type).validate_python(value)
    except ValidationError:
        return False
    return True


def _assert_result_type_semantics_equal(
    original_result_type: Any,
    candidate_schema_json: dict[str, Any],
    valid_results: list[Any],
    invalid_results: list[Any],
    context: str,
) -> None:
    hydrated = BaseEvent[Any].model_validate({'event_type': 'SchemaSemanticsEvent', 'event_result_type': candidate_schema_json})
    candidate_result_type = hydrated.event_result_type
    assert candidate_result_type is not None, f'{context}: missing candidate result type after hydration'

    for value in valid_results:
        original_ok = _accepts_result_type(original_result_type, value)
        candidate_ok = _accepts_result_type(candidate_result_type, value)
        assert original_ok, f'{context}: original schema should accept {_value_repr(value)}'
        assert candidate_ok, f'{context}: candidate schema should accept {_value_repr(value)}'

    for value in invalid_results:
        original_ok = _accepts_result_type(original_result_type, value)
        candidate_ok = _accepts_result_type(candidate_result_type, value)
        assert not original_ok, f'{context}: original schema should reject {_value_repr(value)}'
        assert not candidate_ok, f'{context}: candidate schema should reject {_value_repr(value)}'

    for value in [*valid_results, *invalid_results]:
        original_ok = _accepts_result_type(original_result_type, value)
        candidate_ok = _accepts_result_type(candidate_result_type, value)
        assert candidate_ok == original_ok, (
            f'{context}: schema decision mismatch for {_value_repr(value)} (expected {original_ok}, got {candidate_ok})'
        )


def _build_python_roundtrip_cases() -> list[RoundtripCase]:
    parent = PyTsIntResultEvent(
        value=7,
        label='parent',
        event_path=['PyBus#aaaa'],
        event_timeout=12.5,
    )

    screenshot_event = PyTsScreenshotEvent(
        target_id='tab-1',
        quality='high',
        event_parent_id=parent.event_id,
        event_path=['PyBus#aaaa', 'TsBridge#bbbb'],
        event_timeout=33.0,
    )

    float_event = PyTsFloatResultEvent(
        marker='float',
        event_parent_id=parent.event_id,
        event_path=['PyBus#aaaa'],
    )
    string_event = PyTsStringResultEvent(
        marker='string',
        event_parent_id=parent.event_id,
        event_path=['PyBus#aaaa'],
    )
    bool_event = PyTsBoolResultEvent(
        marker='bool',
        event_path=['PyBus#aaaa'],
    )
    null_event = PyTsNullResultEvent(
        marker='null',
        event_path=['PyBus#aaaa'],
    )
    list_event = PyTsStringListResultEvent(
        marker='list[str]',
        event_parent_id=parent.event_id,
        event_path=['PyBus#aaaa'],
    )
    dict_event = PyTsDictResultEvent(
        marker='dict[str,int]',
        event_path=['PyBus#aaaa'],
    )
    nested_map_event = PyTsNestedMapResultEvent(
        marker='dict[str,list[int]]',
        event_path=['PyBus#aaaa'],
    )
    typed_dict_event = PyTsTypedDictResultEvent(
        marker='typeddict',
        event_path=['PyBus#aaaa'],
    )
    dataclass_event = PyTsDataclassResultEvent(
        marker='dataclass',
        event_path=['PyBus#aaaa'],
    )

    return [
        RoundtripCase(
            event=parent,
            valid_results=[0, -5, 42],
            invalid_results=[{}, [], 'not-int'],
        ),
        RoundtripCase(
            event=float_event,
            valid_results=[0.5, 12.25, 3],
            invalid_results=[{}, [], 'not-number'],
        ),
        RoundtripCase(
            event=string_event,
            valid_results=['ok', ''],
            invalid_results=[{}, [], 123],
        ),
        RoundtripCase(
            event=bool_event,
            valid_results=[True, False],
            invalid_results=[{}, [], 'not-bool'],
        ),
        RoundtripCase(
            event=null_event,
            valid_results=[None],
            invalid_results=[{}, [], 0, False, 'not-null'],
        ),
        RoundtripCase(
            event=list_event,
            valid_results=[['a', 'b'], []],
            invalid_results=[{}, 'not-list', 123],
        ),
        RoundtripCase(
            event=dict_event,
            valid_results=[{'ok': 1, 'failed': 2}, {}],
            invalid_results=[['not', 'dict'], 'bad', 123],
        ),
        RoundtripCase(
            event=nested_map_event,
            valid_results=[{'a': [1, 2], 'b': []}, {}],
            invalid_results=[{'a': 'not-list'}, ['bad'], 123],
        ),
        RoundtripCase(
            event=typed_dict_event,
            valid_results=[{'name': 'alpha', 'active': True, 'count': 2}],
            invalid_results=[{'name': 'alpha'}, {'name': 123, 'active': True, 'count': 2}],
        ),
        RoundtripCase(
            event=dataclass_event,
            valid_results=[{'name': 'model', 'score': 0.85, 'tags': ['a', 'b']}],
            invalid_results=[{'name': 'model', 'score': 'not-number', 'tags': ['a']}, {'name': 'model', 'score': 1.0}],
        ),
        RoundtripCase(
            event=screenshot_event,
            valid_results=[
                {
                    'image_url': 'https://img.local/1.png',
                    'width': 1920,
                    'height': 1080,
                    'tags': ['hero', 'dashboard'],
                    'is_animated': False,
                    'confidence_scores': [0.95, 0.89],
                    'metadata': {'score': 0.99, 'variance': 0.01},
                    'regions': [
                        {'id': 'r1', 'label': 'face', 'score': 0.9, 'visible': True},
                        {'id': 'r2', 'label': 'button', 'score': 0.7, 'visible': False},
                    ],
                }
            ],
            invalid_results=[
                {
                    'image_url': 123,
                    'width': 1920,
                    'height': 1080,
                    'tags': ['hero'],
                    'is_animated': False,
                    'confidence_scores': [0.95],
                    'metadata': {'score': 0.99},
                    'regions': [{'id': 'r1', 'label': 'face', 'score': 0.9, 'visible': True}],
                },
                {
                    'image_url': 'https://img.local/1.png',
                    'width': 1920,
                    'height': 1080,
                    'tags': ['hero'],
                    'is_animated': False,
                    'confidence_scores': [0.95],
                    'metadata': {'score': 0.99},
                    'regions': [{'id': 123, 'label': 'face', 'score': 0.9, 'visible': True}],
                },
            ],
        ),
    ]


def _ts_roundtrip_events(payload: list[dict[str, Any]], tmp_path: Path) -> list[dict[str, Any]]:
    node_bin = shutil.which('node')
    if node_bin is None:
        pytest.skip('node is required for python<->ts roundtrip tests')
    assert node_bin is not None

    repo_root = Path(__file__).resolve().parents[1]
    ts_root = repo_root / 'bubus-ts'
    if not (ts_root / 'src' / 'index.ts').exists():
        pytest.skip('bubus-ts project not found in repository root')

    in_path = tmp_path / 'python_events.json'
    out_path = tmp_path / 'ts_events.json'
    in_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')

    ts_script = """
import { readFileSync, writeFileSync } from 'node:fs'
import { BaseEvent } from './src/index.js'

const inputPath = process.env.BUBUS_PY_TS_INPUT_PATH
const outputPath = process.env.BUBUS_PY_TS_OUTPUT_PATH
if (!inputPath || !outputPath) {
  throw new Error('missing BUBUS_PY_TS_INPUT_PATH or BUBUS_PY_TS_OUTPUT_PATH')
}

const raw = JSON.parse(readFileSync(inputPath, 'utf8'))
if (!Array.isArray(raw)) {
  throw new Error('expected array payload')
}

const roundtripped = raw.map((item) => BaseEvent.fromJSON(item).toJSON())
writeFileSync(outputPath, JSON.stringify(roundtripped, null, 2), 'utf8')
"""

    env = os.environ.copy()
    env['BUBUS_PY_TS_INPUT_PATH'] = str(in_path)
    env['BUBUS_PY_TS_OUTPUT_PATH'] = str(out_path)
    proc = subprocess.run(
        [node_bin, '--import', 'tsx', '-e', ts_script],
        cwd=ts_root,
        env=env,
        capture_output=True,
        text=True,
    )

    if proc.returncode != 0 and 'Cannot find package' in proc.stderr and "'tsx'" in proc.stderr:
        pytest.skip('tsx is not installed in bubus-ts; skipping cross-language roundtrip test')

    assert proc.returncode == 0, f'node/tsx roundtrip failed:\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}'
    return json.loads(out_path.read_text(encoding='utf-8'))


def test_python_to_ts_roundrip_preserves_event_fields_and_result_type_semantics(tmp_path: Path) -> None:
    cases = _build_python_roundtrip_cases()
    events = [entry.event for entry in cases]
    cases_by_type = {entry.event.event_type: entry for entry in cases}
    python_dumped = [event.model_dump(mode='json') for event in events]

    # Ensure Python emits JSONSchema for return value types before sending to TS.
    for event_dump in python_dumped:
        assert 'event_result_type' in event_dump
        assert isinstance(event_dump['event_result_type'], dict)

    ts_roundtripped = _ts_roundtrip_events(python_dumped, tmp_path)
    assert len(ts_roundtripped) == len(python_dumped)

    for i, original in enumerate(python_dumped):
        ts_event = ts_roundtripped[i]
        assert isinstance(ts_event, dict)

        event_type = str(original.get('event_type'))
        semantics_case = cases_by_type.get(event_type)
        assert semantics_case is not None, f'missing semantics case for event_type={event_type}'

        # Every field Python emitted should survive through TS serialization.
        for key, value in original.items():
            assert key in ts_event, f'missing key after ts roundtrip: {key}'
            if key == 'event_result_type':
                assert isinstance(ts_event[key], dict), 'event_result_type should serialize as JSON schema dict'
                _assert_result_type_semantics_equal(
                    semantics_case.event.event_result_type,
                    ts_event[key],
                    semantics_case.valid_results,
                    semantics_case.invalid_results,
                    f'ts roundtrip {event_type}',
                )
            else:
                assert ts_event[key] == value, f'field changed after ts roundtrip: {key}'

        # Verify we can load back into Python BaseEvent and keep the same payload/semantics.
        restored = BaseEvent[Any].model_validate(ts_event)
        restored_dump = restored.model_dump(mode='json')
        for key, value in original.items():
            assert key in restored_dump, f'missing key after python reload: {key}'
            if key == 'event_result_type':
                assert isinstance(restored_dump[key], dict), 'event_result_type should remain JSON schema after reload'
                _assert_result_type_semantics_equal(
                    semantics_case.event.event_result_type,
                    restored_dump[key],
                    semantics_case.valid_results,
                    semantics_case.invalid_results,
                    f'python reload {event_type}',
                )
            else:
                assert restored_dump[key] == value, f'field changed after python reload: {key}'


async def test_python_to_ts_roundtrip_schema_enforcement_after_reload(tmp_path: Path) -> None:
    events = [entry.event for entry in _build_python_roundtrip_cases()]
    python_dumped = [event.model_dump(mode='json') for event in events]
    ts_roundtripped = _ts_roundtrip_events(python_dumped, tmp_path)

    screenshot_payload = next(event for event in ts_roundtripped if event.get('event_type') == 'PyTsScreenshotEvent')

    wrong_bus = EventBus(name='py_ts_py_wrong_shape')

    async def wrong_shape_handler(event: BaseEvent[Any]) -> dict[str, Any]:
        return {
            'image_url': 123,  # wrong: should be string
            'width': '1920',  # wrong: should be int
            'height': 1080,
            'tags': ['a', 'b'],
            'is_animated': 'false',  # wrong: should be bool
            'confidence_scores': [0.9, 0.8],
            'metadata': {'score': 0.99},
            'regions': [{'id': 'r1', 'label': 'face', 'score': 0.9, 'visible': True}],
        }

    wrong_bus.on('PyTsScreenshotEvent', wrong_shape_handler)
    wrong_event = BaseEvent[Any].model_validate(screenshot_payload)
    assert isinstance(wrong_event.event_result_type, type)
    assert issubclass(wrong_event.event_result_type, BaseModel)
    await wrong_bus.dispatch(wrong_event)
    wrong_result = next(iter(wrong_event.event_results.values()))
    assert wrong_result.status == 'error'
    assert wrong_result.error is not None
    await wrong_bus.stop()

    right_bus = EventBus(name='py_ts_py_right_shape')

    async def right_shape_handler(event: BaseEvent[Any]) -> dict[str, Any]:
        return {
            'image_url': 'https://img.local/1.png',
            'width': 1920,
            'height': 1080,
            'tags': ['hero', 'dashboard'],
            'is_animated': False,
            'confidence_scores': [0.95, 0.89],
            'metadata': {'score': 0.99, 'variance': 0.01},
            'regions': [
                {'id': 'r1', 'label': 'face', 'score': 0.9, 'visible': True},
                {'id': 'r2', 'label': 'button', 'score': 0.7, 'visible': False},
            ],
        }

    right_bus.on('PyTsScreenshotEvent', right_shape_handler)
    right_event = BaseEvent[Any].model_validate(screenshot_payload)
    assert isinstance(right_event.event_result_type, type)
    assert issubclass(right_event.event_result_type, BaseModel)
    await right_bus.dispatch(right_event)
    right_result = next(iter(right_event.event_results.values()))
    assert right_result.status == 'completed'
    assert right_result.error is None
    assert right_result.result is not None
    await right_bus.stop()
