import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from bubus import BaseEvent, EventBus


class ScreenshotResult(BaseModel):
    image_url: str
    width: int
    height: int
    tags: list[str]
    is_animated: bool
    confidence_scores: list[float]
    metadata: dict[str, float]


class IntResultEvent(BaseEvent[int]):
    value: int
    label: str


class StringListResultEvent(BaseEvent[list[str]]):
    names: list[str]
    attempt: int


class ScreenshotEvent(BaseEvent[ScreenshotResult]):
    target_id: str
    quality: str


class MetricsEvent(BaseEvent[dict[str, list[int]]]):
    bucket: str
    counters: dict[str, int]


class AdhocEvent(BaseEvent[dict[str, int]]):
    custom_payload: dict[str, Any]
    nested_payload: dict[str, Any]


def _build_python_roundtrip_events() -> list[BaseEvent[Any]]:
    parent = IntResultEvent(
        value=7,
        label='parent',
        event_path=['PyBus#aaaa'],
        event_timeout=12.5,
    )
    child = ScreenshotEvent(
        target_id='tab-1',
        quality='high',
        event_parent_id=parent.event_id,
        event_path=['PyBus#aaaa', 'TsBridge#bbbb'],
        event_timeout=33.0,
    )
    list_event = StringListResultEvent(
        names=['alpha', 'beta', 'gamma'],
        attempt=2,
        event_parent_id=parent.event_id,
        event_path=['PyBus#aaaa'],
    )
    metrics_event = MetricsEvent(
        bucket='images',
        counters={'ok': 12, 'failed': 1},
        event_path=['PyBus#aaaa'],
    )
    adhoc_event = AdhocEvent(
        event_timeout=4.0,
        event_parent_id=parent.event_id,
        event_path=['PyBus#aaaa'],
        event_result_type=dict[str, int],
        custom_payload={'tab_id': 'tab-1', 'bytes': 12345},
        nested_payload={'frames': [1, 2, 3], 'format': 'png'},
    )
    return [parent, child, list_event, metrics_event, adhoc_event]


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


def test_python_to_ts_roundrip_preserves_event_fields_and_result_schemas(tmp_path: Path) -> None:
    events = _build_python_roundtrip_events()
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

        # Every field Python emitted should survive through TS serialization.
        for key, value in original.items():
            assert key in ts_event, f'missing key after ts roundtrip: {key}'
            assert ts_event[key] == value, f'field changed after ts roundtrip: {key}'

        # Verify we can load back into Python BaseEvent and keep the same payload.
        restored = BaseEvent[Any].model_validate(ts_event)
        restored_dump = restored.model_dump(mode='json')
        for key, value in original.items():
            assert key in restored_dump, f'missing key after python reload: {key}'
            assert restored_dump[key] == value, f'field changed after python reload: {key}'


async def test_python_to_ts_roundtrip_schema_enforcement_after_reload(tmp_path: Path) -> None:
    events = _build_python_roundtrip_events()
    python_dumped = [event.model_dump(mode='json') for event in events]
    ts_roundtripped = _ts_roundtrip_events(python_dumped, tmp_path)

    screenshot_payload = next(event for event in ts_roundtripped if event.get('event_type') == 'ScreenshotEvent')

    wrong_bus = EventBus(name='py_ts_py_wrong_shape')

    async def wrong_shape_handler(event: BaseEvent[Any]) -> dict[str, Any]:
        return {
            'image_url': 123,  # wrong: should be string
            'width': '1920',  # wrong: should be number
            'height': 1080,
            'tags': ['a', 'b'],
            'is_animated': 'false',  # wrong: should be boolean
            'confidence_scores': [0.9, 0.8],
            'metadata': {'score': 0.99},
        }

    wrong_bus.on('ScreenshotEvent', wrong_shape_handler)
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
        }

    right_bus.on('ScreenshotEvent', right_shape_handler)
    right_event = BaseEvent[Any].model_validate(screenshot_payload)
    assert isinstance(right_event.event_result_type, type)
    assert issubclass(right_event.event_result_type, BaseModel)
    await right_bus.dispatch(right_event)
    right_result = next(iter(right_event.event_results.values()))
    assert right_result.status == 'completed'
    assert right_result.error is None
    assert right_result.result is not None
    await right_bus.stop()
