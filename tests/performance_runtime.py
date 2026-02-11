from __future__ import annotations

import argparse
import asyncio
import json
import logging
from typing import Any

from performance_scenarios import PERF_SCENARIO_IDS, PerfInput, run_all_perf_scenarios, run_perf_scenario_by_id


TABLE_MATRIX = [
    ('50k-events', '1 bus x 50k events x 1 handler'),
    ('500-buses-x-100-events', '500 busses x 100 events x 1 handler'),
    ('1-event-x-50k-parallel-handlers', '1 bus x 1 event x 50k parallel handlers'),
    ('50k-one-off-handlers', '1 bus x 50k events x 50k one-off handlers'),
    ('worst-case-forwarding-timeouts', 'Worst case (N busses x N events x N handlers)'),
]


def _format_cell(result: dict[str, Any]) -> str:
    if result.get('ok') is False:
        error = str(result.get('error') or 'failed')
        compact = error.replace('\n', ' ').strip()
        if len(compact) > 42:
            compact = compact[:39] + '...'
        return f'`failed: {compact}`'

    ms_per_event = float(result['ms_per_event'])
    unit = str(result.get('ms_per_event_unit', 'event'))
    latency = f'{ms_per_event:.3f}ms/{unit}'

    peak_rss_kb_per_event = result.get('peak_rss_kb_per_event')
    if isinstance(peak_rss_kb_per_event, (int, float)):
        return f'`{latency}`, `{float(peak_rss_kb_per_event):.1f}kb/event`'
    return f'`{latency}`'


def _print_markdown_matrix(runtime_name: str, results: list[dict[str, Any]]) -> None:
    by_scenario = {str(result['scenario_id']): result for result in results}

    header_cols = ['Runtime'] + [label for _, label in TABLE_MATRIX]
    print('| ' + ' | '.join(header_cols) + ' |')
    print('|' + '|'.join([' ------------------ ' for _ in header_cols]) + '|')

    row_cells = [runtime_name]
    for scenario_id, _ in TABLE_MATRIX:
        result = by_scenario.get(scenario_id)
        if result is None:
            row_cells.append('`n/a`')
            continue
        row_cells.append(_format_cell(result))

    print('| ' + ' | '.join(row_cells) + ' |')


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run Python runtime performance scenarios for bubus')
    parser.add_argument('--scenario', type=str, default=None, help=f'One scenario id: {", ".join(PERF_SCENARIO_IDS)}')
    parser.add_argument('--json', action='store_true', help='Print full JSON output')
    return parser


async def _main_async() -> int:
    args = _build_parser().parse_args()
    logging.getLogger('bubus').setLevel(logging.CRITICAL)

    perf_input = PerfInput(runtime_name='python')

    print('[python] runtime perf harness starting')

    if args.scenario:
        if args.scenario not in PERF_SCENARIO_IDS:
            raise ValueError(f'Unknown --scenario value {args.scenario!r}. Expected one of: {", ".join(PERF_SCENARIO_IDS)}')
        result = await run_perf_scenario_by_id(perf_input, args.scenario)
        result['scenario_id'] = args.scenario
        results = [result]
    else:
        raw_results = await run_all_perf_scenarios(perf_input)
        results = []
        for scenario_id, result in zip(PERF_SCENARIO_IDS, raw_results, strict=True):
            result_copy = dict(result)
            result_copy['scenario_id'] = scenario_id
            results.append(result_copy)

    print('[python] runtime perf harness complete')
    print('')
    print('Markdown matrix row (copy into README):')
    _print_markdown_matrix('Python', results)

    if args.json:
        print('')
        print(json.dumps(results, indent=2, default=str))

    return 0


def main() -> int:
    return asyncio.run(_main_async())


if __name__ == '__main__':
    raise SystemExit(main())
