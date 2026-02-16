from __future__ import annotations

import ast
import re
import tomllib
from pathlib import Path


_ROOT = Path(__file__).resolve().parents[1]
_PYPROJECT_PATH = _ROOT / 'pyproject.toml'
_THIRD_PARTY_SQLITE_PACKAGES = {'sqlite3', 'aiosqlite', 'pysqlite3', 'pysqlite3-binary'}


def _load_pyproject() -> object:
    return tomllib.loads(_PYPROJECT_PATH.read_text(encoding='utf-8'))


def _as_list_of_str(value: object, *, context: str) -> list[str]:
    if not isinstance(value, list):
        raise AssertionError(f'Expected list for {context}, got {type(value).__name__}')
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise AssertionError(f'Expected list[str] for {context}')
        normalized.append(item)
    return normalized


def _as_str_keyed_dict(value: object, *, context: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise AssertionError(f'Expected dict for {context}, got {type(value).__name__}')
    normalized: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise AssertionError(f'Expected str keys for {context}')
        normalized[key] = item
    return normalized


def _as_mapping_of_str_to_str_list(value: object, *, context: str) -> dict[str, list[str]]:
    mapping = _as_str_keyed_dict(value, context=context)
    normalized: dict[str, list[str]] = {}
    for key, item in mapping.items():
        normalized[key] = _as_list_of_str(item, context=f'{context}.{key}')
    return normalized


def _dependency_names(requirements: list[str]) -> set[str]:
    names: set[str] = set()
    for requirement in requirements:
        match = re.match(r'^\s*([A-Za-z0-9_.-]+)', requirement)
        if match is not None:
            names.add(match.group(1).lower())
    return names


def _ast_import_roots(path: Path) -> set[str]:
    parsed = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    roots: set[str] = set()
    for node in ast.walk(parsed):
        if isinstance(node, ast.Import):
            for alias in node.names:
                roots.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            roots.add(node.module.split('.')[0])
    return roots


def test_bridge_dependencies_are_optional_in_pyproject() -> None:
    pyproject = _as_str_keyed_dict(_load_pyproject(), context='pyproject')
    project = _as_str_keyed_dict(pyproject.get('project'), context='project')
    optional_dependencies = _as_mapping_of_str_to_str_list(
        project.get('optional-dependencies'),
        context='project.optional-dependencies',
    )
    dependencies = _as_list_of_str(project.get('dependencies'), context='project.dependencies')

    core_dependency_names = _dependency_names(dependencies)
    assert 'asyncpg' not in core_dependency_names
    assert 'nats-py' not in core_dependency_names
    assert 'redis' not in core_dependency_names

    assert 'asyncpg' in _dependency_names(optional_dependencies['postgres'])
    assert 'nats-py' in _dependency_names(optional_dependencies['nats'])
    assert 'redis' in _dependency_names(optional_dependencies['redis'])
    assert _dependency_names(optional_dependencies['bridges']) == {'asyncpg', 'nats-py', 'redis'}


def test_no_third_party_sqlite_dependency_in_pyproject() -> None:
    pyproject = _as_str_keyed_dict(_load_pyproject(), context='pyproject')
    project = _as_str_keyed_dict(pyproject.get('project'), context='project')
    optional_dependencies = _as_mapping_of_str_to_str_list(
        project.get('optional-dependencies'),
        context='project.optional-dependencies',
    )
    dependency_groups = _as_mapping_of_str_to_str_list(
        pyproject.get('dependency-groups', {}),
        context='dependency-groups',
    )
    dependencies = _as_list_of_str(project.get('dependencies'), context='project.dependencies')

    all_dependency_names: set[str] = set()
    all_dependency_names |= _dependency_names(dependencies)
    for requirements in optional_dependencies.values():
        all_dependency_names |= _dependency_names(requirements)
    for requirements in dependency_groups.values():
        all_dependency_names |= _dependency_names(requirements)

    assert _THIRD_PARTY_SQLITE_PACKAGES.isdisjoint(all_dependency_names)


def test_bridge_modules_do_not_eager_import_optional_packages() -> None:
    bridge_modules = {
        _ROOT / 'bubus' / 'bridge_postgres.py': {'asyncpg'},
        _ROOT / 'bubus' / 'bridge_nats.py': {'nats'},
        _ROOT / 'bubus' / 'bridge_redis.py': {'redis'},
    }

    for path, forbidden_roots in bridge_modules.items():
        imported_roots = _ast_import_roots(path)
        assert forbidden_roots.isdisjoint(imported_roots), f'{path} eagerly imports {forbidden_roots & imported_roots}'
