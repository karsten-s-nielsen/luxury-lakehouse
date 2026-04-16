"""Guardrail: main.tf task + environment blocks stay alphabetical.

The Databricks Terraform provider matches nested blocks positionally against
state. State stores blocks sorted alphabetically by key; declaring them in
any other order produces phantom drift in every `terraform plan`. This test
keeps main.tf aligned so CI plan reviews stay signal, not noise.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_MAIN_TF = Path(__file__).resolve().parents[2] / "terraform" / "modules" / "workflows" / "main.tf"


def _extract_top_level_block_keys(text: str, block_type: str, key_field: str) -> list[str]:
    """Return the ordered list of ``{key_field} = "..."`` values for every
    top-level ``{block_type} {{ ... }}`` block inside the first
    ``resource "databricks_job" "data_ingestion"`` body.

    Uses brace-depth tracking so nested blocks (e.g. ``depends_on`` inside a
    ``task``) are skipped — only depth-2 blocks count (depth 1 = resource body,
    depth 2 = top-level child block).
    """
    lines = text.splitlines()
    keys: list[str] = []
    depth = 0
    in_resource = False
    current_block: str | None = None
    block_start_depth: int | None = None
    key_pattern = re.compile(rf'^\s*{re.escape(key_field)}\s*=\s*"([^"]+)"')
    resource_start = re.compile(r'^resource\s+"databricks_job"\s+"data_ingestion"\s*\{')
    block_start = re.compile(rf"^\s*{re.escape(block_type)}\s*\{{")

    for line in lines:
        if not in_resource:
            if resource_start.search(line):
                in_resource = True
                depth = 1
            continue
        open_braces = line.count("{")
        close_braces = line.count("}")
        if current_block is None and depth == 1 and block_start.match(line):
            current_block = block_type
            block_start_depth = depth + open_braces
        if current_block is not None and key_pattern.match(line) and depth == block_start_depth:
            m = key_pattern.match(line)
            assert m is not None
            keys.append(m.group(1))
            current_block = None
            block_start_depth = None
        depth += open_braces - close_braces
        if depth <= 0:
            break
    return keys


def test_environment_blocks_alphabetical() -> None:
    text = _MAIN_TF.read_text(encoding="utf-8")
    env_keys = _extract_top_level_block_keys(text, "environment", "environment_key")
    assert env_keys, "no top-level environment blocks found — parser regression?"
    assert env_keys == sorted(env_keys), (
        f"environment blocks must be sorted alphabetically to match Databricks "
        f"provider state-storage order. Got {env_keys}, expected {sorted(env_keys)}."
    )


def test_task_blocks_alphabetical() -> None:
    text = _MAIN_TF.read_text(encoding="utf-8")
    task_keys = _extract_top_level_block_keys(text, "task", "task_key")
    assert task_keys, "no top-level task blocks found — parser regression?"
    assert task_keys == sorted(task_keys), (
        f"task blocks must be sorted alphabetically by task_key. Got {task_keys}, expected {sorted(task_keys)}."
    )


def test_parser_returns_expected_count_sanity() -> None:
    """Anchor the parser against known counts so a future regex regression
    producing an empty list is caught as a parser bug, not a false pass."""
    text = _MAIN_TF.read_text(encoding="utf-8")
    env_keys = _extract_top_level_block_keys(text, "environment", "environment_key")
    task_keys = _extract_top_level_block_keys(text, "task", "task_key")
    # Verified live 2026-04-16: job has 7 environments + 29 tasks.
    assert len(env_keys) == 7, f"expected 7 environment blocks, parser found {len(env_keys)}"
    assert len(task_keys) == 29, f"expected 29 task blocks, parser found {len(task_keys)}"


_DEPENDS_ON_RE = re.compile(r'depends_on\s*\{\s*task_key\s*=\s*"([^"]+)"', re.MULTILINE)


def _extract_depends_on_by_task(text: str) -> dict[str, list[str]]:
    """Walk the data_ingestion resource, and for each top-level `task { ... }`
    block, collect the ordered list of `task_key` values from each nested
    `depends_on { ... }` block.

    Handles BOTH syntaxes:
      depends_on {
        task_key = "X"
      }
      depends_on { task_key = "X" }

    Returns {outer_task_key: [dep_task_key_1, dep_task_key_2, ...]}.
    """
    lines = text.splitlines(keepends=True)
    result: dict[str, list[str]] = {}
    depth = 0
    in_resource = False
    task_start_depth: int | None = None
    task_start_idx: int | None = None
    task_outer_key: str | None = None
    resource_start = re.compile(r'^resource\s+"databricks_job"\s+"data_ingestion"\s*\{')
    task_open = re.compile(r"^  task\s*\{\s*$")
    # Outer task_key is at indent 4 (inside resource at indent 2, task at indent 4).
    outer_task_key_re = re.compile(r'^    task_key\s*=\s*"([^"]+)"')

    for idx, line in enumerate(lines):
        if not in_resource:
            if resource_start.search(line):
                in_resource = True
                depth = 1
            continue
        opens = line.count("{")
        closes = line.count("}")
        if task_start_depth is None and depth == 1 and task_open.match(line):
            task_start_depth = depth + opens
            task_start_idx = idx
            task_outer_key = None
        elif task_start_depth is not None and task_outer_key is None:
            m = outer_task_key_re.match(line)
            if m:
                task_outer_key = m.group(1)

        depth += opens - closes

        if task_start_depth is not None and depth < task_start_depth:
            # Task block just closed. Extract depends_on deps from its body.
            assert task_start_idx is not None
            body = "".join(lines[task_start_idx : idx + 1])
            deps = _DEPENDS_ON_RE.findall(body)
            if task_outer_key and deps:
                result[task_outer_key] = deps
            task_start_depth = None
            task_start_idx = None
            task_outer_key = None
        if depth <= 0:
            break
    return result


def test_depends_on_blocks_alphabetical_within_each_task() -> None:
    """The Databricks provider matches nested depends_on blocks positionally
    too. Within each task, depends_on{} blocks must be sorted alphabetically
    by their referenced task_key."""
    text = _MAIN_TF.read_text(encoding="utf-8")
    deps = _extract_depends_on_by_task(text)
    assert deps, "no depends_on blocks found — parser regression?"
    errors: list[str] = []
    for task_key, dep_list in deps.items():
        if dep_list != sorted(dep_list):
            errors.append(f"task {task_key!r}: depends_on order {dep_list} is not sorted; expected {sorted(dep_list)}")
    assert not errors, "\n".join(errors)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
