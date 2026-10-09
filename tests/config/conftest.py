"""Helpers for config tests: copy the sample conf/ and break it on purpose."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from tests.conftest import REPO_ROOT

SAMPLE_CONF = REPO_ROOT / "conf"


@pytest.fixture
def conf(tmp_path: Path) -> Path:
    """A writable copy of the sample configuration."""
    target = tmp_path / "conf"
    shutil.copytree(SAMPLE_CONF, target)
    return target


def edit(conf: Path, rel: str, old: str, new: str) -> None:
    path = conf / rel
    text = path.read_text(encoding="utf-8")
    assert old in text, f"{old!r} not in {rel}"
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def line_with(conf: Path, rel: str, needle: str) -> int:
    lines = (conf / rel).read_text(encoding="utf-8").splitlines()
    return next(i for i, line in enumerate(lines, 1) if needle in line)
