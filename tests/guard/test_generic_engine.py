"""Guard: the engine is generic (hard rule 3, step 6 review).

Engine code, the config schema, pattern YAML and SQL templates (everything under
src/) must not mention source-specific words. They may appear only in conf/ and
tests/. Add a word here whenever a source-specific name is discovered.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.conftest import SRC_ROOT

# Source-specific words, matched case-insensitively as whole words.
DENYLIST = ("stopper", "handoff", "file_date", "sub_id", "rms")
_SCANNED = {".py", ".yaml", ".yml", ".sql", ".j2", ".json", ".txt", ".toml", ".cfg"}
_WORDS = re.compile(r"(?<![A-Za-z0-9])(" + "|".join(re.escape(w) for w in DENYLIST) + r")(?![A-Za-z0-9])", re.I)


def source_specific(text: str) -> list[str]:
    return [m.group(1).lower() for m in _WORDS.finditer(text)]


def scan(root: Path) -> list[str]:
    hits = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.suffix in _SCANNED and "__pycache__" not in path.parts:
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                for word in source_specific(line):
                    hits.append(f"{path.relative_to(root.parent)}:{number}: {word}")
    return hits


def test_engine_has_no_source_specific_words() -> None:
    hits = scan(SRC_ROOT)
    assert not hits, "source-specific words in engine code (move them to conf/):\n" + "\n".join(hits)


@pytest.mark.parametrize("text", [
    "stopper_file: str | None = None", "partition_handoff_file", "PARTITION BY file_date",
    "regexp_replace(sub_id, '^0+', '')", "feed_filter: src = 'RMS'", "the rms feed",
])
def test_denylist_catches(text: str) -> None:
    assert source_specific(text), text


@pytest.mark.parametrize("text", [
    "transforms the terms in forms", "sub_identifier", "file_dates_seen", "load_hold_marker", "partition_cursor",
])
def test_denylist_allows(text: str) -> None:
    assert source_specific(text) == [], text
