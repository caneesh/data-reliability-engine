"""Static scanner behind the read-only guard test (spec section 9, hard rule 1).

It looks for write statements in source text and reports each one whose
target is not the dq store. The store's database is referenced either as the
literal `dq` or, because its real name comes from config, through one of the
placeholders below. Any other target fails, including a target the scanner
cannot read (for example a table name built by string concatenation): build
write statements so the target is visible in the text.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

DQ_DATABASE = "dq"
# Ways engine code may name the dq database when the real name comes from config.
DQ_PLACEHOLDERS = ("{{dq_database}}", "{dq_database}")

# A target is either a Jinja expression (which may contain spaces) followed by
# the rest of a dotted name, or a plain run of non-space characters.
_TARGET = r"(?P<target>\{\{\s*\w+\s*\}\}[^\s(;,]*|[^\s(;,]+)"

# (label, pattern, target_is_database)
_SQL_WRITES: list[tuple[str, re.Pattern[str], bool]] = [
    ("INSERT", re.compile(rf"\bINSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?{_TARGET}", re.I), False),
    ("MERGE", re.compile(rf"\bMERGE\s+INTO\s+{_TARGET}", re.I), False),
    ("UPDATE", re.compile(rf"\bUPDATE\s+{_TARGET}\s+SET\b", re.I), False),
    ("DELETE", re.compile(rf"\bDELETE\s+FROM\s+{_TARGET}", re.I), False),
    ("DROP", re.compile(rf"\bDROP\s+(?:TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?{_TARGET}", re.I), False),
    ("DROP", re.compile(rf"\bDROP\s+(?:DATABASE|SCHEMA)\s+(?:IF\s+EXISTS\s+)?{_TARGET}", re.I), True),
    ("ALTER", re.compile(rf"\bALTER\s+(?:TABLE|VIEW)\s+{_TARGET}", re.I), False),
    ("ALTER", re.compile(rf"\bALTER\s+(?:DATABASE|SCHEMA)\s+{_TARGET}", re.I), True),
    ("TRUNCATE", re.compile(rf"\bTRUNCATE\s+TABLE\s+{_TARGET}", re.I), False),
    (
        "CREATE",
        re.compile(
            rf"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+)?(?:TABLE|VIEW)\s+"
            rf"(?:IF\s+NOT\s+EXISTS\s+)?{_TARGET}",
            re.I,
        ),
        False,
    ),
    (
        "CREATE",
        re.compile(rf"\bCREATE\s+(?:DATABASE|SCHEMA)\s+(?:IF\s+NOT\s+EXISTS\s+)?{_TARGET}", re.I),
        True,
    ),
    # DataFrame writer calls: the first argument is the target table.
    ("WRITER", re.compile(r"\.(?:saveAsTable|insertInto)\(\s*(?P<target>[^,)]+)"), False),
]

# HDFS moves and deletes are never allowed: the dq store is a database, not a path.
_HDFS_WRITES = re.compile(
    r"\b(?:hdfs\s+dfs|hadoop\s+fs)\s+-(?:rm|rmr|rmdir|mv)\b"
    r"|[\"'](?:-rm|-rmr|-rmdir|-mv)[\"']"
)

_SCANNED_SUFFIXES = {".py", ".sql", ".j2", ".yaml", ".yml", ".sh", ".txt", ".cfg", ".toml", ".json"}


@dataclass(frozen=True)
class Violation:
    path: str
    line: int
    statement: str
    target: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.statement} targets {self.target!r}, not the dq store"


def _clean(target: str) -> str:
    t = re.sub(r"^[rbfu]{0,2}[\"']", "", target.strip(), flags=re.I)  # opening quote and prefix
    t = t.rstrip("\"'")
    return re.sub(r"[\s`]+", "", t)  # whitespace and identifier backticks


def _is_dq(target: str, is_database: bool) -> bool:
    t = _clean(target)
    names = (DQ_DATABASE, *DQ_PLACEHOLDERS)
    if is_database:
        return t.lower() in names
    return any(t.lower().startswith(f"{name}.") for name in names)


def scan_text(text: str, path: str = "<text>") -> list[Violation]:
    found: list[Violation] = []

    def line_of(pos: int) -> int:
        return text.count("\n", 0, pos) + 1

    for label, pattern, is_database in _SQL_WRITES:
        for m in pattern.finditer(text):
            target = m.group("target")
            if not _is_dq(target, is_database):
                found.append(Violation(path, line_of(m.start()), label, _clean(target)))
    for m in _HDFS_WRITES.finditer(text):
        found.append(Violation(path, line_of(m.start()), "HDFS", m.group(0)))
    return sorted(found, key=lambda v: (v.path, v.line))


def scan_tree(root: Path) -> list[Violation]:
    found: list[Violation] = []
    for file in sorted(root.rglob("*")):
        if not file.is_file() or file.suffix not in _SCANNED_SUFFIXES or "__pycache__" in file.parts:
            continue
        rel = file.relative_to(root.parent).as_posix()
        found.extend(scan_text(file.read_text(encoding="utf-8"), rel))
    return found


def files_mentioning(root: Path, needle: str) -> list[str]:
    """Files under root whose text contains needle (used by the email guard)."""
    return sorted(
        f.relative_to(root).as_posix()
        for f in root.rglob("*")
        if f.is_file() and "__pycache__" not in f.parts and needle in f.read_text(encoding="utf-8")
    )
