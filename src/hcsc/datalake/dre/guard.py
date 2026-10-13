"""The static scanner behind the read-only and append-only guards (hard rules 1 and 5, spec
section 9, docs/decisions.md "Guard rules"), inside the package so `dre guard <path>` can run it
over an extension's source (spec section 2, "Extension packages"). tests/guard/scan.py re-exports it.

What it enforces, in words (the statement forms themselves are in the patterns below):

- Appending rows is the only data change allowed, and only into the dq store.
- Updating, deleting, merging, truncating and overwriting are banned everywhere, dq included,
  as are DataFrame overwrite modes.
- Creating tables and views is allowed on dq only, and only in store/ddl.sql (applied by
  `dre install`). Creating a database is allowed only for dq and only in store/local_setup.py.
  Dropping tables, views or databases and every other ALTER are banned.
- Dropping a partition of a dq table is allowed only in store/retention.py.
- The DataFrame write APIs are allowed only in store/writer.py; writeTo with overwrite, create
  or replace is banned.
- DataFrame path writes, Hadoop FileSystem delete, rename and mkdirs, os/shutil file removal
  and moves, and `hdfs dfs` deletes, moves and mkdirs are flagged unless the target is the dq
  store. The dq store is a database addressed by table name, so a path target never qualifies.

The dq database is named either as the literal `dq` or, because the real name comes from
config, through one of DQ_PLACEHOLDERS. A target the scanner cannot read (for example one
built by string concatenation) fails: write statements so the target is visible in the text.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

DQ_DATABASE = "dq"
# Ways engine code may name the dq database when the real name comes from config.
DQ_PLACEHOLDERS = ("{{dq_database}}", "{dq_database}")
# Modules with a path-based allowance (see the rules above).
RETENTION_MODULE = "hcsc/datalake/dre/store/retention.py"
WRITER_MODULE = "hcsc/datalake/dre/store/writer.py"
LOCAL_SETUP_MODULE = "hcsc/datalake/dre/store/local_setup.py"
DDL_FILE = "hcsc/datalake/dre/store/ddl.sql"
# This module: its text is the patterns themselves, so it is not scanned (a test proves it holds
# no Spark or file operations of its own).
GUARD_MODULE = "hcsc/datalake/dre/guard.py"

# A target is either a Jinja expression (which may contain spaces) followed by
# the rest of a dotted name, or a plain run of non-space characters.
_TARGET = r"(?P<target>\{\{\s*\w+\s*\}\}[^\s(;,]*|[^\s(;,]+)"
_ARG = r"\(\s*(?P<target>[^,)]*)"  # first call argument
_I = re.I

# Banned everywhere, whatever the target.
_BANNED: list[tuple[str, re.Pattern[str]]] = [
    ("INSERT OVERWRITE", re.compile(rf"\bINSERT\s+OVERWRITE\s+(?:TABLE\s+)?{_TARGET}", _I)),
    ("MERGE", re.compile(rf"\bMERGE\s+INTO\s+{_TARGET}", _I)),
    ("UPDATE", re.compile(rf"\bUPDATE\s+{_TARGET}\s+SET\b", _I)),
    ("DELETE", re.compile(rf"\bDELETE\s+FROM\s+{_TARGET}", _I)),
    ("TRUNCATE", re.compile(rf"\bTRUNCATE\s+TABLE\s+{_TARGET}", _I)),
    ("DROP", re.compile(rf"\bDROP\s+(?:TABLE|VIEW|DATABASE|SCHEMA)\s+(?:IF\s+EXISTS\s+)?{_TARGET}", _I)),
    ("ALTER", re.compile(rf"\bALTER\s+(?:VIEW|DATABASE|SCHEMA)\s+{_TARGET}", _I)),
    ("saveAsTable", re.compile(rf"\.saveAsTable{_ARG}")),
    ("writeTo overwrite/create", re.compile(
        r"(?P<target>\.writeTo\((?:[^()]|\([^()]*\))*\)(?:[\s\\]*\.\s*\w+\((?:[^()]|\([^()]*\))*\))*[\s\\]*\.\s*"
        r"(?:overwrite|overwritePartitions|create|replace|createOrReplace)\()"
    )),
    ("overwrite mode", re.compile(r"(?P<target>\.mode\(\s*[\"']overwrite[\"']\s*\)|\bmode\s*=\s*[\"']overwrite[\"'])", _I)),
    ("overwrite=True", re.compile(r"(?P<target>\boverwrite\s*=\s*True\b)")),
    ("HDFS", re.compile(
        r"(?P<target>\b(?:hdfs\s+dfs|hadoop\s+fs)\s+-(?:rm|rmr|rmdir|mv|mkdir)\b"
        r"|[\"'](?:-rm|-rmr|-rmdir|-mv|-mkdir)[\"'])"
    )),
]

# Allowed only when the target is the dq store: (label, pattern, target_is_database).
_DQ_ONLY: list[tuple[str, re.Pattern[str], bool]] = [
    ("INSERT INTO", re.compile(rf"\bINSERT\s+INTO\s+(?:TABLE\s+)?{_TARGET}", _I), False),
    ("insertInto", re.compile(rf"\.insertInto{_ARG}"), False),
    ("writeTo", re.compile(rf"\.writeTo{_ARG}"), False),
    # DataFrameWriter path writes: df.write[.option(..)...].save/orc/parquet/csv/json/text(path)
    ("path write", re.compile(
        r"\.write(?:Stream)?(?:[\s\\]*\.\s*\w+\((?:[^()]|\([^()]*\))*\))*[\s\\]*\.\s*"
        rf"(?:save|orc|parquet|csv|json|text){_ARG}"
    ), False),
    ("FileSystem", re.compile(rf"\.(?:delete|rename|mkdirs){_ARG}"), False),
    ("os/shutil", re.compile(rf"\b(?:os\.(?:remove|unlink|rmdir|removedirs|rename|replace)|shutil\.(?:rmtree|move)){_ARG}"), False),
]

_CREATE_OBJECT = re.compile(
    rf"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?{_TARGET}", _I
)
_CREATE_DATABASE = re.compile(rf"\bCREATE\s+(?:DATABASE|SCHEMA)\s+(?:IF\s+NOT\s+EXISTS\s+)?{_TARGET}", _I)
_WRITE_API = re.compile(r"(?P<target>\.(?:write|writeTo|writeStream|insertInto)\b)")
_ALTER_TABLE = re.compile(rf"\bALTER\s+TABLE\s+{_TARGET}", _I)
_DROP_PARTITION_TAIL = re.compile(r"\s+DROP\s+(?:IF\s+EXISTS\s+)?PARTITION\b", _I)

_SCANNED_SUFFIXES = {".py", ".sql", ".j2", ".yaml", ".yml", ".sh", ".txt", ".cfg", ".toml", ".json"}


@dataclass(frozen=True)
class Violation:
    path: str
    line: int
    statement: str
    target: str
    reason: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.statement} {self.target!r}: {self.reason}"


def _clean(target: str) -> str:
    t = re.sub(r"^[rbfu]{0,2}[\"']", "", target.strip(), flags=re.I)  # opening quote and prefix
    t = t.rstrip("\"')")  # closing quote, and the call's closing paren
    return re.sub(r"[\s`]+", "", t)  # whitespace and identifier backticks


def _is_dq(target: str, is_database: bool) -> bool:
    t = _clean(target).lower()
    names = (DQ_DATABASE, *DQ_PLACEHOLDERS)
    if is_database:
        return t in names
    return any(t.startswith(f"{name}.") for name in names)


def scan_text(text: str, path: str = "<text>", allowances: bool = True) -> list[Violation]:
    """Violations in one file's text. `allowances` applies the engine's own path allowances (the
    writer, the DDL file, retention, local setup); `dre guard` scans other trees without them."""
    found: list[Violation] = []

    def add(pos: int, label: str, target: str, reason: str) -> None:
        found.append(Violation(path, text.count("\n", 0, pos) + 1, label, _clean(target), reason))

    for label, pattern in _BANNED:
        for m in pattern.finditer(text):
            add(m.start(), label, m.group("target"), "banned everywhere (read-only, append-only)")

    for label, pattern, is_database in _DQ_ONLY:
        for m in pattern.finditer(text):
            if not _is_dq(m.group("target"), is_database):
                add(m.start(), label, m.group("target"), "allowed only on the dq store")

    norm = path.replace("\\", "/")
    allowed = (lambda module: allowances and (norm == module or norm.endswith("/" + module)))  # noqa: E731
    in_retention = allowed(RETENTION_MODULE)

    if not allowed(WRITER_MODULE):
        for m in _WRITE_API.finditer(text):
            add(m.start(), "write API", m.group("target"), f"allowed only in {WRITER_MODULE}")

    for m in _CREATE_OBJECT.finditer(text):
        target = m.group("target")
        if not _is_dq(target, False):
            add(m.start(), "CREATE", target, "allowed only on the dq store")
        elif not allowed(DDL_FILE):
            add(m.start(), "CREATE", target, f"allowed only in {DDL_FILE} (applied by dre install)")

    for m in _CREATE_DATABASE.finditer(text):
        target = m.group("target")
        if not _is_dq(target, True):
            add(m.start(), "CREATE DATABASE", target, "allowed only for the dq database")
        elif not allowed(LOCAL_SETUP_MODULE):
            add(m.start(), "CREATE DATABASE", target, f"allowed only in {LOCAL_SETUP_MODULE}")

    for m in _ALTER_TABLE.finditer(text):
        target = m.group("target")
        if _DROP_PARTITION_TAIL.match(text, m.end()):
            if not _is_dq(target, False):
                add(m.start(), "DROP PARTITION", target, "allowed only on the dq store")
            elif not in_retention:
                add(m.start(), "DROP PARTITION", target, f"allowed only in {RETENTION_MODULE}")
        else:
            add(m.start(), "ALTER", target, "banned everywhere; only retention may DROP PARTITION")

    return sorted(found, key=lambda v: (v.path, v.line))


def scan_tree(root: Path, allowances: bool = True) -> list[Violation]:
    """Violations under a tree. With `allowances` (the engine's own scan from src/) the engine's
    privileged files are allowed their one operation each and the scanner module is skipped;
    without them (`dre guard` on any other tree) every file is held to every rule."""
    found: list[Violation] = []
    for file in sorted(root.rglob("*")):
        if not file.is_file() or file.suffix not in _SCANNED_SUFFIXES or "__pycache__" in file.parts:
            continue
        rel = file.relative_to(root.parent).as_posix()
        if allowances and rel.endswith("/" + GUARD_MODULE):
            continue
        found.extend(scan_text(file.read_text(encoding="utf-8", errors="replace"), rel, allowances))
    return found


def files_mentioning(root: Path, needle: str) -> list[str]:
    """Scanned files under root whose text contains needle (used by the email and local-setup guards)."""
    return sorted(
        f.relative_to(root).as_posix()
        for f in root.rglob("*")
        if f.is_file() and f.suffix in _SCANNED_SUFFIXES and "__pycache__" not in f.parts
        and needle in f.read_text(encoding="utf-8", errors="replace")
    )


def guard_command(root: Path) -> int:
    """`dre guard <path>`: scan a source tree (an extension package, say) and print each violation;
    exit 1 when there is any. The same rules the engine's own CI enforces."""
    if not root.exists():
        print(f"dre guard: {root} does not exist")
        return 3
    if root.is_dir():
        violations = scan_tree(root, allowances=False)
    else:
        violations = scan_text(root.read_text(encoding="utf-8", errors="replace"), root.name, allowances=False)
    for violation in violations:
        print(violation)
    print(f"dre guard: {len(violations)} violation(s) under {root}")
    return 1 if violations else 0
