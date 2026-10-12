"""Static scanner behind the read-only and append-only guard tests.

Rules (hard rules 1 and 5, spec section 9, docs/decisions.md "Guard rules"):

- INSERT INTO is the only DML allowed, and only into the dq store.
- UPDATE, DELETE, MERGE, TRUNCATE and INSERT OVERWRITE are banned everywhere,
  dq included, as are DataFrame overwrite modes.
- CREATE TABLE and CREATE [OR REPLACE] VIEW are allowed on dq only, and only
  in store/ddl.sql (applied by `dre install`). CREATE DATABASE is allowed
  only for dq and only in store/local_setup.py. DROP TABLE/VIEW/DATABASE and
  every other ALTER are banned.
- ALTER TABLE <dq>.<t> DROP PARTITION is allowed only in store/retention.py.
- The DataFrame write APIs (.write, .writeTo, .writeStream, insertInto) are
  allowed only in store/writer.py; writeTo overwrite/create/replace is banned.
- DataFrame path writes, Hadoop FileSystem delete/rename/mkdirs, os/shutil
  file removal and moves, and `hdfs dfs` deletes, moves and mkdirs are flagged
  unless the target is the dq store. The dq store is a database addressed by
  table name, so in practice a path target never qualifies.

The dq database is named either as the literal `dq` or, because the real name
comes from config, through one of DQ_PLACEHOLDERS. A target the scanner cannot
read (for example one built by string concatenation) fails: write statements
so the target is visible in the text.
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


def scan_text(text: str, path: str = "<text>") -> list[Violation]:
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
    in_retention = norm.endswith(RETENTION_MODULE)

    if not norm.endswith(WRITER_MODULE):
        for m in _WRITE_API.finditer(text):
            add(m.start(), "write API", m.group("target"), f"allowed only in {WRITER_MODULE}")

    for m in _CREATE_OBJECT.finditer(text):
        target = m.group("target")
        if not _is_dq(target, False):
            add(m.start(), "CREATE", target, "allowed only on the dq store")
        elif not norm.endswith(DDL_FILE):
            add(m.start(), "CREATE", target, f"allowed only in {DDL_FILE} (applied by dre install)")

    for m in _CREATE_DATABASE.finditer(text):
        target = m.group("target")
        if not _is_dq(target, True):
            add(m.start(), "CREATE DATABASE", target, "allowed only for the dq database")
        elif not norm.endswith(LOCAL_SETUP_MODULE):
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
