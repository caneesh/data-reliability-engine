"""Guard: `dre run` never creates the dq database.

CREATE DATABASE is allowed only in store/local_setup.py (read-only guard), and
no other engine module may reference that module, so nothing reachable from
the CLI can create the database. Tests and local runs import it directly.
"""

from __future__ import annotations

from pathlib import Path

from tests.conftest import PACKAGE_ROOT
from tests.guard.scan import files_mentioning

SETUP = "store/local_setup.py"


def _offenders(root: Path) -> list[str]:
    return [f for f in files_mentioning(root, "local_setup") if f != SETUP]


def test_engine_never_references_local_setup() -> None:
    assert not _offenders(PACKAGE_ROOT), f"only tests may use {SETUP}: {_offenders(PACKAGE_ROOT)}"


def test_guard_catches_a_reference(tmp_path: Path) -> None:
    (tmp_path / "store").mkdir()
    (tmp_path / "store" / "local_setup.py").write_text("def create_database(): ...\n", encoding="utf-8")
    (tmp_path / "cli.py").write_text("from hcsc.datalake.dre.store.local_setup import create_database\n", encoding="utf-8")
    (tmp_path / "ok.py").write_text("from hcsc.datalake.dre.store import writer\n", encoding="utf-8")
    assert _offenders(tmp_path) == ["cli.py"]
