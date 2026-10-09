"""Guard: the dq tables and views are created only through `dre install`.

CREATE TABLE/VIEW is allowed only in store/ddl.sql (read-only guard), and only
store/install.py (behind `dre install --apply`) and store/local_setup.py (tests
and local runs) may reference apply_ddl. `dre run` checks the store exists; it
never creates it.
"""

from __future__ import annotations

from pathlib import Path

from tests.conftest import PACKAGE_ROOT
from tests.guard.scan import files_mentioning

ALLOWED = {"store/install.py", "store/local_setup.py"}


def _offenders(root: Path) -> list[str]:
    return [f for f in files_mentioning(root, "apply_ddl") if f not in ALLOWED]


def test_only_install_path_applies_ddl() -> None:
    assert not _offenders(PACKAGE_ROOT), f"apply_ddl referenced outside {sorted(ALLOWED)}: {_offenders(PACKAGE_ROOT)}"


def test_guard_catches_a_reference(tmp_path: Path) -> None:
    (tmp_path / "store").mkdir()
    (tmp_path / "store" / "install.py").write_text("def apply_ddl(): ...\n", encoding="utf-8")
    (tmp_path / "run.py").write_text("from hcsc.datalake.dre.store.install import apply_ddl\n", encoding="utf-8")
    assert _offenders(tmp_path) == ["run.py"]
