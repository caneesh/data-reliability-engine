"""Guard: email text never contains key_value (hard rule 6, spec section 9).

Until the digest builder exists (build step 9) this is a static check: nothing
in the notify package may reference key_value. Step 9 adds a test on the
rendered email text alongside it.
"""

from __future__ import annotations

from pathlib import Path

from tests.conftest import PACKAGE_ROOT
from tests.guard.scan import files_mentioning

NOTIFY_ROOT = PACKAGE_ROOT / "notify"


def test_notify_package_never_references_key_value() -> None:
    offenders = files_mentioning(NOTIFY_ROOT, "key_value")
    assert not offenders, f"notify/ must not reference key_value: {offenders}"


def test_guard_catches_key_value_reference(tmp_path: Path) -> None:
    (tmp_path / "email.py").write_text("line = f'{row.key_value}'\n", encoding="utf-8")
    (tmp_path / "ok.py").write_text("line = f'{row.key_hash}'\n", encoding="utf-8")
    assert files_mentioning(tmp_path, "key_value") == ["email.py"]
