"""Guard: package layout conventions from CLAUDE.md and spec section 2."""

from __future__ import annotations

from tests.conftest import PACKAGE_ROOT, SRC_ROOT


def test_namespace_packages_have_no_init() -> None:
    assert not (SRC_ROOT / "hcsc" / "__init__.py").exists()
    assert not (SRC_ROOT / "hcsc" / "datalake" / "__init__.py").exists()
    assert (PACKAGE_ROOT / "__init__.py").exists()


def test_package_imports_through_namespace() -> None:
    import hcsc
    import hcsc.datalake
    import hcsc.datalake.dre as dre

    assert getattr(hcsc, "__file__", None) is None
    assert getattr(hcsc.datalake, "__file__", None) is None
    assert dre.__version__
