"""Release artifacts (build step 11): the --py-files zip carries the package with its templates
and imports from the zip alone; the wheel ships the same package data."""

from __future__ import annotations

import subprocess
import sys
import zipfile

from tests.conftest import REPO_ROOT

sys.path.insert(0, str(REPO_ROOT / "scripts"))
import package  # noqa: E402

DATA = ["hcsc/datalake/dre/checks/sql/t1_due_loads.sql.j2", "hcsc/datalake/dre/patterns/file_cyclic.yaml",
        "hcsc/datalake/dre/store/ddl.sql", "hcsc/datalake/dre/notify/digest.txt.j2"]


def test_pyfiles_zip_holds_the_package_and_imports_from_the_zip_alone(tmp_path) -> None:
    pyfiles = package.build_pyfiles(tmp_path, package.version())
    names = zipfile.ZipFile(pyfiles).namelist()
    assert all(name in names for name in DATA)
    assert not [n for n in names if "__pycache__" in n or n.endswith(".pyc") or not n.startswith("hcsc/")]
    # pkgutil-style namespace packages, only in the zip (zipimport cannot resolve native ones).
    for namespace in ("hcsc/__init__.py", "hcsc/datalake/__init__.py"):
        assert "extend_path" in zipfile.ZipFile(pyfiles).read(namespace).decode()
        assert not (REPO_ROOT / "src" / namespace).exists()
    # A fresh interpreter without site hooks (so the editable install cannot answer): the zip first,
    # then site-packages for the dependencies only.
    import sysconfig

    probe = (
        "import sys; sys.path[:0] = [sys.argv[1], sys.argv[2]];"
        "from hcsc.datalake.dre.checks.base import render_sql;"
        "from hcsc.datalake.dre.store.schema import render_ddl;"
        "from hcsc.datalake.dre.checks.registry import pattern_causes;"
        "import hcsc.datalake.dre.checks.base as b; assert b.__file__.startswith(sys.argv[1]), b.__file__;"
        "assert 'MAX' in render_sql('cause_max_load.sql.j2', table='t', feed_filter=None, load_expr='x');"
        "assert render_ddl('dq') and pattern_causes('FILE_CYCLIC'); print('ok')"
    )
    out = subprocess.run([sys.executable, "-I", "-S", "-c", probe, str(pyfiles), sysconfig.get_paths()["purelib"]],
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr[-800:]
    assert out.stdout.strip() == "ok"


def test_wheel_ships_the_package_data(tmp_path) -> None:
    wheel = package.build_wheel(tmp_path)
    names = zipfile.ZipFile(wheel).namelist()
    assert all(name in names for name in DATA)
    assert not [n for n in names if n.startswith(("tests/", "conf/"))]
