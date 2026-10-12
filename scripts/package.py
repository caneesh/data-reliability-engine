"""Build the release artifacts into dist/ (spec section 10, step 11).

- hcsc_datalake_dre-<version>-py3-none-any.whl: the wheel (pip install on the driver host).
- dre-pyfiles-<version>.zip: the package alone, for spark-submit --py-files (executors import
  it for the key-hash function; it carries the SQL, pattern and email templates).
- dre-deps-<version>-<platform>.zip: wheels of the runtime dependencies other than PySpark
  (which the cluster provides), for an offline `pip install --no-index` on the driver host.
  pydantic needs its compiled core, so the dependencies are installed, not shipped in --py-files.
- dre_main.py: the spark-submit entry point.

    python scripts/package.py [--platform manylinux2014_x86_64] [--python-version 3.10] [--no-deps]
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUNTIME_DEPS = ["pydantic>=2,<3", "PyYAML>=6", "Jinja2>=3.1"]  # pyproject.toml, without pyspark
NAMESPACE_INIT = "__path__ = __import__('pkgutil').extend_path(__path__, __name__)\n"


def version() -> str:
    sys.path.insert(0, str(ROOT / "src"))
    from hcsc.datalake.dre import __version__

    return __version__


def build_wheel(dist: Path) -> Path:
    subprocess.run([sys.executable, "-m", "pip", "wheel", "--no-deps", "--quiet", "-w", str(dist), str(ROOT)],
                   check=True)
    return next(dist.glob("hcsc_datalake_dre-*.whl"))


def build_pyfiles(dist: Path, ver: str) -> Path:
    target = dist / f"dre-pyfiles-{ver}.zip"
    src = ROOT / "src"
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as zf:
        # Python's zip importer cannot resolve native namespace packages, so inside the zip only,
        # hcsc/ and hcsc/datalake/ get pkgutil-style __init__.py files, which still merge with
        # other HCSC tools' portions. The source keeps native namespace packages (CLAUDE.md).
        for namespace in ("hcsc", "hcsc/datalake"):
            zf.writestr(f"{namespace}/__init__.py", NAMESPACE_INIT)
        for path in sorted((src / "hcsc").rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc":
                zf.write(path, path.relative_to(src).as_posix())
    return target


def build_deps(dist: Path, ver: str, platform: str, python_version: str) -> Path:
    target = dist / f"dre-deps-{ver}-{platform}.zip"
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run([sys.executable, "-m", "pip", "download", "--quiet", "--only-binary=:all:",
                        "--platform", platform, "--python-version", python_version, "-d", tmp, *RUNTIME_DEPS],
                       check=True)
        with zipfile.ZipFile(target, "w", zipfile.ZIP_STORED) as zf:
            for wheel in sorted(Path(tmp).glob("*.whl")):
                zf.write(wheel, wheel.name)
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build dre release artifacts into dist/")
    parser.add_argument("--dist", default=str(ROOT / "dist"))
    parser.add_argument("--platform", default="manylinux2014_x86_64", help="the cluster's pip platform tag")
    parser.add_argument("--python-version", default="3.10", help="the cluster's Python version")
    parser.add_argument("--no-deps", action="store_true", help="skip the dependency wheels (needs no network)")
    args = parser.parse_args(argv)
    dist = Path(args.dist)
    if dist.exists():
        shutil.rmtree(dist)
    dist.mkdir(parents=True)
    ver = version()
    built = [build_wheel(dist), build_pyfiles(dist, ver)]
    if not args.no_deps:
        built.append(build_deps(dist, ver, args.platform, args.python_version))
    shutil.copy(ROOT / "scripts" / "dre_main.py", dist / "dre_main.py")
    built.append(dist / "dre_main.py")
    for path in built:
        print(f"{path.relative_to(dist.parent) if dist.parent in path.parents else path} ({path.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
