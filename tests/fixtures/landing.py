"""Synthetic landed files: real sequence files (spec section 9), and plain text files."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

_TEMPLATE: dict[str, bytes] = {}


def sequence_file_bytes(spark) -> bytes:
    """The bytes of a small, valid Hadoop sequence file with one synthetic record."""
    if "seq" not in _TEMPLATE:
        out = Path(tempfile.mkdtemp()) / "seq"
        spark.sparkContext.parallelize([("k1", "synthetic message")], 1).saveAsSequenceFile(str(out))
        _TEMPLATE["seq"] = next(out.glob("part-*")).read_bytes()
        shutil.rmtree(out.parent, ignore_errors=True)
    return _TEMPLATE["seq"]


def land_sequence_files(spark, folder: Path, *names: str) -> list[Path]:
    folder.mkdir(parents=True, exist_ok=True)
    paths = []
    for name in names:
        path = folder / name
        path.write_bytes(sequence_file_bytes(spark))
        paths.append(path)
    return paths
