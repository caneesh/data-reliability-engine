"""Landing listing (Hadoop FileSystem API on local files) and the dq_file registry."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from hcsc.datalake.dre.sources.hdfs import LandingError, list_landing
from hcsc.datalake.dre.store.files import known_files, register_files
from hcsc.datalake.dre.store.local_setup import create_store
from hcsc.datalake.dre.store.runs import new_run

UTC = timezone.utc


def landed(root: Path, name: str, content: str = "x") -> Path:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_lists_recursively_with_pattern_and_skips_hidden_files(spark, tmp_path) -> None:
    landed(tmp_path, "rt_0001.seq", "abc")
    landed(tmp_path, "2026-01-15/rt_0002.seq", "abcdef")
    landed(tmp_path, "2026-01-15/_SUCCESS")
    landed(tmp_path, "2026-01-15/.rt_0002.seq.crc")
    landed(tmp_path, "notes.txt")
    # A real sequence file in the landing folder, as the real-time feed lands them.
    spark.sparkContext.parallelize([("k", "v")]).saveAsSequenceFile(str(tmp_path / "seq_batch"))
    files = list_landing(spark, [str(tmp_path)], "*.seq")
    sizes = {Path(f.path).name: f.size_bytes for f in files}
    assert sizes == {"rt_0001.seq": 3, "rt_0002.seq": 6}  # the dated subfolder is listed too
    assert [f.path for f in files] == sorted(f.path for f in files)
    assert all(f.modified_at.tzinfo is not None for f in files)
    everything = [Path(f.path).name for f in list_landing(spark, [str(tmp_path)])]
    assert "part-00000" in everything and "_SUCCESS" not in everything


def test_missing_root_is_landing_unreadable(spark, tmp_path) -> None:
    with pytest.raises(LandingError) as caught:
        list_landing(spark, [str(tmp_path / "absent")])
    assert caught.value.code == "landing_unreadable"


def test_registry_appends_new_and_changed_files_only(spark, tmp_path) -> None:
    create_store(spark, "dq_landing")
    root = tmp_path / "landing"
    landed(root, "rt_0001.seq", "abc")
    landed(root, "rt_0002.seq", "abc")
    first_run, t1 = new_run(now=datetime(2026, 1, 15, 6, tzinfo=UTC)), datetime(2026, 1, 15, 6, tzinfo=UTC)
    assert register_files(spark, "dq_landing", "example_realtime", list_landing(spark, [str(root)]), first_run, t1) == 2

    # Nothing changed: nothing appended.
    t2 = t1 + timedelta(hours=4)
    assert register_files(spark, "dq_landing", "example_realtime", list_landing(spark, [str(root)]),
                          new_run(now=t2), t2) == 0

    # One file grows (still being written when first seen); one new file arrives.
    grown = landed(root, "rt_0002.seq", "abcdef")
    os.utime(grown, (grown.stat().st_atime, grown.stat().st_mtime + 60))
    landed(root, "rt_0003.seq", "abc")
    t3 = t2 + timedelta(hours=4)
    assert register_files(spark, "dq_landing", "example_realtime", list_landing(spark, [str(root)]),
                          new_run(now=t3), t3) == 2

    status = {Path(p).name: v for p, v in known_files(spark, "dq_landing", "example_realtime").items()}
    assert status["rt_0002.seq"][:2] == (t1, 6)  # first seen at t1, latest size 6
    assert status["rt_0003.seq"][0] == t3
    assert spark.table("dq_landing.dq_file").count() == 4
    assert known_files(spark, "dq_landing", "other_feed") == {}
