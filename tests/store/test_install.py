"""dre install --print/--apply/--check, and dre run refusing to start without a store."""

from __future__ import annotations

import re
import shutil
from pathlib import Path

import pytest

from hcsc.datalake.dre.cli import main
from hcsc.datalake.dre.store.local_setup import create_database
from hcsc.datalake.dre.store.names import DQ_TABLES
from hcsc.datalake.dre.store.schema import VIEWS
from tests.conftest import REPO_ROOT


def conf_for(tmp_path: Path, database: str) -> str:
    conf = tmp_path / "conf"
    shutil.copytree(REPO_ROOT / "conf", conf)
    defaults = conf / "defaults.yaml"
    defaults.write_text(defaults.read_text(encoding="utf-8").replace("dq_database: dq\n", f"dq_database: {database}\n"),
                        encoding="utf-8")
    return str(conf)


def install(conf: str, mode: str, capsys) -> tuple[int, str]:
    code = main(["install", "--conf", conf, f"--{mode}"])
    return code, capsys.readouterr().out


def test_print_renders_ddl_for_configured_database(tmp_path, capsys) -> None:
    code, out = install(conf_for(tmp_path, "dq_printed"), "print", capsys)
    assert code == 0
    statements = [s.strip() for s in out.split(";") if s.strip()]
    assert len(statements) == len(DQ_TABLES) + len(VIEWS)
    for statement in statements:
        assert re.match(r"CREATE (TABLE|VIEW) IF NOT EXISTS dq_printed\.\w+", statement), statement


def test_install_mode_is_required(tmp_path) -> None:
    with pytest.raises(SystemExit):
        main(["install", "--conf", conf_for(tmp_path, "dq_x")])


def test_apply_then_check(spark, tmp_path, capsys) -> None:
    conf = conf_for(tmp_path, "dq_install_apply")
    create_database(spark, "dq_install_apply")  # the platform team's part
    assert install(conf, "check", capsys)[0] == 1  # empty database: everything missing

    code, out = install(conf, "apply", capsys)
    assert code == 0 and "in place" in out
    spark.sql("INSERT INTO dq_install_apply.dq_file PARTITION (run_date = DATE'2026-01-01') "
              "VALUES ('/data/landing/example_feed/f1', 'example_realtime', NULL, 1, NULL, NULL, 'r')")
    assert install(conf, "apply", capsys)[0] == 0  # safe to rerun
    assert spark.table("dq_install_apply.dq_file").count() == 1  # and keeps rows

    code, out = install(conf, "check", capsys)
    assert code == 0, out
    assert out.count("OK ") == len(DQ_TABLES) + len(VIEWS)
    assert "matches the DDL" in out


def test_check_reports_missing_and_different_objects(spark, tmp_path, capsys) -> None:
    conf = conf_for(tmp_path, "dq_install_drift")
    create_database(spark, "dq_install_drift")
    # An older dq_cause_result with a different shape, made before install (IF NOT EXISTS leaves it alone).
    spark.sql("CREATE TABLE dq_install_drift.dq_cause_result (run_id STRING) "
              "PARTITIONED BY (run_date DATE) STORED AS ORC")
    install(conf, "apply", capsys)
    spark.sql("DROP VIEW dq_install_drift.v_open_keys")

    code, out = install(conf, "check", capsys)
    assert code == 1
    assert re.search(r"DIFFERS\s+dq_install_drift\.dq_cause_result: columns", out)
    assert re.search(r"MISSING\s+dq_install_drift\.v_open_keys", out)
    assert "2 of" in out


def test_apply_and_check_need_the_database(spark, tmp_path, capsys) -> None:
    conf = conf_for(tmp_path, "dq_never_created")
    for mode in ("apply", "check"):
        code, out = install(conf, mode, capsys)
        assert code == 3
        assert "platform team creates the empty database" in out
    assert not spark.catalog.databaseExists("dq_never_created")  # install never creates it


def test_run_exits_3_naming_install_until_the_store_exists(spark, tmp_path, capsys) -> None:
    conf = conf_for(tmp_path, "dq_run_preflight")
    create_database(spark, "dq_run_preflight")
    assert main(["run", "--conf", conf]) == 3
    out = capsys.readouterr().out
    assert "not installed" in out and "dre install --apply" in out
    assert not spark.catalog.tableExists("dq_run_preflight.dq_run")  # run never creates the store

    install(conf, "apply", capsys)
    assert main(["run", "--conf", conf]) == 0  # store found: the run goes ahead
    out = capsys.readouterr().out
    assert "not installed" not in out
    # The sample's tables do not exist here: the check is DID_NOT_RUN, and the run still completes.
    assert "T1_KEY_DUPLICATES: DID_NOT_RUN (CONFIGURATION/table_missing)" in out
    assert "dre run: COMPLETED" in out


def test_run_exits_3_on_invalid_config(tmp_path, capsys) -> None:
    conf = conf_for(tmp_path, "dq_bad_conf")
    feed = Path(conf) / "feeds" / "example_realtime.yaml"
    feed.write_text(feed.read_text(encoding="utf-8").replace("pattern: FILE_CYCLIC", "pattern: NOPE"), encoding="utf-8")
    assert main(["run", "--conf", conf]) == 3
    assert "dre validate" in capsys.readouterr().out


def test_apply_reports_a_failing_statement(spark, tmp_path, capsys) -> None:
    conf = conf_for(tmp_path, "dq_install_broken")
    create_database(spark, "dq_install_broken")
    # A dq_run without a status column: the v_latest_run view cannot be created over it.
    spark.sql("CREATE TABLE dq_install_broken.dq_run (run_id STRING) PARTITIONED BY (run_date DATE) STORED AS ORC")
    code, out = install(conf, "apply", capsys)
    assert code == 1
    assert "failed at `CREATE VIEW IF NOT EXISTS dq_install_broken.v_latest_run" in out
    assert "dre install --check" in out
