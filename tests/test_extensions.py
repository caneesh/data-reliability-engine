"""Extension packages (spec section 2): an installed package registers capabilities through an
entry point in the group dre.extensions, loads only when defaults.yaml lists it, passes the
testing kit's contracts, and is held to the guard rules by `dre guard`."""

from __future__ import annotations

import importlib
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from hcsc.datalake.dre import extensions, testing
from hcsc.datalake.dre.causes.base import BUILTINS, CauseContext, Failure
from hcsc.datalake.dre.checks.calendars import CALENDARS
from hcsc.datalake.dre.checks.keys import NORMALISERS
from hcsc.datalake.dre.checks.registry import CHECKS
from hcsc.datalake.dre.checks.rules.template import TEMPLATES
from hcsc.datalake.dre.cli import main
from hcsc.datalake.dre.config.loader import load
from hcsc.datalake.dre.config.models import Dataset
from hcsc.datalake.dre.config.probes import PROBES
from hcsc.datalake.dre.config.validate import validate_conf
from hcsc.datalake.dre.notify.sinks import SINKS
from tests.config.conftest import SAMPLE_CONF, edit, line_with
from tests.fixtures.layers import GOLD_COLUMNS, create_table, gold_row

UTC = timezone.utc
EVENT = testing.event("gold", datetime(2026, 1, 15, 12), datetime(2026, 1, 16, 12))
DIST = "dre_ext_example-0.1.dist-info"


@pytest.fixture
def installed(tmp_path: Path, monkeypatch) -> Path:
    """The example extension, installed the way pip would: a dist-info with its entry point on sys.path."""
    site = tmp_path / "site"
    (site / DIST).mkdir(parents=True)
    (site / DIST / "METADATA").write_text("Metadata-Version: 2.1\nName: dre-ext-example\nVersion: 0.1\n")
    (site / DIST / "entry_points.txt").write_text("[dre.extensions]\nexample = tests.extensions.dre_ext_example\n")
    monkeypatch.syspath_prepend(str(site))
    importlib.invalidate_caches()
    yield site
    extensions.unload("example")
    assert "X_ROWS_AT_MOST" not in CHECKS and "x_flag" not in PROBES and "x_at_most_rows" not in TEMPLATES


@pytest.fixture
def conf(tmp_path: Path) -> Path:
    target = tmp_path / "conf"
    shutil.copytree(SAMPLE_CONF, target)
    return target


def gold(table: str, **extra) -> Dataset:
    return Dataset(dataset="gold", table=table, layer="GOLD", key=["sub_id"], **extra)


def test_an_extension_loads_by_name_and_registers_one_of_each_kind(installed) -> None:
    assert "example" in extensions.available()
    loaded, problems = extensions.load_extensions(["example", "nope"])
    [record] = loaded
    assert record.name == "example" and record.module == "tests.extensions.dre_ext_example"
    assert record.registered == {"checks": ["X_ROWS_AT_MOST"], "probes": ["x_flag"], "builtin causes": ["X_ALWAYS"],
                                 "rule templates": ["x_at_most_rows"], "normalisers": ["x_reverse"],
                                 "calendars": ["X_ODD_DAYS"], "sinks": ["x_pigeon"]}
    assert "checks X_ROWS_AT_MOST" in record.summary()
    [problem] = problems
    assert "extension 'nope' is not installed" in problem and "example" in problem
    assert "X_ROWS_AT_MOST" in CHECKS and "X_ALWAYS" in BUILTINS and "X_ODD_DAYS" in CALENDARS and "x_pigeon" in SINKS
    again, _ = extensions.load_extensions(["example"])  # idempotent: nothing registers twice
    assert again == loaded and extensions.loaded() == loaded


def test_nothing_loads_unless_defaults_lists_it(installed, conf: Path) -> None:
    config, errors = load(conf)
    assert errors == [] and "X_ROWS_AT_MOST" not in CHECKS
    edit(conf, "datasets/gold_member_coverage.yaml", "strip_leading_zeros", "x_reverse")
    _, errors, _ = validate_conf(conf)
    assert any("'x_reverse' is not allowed" in e.problem for e in errors)
    (conf / "defaults.yaml").write_text((conf / "defaults.yaml").read_text() + "extensions: [example]\n")
    _, errors, _ = validate_conf(conf)
    assert errors == [], "\n".join(map(str, errors))
    assert "x_reverse" in NORMALISERS


def test_a_missing_extension_is_a_config_error_naming_the_line(conf: Path) -> None:
    (conf / "defaults.yaml").write_text((conf / "defaults.yaml").read_text() + "extensions: [absent]\n")
    _, errors, _ = validate_conf(conf)
    [e] = [e for e in errors if e.field == "extensions"]
    assert "extension 'absent' is not installed" in e.problem and e.line == line_with(conf, "defaults.yaml", "extensions:")


def test_validate_prints_what_an_extension_registered(installed, conf: Path, capsys) -> None:
    (conf / "defaults.yaml").write_text((conf / "defaults.yaml").read_text() + "extensions: [example]\n")
    assert main(["validate", "--conf", str(conf)]) == 0
    out = capsys.readouterr().out
    assert "extensions: example (tests.extensions.dre_ext_example): checks X_ROWS_AT_MOST" in out


# --- the testing kit, on the example extension ---

def test_the_kit_holds_the_example_check_to_the_contract(spark, installed) -> None:
    extensions.load_extensions(["example"])
    create_table(spark, "kit.three", GOLD_COLUMNS, [gold_row(sub_id=s) for s in ("1", "2", "3")])
    create_table(spark, "kit.one", GOLD_COLUMNS, [gold_row()])
    create_table(spark, "kit.none", GOLD_COLUMNS, [])
    check = CHECKS["X_ROWS_AT_MOST"]
    results = testing.check_contract(spark, check, gold("kit.three"), gold("kit.one"), gold("kit.none"), EVENT)
    assert results["failing"][0].violations == 1
    with pytest.raises(AssertionError, match="expected a FAILED result"):
        testing.check_contract(spark, check, gold("kit.one"), gold("kit.one"), gold("kit.none"), EVENT)


def test_the_kit_holds_the_example_probe_and_template_to_their_contracts(spark, installed, conf: Path) -> None:
    extensions.load_extensions(["example"])
    config, _ = load(conf)
    feed = config.feeds["example_realtime"].model_copy(update={"probes": {"flag": {"value": "yes"}}})
    ds = config.datasets["gold_member_coverage"]
    cx = CauseContext(testing.context(spark, ds, feed), EVENT, config, "T1_ZERO_ROWS")
    outcome = testing.probe_contract(cx, "x_flag", "flag", Failure(ref="2026-01-15T12:00:00+00:00", slot=EVENT.window_start))
    assert outcome.state == "CONFIRMED"
    create_table(spark, "kit.rows", GOLD_COLUMNS, [gold_row()])
    columns = testing.template_contract(spark, "x_at_most_rows", gold("kit.rows"), {"max": 1}, ["src_sys_nm"])
    assert columns == ["g_1", "population", "violations"]
    assert testing.template_contract(spark, "max_rows_per_key", gold("kit.rows"), {"max": 1}) == ["population", "violations"]


# --- dre guard ---

def test_dre_guard_passes_the_example_and_fails_a_write(tmp_path: Path, capsys) -> None:
    example = Path(__file__).parent / "extensions" / "dre_ext_example"
    assert main(["guard", str(example)]) == 0
    assert "0 violation(s)" in capsys.readouterr().out
    bad = tmp_path / "bad_ext"
    bad.mkdir()
    (bad / "mod.py").write_text("def run(df):\n    df.write.mode('overwrite').saveAsTable('prod.t')\n")
    assert main(["guard", str(bad)]) == 1
    out = capsys.readouterr().out
    assert "overwrite mode" in out and "saveAsTable" in out and "write API" in out and "3 violation(s)" in out
    assert main(["guard", str(tmp_path / "missing")]) == 3


def test_the_guard_module_is_the_one_file_the_scan_skips_and_holds_no_operations() -> None:
    from hcsc.datalake.dre import guard

    text = Path(guard.__file__).read_text(encoding="utf-8")
    assert "pyspark" not in text and ".sql(" not in text and "open(" not in text  # patterns only
    assert guard.scan_tree(Path(guard.__file__).parents[3]) == []  # src/: the engine is clean under its own scanner
