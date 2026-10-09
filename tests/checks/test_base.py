"""Check framework: results, denominator rule, preconditions, error mapping, events."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from jinja2 import UndefinedError

from hcsc.datalake.dre.checks import base
from hcsc.datalake.dre.checks.base import (
    REASONS, Check, CheckContext, did_not_run, from_counts, from_exception, render_sql, run_check,
)
from hcsc.datalake.dre.checks.events import Event, evaluation_id
from hcsc.datalake.dre.checks.keys import key_exprs
from hcsc.datalake.dre.config.models import Dataset
from tests.fixtures.settings import SETTINGS
from tests.fixtures.layers import GOLD_COLUMNS, create_table, gold_row

EVENT = Event("ds", datetime(2025, 12, 31, tzinfo=timezone.utc), datetime(2026, 1, 1, tzinfo=timezone.utc))


def dataset(table: str, **extra) -> Dataset:
    return Dataset(dataset="ds", table=table, layer="GOLD", key=["sub_id", "mem_nbr"], **extra)


def test_reason_codes_match_spec_section_4() -> None:
    assert REASONS["empty_population"] == "DATA_UNAVAILABLE"
    assert REASONS["column_missing"] == "CONFIGURATION"
    assert REASONS["query_failed"] == "PLATFORM"
    assert set(REASONS.values()) == {"DATA_UNAVAILABLE", "PLATFORM", "CONFIGURATION", "BUDGET", "DEPENDENCY", "BASELINE"}


@pytest.mark.parametrize(
    ("population", "violations", "state"),
    [(0, 0, "DID_NOT_RUN"), (None, None, "DID_NOT_RUN"), (5, 0, "PASSED"), (5, None, "PASSED"), (5, 2, "FAILED")],
)
def test_denominator_rule(population, violations, state) -> None:
    result = from_counts(population, violations, "observed", "expected")
    assert result.state == state
    if state == "DID_NOT_RUN":
        assert (result.reason_category, result.reason_code) == ("DATA_UNAVAILABLE", "empty_population")
        assert result.observed is None
    else:
        assert (result.population, result.observed, result.expected) == (population, "observed", "expected")


def test_did_not_run_sets_category() -> None:
    result = did_not_run("budget_exceeded")
    assert (result.state, result.reason_category, result.reason_code) == ("DID_NOT_RUN", "BUDGET", "budget_exceeded")


class SparkError(Exception):
    def __init__(self, error_class: str) -> None:
        super().__init__("message that may hold a member value 000123401")
        self._error_class = error_class

    def getErrorClass(self) -> str:
        return self._error_class


@pytest.mark.parametrize(
    ("exc", "code"),
    [
        (SparkError("UNRESOLVED_COLUMN.WITH_SUGGESTION"), "column_missing"),
        (SparkError("TABLE_OR_VIEW_NOT_FOUND"), "table_missing"),
        (SparkError("DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES"), "incompatible_type"),
        (SparkError("SOMETHING_ELSE"), "query_failed"),
        (RuntimeError("boom 000123401"), "query_failed"),
    ],
)
def test_exceptions_become_did_not_run_without_the_message(exc, code) -> None:
    result = from_exception(exc)
    assert (result.state, result.reason_code) == ("DID_NOT_RUN", code)
    assert "000123401" not in (result.detail or "")  # hard rule 6: no data values in detail


class Exploding(Check):
    check_id = "TEST_EXPLODING"

    def required_columns(self, dataset):
        return ["sub_id"]

    def evaluate(self, ctx, event):
        raise RuntimeError("unexpected")


class NoRows(Exploding):
    def evaluate(self, ctx, event):
        return []


def test_run_check_never_raises(spark) -> None:
    create_table(spark, "chk_base.t", GOLD_COLUMNS, [gold_row()])
    ctx = CheckContext(spark, dataset("chk_base.t"), None, SETTINGS)
    results, duration_ms = run_check(Exploding(), ctx, EVENT)
    assert [(r.state, r.reason_code, r.detail) for r in results] == [("DID_NOT_RUN", "query_failed", "RuntimeError")]
    assert duration_ms >= 0


def test_run_check_with_no_rows_is_empty_population(spark) -> None:
    create_table(spark, "chk_base.t2", GOLD_COLUMNS, [gold_row()])
    results, _ = run_check(NoRows(), CheckContext(spark, dataset("chk_base.t2"), None, SETTINGS), EVENT)
    assert [r.reason_code for r in results] == ["empty_population"]


def test_preconditions_table_missing(spark) -> None:
    results, _ = run_check(Exploding(), CheckContext(spark, dataset("chk_base.absent"), None, SETTINGS), EVENT)
    assert [(r.reason_category, r.reason_code) for r in results] == [("CONFIGURATION", "table_missing")]


def test_preconditions_column_missing(spark) -> None:
    create_table(spark, "chk_base.renamed", GOLD_COLUMNS, [gold_row()], rename={"sub_id": "subscriber_id"})
    results, _ = run_check(Exploding(), CheckContext(spark, dataset("chk_base.renamed"), None, SETTINGS), EVENT)
    assert [(r.reason_code, r.detail) for r in results] == [
        ("column_missing", "column(s) ['sub_id'] not in chk_base.renamed")]


def test_preconditions_metastore_unavailable() -> None:
    class BrokenCatalog:
        def tableExists(self, name):
            raise SparkError("METASTORE_DOWN")

    class BrokenSpark:
        catalog = BrokenCatalog()

    result = base.preconditions(BrokenSpark(), dataset("x.y"), ["sub_id"])
    assert (result.reason_category, result.reason_code) == ("PLATFORM", "metastore_unavailable")


def test_render_sql_is_strict() -> None:
    with pytest.raises(UndefinedError):
        render_sql("t1_key_duplicates.sql.j2", table="t.x", group_by=[], feed_filter=None)  # key_expr missing


def test_key_exprs_apply_key_normalise() -> None:
    ds = dataset("t.x", key_normalise={"sub_id": "strip_leading_zeros"})
    assert key_exprs(ds) == ["regexp_replace(sub_id, '^0+', '')", "mem_nbr"]


def test_event_and_evaluation_ids() -> None:
    first = Event("ds", datetime(2025, 12, 31, tzinfo=timezone.utc), datetime(2026, 1, 1, tzinfo=timezone.utc))
    naive = Event("ds", datetime(2025, 12, 31), datetime(2026, 1, 1))  # naive means UTC
    second = Event("ds", first.window_end, datetime(2026, 1, 2, tzinfo=timezone.utc))
    assert first.event_id == naive.event_id
    assert first.event_id != second.event_id
    assert len(first.event_id) == 64
    a = evaluation_id(first.event_id, "T1_KEY_DUPLICATES", 1, "0.1.0")
    assert a == evaluation_id(first.event_id, "T1_KEY_DUPLICATES", 1, "0.1.0")  # a rerun gets the same id
    assert a != evaluation_id(first.event_id, "T1_KEY_DUPLICATES", 2, "0.1.0")
