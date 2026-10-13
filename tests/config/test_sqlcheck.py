"""Configuration SQL is a query, or nothing (spec section 9, rule 10): the words that would make it
more than a query are refused wherever SQL comes from configuration, and only as SQL words, not
inside literals or comments."""

from __future__ import annotations

import pytest

from hcsc.datalake.dre.checks.rules.conf_template import TemplateFile, check_template_file
from hcsc.datalake.dre.config.models import Dataset, Rule
from hcsc.datalake.dre.config.sqlcheck import blank_literals, forbidden_sql

FORBIDDEN = [
    "INSERT INTO x SELECT 1", "drop table x", "SELECT 1; DELETE FROM x", "CREATE TABLE t AS SELECT 1",
    "MSCK REPAIR TABLE x", "ADD JAR x.jar", "SELECT TRANSFORM(a) USING 'cat' AS (b) FROM t",
    "SELECT java_method('java.lang.System', 'getProperty', 'user.dir')", "SELECT reflect('java.util.UUID', 'randomUUID')",
]


@pytest.mark.parametrize("statement", FORBIDDEN)
def test_forbidden_words_are_named_with_their_line(statement) -> None:
    problem = forbidden_sql(f"SELECT 1 AS population, 0 AS violations\n-- then\n{statement}")
    assert problem is not None and problem.line == 3 and "read-only" in problem.problem and "TRANSFORM" in problem.fix


@pytest.mark.parametrize("text", [
    "SELECT replace(x, 'a', 'b') FROM t WHERE delete_flag = 'N' AND updated_at > '2026'",
    "status = 'LOAD' OR note = \"DROP IT\"",            # literals are not SQL
    "x = 1 -- delete nothing here\n  AND y = 'it''s'",   # a comment, and an escaped quote
    "/* CREATE was here */ a JOIN b USING (k)",          # USING alone is a join clause
    "cache_flag = 1 AND load_dt > '2026-01-01'",         # identifiers that contain the words
])
def test_literals_comments_and_identifiers_are_not_flagged(text) -> None:
    assert forbidden_sql(text) is None


def test_blanking_keeps_positions_and_newlines() -> None:
    text = "a = 'x\ny' -- c\nb"
    blanked = blank_literals(text)
    assert len(blanked) == len(text) and blanked.count("\n") == text.count("\n")
    assert blanked.startswith("a =  ") and blanked.endswith("\nb")


@pytest.mark.parametrize("field", ["feed_filter", "hop_exclusions"])
def test_fragments_in_config_are_scanned(field) -> None:
    bad = "src = 'A' OR EXISTS (SELECT TRANSFORM(1) USING 'cat' AS (x))"
    kwargs = {"feed_filter": bad} if field == "feed_filter" else {"hop_exclusions": {"up": bad}, "key_map": {"up": {"k": "k"}}}
    with pytest.raises(ValueError, match="TRANSFORM is not allowed in configuration SQL"):
        Dataset(dataset="d", table="x.y", layer="GOLD", key=["k"], record_time={"column": "rt"}, **kwargs)
    ok = Dataset(dataset="d", table="x.y", layer="GOLD", key=["k"], feed_filter="status = 'LOAD'")
    assert ok.feed_filter == "status = 'LOAD'"


def test_rule_open_when_is_scanned_through_its_template() -> None:
    from hcsc.datalake.dre.checks.rules.template import TEMPLATES

    with pytest.raises(ValueError, match="JAVA_METHOD is not allowed"):
        TEMPLATES["max_open_rows_per_key"].params_model.model_validate(
            {"open_when": "java_method('java.lang.System', 'exit', 0) IS NULL", "max": 1})
    assert Rule(rule="r", template="max_open_rows_per_key", dataset="d", params={"open_when": "x = 1", "max": 1},
                owner="o", status="approved", severity="low").params["max"] == 1


def test_template_text_and_defaults_are_scanned() -> None:
    base = {"template": "x", "population": "rows", "violation": "rows", "unit": "rows"}
    problem = check_template_file(TemplateFile(**base, sql="SELECT 1 AS population,\n 0 AS violations FROM {{ table }}\n"
                                                           "UNION ALL SELECT TRANSFORM(1) USING 'cat' AS (p)"))
    assert problem is not None and problem.line == 3 and "TRANSFORM" in problem.problem
    assert check_template_file(TemplateFile(**base, sql="SELECT 1 AS population, 0 AS violations FROM {{ table }}")) is None
