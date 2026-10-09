"""Parse SQL fragments from config against Spark (`dre dry-run`).

`dre validate` only rejects `;`. Here `feed_filter` and rule `open_when` are
resolved against their dataset's table (columns must exist), and filter rule
conditions are parsed for syntax (their table is not configured). Nothing is
executed: Spark analyses each query when it is built. Problems are reported
with file, line and field, like validate errors.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.base import error_detail
from hcsc.datalake.dre.config.loader import Config, ConfigError, format_loc

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def check_fragments(spark: SparkSession, config: Config, feed_id: str | None = None) -> list[ConfigError]:
    from pyspark.sql import functions as F

    problems: list[ConfigError] = []

    def report(kind: str, obj_id: str, loc: tuple[Any, ...], what: str, exc: Exception) -> None:
        file, line = config.locate(kind, obj_id, loc)
        problems.append(ConfigError(file, line, format_loc(loc), f"{what} does not parse or resolve: {error_detail(exc)}",
                                    "fix the SQL expression; column names must exist in the table"))

    def resolves(table: str, expression: str) -> None:
        spark.sql(f"SELECT 1 FROM {table} WHERE ({expression}) LIMIT 0")

    def table_ok(table: str) -> bool:
        try:
            return spark.catalog.tableExists(table)
        except Exception:
            return False

    feeds = [config.feeds[feed_id]] if feed_id else list(config.feeds.values())
    dataset_ids = {ds for feed in feeds for ds in feed.datasets if ds in config.datasets}
    for ds_id in sorted(dataset_ids):
        ds = config.datasets[ds_id]
        if ds.feed_filter and table_ok(ds.table):
            try:
                resolves(ds.table, ds.feed_filter)
            except Exception as exc:
                report("dataset", ds_id, ("feed_filter",), "feed_filter", exc)
    for rule_id, rule in config.rules.items():
        open_when = rule.params.get("open_when")
        ds = config.datasets.get(rule.dataset)
        if open_when and ds is not None and (feed_id is None or ds.dataset in dataset_ids) and table_ok(ds.table):
            try:
                resolves(ds.table, open_when)
            except Exception as exc:
                report("rule", rule_id, ("params", "open_when"), "open_when", exc)
    for feed in feeds:
        for i, filter_rule in enumerate(feed.cause_inputs.filter_rules or []):
            try:
                F.expr(filter_rule.condition)
            except Exception as exc:
                report("feed", feed.feed, ("cause_inputs", "filter_rules", i, "condition"), "condition", exc)
    return problems
