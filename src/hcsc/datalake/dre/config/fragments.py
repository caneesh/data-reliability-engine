"""Parse SQL fragments from config against Spark (`dre dry-run`).

`dre validate` only rejects `;`. Here `feed_filter` and rule `open_when` are
resolved against their dataset's table (columns must exist), and probe
conditions (`table_contains`) are parsed for syntax. Nothing is
executed: Spark analyses each query when it is built. Problems are reported
with file, line and field, like validate errors.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.base import error_detail
from hcsc.datalake.dre.config.loader import Config, ConfigError, format_loc
from hcsc.datalake.dre.config.probes import parameter_sets

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
        for key, raw in feed.probes.items():
            many = isinstance(raw, list)
            for i, params in enumerate(parameter_sets(raw)):
                condition = params.get("condition") if isinstance(params, dict) else None
                if not condition:
                    continue
                try:
                    F.expr(condition)
                except Exception as exc:
                    loc = ("probes", key, i, "condition") if many else ("probes", key, "condition")
                    report("feed", feed.feed, loc, "condition", exc)
    return problems
