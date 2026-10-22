"""Parse SQL fragments from config against Spark (`dre dry-run`).

`dre validate` only rejects `;`. Here `feed_filter` and rule `open_when` are
resolved against their dataset's table, `hop_exclusions` and a contract's exclusion and
tombstone conditions against the upstream's table, a contract's `soft_when` against the
dataset's own (columns must exist), and probe
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

    from hcsc.datalake.dre.checks.derived import relation

    feeds = [config.feeds[feed_id]] if feed_id else list(config.feeds.values())
    dataset_ids = {ds for feed in feeds for ds in feed.datasets if ds in config.datasets}
    # derived upstreams of the feeds' datasets: their whole query is parsed
    derived_ids = {up for ds_id in dataset_ids for up in config.datasets[ds_id].key_map
                   if up in config.datasets and config.datasets[up].derived is not None}
    for ds_id in sorted(derived_ids):
        ds = config.datasets[ds_id]
        bases = [config.datasets.get(s) for s in ds.derived.sources]
        if all(b is not None and b.table and table_ok(b.table) for b in bases):
            try:
                spark.sql(f"SELECT * FROM {relation(ds, config.datasets, config.defaults.timezone)} LIMIT 0")
            except Exception as exc:
                report("dataset", ds_id, ("derived",), "derived", exc)
    for ds_id in sorted(dataset_ids):
        ds = config.datasets[ds_id]
        if ds.feed_filter and table_ok(ds.table):
            try:
                resolves(ds.table, ds.feed_filter)
            except Exception as exc:
                report("dataset", ds_id, ("feed_filter",), "feed_filter", exc)
        for up_id, condition in ds.hop_exclusions.items():
            up = config.datasets.get(up_id)
            if up is not None and table_ok(up.table):
                try:
                    resolves(up.table, condition)
                except Exception as exc:
                    report("dataset", ds_id, ("hop_exclusions", up_id), "hop_exclusions", exc)
        for up_id, contract in ds.contracts.items():  # contract conditions: over the upstream, soft_when over here
            up = config.datasets.get(up_id)
            up_ok = up is not None and up.table is not None and table_ok(up.table)
            checks = [(("contracts", up_id, "exclusions", i, "condition"), e.condition, up_ok, up)
                      for i, e in enumerate(contract.exclusions)]
            if contract.deletion is not None:
                checks.append((("contracts", up_id, "deletion", "tombstone"), contract.deletion.tombstone, up_ok, up))
                if contract.deletion.soft_when:
                    checks.append((("contracts", up_id, "deletion", "soft_when"), contract.deletion.soft_when,
                                   table_ok(ds.table), ds))
            for loc, expression, ok, where in checks:
                if ok:
                    try:
                        resolves(where.table, expression)
                    except Exception as exc:
                        report("dataset", ds_id, loc, loc[-1], exc)
    for rule_id, rule in config.rules.items():
        open_when = rule.params.get("open_when")
        ds = config.datasets.get(rule.dataset)
        if open_when and ds is not None and (feed_id is None or ds.dataset in dataset_ids) and table_ok(ds.table):
            try:
                resolves(ds.table, open_when)
            except Exception as exc:
                report("rule", rule_id, ("params", "open_when"), "open_when", exc)
    from hcsc.datalake.dre.checks.rules.template import TEMPLATES

    for rule_id, rule in config.rules.items():  # rules on conf templates: the whole rendered query is analysed
        ds = config.datasets.get(rule.dataset)
        if rule.template not in config.templates or ds is None or not table_ok(ds.table) \
                or (feed_id is not None and ds.dataset not in dataset_ids):
            continue
        spec = TEMPLATES[rule.template]
        try:
            params = spec.params_model.model_validate(rule.params)
            sql = spec.sql(table=ds.table, feed_filter=ds.feed_filter, group_by=rule.group_by,
                           **spec.arguments(ds, params, {}))
            spark.sql(f"SELECT * FROM ({sql}) q LIMIT 0")
        except Exception as exc:
            report("rule", rule_id, ("template",), f"template {rule.template}", exc)
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
