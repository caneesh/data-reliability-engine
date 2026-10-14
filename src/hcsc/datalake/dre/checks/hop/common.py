"""What every hop check shares: the two sides of a hop, which keys are judged, key hashing.

A hop is (this dataset, one upstream in its key_map). Keys are joined on the mapped key
with each side's key_normalise, as one canonical string. Each side's feed_filter applies.
Keys judged in a window: upstream keys whose latest load time + sla_hours falls in the
window (judged once, at their deadline); keys with a row here loaded in the window whose
upstream deadline has passed (so a key that regresses here without an upstream change is
judged again); and keys still open in v_open_keys. On the full sweep day, every upstream
key past its deadline. Upstream rows matching the dataset's hop_exclusions for the upstream
are a business filter: hop checks do not expect them here, so they are never counted as lost.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable

from hcsc.datalake.dre.checks.base import CheckContext, CheckResult, did_not_run, error_detail, from_exception, utc_literal
from hcsc.datalake.dre.checks.derived import source_of
from hcsc.datalake.dre.checks.keys import column_expr, key_string
from hcsc.datalake.dre.checks.times import utc_expr

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset

HASH_FUNCTION = "dre_key_hash"


def hop_label(upstream_id: str, dataset_id: str) -> str:
    """How a hop is named in dq_key_event and dq_cause_result: `<upstream>-><dataset>`."""
    return f"{upstream_id}->{dataset_id}"


def upstream_pairs(ctx: CheckContext) -> list[tuple[str, Dataset | None]]:
    """(upstream id, upstream dataset or None if unknown), in key_map order."""
    return [(up_id, ctx.upstreams.get(up_id)) for up_id in ctx.dataset.key_map]


def per_hop(up_id: str, evaluate_hop: Callable[[], CheckResult]) -> CheckResult:
    """One hop's result. An exception in one hop (say its upstream table is gone) is DID_NOT_RUN
    for that hop only; the other hops' results stand."""
    try:
        return evaluate_hop()
    except Exception as exc:
        return replace(from_exception(exc), group_values={"upstream": up_id})


def hop_params(ctx: CheckContext, event: Event, up_id: str, up: Dataset | None) -> dict[str, Any] | CheckResult:
    """Template parameters for one hop, or a DID_NOT_RUN result when the hop cannot be evaluated."""
    ds = ctx.dataset
    if up is None:
        return did_not_run("invalid_config", detail=f"upstream {up_id} is not configured",
                           group_values={"upstream": up_id})
    if up.load_time is None:
        return did_not_run("invalid_config", detail=f"upstream {up_id} has no load_time to find changed keys",
                           group_values={"upstream": up_id})
    if ds.record_time is None or up.record_time is None:
        return did_not_run("invalid_config", detail="hop checks need record_time on both datasets",
                           group_values={"upstream": up_id})
    mapping = ds.key_map[up_id]  # this column -> upstream column
    tz = ctx.default_timezone
    try:
        up_source = source_of(up, ctx.upstreams, tz)  # the table, or a derived dataset's relation
    except (KeyError, ValueError) as exc:
        return did_not_run("invalid_config", detail=f"upstream {up_id} cannot be read: {type(exc).__name__}",
                           group_values={"upstream": up_id})
    if up.table is not None:  # the hop's own precondition: the analyzer's error for a gone table is not reliable
        try:
            exists = ctx.spark.catalog.tableExists(up.table)
        except Exception as exc:
            return did_not_run("metastore_unavailable", detail=error_detail(exc), group_values={"upstream": up_id})
        if not exists:
            return did_not_run("table_missing", detail=f"table {up.table} not found", group_values={"upstream": up_id})
    sla = timedelta(hours=ctx.settings.sla_hours)
    owned = [{"up": column_expr(up, u), "dn": column_expr(ds, d)} for u, d in ds.owned_columns.items()]
    filters = [f"({up.feed_filter})" if up.feed_filter else None,
               f"NOT coalesce(({ds.hop_exclusions[up_id]}), FALSE)" if up_id in ds.hop_exclusions else None]
    return {
        "up_table": up_source, "up_filter": " AND ".join(f for f in filters if f) or None,
        # for counting what the exclusions keep out: the upstream's own filter, and the exclusion
        "up_base_filter": up.feed_filter, "up_exclusion": ds.hop_exclusions.get(up_id),
        "up_key": key_string([column_expr(up, mapping[c]) for c in ds.key]),
        "up_record": utc_expr(up.record_time, tz), "up_load": utc_expr(up.load_time, tz),
        "up_file": up.file_name_column,
        "dn_table": ds.table, "dn_filter": ds.feed_filter,
        "dn_key": key_string([column_expr(ds, c) for c in ds.key]),
        "dn_record": utc_expr(ds.record_time, tz), "dn_file": ds.file_name_column,
        "dn_load": utc_expr(ds.load_time, tz) if ds.load_time is not None else None,
        "window_start": utc_literal(event.window_start), "window_end": utc_literal(event.window_end),
        "owned": owned,
        "deadline_start": utc_literal(event.window_start - sla), "deadline_end": utc_literal(event.window_end - sla),
        "full_sweep": ctx.full_sweep, "hash_fn": HASH_FUNCTION,
        # hashed: compute key_hash with the secret; use_open: also judge keys open in v_open_keys
        # (NORMAL runs only: the open-key state is today's, not a historical window's)
        "hashed": ctx.key_secret is not None, "use_open": ctx.key_secret is not None and ctx.record_key_events,
        "dq_database": ctx.dq_database, "dataset": ds.dataset, "hop": hop_label(up_id, ds.dataset),
        # open keys are read from dq_key_event's run_date partitions on or after this (None: all of them)
        "events_since": ctx.history_since.isoformat() if ctx.history_since is not None else None,
    }


def not_judged(pending: int | None, excluded: int | None, unit: str, excluded_unit: str | None = None) -> str:
    """What a hop result did not judge and why, e.g. `1 key excluded by hop_exclusions, 2 keys pending
    (within sla_hours)`; empty when nothing was left out."""
    def counted(n: int, what: str) -> str:
        return f"{n} {what}{'' if n == 1 else 's'}"
    parts = []
    if excluded:
        parts.append(f"{counted(excluded, excluded_unit or unit)} excluded by hop_exclusions")
    if pending:
        parts.append(f"{counted(pending, unit)} pending (within sla_hours)")
    return ", ".join(parts)


def key_hash(secret: bytes, key: str) -> str:
    """HMAC-SHA256 of the canonical key string (spec section 5)."""
    return hmac.new(secret, key.encode("utf-8"), hashlib.sha256).hexdigest()


def register_key_hash(spark: SparkSession, secret: bytes) -> None:
    """Make dre_key_hash(k) available to SQL. The secret travels in the function's closure,
    never in SQL text, so it does not show up in query plans or the Spark UI."""
    from pyspark.sql.types import StringType

    spark.udf.register(HASH_FUNCTION, lambda k: None if k is None else key_hash(secret, k), StringType())
