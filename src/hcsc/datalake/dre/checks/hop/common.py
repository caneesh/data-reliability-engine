"""What every hop check shares: the two sides of a hop, which keys are judged, key hashing.

A hop is (this dataset, one upstream in its key_map). Keys are joined on the mapped key
with each side's key_normalise, as one canonical string. Each side's feed_filter applies.
Keys judged in a window: upstream keys whose latest load time + sla_hours falls in the
window (judged once, at their deadline); keys with a row here loaded in the window whose
upstream deadline has passed (so a key that regresses here without an upstream change is
judged again); and keys still open in v_open_keys. On the full sweep day, every upstream
key past its deadline.
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.base import CheckContext, CheckResult, did_not_run, utc_literal
from hcsc.datalake.dre.checks.keys import column_expr, key_string
from hcsc.datalake.dre.checks.times import utc_expr

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset

HASH_FUNCTION = "dre_key_hash"


def upstream_pairs(ctx: CheckContext) -> list[tuple[str, Dataset | None]]:
    """(upstream id, upstream dataset or None if unknown), in key_map order."""
    return [(up_id, ctx.upstreams.get(up_id)) for up_id in ctx.dataset.key_map]


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
    sla = timedelta(hours=ctx.settings.sla_hours)
    owned = [{"up": column_expr(up, u), "dn": column_expr(ds, d)} for u, d in ds.owned_columns.items()]
    return {
        "up_table": up.table, "up_filter": up.feed_filter,
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
        "hashed": ctx.key_secret is not None, "use_open": ctx.key_secret is not None,
        "dq_database": ctx.dq_database, "dataset": ds.dataset,
    }


def key_hash(secret: bytes, key: str) -> str:
    """HMAC-SHA256 of the canonical key string (spec section 5)."""
    return hmac.new(secret, key.encode("utf-8"), hashlib.sha256).hexdigest()


def register_key_hash(spark: SparkSession, secret: bytes) -> None:
    """Make dre_key_hash(k) available to SQL. The secret travels in the function's closure,
    never in SQL text, so it does not show up in query plans or the Spark UI."""
    from pyspark.sql.types import StringType

    spark.udf.register(HASH_FUNCTION, lambda k: None if k is None else key_hash(secret, k), StringType())
