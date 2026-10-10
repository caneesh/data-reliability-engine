"""Cause checks for hops between layers and for loads (spec section 7: "Rows missing between
layers", "Key missing or stale", "Load wrote nothing").

Every comparison of load times is between load times (never record times), each parsed and
converted to UTC, and truncated to this dataset's load_time granularity before comparing.
"""

from __future__ import annotations

import re
from typing import Any

from hcsc.datalake.dre.causes.base import CauseContext, Failure, Outcome, confirmed, not_ready, ruled_out
from hcsc.datalake.dre.checks.base import render_sql, utc_literal
from hcsc.datalake.dre.checks.hop.common import hop_params, register_key_hash
from hcsc.datalake.dre.checks.keys import column_expr, key_string
from hcsc.datalake.dre.checks.times import as_utc, utc_expr

NO_LOAD_TIME = "this dataset has no load_time"
_HASH = re.compile(r"^[0-9a-f]{64}$")


def hop(cx: CauseContext) -> dict[str, Any]:
    """The hop's template parameters (as the hop check built them), key hashing on."""
    def build() -> dict[str, Any]:
        params = hop_params(cx.ctx, cx.event, cx.upstream_id, cx.upstream())
        if not isinstance(params, dict):
            raise ValueError("the hop cannot be evaluated")
        params = {**params, "hashed": cx.ctx.key_secret is not None,
                  "use_open": cx.ctx.key_secret is not None and cx.check_id == "HOP_KEY_CURRENCY"}
        if cx.ctx.key_secret is not None:
            register_key_hash(cx.spark, cx.ctx.key_secret)
        return params
    return cx.cached("hop", build)


def hash_literals(hashes: list[str]) -> list[str]:
    """key_hash values as SQL literals; they are hex digests, checked before use."""
    if not all(_HASH.match(h) for h in hashes):
        raise ValueError("unexpected key_hash value")
    return [f"'{h}'" for h in hashes]


def latest_load_here(cx: CauseContext):
    ds = cx.dataset
    def query():
        sql = render_sql("cause_max_load.sql.j2", table=ds.table, feed_filter=ds.feed_filter,
                         load_expr=utc_expr(ds.load_time, cx.ctx.default_timezone))
        value = cx.spark.sql(sql).collect()[0].max_load
        return None if value is None else as_utc(value)
    return cx.cached("latest_load_here", query)


def hop_files(cx: CauseContext) -> dict[str, Any]:
    """Upstream file base name -> (first_load, up_rows, invalid_rows, rows_here)."""
    def query() -> dict[str, Any]:
        up, ds = cx.upstream(), cx.dataset
        mapping = ds.key_map[cx.upstream_id]
        columns = [{"raw": mapping[c], "normalised": column_expr(up, mapping[c])} for c in ds.key]
        sql = render_sql("cause_hop_files.sql.j2", p=hop(cx), key_columns=columns)
        return {r.f: r for r in cx.spark.sql(sql).collect()}
    return cx.cached("hop_files", query)


def _upstream_load(cx: CauseContext, failure: Failure):
    if failure.key is not None:
        return failure.key["up_load_ts"]
    row = hop_files(cx).get(failure.file_name)
    return None if row is None else row.first_load


def not_run(cx: CauseContext, failure: Failure) -> Outcome:
    """CONFIRMED when nothing loaded into this dataset at or after the upstream rows' load."""
    if cx.dataset.load_time is None:
        return not_ready(NO_LOAD_TIME)
    upstream_load = _upstream_load(cx, failure)
    if upstream_load is None:
        return ruled_out(reason="no upstream load time for this failure")
    upstream_load = cx.at_load_granularity(upstream_load)
    latest = latest_load_here(cx)
    evidence = {"upstream_loaded": upstream_load.isoformat(), "latest_load_here": latest and latest.isoformat()}
    return confirmed(**evidence) if latest is None or latest < upstream_load else ruled_out(**evidence)


def file_skipped(cx: CauseContext, failure: Failure) -> Outcome:
    """CONFIRMED when none of the file's rows are here while a later upstream file's rows are."""
    files = hop_files(cx)
    row = files.get(failure.file_name)
    if row is None:
        return ruled_out(reason="the file is no longer upstream")
    if row.rows_here > 0:
        return ruled_out(rows_here=row.rows_here, upstream_rows=row.up_rows)
    later = [r for r in files.values() if r.first_load > row.first_load and r.rows_here > 0]
    return confirmed(later_files_loaded=len(later)) if later else ruled_out(rows_here=0, later_files_loaded=0)


def invalid_key(cx: CauseContext, failure: Failure) -> Outcome:
    """CONFIRMED when the file has upstream rows with a null, empty or unparseable key column."""
    row = hop_files(cx).get(failure.file_name)
    if row is None:
        return ruled_out(reason="the file is no longer upstream")
    evidence = {"invalid_key_rows": row.invalid_rows, "upstream_rows": row.up_rows}
    return confirmed(**evidence) if row.invalid_rows else ruled_out(**evidence)


def key_mismatch(cx: CauseContext, failure: Failure) -> Outcome:
    """MISSING keys: CONFIRMED when the key matches a key here once one column listed in
    mismatch_probe_drop is left out (on both sides)."""
    ds = cx.dataset
    if not ds.mismatch_probe_drop:
        return not_ready("the dataset has no mismatch_probe_drop")
    if failure.key["key_state"] != "MISSING":
        return ruled_out(reason="the key is present here")
    up = cx.upstream()
    mapping = ds.key_map[cx.upstream_id]
    for dropped in ds.mismatch_probe_drop:
        def query(dropped: str = dropped) -> set[str]:
            kept = [c for c in ds.key if c != dropped]
            sql = render_sql(
                "cause_key_mismatch.sql.j2", p=hop(cx), dropped=dropped,
                up_key_without=key_string([column_expr(up, mapping[c]) for c in kept]),
                dn_key_without=key_string([column_expr(ds, c) for c in kept]),
                hashes=hash_literals(cx.cache["failure_hashes"]),
            )
            return {r.key_hash for r in cx.spark.sql(sql).collect()}
        if failure.ref in cx.cached(f"key_mismatch:{dropped}", query):
            return confirmed(matches_without=dropped)
    return ruled_out(columns_tried=list(ds.mismatch_probe_drop))


def tie_resolved_by_rule(cx: CauseContext, failure: Failure) -> Outcome:
    """CURRENT keys: CONFIRMED when several upstream rows tie at the latest record time with
    different owned-column values, and this dataset kept the row the winner rule ranks first."""
    ds = cx.dataset
    if ds.winner_rule is None:
        return not_ready("the dataset has no winner_rule")
    if failure.key["key_state"] != "CURRENT":
        return ruled_out(reason="not at the latest upstream version")
    def query() -> dict[str, Any]:
        sql = render_sql("cause_key_ties.sql.j2", p=hop(cx), order_by=", ".join(ds.winner_rule.order_by),
                         hashes=hash_literals(cx.cache["failure_hashes"]))
        return {r.key_hash: r for r in cx.spark.sql(sql).collect()}
    row = cx.cached("ties", query).get(failure.ref)
    if row is None:
        return ruled_out(tied_rows=1)
    evidence = {"tied_rows": row.tied, "distinct_values": row.variants, "winner_rule_row_kept": bool(row.winner_here)}
    return confirmed(**evidence) if row.variants > 1 and row.winner_here else ruled_out(**evidence)


def older_version_written_later(cx: CauseContext, failure: Failure) -> Outcome:
    """STALE keys: CONFIRMED when this dataset's row for the key was loaded after the newer
    upstream version was loaded, compared at this dataset's load_time granularity."""
    if failure.key["key_state"] != "STALE":
        return ruled_out(reason="STALE keys only")
    if cx.dataset.load_time is None:
        return not_ready(NO_LOAD_TIME)
    loaded_here = failure.key["dn_load_ts"]
    if loaded_here is None:
        return ruled_out(reason="no load time here for the key")
    here, upstream = cx.at_load_granularity(loaded_here), cx.at_load_granularity(failure.key["up_load_ts"])
    evidence = {"loaded_here": here.isoformat(), "newer_version_loaded_upstream": upstream.isoformat()}
    return confirmed(**evidence) if here > upstream else ruled_out(**evidence)


def no_upstream_data(cx: CauseContext, failure: Failure) -> Outcome:
    """CONFIRMED when every upstream dataset also loaded no rows in the event window."""
    ds = cx.dataset
    if not ds.upstream:
        return not_ready("the dataset has no upstream")
    for up_id in ds.upstream:
        up = cx.config.datasets.get(up_id)
        if up is None or up.load_time is None:
            return not_ready(f"upstream {up_id} is not configured with a load_time")

    def query() -> dict[str, int]:
        counts = {}
        for up_id in ds.upstream:
            up = cx.config.datasets[up_id]
            sql = render_sql("cause_upstream_rows.sql.j2", table=up.table, feed_filter=up.feed_filter,
                             load_expr=utc_expr(up.load_time, cx.ctx.default_timezone),
                             window_start=utc_literal(cx.event.window_start),
                             window_end=utc_literal(cx.event.window_end))
            counts[up_id] = cx.spark.sql(sql).collect()[0].loaded
        return counts
    counts = cx.cached("upstream_rows", query)
    return confirmed(upstream_rows=counts) if not any(counts.values()) else ruled_out(upstream_rows=counts)
