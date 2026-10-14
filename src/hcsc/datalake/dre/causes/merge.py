"""Cause checks for hops between layers and for loads (spec section 7: "Rows missing between
layers", "Key missing or stale", "Load wrote nothing").

Every comparison of load times is between load times (never record times), each parsed and
converted to UTC, and truncated to this dataset's load_time granularity before comparing.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from hcsc.datalake.dre.causes.base import CauseContext, Failure, Outcome, builtin, confirmed, not_ready, ruled_out
from hcsc.datalake.dre.checks.base import render_sql, utc_literal
from hcsc.datalake.dre.checks.derived import UNMATCHED_COLUMN, source_of
from hcsc.datalake.dre.checks.hop.common import hop_params, register_key_hash
from hcsc.datalake.dre.checks.keys import column_expr, key_string
from hcsc.datalake.dre.checks.partitions import partition_filter
from hcsc.datalake.dre.checks.times import as_utc, sql_string, utc_expr

NO_LOAD_TIME = "this dataset has no load_time"
_HASH = re.compile(r"^[0-9a-f]{64}$")


def hop(cx: CauseContext) -> dict[str, Any]:
    """The hop's template parameters (as the hop check built them), key hashing on."""
    def build() -> dict[str, Any]:
        params = hop_params(cx.ctx, cx.event, cx.upstream_id, cx.upstream())
        if not isinstance(params, dict):
            raise ValueError("the hop cannot be evaluated")
        params = {**params, "hashed": cx.ctx.key_secret is not None,
                  "use_open": params["use_open"] and cx.check_id == "HOP_KEY_CURRENCY"}
        if cx.ctx.key_secret is not None:
            register_key_hash(cx.spark, cx.ctx.key_secret)
        return params
    return cx.cached("hop", build)


def hash_literals(hashes: list[str]) -> list[str]:
    """key_hash values as SQL literals; they are hex digests, checked before use."""
    if not all(_HASH.match(h) for h in hashes):
        raise ValueError("unexpected key_hash value")
    return [f"'{h}'" for h in hashes]


def latest_load_here(cx: CauseContext) -> datetime | None:
    ds = cx.dataset

    def query() -> datetime | None:
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


def _upstream_load(cx: CauseContext, failure: Failure) -> datetime | None:
    if failure.key is not None:
        return failure.key["up_load_ts"]
    row = hop_files(cx).get(failure.file_name)
    return None if row is None else row.first_load


@builtin("NOT_RUN")
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


@builtin("FILE_SKIPPED")
def file_skipped(cx: CauseContext, failure: Failure) -> Outcome:
    """CONFIRMED when none of the file's rows are here while a later upstream file's rows are."""
    files = hop_files(cx)
    row = files.get(failure.file_name)
    if row is None:
        return ruled_out(reason="the file is no longer upstream")
    if row.rows_here > 0:
        return ruled_out(rows_here=row.rows_here, upstream_rows=row.up_rows)
    if row.first_load is None:
        return ruled_out(reason="no upstream load time for the file")
    # files whose load times are all null or unparseable have no place in the order: left out
    later = [r for r in files.values()
             if r.first_load is not None and r.first_load > row.first_load and r.rows_here > 0]
    return confirmed(later_files_loaded=len(later)) if later else ruled_out(rows_here=0, later_files_loaded=0)


@builtin("INVALID_KEY")
def invalid_key(cx: CauseContext, failure: Failure) -> Outcome:
    """CONFIRMED when the file has upstream rows with a null, empty or unparseable key column."""
    row = hop_files(cx).get(failure.file_name)
    if row is None:
        return ruled_out(reason="the file is no longer upstream")
    evidence = {"invalid_key_rows": row.invalid_rows, "upstream_rows": row.up_rows}
    return confirmed(**evidence) if row.invalid_rows else ruled_out(**evidence)


@builtin("KEY_MISMATCH")
def key_mismatch(cx: CauseContext, failure: Failure) -> Outcome:
    """MISSING keys: CONFIRMED when the key matches a key here once one column listed in
    mismatch_probe_drop is left out (on both sides)."""
    ds = cx.dataset
    if not ds.mismatch_probe_drop:
        return not_ready("the dataset has no mismatch_probe_drop")
    if failure.key["key_state"] != "MISSING":
        return ruled_out(reason="the key is present here")
    # a single-column key has nothing left to match on once its column is dropped
    droppable = [c for c in ds.mismatch_probe_drop if [k for k in ds.key if k != c]]
    if not droppable:
        return not_ready("mismatch_probe_drop leaves no key column to match on")
    up = cx.upstream()
    mapping = ds.key_map[cx.upstream_id]
    for dropped in droppable:
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
    return ruled_out(columns_tried=droppable)


def winner_order(cx: CauseContext) -> str:
    """The winner rule's ORDER BY over the upstream's rows, time columns parsed (never compared
    as strings): the upstream's record_time and load_time as configured, others by
    winner_rule.formats; any other column as it is stored."""
    up, rule = cx.upstream(), cx.dataset.winner_rule
    tz = cx.ctx.default_timezone
    parsed = {c.column: utc_expr(c, tz) for c in (up.record_time, up.load_time) if c is not None}
    terms = []
    for term in rule.order_by:
        column, *direction = term.split()
        if column in rule.formats:
            expr = f"to_timestamp({column}, {sql_string(rule.formats[column])})"
        else:
            expr = parsed.get(column, column)
        terms.append(" ".join([expr, *direction]))
    return ", ".join(terms)


@builtin("JOIN_UNMATCHED")
def join_unmatched(cx: CauseContext, failure: Failure) -> Outcome:
    """MISSING keys of a hop from a derived upstream with joins: CONFIRMED when a joined dataset had
    no match for the key's group (a left join; with an inner join the row is never expected)."""
    up = cx.upstream()
    if up is None or up.derived is None or not up.derived.joins:
        return not_ready("the upstream is not a derived dataset with joins")
    if failure.key["key_state"] != "MISSING":
        return ruled_out(reason="the key is present here")

    def query() -> dict[str, list[str]]:
        sql = render_sql("cause_join_unmatched.sql.j2", p=hop(cx), unmatched=UNMATCHED_COLUMN,
                         hashes=hash_literals(cx.cache["failure_hashes"]))
        return {r.key_hash: list(r.unmatched) for r in cx.spark.sql(sql).collect()}
    hit = cx.cached("join_unmatched", query).get(failure.ref)
    if hit:
        return confirmed(unmatched=hit)
    return ruled_out(joins=[j.dataset for j in up.derived.joins])


@builtin("TIE_RESOLVED_BY_RULE")
def tie_resolved_by_rule(cx: CauseContext, failure: Failure) -> Outcome:
    """CURRENT keys: CONFIRMED when several upstream rows tie at the latest record time with
    different owned-column values, and this dataset kept the row the winner rule ranks first."""
    ds = cx.dataset
    if ds.winner_rule is None:
        return not_ready("the dataset has no winner_rule")
    if failure.key["key_state"] != "CURRENT":
        return ruled_out(reason="not at the latest upstream version")
    def query() -> dict[str, Any]:
        sql = render_sql("cause_key_ties.sql.j2", p=hop(cx), order_by=winner_order(cx),
                         hashes=hash_literals(cx.cache["failure_hashes"]))
        return {r.key_hash: r for r in cx.spark.sql(sql).collect()}
    row = cx.cached("ties", query).get(failure.ref)
    if row is None:
        return ruled_out(tied_rows=1)
    evidence = {"tied_rows": row.tied, "distinct_values": row.variants, "winner_rule_row_kept": bool(row.winner_here)}
    return confirmed(**evidence) if row.variants > 1 and row.winner_here else ruled_out(**evidence)


@builtin("OLDER_VERSION_WRITTEN_LATER")
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
    if failure.key["up_load_ts"] is None:
        return ruled_out(reason="no upstream load time for the key")
    here, upstream = cx.at_load_granularity(loaded_here), cx.at_load_granularity(failure.key["up_load_ts"])
    evidence = {"loaded_here": here.isoformat(), "newer_version_loaded_upstream": upstream.isoformat()}
    return confirmed(**evidence) if here > upstream else ruled_out(**evidence)


def business_window(cx: CauseContext, slot: datetime) -> tuple[datetime, datetime] | None:
    """The span in which the load due at `slot` could have taken upstream data: from the previous
    cadence slot (the load before it) to this slot's deadline (slot + sla_hours). None when the
    feed has no slot before this one."""
    from datetime import timedelta

    from hcsc.datalake.dre.checks.cadence import previous_slot

    if cx.feed is None:
        return None
    start = previous_slot(cx.feed.cadence, slot)
    if start is None:
        return None
    return start, as_utc(slot) + timedelta(hours=cx.ctx.settings.sla_hours)


@builtin("NO_UPSTREAM_DATA")
def no_upstream_data(cx: CauseContext, failure: Failure) -> Outcome:
    """CONFIRMED only when the upstream data is genuinely absent: every upstream dataset loaded
    no rows in the failing load's business window (business_window), not the run's execution
    window, which is hours after the load. Any upstream rows there rule it out; an upstream that
    cannot be read with a load time makes it NOT_READY. It is ordered after the downstream causes,
    so direct evidence of a downstream failure (WRONG_PARTITION) wins when both are confirmed."""
    ds = cx.dataset
    if not ds.upstream:
        return not_ready("the dataset has no upstream")
    for up_id in ds.upstream:
        up = cx.config.datasets.get(up_id)
        if up is None or up.load_time is None:
            return not_ready(f"upstream {up_id} is not configured with a load_time")
    if failure.slot is None:
        return not_ready("the failure names no load slot")
    window = business_window(cx, failure.slot)
    if window is None:
        return not_ready("the feed has no earlier slot to bound the load's business window")
    start, end = window

    def query() -> dict[str, int]:
        counts = {}
        for up_id in ds.upstream:
            up = cx.config.datasets[up_id]
            sql = render_sql("cause_upstream_rows.sql.j2",
                             table=source_of(up, cx.config.datasets, cx.ctx.default_timezone),
                             feed_filter=up.feed_filter,
                             load_expr=utc_expr(up.load_time, cx.ctx.default_timezone),
                             window_start=utc_literal(start), window_end=utc_literal(end),
                             partition_filter=partition_filter(up, start, end, cx.ctx.default_timezone))
            counts[up_id] = cx.spark.sql(sql).collect()[0].loaded
        return counts
    counts = cx.cached(f"upstream_rows:{start.isoformat()}:{end.isoformat()}", query)
    evidence = {"upstream_rows": counts, "business_window_start": start.isoformat(),
                "business_window_end": end.isoformat()}
    return confirmed(**evidence) if not any(counts.values()) else ruled_out(**evidence)
