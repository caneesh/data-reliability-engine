"""`dre trace --dataset D --key "k1,k2,..."` (spec section 8).

Follows one key from D up its `upstream` chain (through each key_map) and prints one line per
layer, most upstream first: present or not, record time, load time and file of its latest
version. Then the first hop where the key is absent or older downstream, and that hop's cause
line from the dq store (HOP_KEY_CURRENCY causes, found by the key's key_hash). Read-only. The
key itself is never printed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import render_sql
from hcsc.datalake.dre.checks.keys import column_expr, normalised
from hcsc.datalake.dre.checks.times import as_utc, sql_string, utc_expr
from hcsc.datalake.dre.store.names import validate_dq_database, validate_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.config.loader import Config
    from hcsc.datalake.dre.config.models import Dataset


@dataclass
class Layer:
    dataset: Dataset
    values: dict[str, str]          # key column -> normalised value, in this layer's columns
    downstream: str | None = None   # the dataset this layer feeds, along the traced chain
    rows: int = 0
    record: datetime | None = None
    load: datetime | None = None
    file: str | None = None


def _when(value: datetime | None) -> str:
    return "-" if value is None else as_utc(value).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalise_values(spark: SparkSession, ds: Dataset, raw: list[str]) -> dict[str, str]:
    """The given key values (in ds.key order) with ds's key_normalise applied, through Spark."""
    exprs = [normalised(sql_string(v), ds.key_normalise[c]) if c in ds.key_normalise else sql_string(v)
             for c, v in zip(ds.key, raw)]
    row = spark.sql("SELECT " + ", ".join(f"{e} AS k{i}" for i, e in enumerate(exprs))).collect()[0]
    return {c: row[f"k{i}"] for i, c in enumerate(ds.key)}


def chain(config: Config, ds: Dataset, values: dict[str, str]) -> list[Layer]:
    """The dataset and every upstream reachable through key_map, most upstream first."""
    layers: list[Layer] = []
    seen: set[str] = set()

    def visit(layer: Layer) -> None:
        if layer.dataset.dataset in seen:
            return
        seen.add(layer.dataset.dataset)
        for up_id, mapping in layer.dataset.key_map.items():
            up = config.datasets.get(up_id)
            if up is not None:
                visit(Layer(up, {mapping[c]: v for c, v in layer.values.items() if c in mapping},
                            layer.dataset.dataset))
        layers.append(layer)
    visit(Layer(ds, values))
    return layers


def look_up(spark: SparkSession, layer: Layer, default_tz: str) -> None:
    ds = layer.dataset
    if ds.record_time is None:
        raise ValueError(f"{ds.dataset} has no record_time")
    match = [(column_expr(ds, c), sql_string(v)) for c, v in layer.values.items()]
    sql = render_sql("trace_layer.sql.j2", table=ds.table, feed_filter=ds.feed_filter, match=match,
                     record_expr=utc_expr(ds.record_time, default_tz),
                     load_expr=utc_expr(ds.load_time, default_tz) if ds.load_time else None,
                     file_column=ds.file_name_column)
    row = spark.sql(sql).collect()[0]
    layer.rows, layer.record, layer.load, layer.file = row.row_count, row.record_ts, row.load_ts, row.file_name


def _gap(layers: list[Layer]) -> tuple[Layer, Layer, str] | None:
    """The first hop (in flow order) where the key is absent downstream or held at an older version."""
    by_id = {layer.dataset.dataset: layer for layer in layers}
    for up in layers:
        down = by_id.get(up.downstream) if up.downstream else None
        if down is None or not up.rows:
            continue
        if not down.rows:
            return up, down, "MISSING"
        if down.record is not None and up.record is not None and as_utc(down.record) < as_utc(up.record):
            return up, down, "STALE"
    return None


def cause_line(spark: SparkSession, config: Config, down: Layer, key_secret: bytes | None) -> str:
    """The latest HOP_KEY_CURRENCY cause of this key at this hop, from the dq store."""
    from hcsc.datalake.dre.causes.engine import failure_type, resolve
    from hcsc.datalake.dre.checks.hop.common import key_hash

    if key_secret is None:
        return "cause: not available (hmac_secret_file is not set, so causes are not kept per key)"
    ds = down.dataset
    canonical = "|".join(down.values[c] if down.values[c] is not None else "<null>" for c in ds.key)
    ref = key_hash(key_secret, canonical)
    dq_database = validate_dq_database(config.defaults.dq_database)
    rows = [r.asDict() for r in spark.sql(
        f"SELECT c.*, r.evaluated_at AS result_at FROM {dq_database}.dq_cause_result c "
        f"JOIN {dq_database}.dq_check_result r ON r.evaluation_id = c.evaluation_id AND r.run_id = c.run_id "
        f"WHERE r.dataset = '{validate_identifier(ds.dataset, 'dataset id')}' AND r.check_id = 'HOP_KEY_CURRENCY' "
        f"AND c.failure_ref = '{ref}'").collect()]
    feed = config.feed_of(ds.dataset)
    ft = failure_type(feed.pattern if feed else None, "HOP_KEY_CURRENCY")
    if not rows or ft is None:
        return "cause: not evaluated (no failed HOP_KEY_CURRENCY for this key in the dq store)"
    latest = max(r["result_at"] for r in rows)
    return resolve([r for r in rows if r["result_at"] == latest], ft).text()


def trace(spark: SparkSession, config: Config, dataset_id: str, raw_key: list[str],
          key_secret: bytes | None) -> list[str]:
    ds = config.datasets[dataset_id]
    layers = chain(config, ds, normalise_values(spark, ds, raw_key))
    lines = []
    for layer in layers:
        look_up(spark, layer, config.defaults.timezone)
        where = f"{layer.dataset.dataset} ({layer.dataset.table})"
        if not layer.rows:
            lines.append(f"{where}: absent")
            continue
        lines.append(f"{where}: present, {layer.rows} row{'s' if layer.rows != 1 else ''}; "
                     f"record {_when(layer.record)}; loaded {_when(layer.load)}; file {layer.file or '-'}")
    gap = _gap(layers)
    if gap is None:
        lines.append("no gap: every layer that has the key holds its latest version")
        return lines
    up, down, state = gap
    what = "does not have the key" if state == "MISSING" else "holds an older version"
    lines.append(f"first gap: {up.dataset.dataset} -> {down.dataset.dataset}: "
                 f"{down.dataset.dataset} {what} ({state})")
    lines.append(cause_line(spark, config, down, key_secret))
    return lines


def run_trace(spark: SparkSession, conf: str, dataset_id: str, key: str) -> int:
    from hcsc.datalake.dre.runner import _load, read_key_secret

    config = _load(conf, "trace")
    if config is None:
        return 3
    ds = config.datasets.get(dataset_id)
    if ds is None:
        print(f"dre trace: unknown dataset {dataset_id!r}")
        return 3
    raw_key = [part.strip() for part in key.split(",")]
    if len(raw_key) != len(ds.key):
        print(f"dre trace: {dataset_id} has a {len(ds.key)}-part key ({', '.join(ds.key)}); "
              f"got {len(raw_key)} value(s)")
        return 3
    try:
        secret = read_key_secret(config)
    except OSError as exc:
        print(f"dre trace: cannot read hmac_secret_file ({type(exc).__name__})")
        return 3
    for line in trace(spark, config, dataset_id, raw_key, secret):
        print(line)
    return 0
