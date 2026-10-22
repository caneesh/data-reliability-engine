"""`dre trace --dataset D --key "k1,k2,..."` (spec section 8).

Follows one key from D up its `upstream` chain (through each key_map) and prints one line per
layer, most upstream first: present or not, record time, load time and file of its latest
version. Then the first hop where the key is absent or older downstream, and that hop's cause
line from the dq store (HOP_KEY_CURRENCY causes, found by the key's key_hash). Read-only. The
key itself is never printed, and it travels as data: a one-row temp view the layer queries join,
so it appears in no SQL text and no query plan (`--key-file` or `--key -` keep it off the command
line too).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import render_sql
from hcsc.datalake.dre.checks.keys import column_expr, normalised
from hcsc.datalake.dre.checks.times import as_utc, utc_expr
from hcsc.datalake.dre.runner import load_config, read_key_secret
from hcsc.datalake.dre.store.names import validate_dq_database, validate_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.config.loader import Config
    from hcsc.datalake.dre.config.models import Dataset


KEY_VIEW = "dre_trace_key"  # the one-row temp view holding the traced key's raw parts (dre_k0, dre_k1, ...)


@dataclass
class Layer:
    dataset: Dataset
    values: dict[str, str]          # key column -> SQL for the traced value (normalised), in this layer's columns
    downstream: str | None = None   # the dataset this layer feeds, along the traced chain
    rows: int = 0
    record: datetime | None = None
    load: datetime | None = None
    file: str | None = None


def _when(value: datetime | None) -> str:
    return "-" if value is None else as_utc(value).strftime("%Y-%m-%d %H:%M:%S UTC")


def key_view(spark: SparkSession, raw: list[str]) -> str:
    """Hold the raw key parts in a one-row temp view: the values are data in a local relation, not
    literals in SQL text, so they show up in no plan or history-server page."""
    schema = ", ".join(f"dre_k{i} STRING" for i in range(len(raw)))
    spark.createDataFrame([tuple(raw)], schema).createOrReplaceTempView(KEY_VIEW)
    return KEY_VIEW


def value_exprs(ds: Dataset) -> dict[str, str]:
    """Key column -> SQL for the traced value in the starting dataset's normalised form (the form
    every layer's own normaliser produces for the same key), read from the key view."""
    return {c: normalised(f"{KEY_VIEW}.dre_k{i}", ds.key_normalise[c]) if c in ds.key_normalise
            else f"{KEY_VIEW}.dre_k{i}" for i, c in enumerate(ds.key)}


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


def look_up(spark: SparkSession, layer: Layer, config: Config) -> None:
    from hcsc.datalake.dre.checks.derived import source_of

    ds = layer.dataset
    default_tz = config.defaults.timezone
    if ds.record_time is None:
        raise ValueError(f"{ds.dataset} has no record_time")
    match = [(column_expr(ds, c), expr) for c, expr in layer.values.items()]
    sql = render_sql("trace_layer.sql.j2", table=source_of(ds, config.datasets, default_tz),
                     feed_filter=ds.feed_filter, match=match, key_view=KEY_VIEW,
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


def layer_key_hash(spark: SparkSession, layer: Layer, key_secret: bytes) -> str:
    """The key_hash the hop checks give this layer's key: the same key_string and dre_key_hash, in SQL."""
    from hcsc.datalake.dre.checks.hop.common import HASH_FUNCTION, register_key_hash
    from hcsc.datalake.dre.checks.keys import key_string

    register_key_hash(spark, key_secret)
    ds = layer.dataset
    parts = [layer.values[c] for c in ds.key]
    return spark.sql(f"SELECT {HASH_FUNCTION}({key_string(parts)}) AS h FROM {KEY_VIEW}").collect()[0].h


def cause_line(spark: SparkSession, config: Config, up: Layer, down: Layer, key_secret: bytes | None) -> str:
    """The latest HOP_KEY_CURRENCY cause of this key at this hop (up -> down), from the dq store."""
    from hcsc.datalake.dre.causes.engine import failure_type, resolve
    from hcsc.datalake.dre.checks.hop.common import hop_label

    if key_secret is None:
        return "cause: not available (hmac_secret_file is not set, so causes are not kept per key)"
    ds = down.dataset
    ref = layer_key_hash(spark, down, key_secret)
    dq_database = validate_dq_database(config.defaults.dq_database)
    hop = hop_label(validate_identifier(up.dataset.dataset, "dataset id"),
                    validate_identifier(ds.dataset, "dataset id"))
    rows = [r.asDict() for r in spark.sql(  # the latest evaluation's rows only, ranked in SQL
        f"SELECT * FROM (SELECT c.*, r.evaluated_at AS result_at, "
        f"DENSE_RANK() OVER (ORDER BY r.evaluated_at DESC) AS latest_rank "
        f"FROM {dq_database}.v_committed_cause c "
        f"JOIN {dq_database}.v_committed_result r ON r.evaluation_id = c.evaluation_id AND r.run_id = c.result_run_id "
        f"WHERE r.dataset = '{ds.dataset}' AND r.check_id = 'HOP_KEY_CURRENCY' "
        f"AND c.hop = '{hop}' AND c.failure_ref = '{ref}') ranked WHERE latest_rank = 1").collect()]
    feed = config.feed_of(ds.dataset)
    ft = failure_type(feed.pattern if feed else None, "HOP_KEY_CURRENCY")
    if not rows or ft is None:
        return "cause: not evaluated (no failed HOP_KEY_CURRENCY for this key at this hop in the dq store)"
    return resolve(rows, ft).text()


def other_encodings(spark: SparkSession, config: Config, up: Layer, down: Layer) -> dict[int, int]:
    """Key encodings other than this engine's with committed key events at the hop (up -> down), and how
    many identities each: keys a later engine wrote, which this engine cannot compute (Gate A
    remediation, spec section 8, "Trace")."""
    from hcsc.datalake.dre.checks.hop.common import hop_label
    from hcsc.datalake.dre.checks.keys import KEY_ENCODING

    dq_database = validate_dq_database(config.defaults.dq_database)
    ds = down.dataset
    hop = hop_label(validate_identifier(up.dataset.dataset, "dataset id"),
                    validate_identifier(ds.dataset, "dataset id"))
    rows = spark.sql(
        f"SELECT coalesce(key_encoding, 1) AS encoding, COUNT(DISTINCT key_hash) AS n "
        f"FROM {dq_database}.v_committed_key_event WHERE dataset = '{ds.dataset}' "
        f"AND check_id = 'HOP_KEY_CURRENCY' AND hop = '{hop}' AND coalesce(key_encoding, 1) <> {KEY_ENCODING} "
        f"GROUP BY coalesce(key_encoding, 1) ORDER BY encoding").collect()
    return {r.encoding: r.n for r in rows}


def cause_lines(spark: SparkSession, config: Config, up: Layer, down: Layer, key_secret: bytes | None) -> list[str]:
    """The cause line, labelled by its key encoding when the hop also holds key events of another
    encoding, followed then by one line per such encoding: present, but not searchable by this
    engine's key. With one encoding at the hop the output is the cause line alone, as before."""
    from hcsc.datalake.dre.checks.keys import KEY_ENCODING

    line = cause_line(spark, config, up, down, key_secret)
    if key_secret is None:
        return [line]
    others = other_encodings(spark, config, up, down)
    if not others:
        return [line]
    return [f"{line} [key encoding {KEY_ENCODING}]",
            *(f"key encoding {enc}: {n} key{'s' if n != 1 else ''} with events at this hop, written by a later "
              "engine; not searchable by this engine's key" for enc, n in others.items())]


def trace(spark: SparkSession, config: Config, dataset_id: str, raw_key: list[str],
          key_secret: bytes | None) -> list[str]:
    ds = config.datasets[dataset_id]
    key_view(spark, raw_key)
    layers = chain(config, ds, value_exprs(ds))
    lines = []
    for layer in layers:
        look_up(spark, layer, config)
        where = f"{layer.dataset.dataset} ({layer.dataset.table or 'derived'})"
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
    lines.extend(cause_lines(spark, config, up, down, key_secret))
    return lines


def read_key(key: str | None, key_file: str | None) -> str | None:
    """The key text: --key as given, or the first non-empty line of --key-file ("-" is stdin)."""
    import sys

    if key is not None:
        return key
    if key_file is None:
        return None
    text = sys.stdin.read() if key_file == "-" else Path(key_file).read_text(encoding="utf-8")
    return next((line.strip() for line in text.splitlines() if line.strip()), None)


def run_trace(spark: SparkSession, conf: str, dataset_id: str, key: str | None, key_file: str | None = None) -> int:
    config = load_config(conf, "trace")
    if config is None:
        return 3
    ds = config.datasets.get(dataset_id)
    if ds is None:
        print(f"dre trace: unknown dataset {dataset_id!r}")
        return 3
    try:
        key_text = read_key(key, key_file)
    except OSError as exc:
        print(f"dre trace: cannot read the key file ({type(exc).__name__})")
        return 3
    if not key_text:
        print("dre trace: give the key with --key-file FILE (its first line; - for stdin) or --key")
        return 3
    raw_key = [part.strip() for part in key_text.split(",")]
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
