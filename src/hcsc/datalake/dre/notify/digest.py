"""The email digest (spec section 8): one plain-text digest per owner per run, built from the
dq store.

Order: subject; checks that changed state since the last run, then DID_NOT_RUN checks, each
with observed versus expected and the cause line; still-failing checks with the date first
failed; proposed rules under "report only"; passing checks as a count. Approved rules and
checks count toward alerts; proposed rules never do.

Also: when T1_ON_TIME and T1_ZERO_ROWS fail for the same loads of a dataset, one alert is
shown, not two; groups reported by a check's previous run and missing now are listed, for
groups over configured columns only (the dataset's or rule's group_by, and a hop's upstream),
since per-load groups such as T1_VOLUME's slot change every run by design.

Only counts, check ids, table and dataset names, and codes go in a digest (hard rule 6):
key_hash samples only when the feed sets email_sample_keys, and never the key itself.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from jinja2 import Environment, PackageLoader, StrictUndefined

from hcsc.datalake.dre.checks.base import utc_literal
from hcsc.datalake.dre.checks.times import as_utc
from hcsc.datalake.dre.store.names import validate_dq_database, validate_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.config.loader import Config
    from hcsc.datalake.dre.store.runs import Run

SAMPLE_KEYS = 5
_RUN_ID = re.compile(r"^[0-9a-fA-F-]{1,64}$")
_TEXT = Environment(loader=PackageLoader("hcsc.datalake.dre.notify", "."), undefined=StrictUndefined,
                    autoescape=False, trim_blocks=True, lstrip_blocks=True, keep_trailing_newline=True)


@dataclass
class Item:
    """One check result in a digest."""

    dataset: str
    check_id: str
    groups: str
    state: str
    prev_state: str | None = None
    reason: str | None = None            # CATEGORY/code, for DID_NOT_RUN
    observed: str | None = None
    expected: str | None = None
    violations: int | None = None
    prev_violations: int | None = None
    first_failed: date | None = None
    rule: bool = False
    causes: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    samples: list[str] = field(default_factory=list)

    @property
    def label(self) -> str:
        return f"{self.dataset} {self.check_id}" + (f" [{self.groups}]" if self.groups else "")

    @property
    def change(self) -> str:
        """For rules: violations now against the previous run (the first result is its baseline)."""
        if self.prev_violations is None:
            return f"{self.violations or 0} violations (baseline)"
        return f"{self.violations or 0} violations (was {self.prev_violations})"


@dataclass
class Digest:
    owner: str
    recipients: list[str]
    environment: str
    run_id: str
    run_started: datetime
    changed: list[Item] = field(default_factory=list)
    gone: list[Item] = field(default_factory=list)      # groups the previous run reported, missing now
    did_not_run: list[Item] = field(default_factory=list)
    still_failing: list[Item] = field(default_factory=list)
    report_only: list[Item] = field(default_factory=list)
    passing: int = 0

    @property
    def new_failures(self) -> int:
        return sum(1 for i in self.changed if i.state == "FAILED")

    @property
    def all_clear(self) -> bool:
        return not (self.new_failures or self.did_not_run or self.still_failing)

    @property
    def subject(self) -> str:
        if self.all_clear:
            return f"DRE {self.environment}: all clear"
        return (f"DRE {self.environment}: {self.new_failures} new failures, "
                f"{len(self.did_not_run)} checks did not run")

    def text(self) -> str:
        return _TEXT.get_template("digest.txt.j2").render(d=self)


# --- reading the store ---

GROUPS_SQL = ("array_join(array_sort(transform(map_keys(coalesce(group_values, map())), "
              "k -> concat(k, '=', coalesce(group_values[k], '')))), ',')")


def _groups(values: dict[str, str | None] | None) -> str:
    return ",".join(f"{k}={'' if v is None else v}" for k, v in sorted((values or {}).items()))


def read_run(spark: SparkSession, dq_database: str, run_id: str) -> dict[str, Any]:
    """Everything a run's digests need from the dq store."""
    validate_dq_database(dq_database)
    if not _RUN_ID.match(run_id):
        raise ValueError("unexpected run id")
    current = [r.asDict() for r in spark.sql(
        f"SELECT *, {GROUPS_SQL} AS groups FROM {dq_database}.dq_check_result WHERE run_id = '{run_id}'").collect()]
    if not current:
        return {"current": [], "history": {}, "previous_groups": {}, "causes": [], "samples": {}}
    started = min(as_utc(r["evaluated_at"]) for r in current)
    pairs = sorted({(r["dataset"], r["check_id"]) for r in current})
    datasets = ", ".join(f"'{validate_identifier(d, 'dataset id')}'" for d in sorted({d for d, _ in pairs}))
    checks = ", ".join(f"'{validate_identifier(c, 'check id')}'" for c in sorted({c for _, c in pairs}))
    earlier = (f"SELECT *, {GROUPS_SQL} AS groups FROM {dq_database}.dq_check_result "
               f"WHERE execution_type = 'NORMAL' AND run_id <> '{run_id}' AND evaluated_at < {utc_literal(started)} "
               f"AND dataset IN ({datasets}) AND check_id IN ({checks})")
    history = {
        (r.dataset, r.check_id, r.groups): r.asDict() for r in spark.sql(
            f"WITH h AS ({earlier}), "
            f"ranked AS (SELECT h.*, ROW_NUMBER() OVER (PARTITION BY dataset, check_id, groups "
            f"  ORDER BY evaluated_at DESC, run_id DESC) AS rn FROM h), "
            f"last_ok AS (SELECT dataset, check_id, groups, MAX(evaluated_at) AS ok_at FROM h "
            f"  WHERE state <> 'FAILED' GROUP BY dataset, check_id, groups), "
            f"streak AS (SELECT h.dataset, h.check_id, h.groups, MIN(h.evaluated_at) AS failed_since FROM h "
            f"  LEFT JOIN last_ok o ON o.dataset = h.dataset AND o.check_id = h.check_id AND o.groups = h.groups "
            f"  WHERE h.state = 'FAILED' AND (o.ok_at IS NULL OR h.evaluated_at > o.ok_at) "
            f"  GROUP BY h.dataset, h.check_id, h.groups) "
            f"SELECT r.dataset, r.check_id, r.groups, r.state, r.violations, s.failed_since "
            f"FROM ranked r LEFT JOIN streak s ON s.dataset = r.dataset AND s.check_id = r.check_id "
            f"  AND s.groups = r.groups WHERE r.rn = 1").collect()
    }
    previous_groups: dict[tuple[str, str], dict[str, str]] = {}
    for r in spark.sql(
            f"WITH h AS ({earlier}), "
            f"last_run AS (SELECT dataset, check_id, MAX_BY(run_id, evaluated_at) AS run_id FROM h "
            f"  GROUP BY dataset, check_id) "
            f"SELECT h.dataset, h.check_id, h.groups, h.state FROM h JOIN last_run l "
            f"  ON l.dataset = h.dataset AND l.check_id = h.check_id AND l.run_id = h.run_id").collect():
        previous_groups.setdefault((r.dataset, r.check_id), {})[r.groups] = r.state
    causes = [r.asDict() for r in spark.sql(
        f"SELECT * FROM {dq_database}.dq_cause_result WHERE run_id = '{run_id}'").collect()]
    samples: dict[str, list[str]] = {}
    for r in spark.sql(
            f"SELECT evaluation_id, key_hash FROM {dq_database}.dq_key_event WHERE run_id = '{run_id}' "
            f"AND event IN ('FLAGGED', 'STILL_FLAGGED') ORDER BY evaluation_id, key_hash").collect():
        samples.setdefault(r.evaluation_id, []).append(r.key_hash)
    return {"current": current, "history": history, "previous_groups": previous_groups,
            "causes": causes, "samples": samples}


# --- building the digests ---

def owner_of(config: Config, row: dict[str, Any]) -> str | None:
    rule = config.rules.get(row["check_id"])
    if rule is not None and rule.dataset == row["dataset"]:
        return rule.owner
    if row.get("feed") and row["feed"] in config.feeds:
        return config.feeds[row["feed"]].owner
    ds = config.datasets.get(row["dataset"])
    return ds.owner if ds is not None else None


def _cause_lines(config: Config, row: dict[str, Any], causes: list[dict[str, Any]]) -> list[str]:
    """One cause line per distinct resolved cause of the evaluation, with how many failures it covers."""
    from collections import Counter

    from hcsc.datalake.dre.causes.engine import failure_type, summarise

    rows = [c for c in causes if c["evaluation_id"] == row["evaluation_id"]]
    feed = config.feeds.get(row.get("feed") or "")
    ft = failure_type(feed.pattern if feed else None, row["check_id"])
    if not rows or ft is None:
        return []
    counts = Counter(line.text() for line in summarise(rows, ft).values())
    return [text if len(counts) == 1 and n == 1 else f"{text} [{n} failing]" for text, n in sorted(counts.items())]


def _stable_group_columns(config: Config, dataset: str, check_id: str) -> set[str]:
    rule = config.rules.get(check_id)
    if rule is not None and rule.dataset == dataset:
        return set(rule.group_by)
    ds = config.datasets.get(dataset)
    return {*(ds.group_by if ds else []), "upstream"}


def _slots(row: dict[str, Any] | None) -> set[str]:
    try:
        return set(json.loads(row["detail"]).get("slots", [])) if row and row.get("detail") else set()
    except (ValueError, AttributeError):
        return set()


def build_digests(config: Config, run: Run, data: dict[str, Any]) -> list[Digest]:
    """One digest per owner with results in this run, in owner order."""
    defaults = config.defaults
    digests: dict[str, Digest] = {}
    current = data["current"]
    # One alert for T1_ON_TIME and T1_ZERO_ROWS failing for the same loads of a dataset.
    merged: set[str] = set()
    by_check = {(r["dataset"], r["check_id"], r["groups"]): r for r in current}
    on_time_notes: dict[str, str] = {}
    for (ds, check, groups), row in by_check.items():
        if check != "T1_ZERO_ROWS" or row["state"] != "FAILED":
            continue
        on_time = by_check.get((ds, "T1_ON_TIME", groups))
        if on_time is not None and on_time["state"] == "FAILED":
            shared = _slots(row) & _slots(on_time)
            if shared and shared == _slots(row):
                merged.add(row["evaluation_id"])
                on_time_notes[on_time["evaluation_id"]] = (
                    f"T1_ZERO_ROWS failed for the same {len(shared)} loads (shown once)")

    def digest_for(row: dict[str, Any]) -> Digest | None:
        owner = owner_of(config, row)
        if owner is None:
            return None
        if owner not in digests:
            digests[owner] = Digest(owner, list(defaults.recipients.get(owner, [])), defaults.environment,
                                    run.run_id, as_utc(run.started_at))
        return digests[owner]

    for row in sorted(current, key=lambda r: (r["dataset"], r["check_id"], r["groups"])):
        digest = digest_for(row)
        if digest is None or row["evaluation_id"] in merged:
            continue
        prev = data["history"].get((row["dataset"], row["check_id"], row["groups"]))
        rule = config.rules.get(row["check_id"])
        is_rule = rule is not None and rule.dataset == row["dataset"]
        item = Item(
            dataset=row["dataset"], check_id=row["check_id"], groups=row["groups"], state=row["state"],
            prev_state=prev["state"] if prev else None,
            reason=f"{row['reason_category']}/{row['reason_code']}" if row["state"] == "DID_NOT_RUN" else None,
            observed=row.get("observed"), expected=row.get("expected"),
            violations=row.get("violations"), prev_violations=prev["violations"] if prev else None,
            rule=is_rule,
        )
        if is_rule and rule.status == "proposed":
            digest.report_only.append(item)
            continue
        if row["state"] == "DID_NOT_RUN":
            digest.did_not_run.append(item)
        elif row["state"] == "FAILED":
            if item.prev_state == "FAILED":
                since = prev.get("failed_since") if prev else None
                item.first_failed = as_utc(since).date() if since else None
                digest.still_failing.append(item)
            else:
                item.causes = _cause_lines(config, row, data["causes"])
                if row["evaluation_id"] in on_time_notes:
                    item.notes.append(on_time_notes[row["evaluation_id"]])
                if config.dataset_settings(row["dataset"]).email_sample_keys:
                    item.samples = data["samples"].get(row["evaluation_id"], [])[:SAMPLE_KEYS]
                digest.changed.append(item)
        else:
            digest.passing += 1
            if item.prev_state in ("FAILED", "DID_NOT_RUN"):
                digest.changed.append(item)
    # Groups the check's previous run reported that this run did not (configured group columns only).
    now_groups: dict[tuple[str, str], set[str]] = {}
    for row in current:
        now_groups.setdefault((row["dataset"], row["check_id"]), set()).add(row["groups"])
    for (ds, check), groups in sorted(now_groups.items()):
        stable = _stable_group_columns(config, ds, check)
        previous = {g for g in data["previous_groups"].get((ds, check), {})
                    if g and {part.split("=", 1)[0] for part in g.split(",")} <= stable}
        for gone in sorted(previous - groups):
            row = next(r for r in current if (r["dataset"], r["check_id"]) == (ds, check))
            digest = digest_for(row)
            if digest is not None and gone:
                digest.gone.append(Item(ds, check, gone, "NOT_REPORTED",
                                        prev_state=data["previous_groups"][(ds, check)][gone]))
    return [digests[o] for o in sorted(digests)]


def digests_for_run(spark: SparkSession, config: Config, run: Run) -> list[Digest]:
    return build_digests(config, run, read_run(spark, config.defaults.dq_database, run.run_id))


def to_send(digests: list[Digest], defaults, run_started: datetime) -> list[Digest]:
    """The digests to send: an all-clear digest only as email.all_clear_digest allows (every run,
    once a day from the run starting in the hour from rule_run_at, or never)."""
    from zoneinfo import ZoneInfo

    mode = defaults.email.all_clear_digest
    if mode == "every_run":
        return list(digests)
    local = as_utc(run_started).astimezone(ZoneInfo(defaults.timezone))
    hour, minute = (int(p) for p in defaults.rule_run_at.split(":"))
    start = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    daily_run = start <= local < start + timedelta(hours=1)
    return [d for d in digests if not d.all_clear or (mode == "daily" and daily_run)]
