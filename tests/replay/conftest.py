"""Replay scenarios: the sample config pointed at synthetic tables and a fresh dq store."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from hcsc.datalake.dre.store.local_setup import create_store
from tests.conftest import REPO_ROOT


@dataclass(frozen=True)
class Replay:
    conf: Path
    dq: str
    gold: str
    curated: str
    provider_raw: str = ""
    provider_gold: str = ""


def replay_conf(spark, tmp_path: Path, name: str, edits: dict[str, list[tuple[str, str]]] | None = None) -> Replay:
    """Copy conf/, rename its tables and dq database for this scenario, apply edits, create the store."""
    replay = Replay(tmp_path / "conf", f"dq_{name}", f"{name}_gold.member_coverage", f"{name}_curated.enrollment",
                    f"{name}_landing.provider_roster", f"{name}_gold.provider_directory")
    shutil.copytree(REPO_ROOT / "conf", replay.conf)
    for path in replay.conf.rglob("*.yaml"):
        text = path.read_text(encoding="utf-8")
        text = text.replace("gold_db.member_coverage", replay.gold).replace("curated_db.enrollment", replay.curated)
        text = text.replace("landing_db.provider_roster", replay.provider_raw)
        text = text.replace("gold_db.provider_directory", replay.provider_gold)
        text = text.replace("dq_database: dq\n", f"dq_database: {replay.dq}\n")
        path.write_text(text, encoding="utf-8")
    for rel, pairs in (edits or {}).items():
        path = replay.conf / rel
        text = path.read_text(encoding="utf-8")
        for old, new in pairs:
            assert old in text, (rel, old)
            text = text.replace(old, new)
        path.write_text(text, encoding="utf-8")
    create_store(spark, replay.dq)
    return replay


# The first synthetic feed's datasets (feed example_realtime, plus the table-wide one over its gold table).
REALTIME_DATASETS = {"gold_member_coverage", "example_curated_enrollment", "gold_member_coverage_all"}


def latest(spark, replay: Replay) -> dict[tuple[str, str], list]:
    """v_latest_result rows by (dataset, check_id)."""
    out: dict[tuple[str, str], list] = {}
    for row in spark.table(f"{replay.dq}.v_latest_result").collect():
        out.setdefault((row.dataset, row.check_id), []).append(row)
    return out


def causes(spark, replay: Replay, dataset: str, check_id: str, pattern: str) -> dict:
    """failure_ref -> resolved CauseLine, for the latest evaluation(s) of (dataset, check_id)."""
    from hcsc.datalake.dre.causes.engine import failure_type, summarise

    evaluations = {r.evaluation_id for r in latest(spark, replay).get((dataset, check_id), [])}
    rows = [r.asDict() for r in spark.table(f"{replay.dq}.dq_cause_result").collect()
            if r.evaluation_id in evaluations]
    return summarise(rows, failure_type(pattern, check_id))


def cause_rows(spark, replay: Replay, dataset: str, check_id: str) -> list:
    evaluations = {r.evaluation_id for r in latest(spark, replay).get((dataset, check_id), [])}
    return sorted((r for r in spark.table(f"{replay.dq}.dq_cause_result").collect() if r.evaluation_id in evaluations),
                  key=lambda r: (r.failure_ref or "", r.order_no))
