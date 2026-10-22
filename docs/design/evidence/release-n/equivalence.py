"""Release N equivalence evidence (Gate A remediation): the engine before release N (a git worktree of
the baseline commit) and release N give the same results on the same synthetic store.

Not a test (it needs two checkouts and runs `dre` as separate processes, as CI's e2e job does).
Run from the repository root with the project's virtualenv:

    PYSPARK_PYTHON=$PWD/.venv/bin/python .venv/bin/python docs/design/evidence/release-n/equivalence.py \\
        <baseline-worktree> <scratch-dir>

Scenarios, each in its own work directory (its own metastore and warehouse):
  fresh:   the baseline engine and release N each build the synthetic store and run once;
  upgrade: run 1 by the baseline engine; then either the baseline engine again (control), or
           `dre install --apply` by release N (migration 2) and run 2 by release N.
Both use an HMAC secret (written to the scratch dir), and gold loses four synthetic keys after the
build, so HOP_KEY_CURRENCY flags them (run 1) and judges them again as open keys (run 2): key events
are compared too. Compared per
run: every result's (dataset, check, group, state, reason, population, violations, observed with
times masked, qualifications) and every key event's (dataset, check, hop, key, event, detail).
Release N's own columns (key_encoding, predecessor_hash) are reported separately. Prints a summary;
exit 1 on any difference.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
PY = sys.executable
DUMP = r'''
import json, sys
from hcsc.datalake.dre.session import get_spark
spark = get_spark("dre-equivalence-dump")
results = [r.asDict(recursive=True) for r in spark.table("dq.dq_check_result").collect()]
events = [r.asDict(recursive=True) for r in spark.table("dq.dq_key_event").collect()]
print(json.dumps({"results": results, "events": events}, default=str))
'''
TIME = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(\+00:00| UTC)?")


def env(code: Path) -> dict[str, str]:
    e = dict(os.environ)
    e["PYTHONPATH"] = str(code / "src")
    return e


def dre(code: Path, work: Path, *args: str) -> int:
    cmd = [PY, "-c", "import sys; from hcsc.datalake.dre.cli import main; sys.exit(main(sys.argv[1:]))", *args]
    return subprocess.run(cmd, cwd=work, env=env(code), stdout=subprocess.DEVNULL).returncode


def build(code: Path, work: Path, secret: Path) -> None:
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    subprocess.run([PY, str(code / "tests" / "e2e" / "build_synthetic.py")], cwd=work, env=env(code), check=True,
                   stdout=subprocess.DEVNULL)
    defaults = work / "conf" / "defaults.yaml"
    defaults.write_text(defaults.read_text(encoding="utf-8").replace("hmac_secret_file: null",
                                                                     f"hmac_secret_file: {secret}"), encoding="utf-8")


BREAK = r'''
from hcsc.datalake.dre.session import get_spark
spark = get_spark("dre-equivalence-break")
gone = "('000123405', '000123407', '000123409', '000123411')"
spark.sql(f"CREATE TABLE gold_db.member_coverage_kept AS SELECT * FROM gold_db.member_coverage WHERE sub_id NOT IN {gone}")
spark.sql("INSERT OVERWRITE TABLE gold_db.member_coverage SELECT * FROM gold_db.member_coverage_kept")
spark.sql("DROP TABLE gold_db.member_coverage_kept")
print(spark.table("gold_db.member_coverage").count())
'''


def break_gold(code: Path, work: Path) -> None:
    """Synthetic test data only: gold loses four recently loaded keys, so HOP_KEY_CURRENCY flags them in
    run 1 and judges them again as open keys in run 2 (key events are then compared too)."""
    subprocess.run([PY, "-c", BREAK], cwd=work, env=env(code), check=True, stdout=subprocess.DEVNULL)


def dump(code: Path, work: Path) -> dict:
    out = subprocess.run([PY, "-c", DUMP], cwd=work, env=env(code), check=True, capture_output=True, text=True)
    return json.loads(out.stdout.strip().splitlines()[-1])


def normal(d: dict, run_ids: set[str] | None = None) -> tuple[list, list, list]:
    def keep(row: dict) -> bool:
        return run_ids is None or row["run_id"] in run_ids
    results = sorted(json.dumps([r["dataset"], r["check_id"], r.get("feed"), r.get("group_values"), r["state"],
                                 r.get("reason_code"), r.get("population"), r.get("violations"),
                                 TIME.sub("<t>", r.get("observed") or ""), r.get("qualifications")], default=str)
                     for r in d["results"] if keep(r))
    events = sorted(json.dumps([e["dataset"], e["check_id"], e["hop"], e["key_value"], e["event"], e["state_detail"]])
                    for e in d["events"] if keep(e))
    extra = sorted({(e.get("key_encoding"), e.get("predecessor_hash")) for e in d["events"] if keep(e)}, key=str)
    return results, events, extra


def runs_of(d: dict) -> list[str]:
    seen = []
    for r in sorted(d["results"], key=lambda r: str(r["evaluated_at"])):
        if r["run_id"] not in seen:
            seen.append(r["run_id"])
    return seen


def main() -> int:
    baseline, scratch = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()
    secret = scratch / "hmac.secret"
    scratch.mkdir(parents=True, exist_ok=True)
    secret.write_bytes(b"synthetic-equivalence-secret\n")
    problems, report = [], {}

    # fresh: one run each
    fresh = {}
    for name, code in (("baseline", baseline), ("release_n", REPO)):
        work = scratch / f"fresh_{name}"
        build(baseline, work, secret)  # one builder for both: the same synthetic shape
        break_gold(baseline, work)
        assert dre(code, work, "install", "--conf", "conf", "--apply") == 0, name
        report[f"fresh {name} run exit"] = dre(code, work, "run", "--conf", "conf")
        fresh[name] = normal(dump(code, work))
    report["fresh results compared"] = len(fresh["baseline"][0])
    if not fresh["baseline"][1]:
        problems.append("fresh: no key events to compare (the comparison would be vacuous)")
    report["fresh key events compared"] = len(fresh["baseline"][1])
    report["fresh release N key_encoding / predecessor_hash values"] = fresh["release_n"][2]
    if fresh["baseline"][:2] != fresh["release_n"][:2]:
        problems.append("fresh: results or key events differ")

    # upgrade: run 1 by the baseline engine, then run 2 by baseline (control) or by release N after migration 2
    second = {}
    feeds = ["example_realtime", "provider_roster_monthly", "provider_directory_merge"]
    for name in ("control", "release_n"):
        work = scratch / f"upgrade_{name}"
        build(baseline, work, secret)
        break_gold(baseline, work)
        assert dre(baseline, work, "install", "--conf", "conf", "--apply") == 0
        report[f"upgrade {name} run 1 exit"] = dre(baseline, work, "run", "--conf", "conf")
        code = baseline if name == "control" else REPO
        if name == "release_n":
            report["upgrade install --check before apply (1: migration pending)"] = dre(
                REPO, work, "install", "--conf", "conf", "--check")
            report["upgrade run refused before apply (3: cannot start)"] = dre(REPO, work, "run", "--conf", "conf")
            assert dre(REPO, work, "install", "--conf", "conf", "--apply") == 0
            report["upgrade install --check after apply (0)"] = dre(REPO, work, "install", "--conf", "conf", "--check")
        exits = [dre(code, work, "run", "--conf", "conf", "--feed", f) for f in feeds]
        report[f"upgrade {name} run 2 exits"] = exits
        d = dump(code, work)
        run_ids = runs_of(d)
        report[f"upgrade {name} runs"] = len(run_ids)
        second[name] = normal(d, set(run_ids[1:]))
        first = normal(d, {run_ids[0]})
        second[name + "_first"] = first
    report["upgrade run-2 results compared"] = len(second["control"][0])
    if not second["control"][1]:
        problems.append("upgrade: no run-2 key events to compare (the comparison would be vacuous)")
    report["upgrade run-2 key events compared"] = len(second["control"][1])
    report["upgrade release N run-2 key_encoding / predecessor_hash values"] = second["release_n"][2]
    if second["control"][:2] != second["release_n"][:2]:
        problems.append("upgrade: run-2 results or key events differ")
    if second["control_first"][:2] != second["release_n_first"][:2]:
        problems.append("upgrade: run-1 results differ between the two work directories")

    for k, v in report.items():
        print(f"{k}: {v}")
    if problems:
        for name, (a, b) in (("fresh", (fresh["baseline"], fresh["release_n"])),
                             ("upgrade", (second["control"], second["release_n"]))):
            for i, label in ((0, "results"), (1, "events")):
                only_a, only_b = set(a[i]) - set(b[i]), set(b[i]) - set(a[i])
                if only_a or only_b:
                    print(f"{name} {label}: only baseline/control {sorted(only_a)[:5]}; only release N {sorted(only_b)[:5]}")
    print("EQUIVALENT" if not problems else f"DIFFERENT: {problems}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
