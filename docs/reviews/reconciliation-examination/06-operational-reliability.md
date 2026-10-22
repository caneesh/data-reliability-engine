# 06 · Operational reliability, root-cause accuracy and performance

## 1. Execution and evidence integrity

| Concern | Behaviour (code) | Evidence | Verdict |
| --- | --- | --- | --- |
| Exception inside a check | `run_check` turns every exception into DID_NOT_RUN with a reason (`checks/base.py:291-344`) | a23; existing `test_evidence_integrity`, `test_runner` | Correct: never PASSED |
| Spark SQL failure, missing table or column | DID_NOT_RUN | a23, a24 | Correct |
| Empty population | `from_counts` and the ledger: DID_NOT_RUN / empty_population | a24 | Correct, but: an orphan in that window is lost (F-06); `volume_tolerance_pct` ≥ 100 lets T1_VOLUME PASS with population 0 (F-17, o03); NULL violations PASS (F-15, o01) |
| Partial commits | `Publisher` counts stored rows before committing; mismatched units are withheld; the run is PARTIAL; windows are held | o04; existing `test_commit_protocol` | Correct for evidence. The **digest does not say** that failures were withheld or the run was PARTIAL (**F-18**, o04) |
| Interrupted application | The run stays STARTED; its rows are uncommitted and invisible; the watchdog alerts | existing `test_recovery` | Correct, with watchdog blind spots (S-17) |
| Failed writes, readback | Repair publishes only confirmed units; conflicts are recorded in `dq_integrity` | existing | Correct |
| Run ID or evaluation ID collisions | Run ids are UUIDs. **Evaluation ids collide** for group values NULL vs `''` and for values containing `,` or `=` (`events.py:70-72`). The evaluation is then withheld on every run (count mismatch), the run is PARTIAL, and after `max_window_hours` a WINDOW_GAP is written | o02 (collision executed; withholding by deduction) | **F-16** (fails closed, but the result never publishes) |
| Duplicate or concurrent NORMAL runs | `runs.in_progress` treats a STARTED run older than `watchdog_grace_minutes` (30) as dead, while `run_budget_minutes` is 40. The check-then-start is not atomic. Two runs can evaluate the same windows, and each has its own concurrency gate | static | **S-16** (Medium): no evidence corruption (append-only, commits per run, `v_latest_result` ranking), but duplicate key events and a doubled Spark load |
| Retries | Investigations are retried next run against **current** data; their causes never reach a later digest | static | S-07 (Low) |
| Configuration change between runs | `contract_hash` is in the scope; coverage continuity is per check and group | existing | Correct (disclosed) |
| Retention boundaries | CA-01, CA-02, KI-19 | existing tests | Correct (verified 2026-10-21) |
| Historical replay idempotency | `stored_outcomes` skips unchanged outcomes. The `OUTCOME` tuple leaves out `qualifications`, so a re-run that is now qualified is "unchanged" and the old unqualified PASS stays healthy. `healthy` ignores `evaluation_mode` | static | S-19 (Low-Medium) |
| Resource exhaustion | Per-check compute budget, run budget, concurrency gate (O6), kill switch | existing `test_concurrency`, `test_operations` | Correct, with KI-15 and KI-17 disclosed |

**Can DRE silently convert an internal failure into a successful result?** Not for a check: every failure path is DID_NOT_RUN. Two indirect paths exist:
1. **F-18.** A withheld FAILED result is invisible to the digest, which reports the remaining results without mentioning that the run was PARTIAL. `v_latest_result` keeps the previous state, possibly a healthy PASS, until the held window is re-evaluated.
2. **F-15 and F-17.** Operator SQL or configuration can yield PASSED from a NULL or zero population.

## 2. Does DRE disturb the monitored pipelines?

**Verified statically, and by the existing guard tests (`tests/guard/`):**
- The only writes are `store/writer.append` (insertInto, append only), retention's DROP PARTITION on dq tables, and migrations' ADD COLUMNS. Every one of them targets the configured `dq` database.
- No configured string reaches a write path.
- The queue check refuses protected queues (ADR-08).

**Residual risks:**
- Nothing prevents `dq_database` from naming a monitored database. `dre install --apply` would then create views there, and retention would act on any `dq_*` tables there (S-20, Low: a deployment guard).
- Raw SQL fragments may call permanent Hive UDFs (speculative).
- Read load on shared clusters: see §4.

## 3. Root-cause analysis accuracy (`causes/`)

**Vocabulary.** A cause is CONFIRMED by a cause check. The digest says "suspected" only when every cause check was RULED_OUT, and "insufficient evidence" when any was NOT_READY or ERROR (`engine.py:133-139`). HOP_LEDGER has no cause investigation; it prints triage lines. The chain names the "first observed gap … not necessarily where the loss began" (c03). These are good distinctions.

**Where certainty is overstated:**

| ID | Location | Behaviour | Effect |
| --- | --- | --- | --- |
| S-04 | `causes/landing.py:135-138` | Any read exception other than file-not-found adds the file to `bad`, so UNREADABLE is **CONFIRMED** | A permissions error, an expired Kerberos ticket or a NameNode outage reads as "cause: UNREADABLE (confirmed)" |
| S-05 | `causes/probes.py` `log_contains` (no logs: `ruled_out(logs=0)`); names matched as substrings; `causes/merge.py` (no upstream load time; file no longer upstream) | Missing evidence is recorded as **RULED_OUT** | The fallback is then printed "suspected" instead of "insufficient evidence". A substring match (`file.dat` in `myfile.dat`) can give a false CONFIRMED |
| S-06 | `engine.py:155` `resolve` | The first CONFIRMED by order wins, even when an earlier stage was ERROR or NOT_READY | "LOAD_ERROR (confirmed)" without saying that an earlier stage was never evaluated (spec-mandated) |
| S-07 | `runner.py:774-803` | Retries read current data; their result never reaches a digest | Delayed downstream processing can change or erase the cause; the reader never sees it |

**Multiple simultaneous failures, missing logs, incomplete lineage, overlapping incidents:**
- Each failure is investigated separately.
- Missing logs become RULED_OUT (S-05).
- Missing lineage gives NOT_READY ("insufficient evidence").
- Overlapping incidents are not correlated. Each check names its own first CONFIRMED cause.

No test here exercised several simultaneous causes; that is recorded as **not executed** in 07.

## 4. Performance and scalability (assessment, not a load test)

**Local controlled benchmark** (`adversarial/bench_ledger.py`; local-mode Spark, 2 cores, 2 shuffle partitions; synthetic). One HOP_LEDGER evaluation, every record due:

| Records | Elapsed | Plan: scans / exchanges / window sorts / broadcast joins |
| --- | --- | --- |
| 10,000 | 8.4 s | 18 / 16 / 8 / 12 |
| 100,000 | 7.5 s | 18 / 16 / 8 / 12 |
| 300,000 | 12.9 s | 18 / 16 / 8 / 12 |

These numbers show only that the plan is stable and that, locally, fixed costs dominate. **They say nothing about production performance.** The load test remains BLOCKED (KI-01).

**Complexity, from the SQL:**

| Operation | Cost at scale | Benchmark needed on a representative cluster |
| --- | --- | --- |
| HOP_LEDGER `up_rows`/`dn_rows` | Each CTE reference re-reads the base table: up to ~9 scans per side per evaluation in the local plan. Upstream reads are bounded by `up_cand` (keys loaded since the range start), but every version of those keys is read; the target is joined with the due keys | Scan volume per hop per hour; whether Hive partition pruning applies (the ledger has no partition filter on either table) |
| Window functions (`MAX OVER PARTITION BY k`, `collect_set OVER`) | A shuffle and a sort per side; skew on hot keys (for example a subscriber with many coverages) | Skewed key distributions |
| The full sweep | Every key of both tables, weekly; backpressure can defer it (SWEEP_DEFERRED) | Elapsed time against the 40-minute run budget |
| The union attribution proof | Reads every row of the target, once per union hop (KI-11) | Yes |
| CHAIN_COMPLETENESS | Re-runs the ledger SQL at each edge, plus forward mappings and local checkpoints | Multi-hop memory and checkpoint I/O |
| `collect()` | Summary rows only (grouped counts), never keys; key events stay distributed until append | Fine |
| The carry and open-key reads after a long gap | Back to `dq_key_event` retention (CA-02, KI-19), only when continuity lapsed | Store size after years |

**Estimated complexity per evaluation.** O(U_cand + D_due), times a constant scan factor (~9 per side in the local plan), plus shuffles proportional to the same. The full sweep is O(U + D).

**Production risk.** Hundreds of millions of historical rows with no partition pruning on the ledger's reads, and several re-scans per CTE, could exceed the 40-minute run budget. That is the first thing a cluster benchmark must measure.
