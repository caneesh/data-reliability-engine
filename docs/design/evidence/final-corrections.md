# Evidence package: final consolidated review corrections (P1-01, P1-02, P1-03, O6)

For the final review. Built on the integrated checkpoints 3 to 8 (`88df3e5`); no redesign, nothing beyond the four corrections, their tests and their documentation. Nothing here is approved, and nothing claims production readiness: the load test still needs a representative cluster (`docs/known-issues.md`, KI-01). Everything is synthetic.

Each correction gives: (1) the source files and changes, (2) the reproducing test and its expected result, (3) the command, exit code and output before and after, (4) the affected regression results. Section 5 is the full suite; the known-issues register is `docs/known-issues.md`. Logs quoted here were kept in the session's scratch directory (`corr/*.log`); the lines below are copied from them.

Commands run from the repository root with `PYSPARK_PYTHON=.venv/bin/python` (local-mode Spark, Python 3.11, 4 CPUs). "Before" runs swap the named source files back to `88df3e5` (`git show HEAD:<file>`), run, and restore them.

## 1. P1-01: a qualified PASS never makes a digest all clear

**Defect.** `Digest.all_clear` ignored `qualified`. A digest whose only passes were qualified (not healthy) was titled "all clear", and `to_send` suppressed it under `all_clear_digest: daily` or `never`.

**(1) Source.** `notify/digest.py`: `all_clear` also requires no qualified pass; the subject adds `, <q> passed with qualifications`. `config/models.py` (the `all_clear_digest` comment), `docs/spec.md` section 8 (subject), `docs/decisions.md` (the checkpoint 3 interpretation marked superseded).

**(2) Tests, `tests/notify/test_digest.py`.**

| Test | Expected |
| --- | --- |
| `test_a_digest_whose_passes_are_all_qualified_is_never_all_clear[every_run, daily, never]` | Qualified only: not all clear; subject `DRE dev: 0 new failures, 0 checks did not run, 1 passed with qualifications`; sent under every `all_clear_digest` setting |
| `test_a_digest_with_failures_and_qualified_passes_counts_both` | Mixed: `DRE dev: 1 new failures, 0 checks did not run, 2 passed with qualifications` |
| `test_only_a_fully_healthy_digest_is_all_clear[every_run, daily, never]` | Fully healthy: `DRE dev: all clear`; sent only as `all_clear_digest` allows |
| `test_subject_counts_new_failures_and_checks_that_did_not_run` (restated) and the `provider-data` golden file | The sample's provider digest holds one qualified HOP\_LEDGER pass: its subject is no longer "all clear". The golden's diff is that one line |

**(3) Runs.**

- Before: `pytest tests/notify/test_digest.py -k "qualified or healthy_digest or subject"` → exit 1, `5 failed, 4 passed`: `assert not True` (all clear with a qualified pass, three settings), the mixed subject without the qualified count, and the provider subject `'DRE dev: all clear'`. The fully healthy test passes before and after, as it should.
- After: `pytest tests/notify` → exit 0, `29 passed`.

## 2. P1-02: coverage continuity beyond `history_lookback_days`

**Defect.** `judged_range` read committed coverage only within the lookback. A hop idle for longer (or whose only results inside it were INVALID) restarted its range at the window (or at an INVALID result's start): the records loaded while it was idle were judged only by the weekly full sweep, silently.

**(1) Source.** `checks/hop/coverage.py`: `judged()` returns the range and its basis; when the lookback holds no trusted coverage, a second read bounded by `CheckContext.coverage_since` (the first `run_date` `dq_check_result` retains: `run_date - retention_months`) finds the latest trusted coverage before the lookback, and the range continues from it (never later than an INVALID result's start inside the lookback); with nothing retained, the range starts at the window and the basis states an unresolved coverage gap. `checks/base.py` (`coverage_since`), `runner.py` (`coverage_since()`, passed in `check_context`), `checks/hop/ledger.py` (`coverage_basis` in the scope). Spec section 6, Coverage.

**(2) Tests, `tests/checks/test_hop_coverage.py`.**

| Test | Expected |
| --- | --- |
| `test_coverage_older_than_the_lookback_is_found_and_the_idle_span_is_judged` | Long inactivity: last coverage 2025-06-01 04:00, lookback from 2025-12-01. Range starts at 2025-06-01 04:00; the key loaded while idle is judged (population 2); basis "committed coverage before the lookback, from 2025-06-01" |
| `test_invalid_coverage_inside_the_lookback_falls_back_to_trusted_coverage_before_it` | Invalid evidence: the only result inside the lookback is INVALID. Range starts at the trusted coverage before it (2025-06-01 04:00); the idle key is judged |
| `test_once_caught_up_the_range_continues_from_the_catch_up` | Catch-up: the next window continues from the catch-up's coverage, judges nothing twice (empty\_population); basis "committed coverage" |
| `test_coverage_not_retained_is_an_explicit_gap_never_a_silent_restart` | Beyond retention: range at the window, and the scope says "unresolved coverage gap ... judged only by the full sweep" |

**(3) Runs.**

- Before (`coverage.py` and `ledger.py` at `88df3e5`): `pytest tests/checks/test_hop_coverage.py -k "idle or lookback or caught_up or not_retained"` → exit 1, `4 failed`. The first two show the defect: the range started at the window, and at `2026-01-14 04:00` instead of `2025-06-01 04:00`. The last two fail only on the missing `coverage_basis` (their ranges were already right).
- After: `pytest tests/checks/test_hop_coverage.py tests/checks/test_hop_ledger.py tests/checks/test_hop_ledger_sources.py tests/checks/test_chain.py tests/notify tests/store/test_reconciliation.py tests/guard` → exit 0, `215 passed`.

## 3. P1-03: a chain record pending at a later edge is judged when it becomes due

**Defect, demonstrated.** The chain's range moves on by the source's chain deadline. A record pending at a later edge (it reached that edge's upstream late) was outside `due`, and the next window's range had already moved past it: it was not judged again before the full sweep. On the old code the next window is DID\_NOT\_RUN / empty\_population whether the record arrived (never shown reached) or not (never shown lost).

**(1) Source.** `checks/hop/chain.py`: in a NORMAL run with key events, the records pending at a later edge are recorded PENDING; the next evaluation reads the chain's committed PENDING keys (`v_committed_key_event`, within the lookback) and judges them at the first edge whatever the range; it records RESOLVED for a carried record now reached or accounted (LOST, UNOBSERVABLE or PENDING again otherwise). `checks/sql/chain.sql.j2` (events), `checks/sql/hop_ledger.sql.j2` (optional `recheck` keys: candidates and due whatever the range; HOP\_LEDGER's own output unchanged). The scope gives `carried_in`; without key events the blind spot stays, named. Not by holding the range back: that would judge again, and count twice, the records the range already judged. Spec section 6, CHAIN\_COMPLETENESS; section 5, the event values.

**(2) Tests, `tests/checks/test_chain.py`.**

| Test | Expected |
| --- | --- |
| `test_a_record_pending_at_a_later_edge_is_judged_when_it_becomes_due` | Window 1: due 1, the record pending at curated -> gold, one PENDING event. Window 2 (gold now holds it): due 1, reached 1, `carried_in` 1, one RESOLVED event. Window 3: empty\_population, `carried_in` 0. Counted once |
| `test_a_carried_record_that_never_arrives_is_lost_never_a_false_pass` | Window 2 (gold still lacks it): FAILED, 1 violation, first observed gap curated -> gold, one LOST event |
| `test_without_key_events_a_pending_record_is_not_carried_and_the_scope_says_so` | No secret: no events, and the scope's blind spot says the record is not carried |

**(3) Runs.**

- Before (`chain.py`, `chain.sql.j2`, `hop_ledger.sql.j2` at `88df3e5`): `pytest tests/checks/test_chain.py -k "becomes_due or never_arrives or not_carried"` → exit 1, `3 failed`: no PENDING event recorded; window 2 `('DID_NOT_RUN', None, None) == ('FAILED', 1, 1)` (the carried record never judged); the scope has no "not carried" statement.
- After: `pytest tests/checks/test_chain.py` → exit 0, `18 passed` (15 existing, 3 new); and the 215-test regression above.

## 4. O6: an enforceable concurrency limit, including after cancellation

**Defect, demonstrated.** Nothing enforced ADR-08's N. A check cancelled by its budget kept its thread, and Spark's `cancelJobGroup` stops only the jobs active at that moment, so the cancelled check could start more jobs while the next one ran. Reproduction (`corr/o6_repro.py`: a unit that swallows its cancellation and starts another 4-second Spark job, budget 1 second, then the next unit):

```
== pre-correction checks/budget.py (HEAD 88df3e5)
cancelled unit alive when the next unit ran: True
cancelled unit alive when the next unit was requested: True
the cancelled unit's jobs: ['finished', 'finished']
exit 0
== corrected checks/budget.py
cancelled unit alive when the next unit ran: False
cancelled unit alive when the next unit was requested: True
the cancelled unit's jobs: ['cancelled', 'cancelled']
exit 0
```

**(1) Source.** `checks/budget.py`: `Gate` admits at most `max_concurrent_jobs` live units (every check, investigation and evidence write runs through `within_budget`, in its own job group); a cancelled unit stays live, and counts, until its thread stops, and its job group is cancelled again every 0.5 seconds while it lingers (a reaper thread, and while a unit waits); a unit not admitted within `cancel_grace_seconds` raises `ConcurrencyLimit`. `checks/base.py` (`run_check`: DID\_NOT\_RUN / BUDGET / concurrency\_limit), `config/models.py` (`max_concurrent_jobs` default 1, `cancel_grace_seconds` default 60), `runner.py` (configures the gate at run start, prints the peak), `conf/defaults.yaml`. Spec sections 3, 4 and 8.

**(2) Tests, `tests/test_concurrency.py`** (and two validation cases in `tests/config/test_operations_config.py`).

| Test | Expected |
| --- | --- |
| `test_a_cancelled_unit_still_counts_until_it_stops_and_the_next_waits_for_it` | N = 1: after a cancellation the lingering unit still counts; the next unit waits for it, then runs; peak 1 |
| `test_a_unit_that_cannot_start_within_the_grace_is_refused_never_run_alongside` | The next unit is refused (ConcurrencyLimit), its work never runs; peak 1 |
| `test_a_cancelled_unit_s_later_spark_jobs_are_cancelled_too` | A unit swallowing its cancellation and starting more Spark jobs: every one is cancelled, then the next unit runs; peak 1 |
| `test_a_check_refused_by_the_limit_is_did_not_run_and_holds_its_window` | Through `run_check`: DID\_NOT\_RUN / BUDGET / concurrency\_limit; BUDGET holds the window |
| `test_two_units_may_overlap_only_when_the_limit_allows_it` | N = 2: two live units allowed, a third refused; peak 2 |

**(3) Runs.** After: `pytest tests/test_concurrency.py` → exit 0, `5 passed`; `pytest tests/config/test_operations_config.py` → exit 0, `11 passed`. Before: the tests cannot import the gate (it did not exist); the reproduction above shows the behaviour. Regression: `pytest tests/checks/test_budget.py tests/test_recovery.py tests/test_operations_runner.py` → exit 0, `19 passed` (8.4 min; the slowest test 91.6 s, `test_recovery.py`, as before: no unit waited on the grace).

**What the limit does not cover** (KI-04): more than one Spark job inside one Spark action (a broadcast); the YARN queue's share bounds it. N = 1 and the 60-second grace are defaults to ratify.

## 5. Full regression

| Command | On | Exit | Result |
| --- | --- | --- | --- |
| `pytest -q -p no:cacheprovider`, then `tests/e2e/build_synthetic.py`, `dre install --conf conf --apply`, `dre run --conf conf`, `tests/e2e/verify.py` | the corrections commit (tested as its working tree on `88df3e5`; the only commit between, `2cf8def`, changes documentation) | 0, 0, 0, 0, 0 | **977 passed** (70.2 min; 955 before the corrections, 22 new); e2e run COMPLETED, 41 of 41 checks written, `concurrency: at most 1 unit(s) of Spark work at a time; peak 1`; verify: 51 results, no problem |

## 6. Performance (not done)

The representative load test (resource consumption, scan volume, elapsed time, whether the run budget is met) needs platform infrastructure that is not available: KI-01, BLOCKED. `scripts/measure_run.py` is ready for it. Local synthetic figures are in the Gate B evidence and are not a substitute.

## 7. Deferred scenarios

- **R18:** NOT\_STARTED, until an applicable aggregate fixture or contract is available (KI-02). Not counted as passed or completed.
- **R22:** BLOCKED, until file-level record counts are implemented (release 4 step 2; KI-03). Not counted as passed or completed.

## 8. Found while preparing this package

The traceability-matrix updates for C5 to C8 (the replay rows and level R) were made after Gate B's last commit in a temporary worktree that was then removed, so they never reached the branch, although the Gate B package says the matrix was updated. They are restored here, with the corrections (matrix sections 4 to 6b).
