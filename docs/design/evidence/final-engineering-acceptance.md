# Evidence package: final engineering acceptance (KI-19, KI-18, CA-01 and CA-02 regression)

For independent engineering review. This is **developer verification**: the tests below were run by the developer agent, locally and in CI. It is not independent acceptance and not production readiness. Everything is synthetic.

| Item | Classification |
| --- | --- |
| KI-18 | **RATIFIED** (by the user, 2026-10-21) and **VERIFIED** |
| KI-19 | **FIXED AND VERIFIED** |
| CA-01 | **REGRESSION VERIFIED** (no regression) |
| CA-02 | **REGRESSION VERIFIED** (no regression) |
| Full regression | **PASS** (1001 passed, exit 0) |
| End-to-end | **PASS** (51 results, 41 of 41 checks written) |
| Package build | **PASS** (artifacts built; spark-submit validate, install --print, dry-run) |
| CI | See section 7 |

## 0. Baseline (2026-10-21, before any change)

- Branch: `claude/claude-md-documentation-a2lxt0`. Working tree clean. HEAD `9fa68fa`, the same as `origin/claude/claude-md-documentation-a2lxt0`. `bf7c014` (CA-01, CA-02) and `9fa68fa` (its CI record) are ancestors of HEAD (`git merge-base --is-ancestor`).
- Test baseline, re-read from GitHub on 2026-10-21: CI run #103 on `9fa68fa` (HEAD) and #102 on `bf7c014` (the last source commit) completed with conclusion success. #102's three jobs (`test`, `e2e`, `package`) each succeeded. 992 tests at `bf7c014`; the local full run on that code passed 992 (`final-conditional-approval.md`, section C).
- Commands were run from the repository root with `PYSPARK_PYTHON=$PWD/.venv/bin/python` (local-mode Spark, Python 3.11, Java 21, 4 CPUs). Logs are kept in the session's scratch directory (`ki19-before.txt`, `ki19-after.txt`, `ka_gate/`).

## 1. KI-19: root cause and reproducing test

**Root cause.**
- HOP\_KEY\_CURRENCY judges a hop's open keys again on every evaluation: those whose latest committed event is FLAGGED or STILL\_FLAGGED (`checks/sql/hop_common.sql.j2`, `open_keys`).
- It read them from `dq_key_event` partitions with `run_date >= history_since` only (`checks/hop/common.py`, `events_since`).
- Every evaluation records each open key it reads again (STILL\_FLAGGED, or CLEARED once CURRENT), so the read is complete while the hop is evaluated regularly.
- After a gap in evaluation longer than `history_lookback_days`, the open key's latest event is older than the read:
  - a key that had recovered was never CLEARED, so it stayed open in `v_open_keys`. On the full sweep it is judged CURRENT, but with `was_open` false, so it records nothing;
  - a key still missing was not judged as open before the full sweep.

**Is CA-02's correction appropriate here?** Yes, after checking the semantics:
- continuity is the hop's own HOP\_KEY\_CURRENCY coverage;
- a covering NORMAL evaluation with a secret re-emits every open key it reads (`key_currency.py`, events for every judged result);
- CLEARED is terminal for the open state;
- the read goes through `v_committed_key_event` (committed, never INVALID).

The difference: HOP\_KEY\_CURRENCY has no scope JSON, so the retention horizon is disclosed in `observed` or `detail`.

**Reproducing tests**, `tests/checks/test_open_keys_idle.py` (new).
- Setup: key 1 was flagged MISSING in OLD (2025-05-31/06-01). The hop is next evaluated in EVENT (2026-01-15/16) with a lookback from 2025-12-01.

| Test | Scenario | Expected | Unchanged code |
| --- | --- | --- | --- |
| `test_a_key_that_recovered_while_the_hop_was_idle_is_cleared` | Recovered key older than the lookback | PASSED, population 1, CLEARED; no longer in `v_open_keys` | **failed**: `('DID_NOT_RUN', None, None) == ('PASSED', 1, 0)` |
| `test_a_key_still_missing_after_the_hop_was_idle_stays_open` | Missing key older than the lookback | FAILED, STILL\_FLAGGED, "1 key open from earlier runs" | **failed**: `('DID_NOT_RUN', None, None) == ('FAILED', 1, 1)` |
| `test_with_continuous_evaluations_the_open_key_read_stays_inside_the_lookback` | Continuous evaluations | Every open-key read bounded at `history_since`; key 1 still found | **failed**: `[] == [('STILL_FLAGGED', ...)]` (never re-recorded) |
| `test_a_key_flagged_by_invalid_evidence_is_not_open_but_is_judged_again` | INVALID previous evidence | Not open; OLD's range judged again: FLAGGED anew, never STILL\_FLAGGED | passed (already correct) |
| `test_uncommitted_key_events_are_not_open` | Uncommitted key events | Not visible as open | passed (already correct) |
| `test_a_cleared_key_is_cleared_once` | Repeated evaluations | CLEARED once; two later windows record nothing | **failed** (never CLEARED) |
| `test_an_open_key_past_dq_key_event_retention_is_disclosed_and_the_full_sweep_judges_it` | History beyond retention | `observed`/`detail` states "open keys are read back to 2025-07-01, dq_key_event's retention"; the full sweep FLAGs the still-missing key | **failed**: no disclosure |
| `test_catch_up_after_a_long_idle_period_omits_nothing` | Long idle period, then catch-up | EVENT: key 1 STILL\_FLAGGED (open) and key 2 (loaded while idle) FLAGGED, population 2; NEXT: both open | **failed**: `('FAILED', 1, 1) == ('FAILED', 2, 2)` |

```
pytest -q -p no:cacheprovider tests/checks/test_open_keys_idle.py      # src/ unchanged (git status: only the new test)
FAILED tests/checks/test_open_keys_idle.py::test_a_key_that_recovered_while_the_hop_was_idle_is_cleared
FAILED tests/checks/test_open_keys_idle.py::test_a_key_still_missing_after_the_hop_was_idle_stays_open
FAILED tests/checks/test_open_keys_idle.py::test_with_continuous_evaluations_the_open_key_read_stays_inside_the_lookback
FAILED tests/checks/test_open_keys_idle.py::test_a_cleared_key_is_cleared_once
FAILED tests/checks/test_open_keys_idle.py::test_an_open_key_past_dq_key_event_retention_is_disclosed_and_the_full_sweep_judges_it
FAILED tests/checks/test_open_keys_idle.py::test_catch_up_after_a_long_idle_period_omits_nothing
6 failed, 2 passed in 98.53s (0:01:38)
exit=1
```

The test file's first run also failed two tests with `TypeError` on `observed=None`. That was a test bug: a result that judged nothing has no `observed`. The tests now read `observed` and `detail` together, and the run above is after that fix, so every failure has the right cause.

## 2. KI-19: implementation changes

- `checks/hop/common.py` (`hop_params`):
  - `open_since` is `history_since` while the hop's coverage continues from inside the lookback (`Judged.in_lookback`, CA-02), and `key_events_since` (`dq_key_event`'s retention, set by the runner) otherwise;
  - `events_since` is set from it;
  - `events_from_retention` says which bound was taken.
- `checks/hop/key_currency.py`: when the retention bound was taken and the store holds a `dq_key_event` partition at or before it (retention may have dropped events), `observed` adds "open keys are read back to `<date>`, dq\_key\_event's retention: a key opened before then is judged again only by the full sweep". When nothing was judged, `detail` carries it ("no key judged; ...").
- `checks/hop/coverage.py`: `_may_have_dropped(ctx)` becomes `may_have_dropped(ctx, table, since)`, used by CA-01 (`dq_check_result`) and KI-19 (`dq_key_event`). Its behaviour for CA-01 is unchanged.
- Docs:
  - `docs/spec.md` section 6, Key events;
  - `docs/decisions.md`, "Final engineering acceptance";
  - `docs/known-issues.md`: KI-19 closed, KI-18 ratified.

What did not change:
- no new result state, column, view, table or framework;
- the SQL templates are unchanged;
- the commit protocol, evidence scope and reconciliation semantics are untouched.

**An existing test restated** (the rule it encodes changed): `tests/checks/test_hop.py::test_open_keys_are_read_from_recent_partitions_only`.
- Its bounded run started from an old FLAGGED event with no coverage of the hop inside the lookback. That is the KI-19 defect scenario, and under the corrected rule the read reaches back.
- Its setup now adds the precondition the bounded read rests on: one committed covering result inside the lookback, ending where the window's range starts, so the range is unchanged.
- Every assertion is unchanged: the unbounded run FAILs on the open key, the bounded run is `empty_population`, and the plan prunes on `run_date`.
- After the fix, before the restatement, it failed with `('FAILED', None) == ('DID_NOT_RUN', 'empty_population')`. After the restatement it passes.

## 3. KI-18: ratification and verification

Ratified by the user on 2026-10-21 and recorded in `docs/decisions.md`, "Final engineering acceptance". No implementation change was needed.

| Requirement | Evidence |
| --- | --- |
| No unqualified healthy PASS while the gap is unresolved | `test_coverage_gap.py::test_a_hop_with_nothing_retained_in_a_store_older_than_the_retention_is_a_gap`, `::test_no_coverage_retained_is_a_qualified_pass_never_healthy` (`v_latest_result.healthy` false) |
| The digest reports the qualification | `tests/notify/test_digest.py::test_a_coverage_gap_is_reported_in_the_digest_and_is_never_all_clear` (new: listed by name under "Passed with qualifications", counted in the subject, not all clear) |
| Later normal runs do not clear it | `test_coverage_gap.py::test_later_windows_carry_the_gap_until_a_full_sweep_covers_the_hop` (including a full sweep that covered nothing) |
| A covering full sweep clears it | The same test: the full sweep is unqualified and healthy, and the next window is clean |
| A fresh store's first evaluation has no false gap | `test_coverage_gap.py::test_a_first_evaluation_in_a_store_that_lost_nothing_is_healthy`, and the runner-level replays R14, R15, R16 and R19 (healthy first runs) |

**The one test added for KI-18** is the digest test. The existing digest tests covered qualifications in general (`inferred_contract`, `attribution_unverified`), but none rendered `coverage_gap`. It passed on the existing code without a change.

## 4. CA-01 and CA-02 regression evidence

Their implementations were not changed, except that CA-01's store-age helper was generalised (same behaviour). Focused run after the KI-19 correction:

```
pytest -q -p no:cacheprovider tests/checks/test_coverage_gap.py tests/checks/test_chain.py \
  tests/checks/test_open_keys_idle.py tests/checks/test_hop.py tests/notify/test_digest.py
87 passed in 456.20s (0:07:36)
exit=0
```

- **CA-01:** all 8 tests in `test_coverage_gap.py` pass, and so does the chain's gap test in `test_chain.py`. Unresolved coverage cannot give a healthy PASS. The qualification reaches the hop checks (HOP\_LEDGER, HOP\_KEY\_CURRENCY, HOP\_VALUE\_AGREEMENT), the chain, the digest (the new digest test), and later windows until a covering full sweep.
- **CA-02:** the six CA-02 tests and the three P1-03 tests in `test_chain.py` pass. Pending records older than the lookback are carried within retention and judged once, and none disappears silently.
- **KI-19:** all 8 tests pass, and so do all 30 tests in `test_hop.py`, including the restated one.

## 5. Full regression, end-to-end and package

One gate script was run on base `9fa68fa` plus this change's working tree; each command's exit code was logged:

```
pytest -q -p no:cacheprovider                               suite exit=0   1001 passed in 4510.68s (1:15:10)
python tests/e2e/build_synthetic.py                         build exit=0
dre install --conf conf --apply                             install exit=0
dre run --conf conf                                         run exit=0     COMPLETED, 41 of 41 checks written;
                                                                           concurrency: at most 1 unit(s) of Spark work at a time; peak 1
python tests/e2e/verify.py                                  verify exit=0  51 results {'PASSED': 31, 'DID_NOT_RUN': 20}; run ['COMPLETED']
python scripts/package.py                                   package exit=0 dre-pyfiles-0.1.0.zip, dre-deps-0.1.0-manylinux2014_x86_64.zip, dre_main.py
spark-submit --py-files dre-pyfiles-*.zip dre_main.py validate --conf conf             exit=0
spark-submit ... install --conf conf --print  (DDL holds "CREATE TABLE IF NOT EXISTS dq.dq_run")  exit=0, grep exit=0
spark-submit ... dry-run --conf conf --feed provider_roster_monthly  ("dre dry-run: 7 checks evaluated")  exit=0, grep exit=0
```

**About the package check.** It was run with the development virtual environment's Python, so the package was also installed. CI's `package` job additionally proves the artifacts run on a Python without it; see section 7.

**The e2e digests are unchanged** from before the change: `membership-gold: 0 new failures, 14 checks did not run`, and `provider-data: ... 1 passed with qualifications` (the sample's inferred-contract HOP\_LEDGER).

**Regression areas.** The suite covers each area the review named:
- HOP\_LEDGER balancing (`tests/checks/test_hop_ledger*.py`);
- SLA coverage continuity (`test_hop_coverage.py`);
- qualifications and health (`test_coverage_gap.py`, `tests/notify`);
- commit-gated evidence (`tests/store`, `tests/test_commit_protocol.py`, `tests/test_evidence_integrity.py`);
- source attribution and survivorship (`test_source_attribution.py`, `test_hop_ledger_sources.py`);
- chain completeness (`test_chain.py`);
- replay (`tests/replay`, `tests/test_evaluation_modes.py`);
- digest and watchdog (`tests/notify`, `tests/replay/test_r12_watchdog.py`);
- budgets and cancellation (`tests/test_concurrency.py`, `tests/test_operations.py` and the budget tests).

None failed. The suite grew from 992 to 1001 tests: 8 KI-19 tests and 1 digest test.

## 6. Git commits

| Commit | What | CI |
| --- | --- | --- |
| `bf7c014` | CA-01, CA-02 | #102 green |
| `9fa68fa` | CI record | #103 green (docs only) |
| COMMIT_PENDING | KI-19 correction, KI-18 ratification, this package | CI_PENDING |

## 7. CI

CI_SECTION_PENDING

## 8. Known limitations and deferred capabilities

**Operational limitations** (`docs/known-issues.md`):
- KI-07: the chain carry and KI-19's open-key read need an `hmac_secret_file`. Without one, nothing is carried or open.
- KI-08: coverage past retention is a qualified gap until the full sweep.
- KI-09: `coverage_basis` is only in HOP\_LEDGER's and the chain's scope.
- KI-04, KI-15, KI-17: concurrency limits.
- KI-11: the cost of the union attribution proof is unmeasured.
- KI-12: HOP\_CONTRIBUTION is strict.
- KI-13: an old store is reported, not migrated.
- KI-14: chain path checks happen at run time.
- KI-10: one flaky observation under contention.

**Deferred capabilities**, none counted as passed or completed:
- R18 (aggregation through `dre run`): **NOT_STARTED** (KI-02).
- R22 (file-level record counts): **BLOCKED** on release 4 step 2 (KI-03).
- The representative load test (resources, scan volume, elapsed time, the 40-minute run budget): **BLOCKED**, as no cluster is available (KI-01). It was not performed.

## 9. Production deployment prerequisites

- Deployment values: `yarn_queue`, `protected_queues`, `kill_switch_file` (KI-05).
- An `hmac_secret_file` for key events, the chain carry and the open-key read.
- Approved `history_lookback_days` and `retention_months`, and a `dre retention` schedule.
- Ratification of the pending decisions: the revised checkpoint 3, Gate A, Gate B, the final review defaults (N = 1, grace 60 s), and CA-01 and CA-02 (KI-16).
- The load test on a representative cluster (KI-01).
- Independent engineering acceptance of this package.
