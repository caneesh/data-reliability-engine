# 07 · Adversarial test results

## 1. How to reproduce

```
cd <repository root>
PYSPARK_PYTHON=$PWD/.venv/bin/python .venv/bin/python -m pytest -q -p no:cacheprovider -rA \
    docs/reviews/reconciliation-examination/adversarial
```

**Executed** on 2026-10-21, HEAD `c5e6237`, working tree clean except `docs/reviews/` (untracked):

```
52 passed in 204.80s (0:03:24)
exit=0
```

The benchmark (`bench_ledger.py`, run separately with `-s`) gave 3 passed (06 §4).

**Location and scope.** The tests sit outside the project's `testpaths` (`tests/`), so they never run in its suite. They use the project's synthetic fixtures (`tests/fixtures/layers.py`, `tests/checks/test_hop*.py`, `test_chain.py`, `test_hop_contribution.py`, `tests/replay/`) and change nothing in them.

**How to read a result.**
- Each test asserts the behaviour **observed**.
- Its docstring says **CORRECT** or **FINDING F-nn**, or **LIMITATION** when the behaviour is a disclosed design boundary.
- A passing test that documents a finding is evidence that the defect **exists**.

**Run history** (kept honest):
- Three tests failed on their first run because of fixture mistakes on my side: `m01` had equal source values, `o04` assumed the wrong exit code, and the benchmark started keys at 0. They were corrected; none of them was an engine behaviour.
- `o04` was rewritten as a comparison against a control run after its first form showed the digest still carried 2 other failures.

**Fixture conventions.**
- Window [2026-01-15 12:00, 2026-01-16 12:00) UTC.
- curated → gold with sla 8 h, so curated versions loaded in [01-15 04:00, 01-16 04:00) are due.
- Chain: raw → curated (4 h) → gold (8 h).
- A "declared" hop is a dedup contract with gold's load time configured (orphans checked) and `owned_columns {enddate → mbr_mbrshp_covrg_end_dt}`.

## 2. The required matrix

| # | Scenario | Expected | Actual | Test | Status | Source |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Every record delivered | PASS | PASSED, healthy | a01 | Correct | `hop_ledger.sql.j2:264` |
| 2 | One upstream record missing | FAIL unless tolerated | FAILED. Tolerated: PASSED and **healthy** | a02, a18 | Correct / **F-03** | `ledger.py:432-434` |
| 3 | Equal counts, different keys | FAIL | FAILED: missing + orphan (missing alone when orphans are unchecked) | a03, c01 | Correct | `:311-316` |
| 4 | Correct key, incorrect attributes | Divergent | Divergent only for `owned_columns`; otherwise delivered, PASSED, healthy | a04, a05, a06 | Correct / **F-01** | `:221, :263-264` |
| 5 | Old target version | Stale | Stale, FAILED | a07, a16 | Correct | `:262` |
| 6 | Valid business exclusion | Accounted for | excluded, PASSED | a09 | Correct | `:256` |
| 7 | Invalid exclusion | FAIL | missing (unapproved filter), FAILED; over `max_share`: FAILED; one tied row: **excluded, PASSED** | a10, a11, l03 | Correct / **F-07** | `:82` |
| 8 | Delete not applied | FAIL | deleted_not_applied, FAILED; tied tombstone: **deleted, PASSED** | a12, l04 | Correct / **F-07** | `:80` |
| 9 | Duplicate CDC event | Correctly resolved | Identical: one record. Conflicting at one version: divergent (fails closed) | a14, a15 | Correct | `:69-84` |
| 10 | Out-of-order CDC | Correct version selected | The latest record time is judged | a16 | Correct | `:70-76` |
| 11 | Missing required contributor | FAIL | unresolved, FAILED (existing `test_hop_contribution`) | m03 (absent reference) | Correct | `hop_contribution.sql.j2:65` |
| 12 | Optional contributor absent | Governed by contract | **FAILED: no optional concept** | m03 | **F-11** (false FAIL) | `:65` |
| 13 | Wrong gold partition (wrong key part) | Detect | missing + orphan, FAILED | a26 | Correct | `:311-316` |
| 14 | Source unexpectedly empty | Detect or report insufficient evidence | DID_NOT_RUN / empty_population (and T1_ZERO_ROWS where configured) | a24 | Correct | `ledger.py:602` |
| 15 | Historical backfill | Correct temporal interpretation | Current state: an older version loaded now is not the version judged; history completeness is not claimed | a17 | Correct for current state; **limitation** for history | `:70-76` |
| 16 | Missing historical evidence | Inconclusive | `coverage_gap` qualification (CA-01), never healthy | existing `test_coverage_gap.py` (8) | Correct | `coverage.py` |
| 17 | Spark SQL execution failure | Not PASS | DID_NOT_RUN | a23 | Correct | `base.py:291-344` |
| 18 | Ledger imbalance | Not PASS | DID_NOT_RUN / ledger_unbalanced (existing). The check cannot see keys lost before `due_keys` (F-05) | existing; l01 | Correct / weak (S-22) | `ledger.py:408-410` |
| 19 | Concurrent execution | No silent corruption | The commit protocol keeps evidence consistent; overlapping NORMAL runs are possible (S-16) | existing `test_concurrency`, `test_commit_protocol` | Correct / S-16 (static) | `runs.py:87-99` |
| 20 | Partial persistence | Explicit partial state | Run PARTIAL, window held; **the digest omits it** | o04 | Correct / **F-18** | `runner.py:1282-1284`; `digest.py:114-115` |

## 3. Additional scenarios found during the examination

| Test | Scenario | Actual | Finding |
| --- | --- | --- | --- |
| a19, a20, c05 | `'|'` inside a key part; NULL vs the literal `'<null>'` | Two records read as one: PASSED | **F-02** |
| l05 | An unparseable date key part vs a NULL date | delivered, PASSED | **F-02** |
| l01, m04 | The newest version has no record time | The key drops from the ledger; HOP_KEY_CURRENCY judges the old version | **F-05** |
| l02, l07 | An orphan in an empty window; an orphan with an unparseable load time | Not reported | **F-06** |
| l06 | An excluded key reloaded downstream | Not judged until the full sweep | **F-08** |
| m01 | Survivorship without owned columns, wrong winner | Both edges PASS | **F-09** |
| m02 | A contributor changed; gold not reloaded | empty_population | **F-10** |
| m05 | A key with an unparseable load time | HOP_KEY_CURRENCY never judges it, even on the sweep; HOP_LEDGER's sweep does | **F-12** |
| m06 | A history-keeping gold; an older version re-loaded as current | delivered, PASSED | **F-13** |
| m07 | A soft void applied at curated, not at gold | Chain PASSED (accounted at the first edge) | **F-14** |
| a13 | An authorised reject, record present and wrong | rejected, PASSED (qualified) | **F-04** |
| o01 | `from_counts(5, None)` | PASSED | **F-15** |
| o02 | Group values NULL vs `''`; `,` and `=` inside values | One evaluation_id | **F-16** |
| o03 | `volume_tolerance_pct` = 100, empty slot | PASSED, population 0 | **F-17** |
| a21 | Untrimmed key whitespace | missing (a false FAIL, never a false PASS) | Configuration |
| a22 | Duplicate gold rows | dedup PASS / identity FAIL | Limitation |
| a25 | Gold row loaded after the cutoff | delivered (the current state is read) | Limitation (documented) |
| c02, c03, c04 | Chain: reappearance, losses at several stages, a wrong value at the last edge | FAILED, with the right first gap | Correct |

## 4. Silent-success pipeline scenarios (section 8 of the brief)

Detection is **guaranteed** when the check is configured and its evidence parses, and **conditional** otherwise.

| # | Scenario | DRE check | Detection | Evidence | Root cause | False PASS possible? |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | A scheduled source file never arrives | T1_ON_TIME / T1_FILES_NOT_LOADED / cadence (calendars) | Guaranteed when a calendar or schedule is configured (R01, R11b) | Result rows; landing registry | Inferred (no file) | Yes if no calendar is configured (no expectation), or a reused base name (S-01) |
| 2 | Files arrive but downstream pickup stops | T1_FILES_NOT_LOADED, HOP_FILE_COMPLETENESS, HOP_LEDGER | Guaranteed for unique file names | Missing files and keys | NOT_RUN cause (CONFIRMED by load times) | Reused base names (S-01) |
| 3 | Raw loading processes only some files | T1_FILES_NOT_LOADED, HOP_FILE_COMPLETENESS | As 2 | Per-file counts | FILE_SKIPPED | S-01 |
| 4 | CDC generation omits some changed records | HOP_LEDGER / HOP_KEY_CURRENCY on the next hop | Guaranteed when the omitted change is upstream of the hop being judged; the source system itself is not read | missing / stale keys | Inferred | If the omission happens before the first observed layer (outside DRE's view) |
| 5 | A merge uses the wrong partition | HOP_LEDGER (wrong key part: missing + orphan) | Guaranteed when the partition column is in the key; otherwise only owned values | a26 | Inferred | If the partition column is neither a key part nor owned (F-01) |
| 6 | A shared parameter file is overwritten | Conf-template rules, T1_SCHEMA_DRIFT; its effects through the hop checks | Conditional (effects only) | — | Not identified as a parameter file | If the effect is on unowned columns (F-01) |
| 7 | A gold load processes zero rows and exits successfully | HOP_LEDGER (missing / stale), T1_ZERO_ROWS, T1_VOLUME | Guaranteed for due upstream records; T1_ZERO_ROWS when configured | missing | NOT_RUN | `volume_tolerance_pct` ≥ 100 (F-17) |
| 8 | A transformation filters valid records | HOP_LEDGER | Guaranteed unless an approved exclusion matches them (within `max_share`) | missing | — | An over-broad approved exclusion; a tied row (F-07) |
| 9 | A target populated with stale data | HOP_LEDGER (stale), HOP_KEY_CURRENCY | Guaranteed for in-place targets | stale | — | History-keeping targets (F-13); a target-stamped record time (F-01) |
| 10 | A job reports success with partial output | HOP_LEDGER, CHAIN | Guaranteed at key level | missing / lost | — | F-02, F-05, F-07 on the affected keys |

## 5. Not executed, and why

| Item | Why not executed | Status |
| --- | --- | --- |
| S-08: reject choice nondeterministic on a timestamp tie | Spark's `max_by` tie behaviour depends on the plan and could not be forced deterministically in local mode | Static only |
| S-01: base-name reuse in file checks | Not built in the time available; deterministic from the SQL (`hop_file_completeness.sql.j2:9,21`; `t1_files_not_loaded.sql.j2:11`) | Strongly supported |
| S-16: concurrent NORMAL runs | Needs two Spark applications | Static (`runs.py:87-99`) |
| Root cause with several simultaneous failures or missing logs | Probes need landing and log fixtures beyond this scope | Static (06 §3) |
| Representative-cluster performance | No cluster (KI-01) | **Not performed** |
