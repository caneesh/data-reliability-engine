# 04 · Regression test plan

## 1. The finding tests and their corrected outcomes

The examination's FINDING tests pass today because they reproduce the defects. They are kept: each is **revised in place** to assert the corrected behaviour, with its docstring changed to "FIXED (was F-nn): …", so the reproduction stays readable. A copy of each corrected assertion moves into the project suite (`tests/`) so CI runs it.

| Existing test (`docs/reviews/reconciliation-examination/adversarial/`) | Today (asserted) | After the fix | Smallest change that turns it |
| --- | --- | --- | --- |
| `test_ledger_adversarial.py::test_a19_*` (`'|'` in a part) | PASSED, due 1 | FAILED: 2 due; the second record missing | `keys.key_string` → encoding 2 |
| `::test_a20_*` (NULL vs `'<null>'`) | PASSED, due 1 | FAILED: 2 due; one missing | same |
| `test_ledger_static_candidates.py::test_l05_*` (unparseable date vs NULL) | PASSED | FAILED: `unassessable 'unparseable key'` | `key_parsed` flag |
| `test_chain_adversarial.py::test_c05_*` | Chain PASSED, due 1 | FAILED: 2 due; 1 lost at raw → curated | encoding 2 |
| `test_ledger_static_candidates.py::test_l01_*` (newest row untimed) | PASSED, due 1 | FAILED: key 1 due, `unassessable 'no record time (1 of 2 rows)'` | untimed counts; due load over all rows |
| `test_mapping_and_time_candidates.py::test_m04_*` (HOP_KEY_CURRENCY) | PASSED, full sweep | FAILED: `UNASSESSABLE` | the same in `hop_common.sql.j2` |
| `test_ledger_static_candidates.py::test_l03_*` (exclusion on one tied row) | PASSED, excluded | FAILED: `unassessable 'tied rows disagree: exclusion drop_test@1'` | MIN/MAX unanimity |
| `::test_l04_*` (tombstone on one tied row) | PASSED, deleted | FAILED: `unassessable 'tied rows disagree: deletion'` | same |

**Corrected tests that stay valid unchanged** (positive controls): a01-a04, a07-a12, a14-a16, a21-a26, c01-c04.
- **a15** (conflicting values at one version give divergent) stays divergent: the approved set rule.
- **a22** (duplicate gold rows under dedup PASS) is unchanged.

## 2. New tests (project suite, `tests/checks/test_gate_a_identity.py`, `…_versions.py`, `…_ties.py`)

### F-02: identity

| # | Level | Test | Asserts |
| --- | --- | --- | --- |
| I1 | Unit | `canonical_key` is injective over generated tuples (property test: random parts drawn from an alphabet with `|`, `"`, `\`, `<null>`, `''`, NULL, Unicode; 10k tuples) | distinct tuples give distinct strings; the Python mirror equals Spark `to_json(array(...))` on a sample |
| I2 | Spark SQL | encoding 2 over INT against STRING parts | equal (cross-layer string comparison preserved) |
| I3 | Ledger | a19, a20, l05 corrected | as §1 |
| I4 | Ledger | two raw keys normalising to one (padding: `0001`, `001`) | `key collision` (existing behaviour kept) |
| I5 | HOP_KEY_CURRENCY | the same collision; `upper` normaliser `'ab'` vs `'AB'` | `UNASSESSABLE 'key collision'` (new guard) |
| I6 | Chain | c05 corrected | as §1 |
| I7 | Contribution | a reference whose part is unparseable | unresolved `unparseable reference` |
| I8a | Writer (release N) | a frame without `key_encoding` appended to a migrated table | written with NULL; a frame missing any other column still fails |
| I8 | Store | migration 2 on a store at checkpoint-3 layout | column added last; existing rows read NULL; `dre install --check` PENDING, then OK |
| I9 | Transition | legacy (encoding 1) FLAGGED event for a key still wrong, and one for a key now right | first run: forced full sweep; legacy hashes `CLEARED 'key encoding changed'`; the wrong key FLAGGED under encoding 2; the right key not flagged; second run: no forced sweep |
| I10 | Transition (chain) | legacy PENDING | forced chain sweep; legacy `RESOLVED 'key encoding changed'`; the record judged |
| I11 | Trace | an event under each encoding | both found, labelled |

### F-05: versions

| # | Level | Test | Asserts |
| --- | --- | --- | --- |
| V1 | Ledger | l01 corrected | due in the window of the untimed row's load; `no record time (1 of 2 rows)` |
| V2 | Ledger | raw `''`, `'garbage'`, `'2026-02-30 10:00'`, and a wrong-format value | `unparseable record time (…)` |
| V3 | Ledger | a downstream row with no record time beside a timed one | `no record time here (…)` |
| V4 | HOP_KEY_CURRENCY / VALUE_AGREEMENT | m04 corrected; normal and full sweep | `UNASSESSABLE` |
| V5 | Ledger | an untimed row loaded **after** the cutoff | not in this window (pending in its own) |
| V6 | Ledger | valid record time, missing load time | unchanged: `untimed` line; `no load time` on the full sweep |
| V7 | Ledger | backfill (older valid record time loaded now) | unchanged: not re-judged |
| V8 | Positive control | out-of-order CDC (a16), duplicate identical events (a14), several updates in a window (a17) | unchanged PASS / stale |
| V9 | Replay (runner) | R19 (CDC versions) and R07 unchanged | existing assertions hold |

### F-07: ties

| # | Level | Test | Asserts |
| --- | --- | --- | --- |
| T1 | Ledger | l03, l04 corrected | `tied rows disagree: …` |
| T2 | Ledger | two tied tombstones; two tied rows both excluded by **different** approved rules | `deleted`; `excluded` with a combined label, each rule's guard counting it |
| T3 | Ledger | identical tied rows, one rule | the rule applies (unchanged) |
| T4 | Ledger | conflicting rejects at one time (authorised and defect) | `rejected` defect, deterministic across 5 runs and across shuffle partitions 1 and 8 |
| T5 | Value agreement | `test_value_agreement_compares_the_tied_latest_rows_as_sets` | unchanged (approved rule) |
| T6 | Replay | R06 unchanged | VALUE_AGREEMENT FAILED; cause `TIE_RESOLVED_BY_RULE` |

### Cross-cutting

| # | Level | Test | Asserts |
| --- | --- | --- | --- |
| X1 | Determinism | the whole adversarial matrix evaluated twice, with `spark.sql.shuffle.partitions` 1 and 8 | identical states, populations, dispositions and labels |
| X2 | Interaction | a collision **and** an untimed row on the same raw-ambiguous key; a tie including an untimed row | the order of 02 §0: `key collision` first; then `no record time` before `tied rows disagree` |
| X3 | Invariant B | a generated population (random keys, versions, NULL times, ties, rules) | every upstream row of every candidate key lands in exactly one line: due, pending or untimed |
| X4 | Integration | the e2e (`tests/e2e`) | unchanged expectations (51 results, 41 of 41) unless the synthetic data contains one of the defects (to be checked; any change explained) |
| X5 | Historical | RERUN of a window stored before the upgrade | a new evaluation id; the stored rows unchanged; the result attributable by `engine_version` |

## 3. Existing tests affected

| Test files | Why | Change allowed |
| --- | --- | --- |
| Literal key strings and hashes: `tests/checks/test_hop.py` (8), `tests/causes/test_causes.py` (3), `tests/test_trace.py` (2), `tests/checks/test_chain.py` (`key_of`), `tests/checks/test_open_keys_idle.py`, `tests/notify/test_digest.py`, `tests/replay/test_r04_to_r07_hops.py`, `tests/replay/test_r19_cdc_versions.py` (~18 literals, 25 `key_hash(SECRET, …)` calls) | They encode the encoding-1 string format | Replace each literal with `canonical_key([...])`. **The assertion semantics do not change** (the same record, the same event); this is declared as a format restatement in the decisions |
| `tests/store/test_ddl.py`, `tests/test_evaluation_modes.py` (migration 1's tests), `tests/guard/test_read_only.py` (ADD COLUMNS allowed only in `store/migrations.py`) | The DDL and migration list gain migration 2 | Extend the expectations; no assertion weakened |
| `tests/test_writer.py` | Release N: the writer tolerates absent migration-added columns (NULL) | Add tests: an absent migration-added column is NULL; an absent other column and an extra column still fail |
| Any test whose fixture contains a partly untimed key or a disagreeing tie | Its outcome becomes unassessable | Inspect each; if the old expectation was PASS, it was asserting the defect. Restate it with the reason in the decisions, as done for KI-19's test |

**Gate for the suite:** every existing assertion either passes unchanged, or is restated with a written reason tied to F-02, F-05 or F-07.
