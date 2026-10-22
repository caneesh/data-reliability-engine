# 04 · Release N acceptance criteria

**Release N is preparatory and the rollback target.** It changes the store's key-event schema and teaches the engine to read and survive what N+1 will write. It changes **no check result** on a store that holds only encoding-1 events.

**Release N contains no LPB key, no disorder bound, and no tie or downstream rule.** Those are N+1 (05).

## 0. Preconditions (before any code)

| ID | Criterion |
| --- | --- |
| N-P1 | Amendments A-3, A-9, A-10 (the reading half: encoding filter, REKEYED as no event, foreign identities, cause encoding) and A-11 approved by the user (U-6) |
| N-P2 | The decision-log entry (A-15, release N part) is written in the same change as the spec text |
| N-P3 | Explicit instruction to begin release N |

## 1. Functional criteria (new tests)

| ID | Test (file to be created) | Proves | Contract |
| --- | --- | --- | --- |
| T-MIG-1 | `tests/store/test_migration_2.py`: a store without the two columns | `pending == [2]`; `dre run` refuses to start (exit 3, "migration pending"); `dre install --apply` adds `key_encoding INT` and `predecessor_hash STRING` at the end of `dq_key_event`'s data columns; no stored row changes; `dre install --check` then reports nothing pending | A-3, R-1, L-8 |
| T-MIG-2 | `tests/store/test_views_open_keys_lineage.py` | REKEYED latest means not open; REKEYED does not end a spell; a provable continuation inherits `first_flagged_at`; a legacy identity "awaiting successors" keeps its own; `key_encoding` and `predecessor_hash` projected; `key_value` absent | A-9, L-2, L-4, L-5 |
| T-MIG-3 | `tests/store/test_writer_migrations.py` | A frame missing a column that a migration added is written with a typed NULL; any other missing column raises; an extra column raises (the failure injection of rev-2 m1 and m2, against the real writer). N's `append_key_events` writes `key_encoding = 1` on every HOP_KEY_CURRENCY and CHAIN_COMPLETENESS event | R-3, L-1 |
| T-MIG-4 | `tests/checks/test_open_keys_encodings.py` | On a store holding encoding-2 open identities and REKEYED legacy events (written by test fixtures, standing in for N+1): N reads encoding 1 only and takes REKEYED as no event, so the legacy spells are open with their original age; N judges the hop as on the full sweep and writes REKEYED (encoding 2) for each foreign identity; every discrepancy open before is open after | R-2, L-6 |
| T-MIG-6 | `tests/checks/test_chain_encodings.py` | Chain PENDING carry filters by encoding; foreign PENDING records force the chain's full recheck and are closed with REKEYED | R-2 |
| T-MIG-8 | `tests/test_run_overlap_versions.py` | A STARTED NORMAL run of a **different** `engine_version`, younger than the grace, makes `dre run` exit 3; older than the grace, it does not block | R-5 |
| T-N-5 | `tests/test_trace_encodings.py` | Trace finds key events and cause lines under either encoding and labels each | A-10, R-7 |
| T-N-6 | `tests/test_session_timezone.py` | `dre run` exits 3 when the session time zone is not UTC after `get_spark`, naming the zone, and runs normally in UTC | A-11, K-9 |

## 2. Non-regression criteria

| ID | Criterion |
| --- | --- |
| N-R1 | The full suite passes with **no existing test restated** (none of release N's changes alters an existing rule). In particular: `tests/test_evaluation_modes.py` (migration 1), `tests/store/test_views.py::test_open_keys`, `tests/store/test_install.py`, `tests/test_execution_types.py`, `tests/checks/test_hop.py`, `tests/checks/test_open_keys_idle.py`, `tests/checks/test_chain.py` |
| N-R2 | **Equivalence.** The e2e synthetic store evaluated by the previous release and by N gives identical `dq_check_result` rows (state, population, violations, observed, qualifications) and identical key events apart from the new columns. Asserted by a test that runs both code paths on one fixture, or by comparing the e2e stores. |
| N-R3 | Replay scenarios R01 to R12 (and the later replays) pass unchanged |
| N-R4 | Guard tests pass: one write path, append-only, ALTER only in `store/migrations.py`, no `key_value` in email |
| N-R5 | `dre validate --conf conf/` passes; the packaging and dry-run steps of CI pass |
| N-R6 | CI is green on the pushed commit. The step summary starts with the CI run number and commit, then the test proving each criterion |

## 3. Operational criteria

| ID | Criterion |
| --- | --- |
| N-O1 | The release notes state: apply migration 2 (`dre install --apply`) before the first `dre run`; `KEY_ENCODING = 1`; rollback **below N** is unsupported once migration 2 is applied (R-4) |
| N-O2 | Migration 2 is executed only on local or scratch stores during development. No migration runs against a shared environment until a deployment is separately authorised |
| N-O3 | An evidence note (`docs/design/evidence/gate-a-release-n.md`) records each criterion, the test and the CI run |

## 4. Not accepted as release N evidence

- Prototype results under `docs/reviews/`: these are models, not the engine.
- Any claim about release N+1 behaviour.
- Any production-readiness statement.
