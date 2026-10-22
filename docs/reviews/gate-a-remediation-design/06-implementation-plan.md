# 06 · Implementation plan (ordered, file by file)

**Not started.** This plan waits for authorisation (`08-approval-checklist.md`). Paths are under `src/hcsc/datalake/dre/` unless shown otherwise.

**Each step's gate:**
1. Write a failing test first, against the unchanged code.
2. Make the change.
3. Run the focused tests.
4. Run the full suite and the e2e before each commit, per the repository's rules.

## Release N (preparatory; ships alone, before any migration)

| # | File | Change | Tests |
| --- | --- | --- | --- |
| N1 | `store/writer.py` | `append` fills NULL for a column the frame lacks **only** when `store/migrations.py` lists it as added by a migration. Every other missing column, and every extra column, still raises | `tests/test_writer.py`: I8a (the three cases) |
| N2 | `store/migrations.py` | Expose `added_columns(table)`, a pure function used by N1 | unit |
| N3 | docs: `docs/decisions.md`, spec section 5 (Migrations) | Record the rollback rule | — |

## Release N+1 (Gate A)

| # | File | Change | Depends | Tests |
| --- | --- | --- | --- | --- |
| 1 | `checks/keys.py` | `KEY_ENCODING = 2`; `key_string` → encoding 2 (02 §1.2); keep the old body as `key_string_v1`; add `canonical_key` (Python mirror) and `key_parsed` | — | I1, I2 |
| 2 | `store/migrations.py`, `store/ddl.sql` | Migration 2: `dq_key_event ADD COLUMNS (key_encoding INT)` | N1 | I8; `tests/store/test_ddl.py`, `tests/guard/test_read_only.py` |
| 3 | `store/key_events.py` | Write `key_encoding = KEY_ENCODING` (with a `key_encoding` argument, so the transition's closing events can write 1) | 2 | unit |
| 4 | `checks/hop/ledger.py` | `ledger_params`: add `up_parsed`/`dn_parsed` (`key_parsed`) and `up_raw_record`/`dn_raw_record` (the raw record-time column); render the reject-authorisation test once, as a macro input; triage lines for the new labels | 1 | §2 of 04 |
| 5 | `checks/sql/hop_ledger.sql.j2` | `up_rows`/`dn_rows`: `parsed`, `raw_record`. `up_ranked`/`dn_ranked`: `untimed_rows`, `unparsed_rows`, `key_rows`, `latest_load`, `key_parsed` (one window). `up_keys`: the due-load rule; MIN/MAX per rule; `excluded_all`. `dn_cand`: drop unparsed rows (counted). `judged`: the `unknown_reason` order of 02 §0. `rejected`: the total order. `placed`: tombstone and exclusion from unanimity. Orphans: `'unparseable key'` | 4 | a19, a20, l01, l03, l04, l05 corrected; V1-V3, V5-V7, T1-T4, X2, X3 |
| 6 | `checks/hop/common.py`, `checks/sql/hop_common.sql.j2` | The same F-02 and F-05 rules for the release 1 checks: `UNASSESSABLE` with a label; the raw-variant guard; `open_keys` reads `coalesce(key_encoding, 1) = 2` | 1, 2 | m04 corrected; V4, I5 |
| 7 | `checks/sql/hop_key_currency.sql.j2`, `checks/hop/key_currency.py` | Carry the new UNASSESSABLE labels into `observed` and `state_detail`. Transition: a `legacy_open` CTE (the latest event per legacy hash is open), present only under a forced sweep with legacy state, emitting `CLEARED 'key encoding changed'` with `key_encoding = 1` | 3, 6 | I9 |
| 8 | `checks/hop/chain.py`, `checks/sql/chain.sql.j2` | Recheck reads encoding 2 only. Transition: legacy PENDING closed `RESOLVED 'key encoding changed'` under the forced chain sweep. The forward mapping uses encoding 2 automatically | 3, 5 | c05 corrected; I6, I10 |
| 9 | `runner.py` | Per run, one query lists (dataset, check, hop or chain) with legacy open or pending state; those are added to the forced-sweep set (beside `compromised` and `sweeps.forced`); print one line per transition | 7, 8 | I9, I10 (runner level) |
| 10 | `checks/hop/contribution.py`, `checks/sql/hop_contribution.sql.j2` | References use encoding 2 automatically; an unparseable reference is `unresolved 'unparseable reference'` | 1 | I7 |
| 11 | `causes/probes.py`, `causes/merge.py`, `checks/sql/cause_*.sql.j2` | No logic change (they call `key_string`). Verify that the hashes compared are computed in the same evaluation | 1 | existing `tests/causes` (with literal keys restated) |
| 12 | `trace.py` | Dual-hash lookup; label each event's encoding | 1, 3 | I11; `tests/test_trace.py` |
| 13 | Tests with literal keys (04 §3) | `canonical_key([...])` in place of the literals | 1 | the suite |
| 14 | `docs/reviews/reconciliation-examination/adversarial/*` | Revise the FINDING tests for F-02, F-05, F-07 to assert the corrected outcome ("FIXED (was F-nn)"); keep every other test as is | 5-8 | 04 §1 |
| 15 | `docs/spec.md` (sections 5 and 6: Identity, Ties, Unassessable labels, key events), `docs/decisions.md` (Gate A decisions G-1 to G-7), `docs/known-issues.md`, a new evidence package | Document | all | — |
| 16 | Full suite, e2e, package, CI; the B1-B6 benchmarks when a cluster is available | Verify | all | 04 §2 X4, X5 |

**The order is forced by dependencies:**
- Release N first, so a rollback is possible.
- Then encoding (1), store (2-3) and ledger (4-5).
- Then the release 1 checks (6-7), the chain (8) and the runner transition (9).
- The rest after.

Steps 5 and 6 can be developed in parallel once 1 is merged.
