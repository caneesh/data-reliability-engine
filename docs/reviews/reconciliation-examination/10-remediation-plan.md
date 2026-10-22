# 10 · Remediation plan

The plan is ordered by risk, then by dependency. Every item:
- stays inside DRE, as a read-only observer. No item changes an ingestion pipeline.
- keeps ADR-01 to ADR-08. The template-level fixes change only how dispositions are computed or reported.
- has a regression test: the named adversarial test in `adversarial/`, inverted to assert the correct behaviour.

**Nothing here has been implemented** (the examination's constraint).

## Mandatory: false PASS and silent loss

| # | Fix | Findings | Where | Depends on | Effort |
| --- | --- | --- | --- | --- | --- |
| 1 | **Collision-free record identity.** Build the canonical key from escaped or length-prefixed parts (or compare struct keys). Treat a part that is NULL after normalisation but non-NULL raw as unassessable "unparseable key", as the model specifies. Add a raw-variant check on the structured key. | F-02 | `checks/keys.py:119-122`; `hop_ledger.sql.j2:71, 202`; `hop_common.sql.j2` | — (it changes key strings: key-event hashes change once, so the next full sweep re-establishes open keys; plan a rebaseline) | M |
| 2 | **No version silently dropped.** A NULL or unparseable record time on any of a key's rows at or after its judged version makes the key unassessable ("partial record time"). Count release 1 keys with no load time as untimed, and as unassessable on the full sweep. | F-05, F-12 | `hop_ledger.sql.j2:69-84, 116-131`; `hop_common.sql.j2:82-124`; `hop_file_completeness.sql.j2` | — | M |
| 3 | **Ties resolved, not merged.** Apply `winner_rule` to order tied rows (dedup, merge). Where none is declared, require **every** tied row to match an exclusion or tombstone (`MIN`, not `MAX`); otherwise unassessable "ambiguous version". | F-07, S-09 | `hop_ledger.sql.j2:77-84, 135-142` | — | M |
| 4 | **Orphans never lost.** Judge orphans before returning empty_population (FAIL on orphans). Report downstream rows with no usable load time. Scan all orphans on the full sweep. | F-06 | `ledger.py:602`; `hop_ledger.sql.j2:311-321` | — | S |
| 5 | **The digest tells the whole run.** Carry the publisher's withheld and unconfirmed counts and the run status into the digest; never "all clear" on a PARTIAL run; count vanished groups. Encode group values unambiguously (JSON with explicit null) so evaluation ids cannot collide. | F-18, F-16, S-03 | `runner.py:1282`; `notify/digest.py:114-115, 379`; `checks/events.py:70-72` | Group encoding changes evaluation ids for grouped checks: a one-time rebaseline | S |
| 6 | **A PASS cannot rest on a NULL or zero population.** In `from_counts`, NULL violations or a negative population → DID_NOT_RUN. Validate `volume_tolerance_pct` < 100. In T1_VOLUME, population 0 → never PASSED. | F-15, F-17 | `checks/base.py:134`; `checks/tier1/volume.py:86`; `config/models.py:288` | — | S |

## Mandatory: claim integrity (what a healthy PASS means)

| # | Fix | Findings | Where | Depends on | Effort |
| --- | --- | --- | --- | --- | --- |
| 7 | **Value coverage qualifies the claim.** `values.coverage` of "none" (and, by decision, "partial") becomes a qualification, so the PASS is never healthy. Survivorship requires `owned_columns` (validate), so the winner's values are compared. Tolerated deficits add a `tolerated_deficit` qualification, and tolerance gets an upper bound or a share form. | F-01, F-09, F-03 | `ledger.py` `qualifications_of`; `config/validate.py`; `models.py` Tolerance | A decision record (a new qualification) | S |
| 8 | **Re-judge what changes downstream or in contributors.** Add keys loaded downstream in the window to the ledger's candidates (as the release 1 checks do). Add a contributor-side population to HOP_CONTRIBUTION: contributor versions loaded in range whose referencing target rows still hold old values. Add `optional: true` contributions. | F-08, F-10, F-11 | `hop_ledger.sql.j2:52-67`; `hop_contribution.sql.j2`; `models.py` EdgeContract | Item 7 (claims) | M-L |

## Recommended (optional enhancements, ordered)

| # | Fix | Findings |
| --- | --- | --- |
| 9 | **File identity by full path or landing date**, not base name, in HOP_FILE_COMPLETENESS and T1_FILES_NOT_LOADED. Strongly recommended: it is a false PASS for reused file names. | S-01 |
| 10 | **Cause certainty.** Absent evidence (no logs, no lineage, file gone) → NOT_READY. Platform read errors → ERROR. Exact name match in logs. Say "earlier stage not evaluated" when a later cause is CONFIRMED. Report retried causes in a later digest. | S-04, S-05, S-06, S-07 |
| — | **Chain follows authorised-but-present records** (soft voids) to the terminal. | F-14 |
| — | **`rejected` only when absent or downstream older** (the L14 table). A deterministic reject order. | F-04, S-08 |
| — | **History-keeping targets:** declare the current-row predicate, or rank by load time for append-only targets. | F-13 |
| — | **Atomic run lease;** grace longer than the budget plus investigations; the watchdog re-judges late runs. | S-16, S-17 |
| — | **Validation:** granularity against SLA; offset formats; `key_map` subset warning; refuse two union contracts on one dataset; reference normalisers; refuse unsupported contract fields on `join_contribution`; refuse a dq database that holds non-dq tables; reject comments in SQL fragments. | S-02, S-10, S-11, S-12, S-14, S-20, S-21 |
| — | **Ledger read cost:** materialise `up_rows` and `dn_rows` once, apply partition pruning where the tables are partitioned by load date, then benchmark on a representative cluster (KI-01). | 06 §4 |
| — | **Documentation:** reconcile the precedence tables, the tie rules and the chain claim wording. | D-01 to D-07 |

## The top 10 improvements

**Mandatory:**
1. Collision-free record identity (item 1).
2. No version silently dropped (item 2).
3. Ties resolved, not merged (item 3).
4. Orphans never lost (item 4).
5. The digest tells the whole run (item 5).
6. No PASS on a NULL or zero population (item 6).
7. Value coverage qualifies the claim (item 7).
8. Re-judge downstream and contributor changes (item 8).

**Optional (strongly recommended):**
9. File identity by path (item 9).
10. Cause certainty (item 10).

**After the mandatory items:** re-run `adversarial/` with every FINDING test inverted, re-run the project suite, and perform the representative load test before any production trust decision.
