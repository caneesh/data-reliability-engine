# 01 · Architecture assessment: the actual implementation

**Examination of** `caneesh/data-reliability-engine`, branch `claude/claude-md-documentation-a2lxt0`, HEAD `c5e6237` (2026-10-21).

**Nature of this review.**
- It is an independent, adversarial examination. It is not an approval, and it changes no production code, configuration, existing test or architecture document.
- All verification is synthetic and local (local-mode Spark 3.5.1, Python 3.11, 4 CPUs).
- Executable evidence: `adversarial/` (52 tests, all run; see `07-adversarial-test-results.md`).
- Static evidence: four read-only review passes over the code, whose candidate findings were re-checked here. Where re-checked by execution, the register says so.

## 1. Verified execution map

```
dre run (cli.py) ─► runner.run (runner.py:1147)
  _preflight: config load + validate, store check, HMAC secret (exit 3 on failure)
  runs.start_run ─► dq_run STARTED
  Halt (kill switch, run budget), concurrency.configure (checks/budget.py Gate), compromised baselines
  NORMAL:  normal_jobs ─► window per dataset from committed history (checks/events.py), gap rows,
           landing refresh; operations.order (completeness first); plan_sweeps (full sweep / backpressure)
  RERUN/REPLAY/AS_OF_REPLAY: historical_jobs, stored_outcomes (idempotency)
  for each (check, window):
      evaluate ─► check_context (runner.py:387: history_since, coverage_since, key_events_since, secret)
               ─► run_check (checks/base.py:291) ─► Check.evaluate ─► Jinja SQL (checks/sql/*.j2) ─► spark.sql
                  every exception ─► DID_NOT_RUN with a reason; population 0/NULL ─► DID_NOT_RUN/empty_population
      result_rows ─► write_evaluation (store/writer.append only) ─► Publisher.add (commits after verified counts)
      explain_failures ─► causes/engine.py (cause checks: CONFIRMED / RULED_OUT / NOT_READY / ERROR)
  Publisher.flush (stored_counts readback; withhold mismatches; repair)
  retry_investigations, integrity_sweep (NORMAL, not halted)
  email_digests (notify/digest.py, NORMAL only) ─► runs.finish_run COMPLETED | PARTIAL | FAILED
Readers: v_committed_result / v_committed_key_event / v_commit_state / v_latest_result (store/ddl.sql)
```

## 2. Components and what they actually guarantee

| Component | Claimed (spec / design) | Implemented, verified |
| --- | --- | --- |
| **HOP_LEDGER** (`checks/hop/ledger.py`, `checks/sql/hop_ledger.sql.j2`) | Every due upstream record in exactly one disposition | One disposition per **canonical key** at its **latest version by record time**. The disposition is the first match in a fixed precedence. Its correctness depends on how the key string is built (F-02), on record times parsing (F-05), and on how ties are handled (F-07). See 02. |
| **HOP_KEY_CURRENCY / HOP_VALUE_AGREEMENT / HOP_FILE_COMPLETENESS** (`hop_common.sql.j2`) | Release 1 key-level checks | These read the latest **record time**, and HOP_VALUE_AGREEMENT also `owned_columns`. Keys with no usable load time are never judged, not even on the full sweep (F-12). Files are matched by base name across all time (S-01). |
| **CHAIN_COMPLETENESS** (`checks/hop/chain.py`) | Every expected business record reached the terminal | Proven independently: each edge's layers are read with HOP_LEDGER's SQL, not inferred from stored hop results (verified: c01 to c05). "Reached" means present at the latest version, with owned values equal, edge by edge. A record that meets an authorised disposition stops being followed (F-14). Intermediate versions are not covered. |
| **HOP_CONTRIBUTION** (`hop_contribution.sql.j2`) | Each contributor's values in the target | Only for target rows **loaded in the window**: references resolve, and the contributed columns equal the contributor version current at that row's load time. An unchanged target row is never re-judged (F-10). There is no notion of an optional contributor (F-11). |
| **Evaluation modes** (`checks/modes.py`) | NORMAL, CURRENT_RECHECK, AS_OF_REPLAY | NORMAL and RERUN read the downstream as it is **now** (a25). AS_OF_REPLAY is refused unless every dataset is `append_only` with drift evidence. |
| **Commit protocol** (ADR-07: `store/commits.py`, `runner.Publisher`) | Only committed, verified evidence is visible | Verified by the existing suite and by o04: withheld units stay invisible and the run is PARTIAL. But the digest does not say so (F-18). |
| **Coverage** (`checks/hop/coverage.py`) | Continuity of judged ranges; CA-01 gaps qualified | Verified by the existing CA-01 tests. It depends on the load time reflecting when a record became visible (S-02). |
| **Cause engine** (`causes/`) | Cause proven vs suspected vs insufficient evidence | A three-level vocabulary exists (`engine.py:133-139`). Some probes overstate certainty (S-04, S-05, S-06). |
| **Read-only** (`guard.py`, `store/writer.py`, `tests/guard/`) | Writes only to `dq` | Verified statically. The only writes are `writer.append` (insertInto), retention's DROP PARTITION, and migrations' ADD COLUMNS, all on the configured dq database. Nothing checks that the dq database is not a monitored one (S-20). |

## 3. Differences between the documents and the code

1. **The precedence of `superseded`.**
   - spec section 6 (spec.md around line 613) lists it after `deleted_not_applied`;
   - the model table puts it 7th;
   - the code (`ledger.py:63-76`) and decision S1 rank it directly after `deleted`.

   The documents disagree with each other; the code follows S1.
2. **`rejected` versus the L14 table.** The spec's L14 table never maps "present, values differ" or "stale" to `rejected`. The code ranks `rejected` above the residual whether or not the record is present (`hop_ledger.sql.j2:259`). See F-04.
3. **Ties.**
   - The model says ties are ordered by `winner_rule`; the ledger ignores `winner_rule` (only `causes/merge.py` reads it). The result is F-07 (false PASS) and S-09 (false FAIL).
   - spec "Ties: tied upstream rows that disagree always fail". The code compares **sets**, so a target that holds both tied rows passes.
4. **"Unparseable key is unassessable"** (model, section 3 row 1). Not implemented. A key part that normalises to NULL is written `<null>` and joins like any other value (F-02, l05).
5. **"Judged at the latest load time + sla_hours"** (`hop/common.py` docstring). The code uses the load time of the rows at the latest record time.
6. **The cutoff applies only in the ledger.** HOP_LEDGER filters `load_ts < window_end` (`up_cut`). The release 1 hop checks read every upstream row as it is now, so "the version judged" can differ between them.
7. **The chain's claim** reads "every expected business record reached the terminal". It means: the **latest version** of each source key whose chain deadline is in range, present edge by edge with owned values equal. It is not event completeness (a17), and not value completeness beyond the owned columns (F-01).

## 4. Assessment

The architecture is sound in its intent and in most of its mechanics:
- an independent observer, reading layers directly;
- a key-level ledger with explicit dispositions and classes;
- three result states, enforced at one choke point (`run_check`, `from_counts`);
- commit-gated evidence;
- disclosure of scope, blind spots and qualifications.

Most of the basic adversarial matrix behaves correctly (07): missing, equal counts with different keys, stale, unauthorised filters, unapplied deletes, out-of-order CDC, failed SQL, empty sources, and chain gaps at each stage.

The false-PASS exposure is concentrated in **five places**:
1. how record identity is built (F-02);
2. how a key's version is chosen when record times are NULL or tied (F-05, F-07);
3. what "delivered" verifies, which is presence and record time, plus values only through `owned_columns` (F-01, F-09, F-13);
4. what is never re-judged, such as contributor staleness, downstream reloads, and orphans in empty windows (F-06, F-08, F-10);
5. what the digest leaves out (F-18).

None of these needs a change to ADR-01 to ADR-08; each is local to a template or a validation rule (`10-remediation-plan.md`).
