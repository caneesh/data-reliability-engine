# Handoff: where the build stands (2026-10-19)

Read this first when resuming. Then read `CLAUDE.md` (working rules), `docs/spec.md` (the source of truth), `docs/decisions.md` (every assumption, with its reason) and `docs/architecture.md` (guarantees and limits). This file only says where things stand and what is waiting.

## Status

- **Merged to `main`.** PR #1 (https://github.com/caneesh/data-reliability-engine/pull/1) was merged as merge commit `96503b9`. CI run #29 on `main` passed all three jobs: `test`, `e2e` and `package`.
- **Branch.** The working branch, `claude/claude-md-documentation-a2lxt0` (repo `caneesh/data-reliability-engine`), was restarted from `main` after the merge. New work goes there. Open a pull request only when the user asks; then merge with a merge commit, because the docs cite commit SHAs.
- **Release 1** (spec section 10, steps 1 to 11) is done. It was then extended by two code reviews and an architecture review.
- **Release 2** has started. Derived datasets (join and aggregate hops) are built.
- **Release 3** (extensibility) is built: the eight-step plan in `docs/design/extensibility.md`, agreed by the user, built in order; each step has a spec amendment and a `docs/decisions.md` entry, and `docs/extending.md` is the how-to.
- **Release 4** (outside-in evidence, `docs/design/outside-in-evidence.md`) is agreed and has started: step 1 of 6 (T1_PARTITION_DRIFT) is built; step 2 (T1_FILE_RECORD_COUNT, 1 GiB per-file cap agreed) is next in that plan, but the Phase 1 build order comes first unless the user says otherwise.
- **Phase 1 design: approved, frozen, and being built step by step (the current work).**
  - **The assessment.** An external static review (2026-10-16) found 12 findings, 3 of them critical. The answer is `docs/design/target-architecture.md`: verdicts on each finding, invariants, ADR-01 to ADR-08, exit criteria and the build order. `docs/design/reconciliation-model.md` is the model: populations, 11 dispositions, mapping kinds, exclusions, rejects, deletions, multi-source shapes, the chain, time modes, examples and the 10 proof scenarios.
  - **Approval.** The user approved all eight ADRs on 2026-10-16, and the design is frozen. `docs/design/traceability-matrix.md` maps every requirement to its code and tests.
  - **Controlled implementation.** Each build step ends at an architectural checkpoint (`docs/design/checkpoints.md`): plan against delivered, conformance by ADR, deviations to ratify, invariants, evidence, risks, the gate. Then **stop for the user's review**. Each step also updates `docs/design/implementation.md` (how it is built) and the matrix in the same commit.
  - **Step 1** (the contract schema and its validation) is built (`f9d86d4`, CI #76). **Checkpoint 1 was ratified on 2026-10-17, approved with conditions.** D1 to D5 are approved. D6, inferring a hop's kind from `key_map`, is approved for backward compatibility on four conditions that bind steps 3 and 6 (`docs/decisions.md`, "Checkpoint 1 ratified").
  - **Step 2** (commit rows and committed-only views, ADR-07) is built (`d8db1ad`, CI #79). **The user reviewed checkpoint 2 on 2026-10-17: changes required before step 3** (CP2-01 to CP2-04; E3 not approved; E1 and E2 approved with conditions; E4 to E6 approved). On the user's "Yes" to the proposed changes, **the revision is built**: cause commits naming the result they explain (no timestamps), stored rows counted before every commit (a unit withheld whole when they differ), a failed commit write read back and completed, single-file commit batches, the REPLAY list from committed results, `commit_batch`, the observation tables documented, CI keeping the per-test report. **The revised checkpoint 2 awaits the user's review** (`docs/design/checkpoints.md`, "Checkpoint 2, revised"; `docs/decisions.md`, "Checkpoint 2 reviewed" and "Checkpoint 2 revision").
  - **Second review, 2026-10-17: corrections R1 to R3**, then the user's ratification "approved for implementation, subject to acceptance testing". **Built:** the consistency model (spec section 5, item 6); evidence states VALID, INVALID, EXPIRED, UNKNOWN (`v_commit_state`; `v_latest_result.healthy` = PASSED and VALID, the one definition); evidence lifetimes on commits (13 months partition states, 84 results) honoured by retention, with investigation holds; the integrity sweep (daily 35 days, weekly full) and `dq_integrity`; compromised baselines (hop full sweep, drift cannot pass, REBASELINED); confirmed repair (readback, CONFIRMED only, conflicts recorded); `result_run_date` on cause commits; freshness in the digest and watchdog; the late-thread fix. **Signed off on 2026-10-18 for controlled development, not production certification; step 3 authorised** (`docs/decisions.md`, "Checkpoint 2 signed off").
  - **Step 3** (HOP_LEDGER and `dq_reconciliation`) is built (`docs/decisions.md`, "Phase 1 build step 3", with deviations L1 to L15 to ratify; `docs/design/implementation.md`, "Step 3"). Checkpoint 3 was reviewed on 2026-10-19: **changes required** (C3-01 to C3-04, conditions on L1, L4, L5, L6, L13, L14, L15; L3, L11, L12 revised). The user ratified the correction plan; **the corrections are built** (coverage continuity for every hop check, reject lineage and rules, union attribution states, persisted qualifications and claim-aware `healthy`, verified and unverified columns, triage, identity uniqueness, LEDGER_UNBALANCED incidents, insufficient_evidence holding the window). **The revised checkpoint 3 awaits independent review and ratification** (`docs/design/checkpoints.md`, "Checkpoint 3, revised"; `docs/decisions.md`, "Checkpoint 3 reviewed"); step 4 is blocked until then.
  - **Steps 4 to 8, built in parallel (2026-10-20)** under the user's master implementation prompt (parallel development of checkpoints 3 to 8, reviewed at Gate A and Gate B): inventory and frozen interfaces in `docs/design/workstreams-c4-c8.md`; each workstream on a local feature branch in its own worktree (four by parallel agents), merged in order. Step 4 (evaluation modes, AS_OF_REPLAY, versioned migrations), step 5 (T1_SOURCE_ATTRIBUTION, HOP_CONTRIBUTION, survivorship), step 6 (CHAIN_COMPLETENESS), step 7 (replays R14 to R17, R19, R20, R23) and step 8 (run budget, kill switch, queue check, backpressure, `scripts/measure_run.py`). Evidence: `docs/design/evidence/gate-a.md` (C3 to C5, gate run on `4da17be`: 901 passed, e2e green) and `gate-b.md` (C6 to C8, gate run on `1b800ca`: 955 passed, e2e green). **None of it is ratified**; the revised checkpoint 3 still awaits its review. Defaults to ratify: `docs/decisions.md` M1 to M9, S1 to S6, K1 to K9, O1 to O7 (M2 widens a guard: ADD COLUMNS in `store/migrations.py` only).
- **Context from 2026-10-15 and 16.** The user's team is building a temporary in-pipeline audit (control table, rejects table, filter rule ids, audit log lines) that modifies the jobs; DRE stays outside the jobs. `docs/evidence-contract.md` fixes what that audit writes so DRE's probes read it by configuration (also a Claude Doc, "DRE Evidence Contract"). A feature catalogue of the engine is a Claude Doc, "DRE: Feature Catalogue" (https://claude.ai/code/artifact/4831d1d5-a7fd-427e-9a49-e67b4dc62e39). The real flow: a scheduled process loads HDFS files up to a hand-off layer (merge or gcf), a continuous process moves them into gold temp, and gold main merges on its own schedule; whether the hand-off table is written by insert-overwrite per batch partition is still unknown (it decides where partition drift matters first).
- **Tests:** 955 (`pytest --collect-only -q | tail -1` gives the exact count). A full local run takes about 56 to 67 minutes on this machine; CI about 30. Replay scenarios R01 to R13, R14 to R17, R19, R20 and R23 pass, plus a three-layer acceptance test; PS8's replay is `tests/test_commit_protocol.py`; R18 is not added (nothing to run it on) and R22 is blocked on release 4 step 2.

## History, in order (each step green in CI)

| What | Commit | CI run |
| --- | --- | --- |
| Steps 1 to 11 (release 1) | up to `5254493` | up to #21 |
| Code review: 27 of 29 findings fixed | `b49bca8` | #23 |
| Review items 6 and 7: NO_UPSTREAM_DATA business window; NORMAL, RERUN and REPLAY | `04bae92` | #24 |
| Code-review document: value ties, UNASSESSABLE keys, recoverable evidence, investigation tracking, evidence levels, `hop_exclusions` | `446abfa` | #25 |
| Architecture-principles review: excluded and pending counts, three-layer acceptance test | `15ccb3f` | #26 |
| Release 2: derived datasets | `4afcfc7` | #27 |
| Merge to `main` (PR #1) | `96503b9` | #29 |
| Handoff for compaction | `525e23c` | #31 |
| Extensibility step 1: self-registering capabilities (`docs/design/extensibility.md`) | `23d1a8a` | #32 |
| Extensibility step 2: check parameters, T1_FRESHNESS, T1_COLUMN_RANGE | `d4f452d` (#33 red: four result counts) and `30fffda` | #34 |
| Extensibility step 3: patterns from configuration, `kind` | `c1f2811` | #35 |
| Extensibility step 4: rule templates from configuration, guard rule 10 | `31fa034` | #36 |
| Extensibility step 5: named calendars | `80317cf` | #37 |
| Extensibility steps 6 and 7: notification sinks, extension packages, `dre guard`, testing kit | `122b76a` | #38 (one run for the push of both commits) |
| Extensibility step 8: `docs/extending.md` | `a8a0107` | #38 |
| Code review of release 3 (`docs/code-review-2026-10-13.md`) and its fixes A1 to A11 | `23de7a7` (#40 red: the review alone), then `9591b1e`, `1243f93`, `614a5b3`, `7eac24e`, `01fcadf` | #41 (one run for the push of all five) |
| Design pass from that review (section B) | `9b3592d`, `d74e5c8`, `7fdc435`, `85d10f8`, `05c9464` | #42 (one run for the push of all five) |
| Independent verification (the uploaded brief; report in `docs/verification-2026-10-13.md`), batch 1: N3, N1, N5, F2, P5, P2, P14 | `c089dbb` (#44 red: a test's duplicate YAML key), `e4d549d` | #45 |
| Verification batch 2: partition pruning (P4), budgeted investigations and writes (P6), released checkpoints (P8), bounded open-key read (P1) | `9782c38` | #46 |
| Verification P3: candidate keys before the hop's window functions | `02274b7` | #47 |
| Verification batch 3a: script transform by its USING, framed run output, bounded reads, validate warnings, null keys | `8e10e19` | #48 |
| Verification batch 3b: retried investigations judge open keys, bounded landing and log reads, `landing.max_files`, daily retry cadence, watchdog exit codes, `dre trace --key-file` | `1594c4a` | #49 |
| Developer question 1: curated load time is `cdc_upd_ts` (sample config, fixtures, e2e) | `1cd75e6` | #52 |
| Curated load time format (`yyyy-MM-dd HH:mm:ss:SSSSSS`) and minute granularity | `5fc3365` | #54 |
| Evidence contract for the team's in-pipeline audit (`docs/evidence-contract.md`) | `e029127` | #56 |
| Release 4 plan proposed: `docs/design/outside-in-evidence.md` (not built) | `55857bd`, revised in `9719cc4` and `04dcc20` | #58 to #60 |
| Release 4 step 1: T1_PARTITION_DRIFT, `dq_partition_state` | `bc52b11` | #62 |
| Release 5 plan proposed: `docs/design/reconciliation-contracts.md` (not built) | `802a274`, handoff in `701c513` and `1376ff0` | #64 to #66 |
| Phase 1 assessment answered: `docs/design/target-architecture.md`, `docs/design/reconciliation-model.md` (proposed; release 5 plan superseded) | `46ae302` | #68 |
| Developer answers (soft-void deletions, no gold aggregates, no survivorship); ADR-01 to ADR-05 approved; traceability matrix `docs/design/traceability-matrix.md` | `c5779a7`, `5a37ee3`, `cd72333`, `f11b5c4` | #70 to #73 |
| ADR-06 to ADR-08 approved; Phase 1 design frozen | `c3b90ec` | #74 |
| Phase 1 build step 1: the contract schema and its validation (`grain`, `contracts`, protected queues, hop exclusions through the contract, `dre validate`'s hop report); checkpoint 1 in `docs/design/checkpoints.md` | `f9d86d4` | #76 |
| Checkpoint 1 ratified with conditions (D6 conditions bind steps 3 and 6); inferred hop kinds labelled | `5e7830f` | #78 |
| Phase 1 build step 2: commit rows and committed-only views (ADR-07), failure-injection tests; checkpoint 2 | `d8db1ad` | #79 |
| Implementation guide `docs/design/implementation.md` (steps 1 and 2) | `029bdda` | #80 |
| Handoff for compaction | `fcaa98f` | #81 |
| Checkpoint 2 CI line | `8129f75` | #82 |
| Checkpoint 2 revision after the user's review: cause commits by identity, verification before commit, repair after a failed commit write, REPLAY list from committed results, `commit_batch`, CI test report | `3112889` | #83 |
| Revised checkpoint 2 CI line | `2c0f3f7` | #84 |
| Checkpoint 2 corrections R1 to R3: evidence states, integrity sweep, lifetimes and holds, confirmed repair, `result_run_date`, freshness reporting; evidence package | `ce7a283` | #85 |
| Checkpoint 2 corrections CI line | `7fe029d` | #86 |
| Checkpoint 2 signed off for controlled development; step 3 authorised | `3d3f3c2` | #87 |
| Phase 1 build step 3: HOP_LEDGER, `dq_reconciliation`, `rejects` on contracts, the scope line; checkpoint 3 | `1f30227` | #88 |
| Checkpoint 3 CI line | `a5e8190` | #89 |
| Checkpoint 3 corrections: coverage continuity, reject lineage, qualifications and health, union attribution, triage, identity, LEDGER_UNBALANCED, window hold; revised checkpoint 3 | `6387989` | #90 |
| Revised checkpoint 3 CI line | `486792a` | #91 |
| Workstreams C4 to C8: inventory, frozen interfaces, ownership | `ab36f21` | #92 |
| Steps 4 and 5 (Gate A): evaluation modes, AS_OF_REPLAY, migrations; source attribution, contribution, survivorship | `4da17be` | #93 (green) |
| Steps 6 to 8 (Gate B): CHAIN_COMPLETENESS, replays, run budget and kill switch | `1b800ca` | #94 (still running at 11:21 UTC; the same code is green in #95) |
| Gate A and Gate B evidence packages; handoff and checkpoints (docs only, on `1b800ca`) | `88df3e5` | #95 (green) |

Each row has a section in `docs/decisions.md` saying what was decided and why.

## What is waiting

**Next action**, in this order unless the user says otherwise:

0. ~~Confirm CI for the corrections commit.~~ Run #90 on `6387989`, green, with the `test-report` and `e2e-logs` artifacts kept.
1. ~~Confirm CI for Gate A and Gate B.~~ #93 green on `4da17be`; #95 green on `88df3e5` (Gate B's code plus documentation); #94 on `1b800ca` itself was still running when recorded.
2. **Wait for the reviews:** the revised checkpoint 3, then Gate A (`docs/design/evidence/gate-a.md`), then Gate B (`gate-b.md`). Record each verdict in `docs/decisions.md`; a change asked for is built the controlled way (failing test first).
3. **Still open in the build order:** R18 (no aggregate configured to replay), R22 (needs release 4 step 2), the concurrency cap N (decisions O6), the load test (a representative cluster).
4. **Pending values**:
   - DRE's YARN queue: the user will provide it. It is never the cluster's high-priority queue, whose name stays out of the repo; deployed config lists it in `protected_queues`.
   - The kill switch's path and owner.
   - Whether curated is append-only (`grain.history`).
   - Developer questions 17 and 19.
5. **Release 4 step 2, T1_FILE_RECORD_COUNT**, on the user's word (it supplies proof scenario 9's evidence): records inside a landed file (lines, delimited rows, an XML record tag by line scan, Spark's reader for sequence, parquet and ORC) against the raw rows per file, counted once per file into a new `dq_file_count` table, 1 GiB per file. Build it like step 1: spec amendment first (sections 1, 3, 5, 6, 8), the check as a parametrised check under `landing.record_count`, tests for FAIL, PASS and every DID_NOT_RUN path, a decisions entry, the e2e synthetic landing (sequence files with a known record count) giving PASSED.
6. **Release 4 step 3, T1_SEQUENCE_GAP**, then steps 4 and 5. (The release 5 plan is superseded by the Phase 1 build order.)

Real keys, tables and columns come from the user in later iterations (the first, the curated load time column, arrived on 2026-10-15). When an answer arrives:

1. Turn it into configuration, or a small change.
2. Add tests, and get CI green.
3. Summarise, starting with the CI result.

**Spec questions from the independent verification (2026-10-13; `docs/verification-2026-10-13.md`, section 9).** Each changes behaviour, so the engine keeps today's reading until the user answers; the finding ids are the report's:

1. Section 4: rows whose `load_time` lands in an already-closed window are never re-read; require `settle_minutes` to exceed the longest write, or re-read the previous window (F9)?
2. Section 4: RERUN "for every dataset planned" and REPLAY "exactly" versus the code's due-ness gate, which skips windows that were due only because files landed (F1)?
3. Section 6: "its latest load time plus sla_hours": the key's latest load time, or the load time of its latest version (F4)?
4. Section 4 and 5: should RERUN and REPLAY write `dq_cause_result` rows under the NORMAL `evaluation_id` (F7)?
5. Section 5: what first-seen means for a file still under its root after its `dq_file` rows aged out (P9)?
6. Section 6: STILL_FLAGGED written per evaluation (every open key, every evaluation), or only changes with "still open" derived in the view (P1)?
7. Section 6: T1_KEY_DUPLICATES per window (keys loaded in it) or whole table (P13)?
8. Section 11: record the platform prerequisites explicitly: service-account grants (SELECT on sources, read on landing and log paths, INSERT on dq only, no HDFS write), the Spark settings for secret and key-string exposure on executors (`spark.authenticate`, `spark.network.crypto.enabled`, `spark.io.encryption.enabled`), plan redaction (`spark.sql.redaction.string.regex`), and dynamic allocation keeping executors with cached blocks.

**Developer: 18 questions.** They are in the user's Claude Doc, "DRE: questions for the developer" (https://claude.ai/code/artifact/0364b496-8c3e-437a-a070-ccd20af9174c):

1. The curated load-time column. **Answered 2026-10-15: `cdc_upd_ts`**, a string in `yyyy-MM-dd HH:mm:ss:SSSSSS`, minute granularity (`docs/decisions.md`, "Curated load time column").
2. Record-time formats. This is now the most urgent: a wrong format parses every record time as null.
3. The raw table's columns and load time.
4. What raw's file-name column holds.
5. How long raw keeps rows.
6. The winner rule across loads.
7. The gold merge clause.
8. Whether there is a CDC table.
9. The hold-marker file.
10. The handoff (cursor) file's path, pattern and date format.
11. How the loader picks a partition (WRONG_PARTITION).
12. Run logs and their error pattern.
13. Filter rules.
14. The rejects table.
15. Query 4.
16. Queries 5 to 7.
17. (new, 2026-10-16) Is the hand-off table (merge or gcf) written by insert-overwrite per batch partition, and what is its partition column and format? Needed to configure T1_PARTITION_DRIFT on a real table.
18. ~~Where are source deletions expressed?~~ Answered 2026-10-16: a soft void, with the end date set equal to the effective date and the row kept at every layer. Also answered: no gold table holds counts or totals; all sources have equal priority, so there is no survivorship (`docs/decisions.md`, "Developer answers on the reconciliation model").
19. (new, 2026-10-16) Is any gold row built by joining columns from two sources, rather than stacked by source? And does a void ever have an end date other than its effective date?

    Questions 12 to 14 are answered by shape, not yet by name: `docs/evidence-contract.md` (2026-10-16) fixes the control table, rejects table, filter rule registry and log line that the team's in-pipeline audit writes; the deployed configuration fills `probes.load_log`, `probes.filter_rules` and `probes.rejects` with the real names once the first batch writes them.

**Platform team:**

- The mail relay: `email.smtp_host`, `smtp_port` and `sender`.
- Client deploy mode on an edge node, with Python 3.10.
- A cluster throughput figure, if checks should be refused before they run (`scan_mb_per_minute`).
- The `dq` database and its service account.
- Where scheduler (Control-M) history can be read, so it can serve as cause evidence.
- DRE's YARN queue (the user will provide the name), and a representative cluster for the ADR-08 load test.
- Whether any layer is genuinely append-only (`grain.history`, needed for AS_OF_REPLAY).

**User decisions waiting now:** ratify checkpoint 2 (E1 to E6).

**User decisions** (each already works on its default):

- WRONG_PARTITION: probe `confirm_when` is `later` (the default) or `different`.
- `email.all_clear_digest`: how often an all-clear digest is sent.
- `compute_budget_scope`: `check` or `dataset`.
- `watchdog_owner`: who receives watchdog alerts.
- Whether the watchdog should keep state, to catch a run that starts late in its hour and then hangs (a known gap).
- Whether hourly table-wide datasets should run when no feed covers their table.

**Still open, deferred on purpose** (`docs/architecture.md`, "Open"):

- **Real derived contracts.** The first real derived dataset waits on actual aggregate or joined gold tables. The sample config has none; the examples are in the spec and the tests.
- **Derived datasets as downstreams.** Count-against-count reconciliation. Build it only if a real case needs it.
- **One-to-many hops.** One upstream row exploded into several target rows.
- **Snapshot replays.** RERUN and REPLAY read today's data. Versioned tables would be needed for true snapshots.
- **Benchmarking store reads.** Some reads of the dq store are not bounded by date. Benchmark them against a store holding a year of hourly runs.
- **A real-cluster run.** Spec section 10's two-week unattended run, and acceptance criterion 7 (an outage or heavy load does not affect source processing). Both are outside this repository.

## Settings that carry an assumption (defaults)

In `defaults.yaml` unless noted. The README table has the full list.

| Setting | Default |
| --- | --- |
| `email.smtp_host` / `smtp_port` / `sender` | null / 25 / null (digests and alerts are printed, not sent) |
| `email.all_clear_digest` | `every_run` (or `daily`, `never`) |
| `watchdog_grace_minutes` | 30 |
| `watchdog_owner` | null (every recipient) |
| `investigation_retry_days` | 7 |
| `compute_budget_scope` | `check` |
| `scan_mb_per_minute` | null |
| `max_window_hours` | 72 |
| `rule_run_at` | `"06:00"` |
| `rule_weekly_day` | null (each dataset's `full_sweep_day`) |
| `settle_minutes` | 15 |
| `initial_lookback_hours` | 24 |
| Feed probe `file_value_compare.confirm_when` | `later` |
| Feed probe `file_value_compare.format` | null (that cause is NOT_READY) |

Production table, column and path names stay null: CLAUDE.md forbids inventing them. The checks that need them report DID_NOT_RUN, and the causes NOT_READY.

## How the engine fits together

- **Run** (`runner.py`). The hourly NORMAL run evaluates only the feeds that are due, and gold rules run on their own frequency.
  - RERUN (`--window-start`, `--window-end`) and REPLAY (`--from`, `--to`) re-evaluate historical windows. They are idempotent: an outcome already stored is not written again. They read and write no key events, register no files and send no email. They read today's data, not a snapshot.
  - A window is held (evaluated again next run) after a PLATFORM or BUDGET reason, or when the run that evaluated it did not finish COMPLETED. A hold is released after `max_window_hours`, with a WINDOW_GAP row.
  - Each FAILED result goes to the cause engine. Investigations are counted in `dq_run` (`investigations_expected`, `investigations_done`) and retried by the next run if they don't finish.
- **Checks.** All check logic is SQL, in `checks/sql/*.j2`.
  - Tier 1 checks are in `checks/tier1/`, gold rules in `checks/rules/`. T1_PARTITION_DRIFT (release 4) runs once per table where `check_params` configures it: it lists each partition's directory, counts only partitions whose files changed, and fails on a changed count outside the window; its state rows ride on the result (`CheckResult.partition_state`) and the runner appends them with the run id, like key events.
  - Hop checks are in `checks/hop/`. Keys are judged at their upstream deadline, plus keys reloaded downstream in the window, plus open keys, with everything on `full_sweep_day`.
  - Key states are MISSING, STALE, UNASSESSABLE (no usable record time) and CURRENT.
  - `observed` counts the keys excluded (by `hop_exclusions`, or by a contract's exclusions, named `id@version`) and the keys still pending (within `sla_hours`).
- **Reconciliation contracts** (Phase 1 step 1: `config/models.py`, `config/contracts.py`, `config/validate._check_contracts`). Per dataset: `grain.history` and `contracts` per upstream, each with:
  - `mapping` (eight kinds);
  - versioned `exclusions` with effective dates, `approved_by` and `max_share`;
  - `reject_policy` (default defect);
  - `deletion` (the member pipeline's soft void: end date equals effective date);
  - `tolerance`;
  - `reference` and `precedence`.

  A hop without a contract has its kind inferred (`dedup`, or `aggregate` for a derived upstream) and is labelled so. `dre validate` prints every hop's kind and blind spots. Hop checks apply contract exclusions only while effective.
  - HOP_VALUE_AGREEMENT compares each side's set of tied latest values.
  - Open keys are tracked per hop (`dq_key_event.hop`).
- **Derived datasets** (release 2: `checks/derived.py`, `checks/sql/derived_relation.sql.j2`). A dataset may be `derived` (from, joins, where, group_by, columns) instead of having a `table`.
  - Wherever an upstream's table would be read, its relation is read instead. Its `key` is the comparison grain.
  - The join mapping is called `match`, not `on`: YAML reads an unquoted `on` as `true`.
  - Derived datasets are never checked themselves.
  - Cause JOIN_UNMATCHED explains a MISSING key whose left-join lookup did not match.
- **Causes** (`causes/`). One row per cause check per failing item, at most 100 items. Each cause line names its evidence level: confirmed, suspected, or insufficient evidence.
  - Generic probes: `file_exists`, `file_value_compare`, `log_contains`, `table_contains` and `size_changed`, plus builtins.
  - NO_UPSTREAM_DATA looks at the load's business window (previous slot to deadline) and comes after WRONG_PARTITION in the cause order.
- **Store** (`store/`). Append-only, written only through `writer.append`.
  - Tables: `dq_run`, `dq_check_result`, `dq_cause_result`, `dq_key_event`, `dq_file`, `dq_partition_state` (release 4) and `dq_commit` (Phase 1 step 2).
  - **Commit protocol** (ADR-07; spec section 5; `docs/design/implementation.md`, step 2). An evaluation writes its evidence, then its results, then commit rows (`kind` result); an investigation writes its cause rows, then a commit (`kind` cause, `result_run_id` naming the result it explains). `runner.Publisher` batches them (`commit_batch`, default 25), counts the stored rows against the commits before writing (a unit that differs is withheld whole), writes each batch as one file, and after a failed write reads back and appends what is missing. Nothing is visible before its commit; causes attach by identity, never by time.
  - Evidence states (`v_commit_state`): INVALID, EXPIRED, UNKNOWN, VALID; `dq_integrity` holds the integrity sweep's records (SWEEP, EVIDENCE_MISMATCH, COMMIT_CONFLICT, REBASELINED). Commits carry `result_run_date` and `evidence_kept_until`; `dre retention` honours lifetimes and `retention_holds`.
  - Views: `v_commit_state`; `v_committed_result` (with `evidence_state`), `v_committed_key_event` and `v_committed_partition_state` (INVALID units' rows hidden), `v_committed_cause` (with `result_run_id`, `result_run_date`, `evidence_state`); `v_commit_integrity` (read by the sweep); `v_latest_run`; `v_latest_result` (latest committed window first); `v_open_keys`; `v_file_status` (observations, not commit-gated); `v_latest_partition_state`. **Every reader uses the committed views.** The one exception is the window history (`checks/events.py`), which reads `dq_check_result` and holds runs that did not finish COMPLETED (decision E1); the REPLAY window list reads committed results.
  - DDL is applied only by `dre install --apply`. Tables are IF NOT EXISTS; views are CREATE OR REPLACE, so `--apply` upgrades a store. `dre retention [--apply]` runs weekly.
- **Email, trace and watchdog.**
  - `notify/` builds the digest; the golden files are in `tests/notify/golden/`.
  - `trace.py` follows one key through the layers.
  - `watchdog.py` counts NORMAL runs only and alerts on incomplete investigations.
- **Packaging.** `python scripts/package.py` builds `dist/` for `spark-submit`; the README covers deployment.
- **Extension points** (release 3). Registries (`registry.py`) for checks, probes, builtin causes, rule templates, normalisers, calendars and sinks; each capability registers itself where it is defined.
  - By configuration: `check_params` (parametrised checks T1_FRESHNESS, T1_COLUMN_RANGE), `conf/patterns/` (with `kind: file | table`), `conf/templates/` (SQL rule templates, run only as a subquery: guard rule 10), `conf/calendars/`, and `notify:` sinks (email, stdout).
  - By code: an extension package with an entry point in `dre.extensions`, loaded only when `defaults.yaml` lists it; `dre guard <path>` runs the guard scanner (now `hcsc.datalake.dre.guard`) over its source; `hcsc.datalake.dre.testing` holds the contracts.

## Working conventions (learned the hard way)

- **Summaries start with CI.** Every summary of a change starts with the CI result (run number and commit), then names the tests that prove the change.
- **Commit trailer.** End commit messages with the `Co-Authored-By:` and `Claude-Session:` lines from the session's attribution reminder. Put no model name anywhere else.
- **Never commit untested work.** Run the full suite (in the background, then read its last line) and the end-to-end check by hand before committing, even when a stop hook asks for a commit.
- **Guard tests** (`tests/guard/`) scan `src/`:
  - SQL on the store names it `{dq_database}`, never `{db}`.
  - No `.write` outside `store/writer.py`.
  - No source-specific words (stopper, handoff, file_date, sub_id, rms).
  - No `key_value` anywhere in `notify/`.
- **Changing tests.** Change a test only when the user changed the rule it encodes. Never weaken a test so it passes.
- **Spark gotchas found here:**
  - **Unused CTEs.** Spark analyses CTEs even when no query uses them, so a broken unused CTE masks the real error. Add a CTE only to the queries that use it.
  - **Null `max_by` ordering.** `max_by` returns null when its ordering column is null.
  - **Driver time zone.** PySpark returns timestamps in the driver process's time zone; `session.use_utc_clock` sets it to UTC.
- **Local environment.** The venv is `.venv` (Python 3.11, Java 21); CI uses Python 3.10 and Java 17.
- **End-to-end check by hand:**
  1. In an empty directory, run `python <repo>/tests/e2e/build_synthetic.py`.
  2. Run `dre install --conf conf --apply`, then `dre run --conf conf`.
  3. Run `python <repo>/tests/e2e/verify.py`. Expect `48 results {'DID_NOT_RUN': 20, 'PASSED': 28}`, run COMPLETED, and `dre run` printing `38 of 38 checks written` (two commit batches). Curated's load time is confirmed, so its windowed checks and gold's hops run, and the provider gold table's T1_PARTITION_DRIFT records its baseline. Set `PYSPARK_PYTHON` to the venv's interpreter if the workers pick up another Python.
- **CI.** Check it with the GitHub MCP `actions_list` tool (load it with ToolSearch `select:mcp__github__actions_list`). After a push, schedule a `send_later` check-in about 25 minutes out (the test job takes about 20) instead of polling; when it fires, read the notification, check the run, fill in the handoff row ("(this commit)" becomes the SHA and run number), commit that docs-only change and push.
- **Gate commits on the suite's exit status** (`grep -q "^exit 0" log && git commit`), never on a command that cannot fail. A docs-only commit (handoff rows, plans) needs no suite run; a YAML comment change gets `dre validate` plus `tests/config` and `tests/guard`.
- **Stop-hook commit requests** are hook feedback, not the user: while a suite is running, say so and wait.
- **Fixtures.** `create_table(..., partition_by=[...])` makes a real partitioned ORC table; `curated_row(loaded=...)` writes the curated load time string; tests that enumerate the store's tables import `DQ_TABLES`; `tests/test_registries.py` lists every built-in check id, so a new check is added there.
- **Spark gotchas added in release 4.** `DESCRIBE FORMATTED t PARTITION (...)` prints two Location rows: the partition's first, then the table's. A view ordered by `observed_at DESC, run_id DESC` ties when a test records twice with the same time and run id: give each simulated run its own.
- **Uploaded review documents.** Check each point against the code before acting; don't follow them blindly. Reproduce a reported defect with a failing test before fixing it.
- **Controlled implementation (Phase 1).** Build one step only on the user's word. State its plan first. End with:
  - a checkpoint report (`docs/design/checkpoints.md`);
  - the implementation guide and traceability matrix updated in the same commit;
  - a decisions entry listing every choice beyond the ADR's text, for ratification.

  Then stop. A ratification is recorded in `docs/decisions.md` before the next step starts.
- **Reading the store.** Read results, key events, partition states and causes only through the committed views. New evidence is written before the result and counted in its `Commit`; its committed view requires that count to be above 0.
- **Store rows in tests.** `tests/store/conftest.append_rows` commits synthetic result, key-event and partition-state rows by default; `commit=False` leaves them unpublished. A helper that writes evidence directly (`append_key_events`, `record_state`) must also `append_commits`, as the runner does.
- **Failure injection.** Simulate a dying process with a `BaseException` raised inside a write: it escapes the run's error handling and leaves the run STARTED. Then patch `runs.in_progress` to `None` for the recovery run, instead of waiting out `watchdog_grace_minutes`.
- **Code changed during a suite run?** Python loaded the old modules, so restart the suite, or re-run every affected file on the new code before committing. Say which in the commit message.
- **Production names.** Production queue names, for example, are never written into the repository, even when the user mentions them; they belong in the deployed configuration.
