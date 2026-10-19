# Architectural checkpoints

Each build step of the frozen Phase 1 design (`docs/design/target-architecture.md`, section 7) ends at a checkpoint. The next step starts only after the user has reviewed it (the user's instruction, 2026-10-16: "a controlled implementation phase").

A checkpoint report states:

- what was planned and what was delivered;
- how the work conforms to each approved ADR;
- every deviation from the written design, for the user to ratify;
- the invariants, re-verified;
- the evidence: CI run, commit, tests;
- what changed for existing configuration;
- the blind spots that remain;
- the risks;
- the gate to the next step.

## Checkpoint 1: the contract schema and its validation (2026-10-16)

**Status:** built; **ratified by the user on 2026-10-17, approved with conditions** (below, "Ratification"). **CI:** run #76 on `f9d86d4`, green (`test`, `e2e`, `package`). **Proving tests:** `tests/config/test_contracts.py` (the schema, its rules, the aliases, the report) and `tests/checks/test_hop_contracts.py::test_a_contract_exclusion_counts_only_rows_loaded_while_it_is_effective` (a contract changes what the hop checks expect, and only as written).

### Planned against delivered

| Planned (step 1 plan, 2026-10-16) | Delivered |
| --- | --- |
| Spec first: section 3 blocks, section 11 values | Spec sections 1, 3, 6, 9 and 11 amended |
| `grain` with key, version rule and history class | `grain.history`; key and version are the existing `key` and `record_time`, with `winner_rule` for ties (deviation D1) |
| One mapping kind per hop | `contracts.<upstream>.mapping`, the eight kinds; rules per kind in `dre validate` |
| Versioned exclusions with effective dates, approval and `max_share` | Built and validated. The hop checks honour them now; `max_share` is applied by the ledger (step 3) |
| `reject_policy`, default defect | Built and validated: `{disposition, max_share}` per reason code (deviation D4) |
| `deletion`: tombstone rule, soft or hard | Built and validated: `tombstone`, `applied_as`, `soft_when` (deviations D2, D3). Conditions parsed by `dre dry-run` |
| Aliases, so today's configuration keeps working | `config/contracts.py`: a hop with no contract is `dedup` (or `aggregate` for a derived upstream). `hop_exclusions` is used exactly as written, with a warning |
| `dre validate`: unsupported mappings, blind spots, tolerance limits, exclusion ids, protected queue | All built. The per-hop report is printed after a valid configuration |
| Sample configuration: gold's soft-void deletion and a `grain` block | Gold: `union_partition`, soft void, `mutable`. Curated: history left null (a platform fact). Provider feed: `append_only` and `mutable` |
| Tests; the aliases giving identical results on today's sample | 26 configuration tests, 5 check tests, 1 dry-run test. Alias equivalence: `test_the_alias_and_an_equivalent_contract_judge_alike`, plus the unchanged full suite and the e2e run |
| Decisions entry; traceability matrix moved to coded and unit-tested | `docs/decisions.md`, "Phase 1 build step 1"; matrix sections 4 to 6 |

### Conformance by ADR

| ADR | What step 1 makes true | Left for later steps |
| --- | --- | --- |
| 01 Populations, dispositions, scope | A tolerance can name only `missing`, `stale` and `divergent`, as non-negative counts. Anything else is a configuration error | The ledger, the identity, scope columns (step 3) |
| 02 Grain, identity, mapping kinds | Every hop has exactly one kind, declared or resolved. A kind that is compared through `key_map` must have an entry, and one that cannot be compared that way must not. `aggregate` is the kind for a derived upstream, and only for one | Key collisions, transforms, DID_NOT_RUN / `unsupported_mapping` (step 3) |
| 03 Multi-source shapes | `union_partition` requires an attribution (`feed_filter`). `join_contribution` requires a reference covering the contributor's key. `survivorship` requires one shared precedence | T1_SOURCE_ATTRIBUTION, HOP_CONTRIBUTION (step 5) |
| 04 Exclusions, rejects, deletions | Exclusions carry an id, version, approval and volume guard, and the hop checks apply them only while effective. Rejects default to defect. Deletions are declared | Volume guards, the reject and deletion dispositions (step 3) |
| 05 Time and modes | Each dataset can declare its history class; an undeclared or mutable one is reported as no as-of replay | Evaluation modes, AS_OF_REPLAY (step 4) |
| 06 Evidence levels | Blind spots per hop are reported by `dre validate` | Levels on dispositions and results (steps 3 and 6) |
| 07 Commit rows | Untouched | Step 2 |
| 08 Isolation | `yarn_queue` can never be a protected queue (static check) | Run budget, kill switch, run-start check (step 8) |

### Deviations to ratify

Each is recorded in `docs/decisions.md`, and the reconciliation model document now matches what was built.

| # | The design said | Built | Why |
| --- | --- | --- | --- |
| D1 | `grain: { key, version, history }` | `grain: { history }`. The key is `key`, and the version is `record_time` plus the hop's `winner_rule` | One source of truth. Repeating the key and version under `grain` would let the two copies disagree |
| D2 | `deletion.formats` lists the date patterns | No `formats`. Conditions parse their dates with `to_date(column, 'pattern')` | Every configuration condition is plain SQL. A pattern map would mean rewriting user SQL |
| D3 | `applied_as: { soft: <SQL> }` | `applied_as: soft` plus `soft_when: <SQL>` | A plain enum, with its condition beside it, validates more clearly |
| D4 | `reject_policy: { CODE: authorised }` | `{ CODE: { disposition, max_share } }` | ADR-04 gives authorised reasons a volume guard, and the short form had nowhere to put it |
| D5 | Union attribution as a contract field | The dataset's `feed_filter` | It already partitions a shared gold table among per-source datasets. A second field could contradict it |
| D6 | A hop without a contract defaults to `identity` (implied) | `dedup` (or `aggregate` for a derived upstream) | `dedup` is what HOP_KEY_CURRENCY judges today: the latest upstream version per key |

### Invariants re-verified

- **Hard constraints 1 to 7.** The guard tests pass. No store table, writer or view changed, and nothing writes anywhere new. The sample's new conditions use spec-example columns only, and the queue names stay out of the repository.
- **Three result states.** No result state, reason code or check result shape changed.
- **Existing configuration is judged identically.** A legacy `hop_exclusions` condition reaches the SQL exactly as before (`test_the_legacy_condition_is_used_exactly_as_written`). The full suite passes unchanged, apart from the new tests, and the e2e run still gives 48 results (28 PASSED, 20 DID_NOT_RUN) with the sample's contract declared.
- **The one behaviour change** is opt-in: a hop that declares contract exclusions has them honoured, and its `observed` text names them by id and version.

### Blind spots now visible on the sample

From `dre validate --conf conf`:

```
example_curated_enrollment->gold_member_coverage: union_partition (declared)
  attribution (each row here owned by exactly one source) is not proven until build step 5
  deletions declared (soft); counted as deleted from build step 3
  as-of replay unavailable: example_curated_enrollment does not declare grain.history
  as-of replay unavailable: gold_member_coverage is mutable
provider_roster_raw->provider_directory: dedup (inferred from key_map)
  kind dedup is inferred from key_map, not declared: its results can never claim contract-proven completeness
  deletions not modelled: a deleted record is judged like any other version
  as-of replay unavailable: provider_directory is mutable
```

### Risks and notes

1. **Over-broad exclusions still pass silently until step 3.** That was already true of `hop_exclusions`. `max_share` is now required on every new exclusion, so the guard needs no configuration change when it arrives.
2. **A `fan_out` or `join_contribution` hop is not judged.** The warning and the blind-spot line are its only trace until step 3 adds the DID_NOT_RUN result, and step 5 adds the contribution check. It can never be judged one-to-one, because keying it is an error.
3. **Effective dates are local dates.** They are read at midnight in `defaults.yaml` `timezone` and compared with the upstream row's load time. A rule meant to apply by record time would need a new field; none was asked for.
4. **Blind spots appear only in `dre validate`.** The digest does not show them yet. They belong on each result's scope (step 3).

### Gate to step 2 (commit rows, ADR-07)

1. The user ratifies deviations D1 to D6, or asks for changes.
2. ~~CI is green on the step 1 commit.~~ Run #76, green.
3. Nothing else blocks step 2. The open inputs (DRE's queue name, the kill switch's path and owner, developer questions 17 and 19, curated's history class) are needed by later steps, not by step 2.

### Ratification (the user, 2026-10-17)

**Decision: approved with conditions.**

- **D1 to D5** are approved as documented.
- **D6** is approved for backward compatibility, on four conditions:
  1. An inferred mapping kind must be distinguishable from an explicitly declared contract.
  2. A legacy dedup inference must not establish contract-proven completeness.
  3. A derived aggregate inference must not authorise aggregation reconciliation without an explicit, validated aggregation rule.
  4. Validation and result reporting must keep unsupported mappings and evidence limits visible.
- **The remaining step 1 gaps** are accepted as temporary, each to be resolved at its assigned checkpoint.
- **Step 2** (ADR-07 commit rows and committed-only views) is authorised, once these conditions are recorded in the decision log (`docs/decisions.md`, "Checkpoint 1 ratified").
- **Step 2 acceptance** requires failure-injection and recovery tests proving that no incomplete evaluation becomes visible as a committed result.
- **Scope of the approval.** It is not an independent verification of CI run #76 or of the source code.

| Condition | Met now | Enforced later |
| --- | --- | --- |
| 1. Inferred is distinguishable | `Hop.declared`. `dre validate` labels every hop "declared" or "inferred from key_map", and an inferred hop carries its own blind-spot line (`config/test_contracts.py::test_validate_prints_each_hop_its_kind_and_its_blind_spots`) | Each result's scope records it (step 3) |
| 2. No contract-proven completeness from an inferred dedup | The blind-spot line says so | The ledger (step 3) writes a result's claim as contract-proven only for a declared contract. An inferred hop's result is scoped "inferred contract", and CHAIN_COMPLETENESS (step 6) treats such a hop as below the claim level, so a chain through it is `partial_chain` |
| 3. No aggregation reconciliation from an inferred aggregate | A blind-spot line on an inferred aggregate hop (`config/test_contracts.py::test_a_hop_without_a_contract_resolves_from_the_release_1_fields`). Today's group-level hop checks (release 2) keep running unchanged | The ledger reconciles an aggregate hop only under a declared `mapping: aggregate`, whose derived definition validation has checked (step 3) |
| 4. Unsupported mappings and evidence limits stay visible | `dre validate` warnings and blind spots | DID_NOT_RUN / `unsupported_mapping` and blind spots on each result's scope (step 3); evidence levels (steps 3 and 6) |

## Checkpoint 2: commit rows and committed-only views (2026-10-17)

**Status:** built; **reviewed by the user on 2026-10-17: changes required before step 3** (`docs/decisions.md`, "Checkpoint 2 reviewed"). Revised below, "Checkpoint 2, revised". **CI of the first build:** run #79 on `d8db1ad`, green (`test`, `e2e`, `package`).

**Acceptance condition** (checkpoint 1 ratification): failure-injection and recovery tests proving that no incomplete evaluation becomes visible as a committed result. The proving tests are in `tests/test_commit_protocol.py`:

| Boundary | Test | What it proves |
| --- | --- | --- |
| The process dies after the evidence, before the results | `test_a_run_that_dies_after_the_evidence_publishes_nothing` | The key events and earlier results are in the tables and in no view. The run stays STARTED. The next run publishes the window once, with its key once |
| The process dies after the results, before the commit | `test_a_run_that_dies_before_the_commit_publishes_nothing` | The FAILED result is in the table and in no view. The next run publishes the same window once |
| The commit cannot be stored | `test_a_commit_that_cannot_be_stored_leaves_the_run_partial_and_nothing_visible` | The run is PARTIAL with 0 checks written, and nothing is visible. The next run is COMPLETED and publishes |
| The evidence is partly stored, then the write fails | `test_partly_stored_evidence_is_never_published_and_the_marker_says_why` | The stored key events stay invisible. The FAILED result is never written. The committed marker says evidence_incomplete. The next run publishes |
| An unpublished result and a RERUN | `test_an_unpublished_result_is_not_stored_for_a_rerun` | A RERUN does not skip a window whose only row is unpublished |
| An unpublished partition state | `test_an_unpublished_partition_state_is_not_a_baseline` | T1_PARTITION_DRIFT never compares against it |
| A cause from a run that never committed | `test_a_cause_from_a_run_that_never_committed_stays_hidden` | It stays out even after another run commits the same id; a later retry's cause shows |
| Counting and batching | `test_an_evaluation_with_no_result_rows_counts_as_written_and_commits_are_batched` | An evaluation counts as written only once its commits are stored. Commits are appended per batch. An evaluation with no result rows counts at once, and a marker is published without counting as written |

Every boundary test checks all seven committed views (`visible()` in the test module), not one, so no reader path is left untested.

### Planned against delivered

| Planned (ADR-07) | Delivered |
| --- | --- |
| Evidence first, then the result, then one commit row | Built. Commit rows are per evaluation id, appended in batches of 25 and at the end of the run, also after a failure |
| `dq_commit` with run id, evaluation id and the rows written to each evidence table | Built, with dataset, check and result rows too (partition states are keyed by run and dataset) |
| Every current-state view shows committed rows only | Built: the three current-state views, plus every other reader (decisions entry, item 3) |
| A crash before the commit is invisible, and the held window is evaluated again | Proven for four boundaries |
| Idempotent retry through deterministic ids | Unchanged. The retry publishes the same ids under its own run |
| Concurrency as today | Unchanged: one NORMAL run at a time |
| Failure injection in `tests/test_recovery.py` | In a module of its own, `tests/test_commit_protocol.py`. `tests/test_recovery.py` keeps its tests and still passes |

### Decisions beyond the ADR's text, to ratify

Recorded in `docs/decisions.md`, "Phase 1 build step 2", item 4.

| # | Decision | Why |
| --- | --- | --- |
| E1 | The window history (and the REPLAY window list) reads `dq_check_result` itself, not the committed view | It holds every event of a run that did not finish COMPLETED, which is stricter, and it keeps the `max_window_hours` limit and the WINDOW_GAP record |
| E2 | A commit's evidence counts gate its evidence: a count of 0 keeps the evidence rows invisible | So an evidence_incomplete marker can be published without publishing partial evidence |
| E3 | *(Not approved; replaced in the revision by cause commits.)* Causes are gated by timing: the evaluation must be committed by the investigating run, or before a retry | Evaluation ids are deterministic, so a plain join on the id would publish an orphan run's causes once another run commits the same id |
| E4 | Views are `CREATE OR REPLACE`; tables stay IF NOT EXISTS | The upgrade path for an installed store: views hold no data. The guard already allowed it in `store/ddl.sql` only |
| E5 | `dq_commit` must be retained at least as long as every table it gates, checked by `dre validate` | Otherwise results would disappear when their commit rows expire |
| E6 | Batch size 25, a constant | The ADR left the size open. The e2e run (38 evaluations) is two batches |

### Invariants re-verified

- **Hard constraints.**
  - Writes go through `store/writer.py` only, to the dq store only, and append only.
  - The only new DDL is in `store/ddl.sql`.
  - The guard tests pass.
- **Three result states.** No new state or result shape. The marker is the release 1 evidence_incomplete row.
- **Existing behaviour.**
  - The e2e run gives the same 48 results (28 PASSED, 20 DID_NOT_RUN), COMPLETED.
  - The full suite passes, with tests that write synthetic store rows now committing them, as a run does.
  - **Tests whose assertions changed with the protocol**, each restated, none weakened:
    - `store/test_install.py` and `store/test_ddl.py`: views are replaced, not created once (E4). The drift test's older table keeps the columns the views read.
    - `checks/test_hop.py::test_open_keys_are_read_from_recent_partitions_only`: the open keys are read through the committed view, and both scans behind it must prune on `run_date`.
    - `test_runner.py::test_a_failed_key_event_write_makes_the_run_partial_and_the_result_is_still_explained`: it asserted that every result is stored even when its key events fail, which ADR-07 reverses. It now asserts:
      - the failing hop publishes only a marker;
      - exactly that check is short in `checks_written`;
      - everything stored is published;
      - every other stored result is explained.
    - `test_recovery.py::test_a_slow_key_event_write_is_cancelled_by_the_budget_and_marked_incomplete`: it publishes the returned commits, as the run does, and asserts that the marker vouches for no evidence.

### Risks and notes

1. **Read cost.** Every committed view is a semi join on `dq_commit`. Run-date pruning reaches both scans: `test_open_keys_are_read_from_recent_partitions_only` asserts it for the key events. The digest and history reads were already bounded by `run_date`.
2. **Invisible rows accumulate.** An interrupted run leaves rows in the tables. They are invisible, not absent, until retention drops their partitions.
3. **A dead run still blocks NORMAL runs** for `watchdog_grace_minutes`, as before. Commit rows do not change that.
4. **Operator reads.** A person reading `dq_check_result` directly can still see unpublished rows. The views are the documented read surface (spec section 5).

### Gate to step 3 (HOP_LEDGER and `dq_reconciliation`)

1. ~~The user ratifies E1 to E6, or asks for changes.~~ Changes required (2026-10-17); see the revision below.
2. ~~CI is green on the step 2 commit.~~ Run #79, green.
3. Step 3 is bound by the D6 conditions from checkpoint 1: contract-proven claims only under a declared contract, no aggregation reconciliation from an inferred aggregate, and unsupported mappings and blind spots on each result's scope.

## Checkpoint 2, revised: identity, verification and repair (2026-10-17)

**Status:** built; **reviewed by the user on 2026-10-17: corrections R1 to R3 required, then ratified for implementation** (below, "Checkpoint 2 corrections"). **CI:** run #83 on `3112889`, green (`test`, `e2e`, `package`), with the `test-report` (JUnit XML, every test by name) and `e2e-logs` artifacts kept with the run. **Decisions:** `docs/decisions.md`, "Checkpoint 2 reviewed" (the review, E1 to E6 verdicts, my assessment) and "Checkpoint 2 revision" (what was built).

### Findings and what answers them

| Finding | Change | Code | Proving tests (`tests/test_commit_protocol.py` unless named) |
| --- | --- | --- | --- |
| CP2-01: a commit does not prove its evidence | Stored rows are counted before each batch of commits; a unit whose rows differ is withheld whole. The guarantee is stated (spec section 5, "What is guaranteed") and audited by `v_commit_integrity` | `store/commits.py` (`stored_counts`, `mismatches`), `runner.Publisher._verified`, `store/ddl.sql` | `test_a_unit_whose_stored_rows_differ_from_its_commits_is_withheld_whole`, `test_a_batch_whose_rows_cannot_be_counted_is_withheld`, `test_a_marker_over_partly_stored_results_is_withheld`, `test_the_integrity_view_lists_commits_whose_rows_were_lost` |
| CP2-02: a commit write that partly succeeds | A batch is one file (stored whole or not at all). After a failed write the run reads back its commits and appends the missing ones; commit identity is (run, evaluation id, kind), duplicates are harmless | `store/commits.py` (`append_commits`, `stored_commits`), `runner.Publisher._repaired` | `test_a_failed_commit_write_is_read_back_and_completed[none, some, all]`, `test_a_unit_left_split_by_a_failed_repair_does_not_count`, and the first build's boundary tests |
| CP2-03, E3: causes by timestamp | Cause commits (`kind` cause, `result_run_id`); the view joins by identity only; trace, retry and digest join through `result_run_id` | `store/ddl.sql` (`v_committed_cause`), `runner.explain_failures`, `cause_commits`, `pending_investigations`, `trace.py`, `notify/digest.py` | `test_a_cause_attaches_to_a_result_by_identity_never_by_time`, `test_a_partly_stored_cause_set_is_never_published_and_the_next_run_retries_it`, `tests/test_trace.py::test_a_retried_investigation_s_cause_is_shown` |
| CP2-04: the file registry | Documented: `dq_run` and `dq_file` record observations, not evaluations; conclusions drawn from them are commit-gated | spec section 5, "Observations are not evaluations" | (documentation) |

### E1 to E6 after the review

| # | Verdict | Status now |
| --- | --- | --- |
| E1 | Approved with conditions | The window history holds every event of a run that did not finish COMPLETED and labels it by run status (`run_partial`, `run_started`). The REPLAY window list now reads committed results only: `test_the_replay_window_list_has_committed_windows_only` |
| E2 | Approved with conditions | Counts are now checked against the stored rows before the commit (CP2-01) |
| E3 | Not approved as written | Replaced by cause commits (CP2-03) |
| E4 | Approved | Unchanged; `store/test_install.py::test_apply_replaces_views_left_by_an_older_release`; every reader and its view: `docs/design/implementation.md` |
| E5 | Approved | Unchanged, enforced by `dre validate`; the shorter evidence retention is documented and audited |
| E6 | Approved, tunable | `commit_batch` in `defaults.yaml` (default 25, 1 to 1000); `config/test_validate.py` case commit-batch-zero. Retry and partial writes are tested above; load is untested (no representative cluster, ADR-08) |

### The review's exit criteria

| Criterion | Where it stands |
| --- | --- |
| Every committed result is linked to one unambiguous evaluation identity | Result commits: (run, evaluation id). Cause commits: (run, evaluation id) naming (result run, evaluation id) |
| Publication cannot falsely claim complete evidence after partial or ambiguous writes | Verification before every commit; partial evidence, partial results under a marker, partial cause sets and failed commit writes are each tested |
| Retries are idempotent across evidence, result, cause and commit rows | Evidence, results and causes of an interrupted unit stay invisible; the retry publishes under its own run with deterministic ids; duplicate commit rows publish nothing more |
| Committed-only views are validated for failure, concurrency and recovery | Failure and recovery: the tests above, and the first build's four boundary tests, which check all seven committed views. Concurrency is unchanged: one NORMAL run at a time (`runs.in_progress`); a run still STARTED past `watchdog_grace_minutes` counts as dead and no longer blocks, as before |
| The guarantee and its limits are documented | Spec section 5, "What is guaranteed" and "Observations are not evaluations" |
| A reviewer can inspect the tests and run evidence | CI keeps `test-report` (JUnit XML, every test by name) and `e2e-logs` with each run |

### Invariants re-verified

- **Hard constraints.** Writes go through `store/writer.py` only, to the dq store only, append only; the new view is in `store/ddl.sql`; the guard tests pass.
- **Three result states.** No new state. A withheld unit is not a result: its window is held and evaluated again, as after any failed write.
- **Existing behaviour.** The e2e run gives the same results, COMPLETED, every check written.
- **Tests restated, none weakened:**
  - `test_a_cause_from_a_run_that_never_committed_stays_hidden` became `test_a_cause_attaches_to_a_result_by_identity_never_by_time`: the same orphan case, now with the orphan's timestamp after the other run's commit, which the timing rule let through.
  - `test_an_evaluation_with_no_result_rows_counts_as_written_and_commits_are_batched` stores the result rows its commits vouch for, since a commit with no stored rows is now withheld.
  - `tests/store/conftest.append_rows` publishes synthetic cause rows by a cause commit, as the run does.

### Risks and notes

1. **Read cost.** `v_committed_cause` semi joins the cause commits on the result commits, which has no `run_date` bound on the result side (a retry explains an older day's result). `dq_commit` has one small row per evaluation; the first build's view had the same unbounded join.
2. **Write cost.** One aggregate query per batch over the run's own rows, plus one read-back after a failed write only.
3. **A withheld unit repeats until its rows match.** If something kept making counts differ (a writer defect), the window would be held until `max_window_hours`, then become a WINDOW_GAP, as for any persistent write failure. The run's output says `integrity: ... withheld` each time.
4. **`v_commit_integrity` lists expired evidence too.** Query it with a `run_date` bound inside the shortest retention to see only real losses.

### Gate to step 3 (HOP_LEDGER and `dq_reconciliation`)

1. The user reviews this revision, or asks for changes.
2. ~~CI is green on the revision commit, with the test report kept.~~ Run #83, green, both artifacts kept.
3. Step 3 is bound by the D6 conditions from checkpoint 1.

## Checkpoint 2 corrections R1 to R3 (2026-10-17)

**Status:** built; **signed off by the user on 2026-10-18 for controlled development (not production certification); step 3 authorised** (`docs/decisions.md`, "Checkpoint 2 signed off"). **CI:** run #85 on `ce7a283`, green (`test`, `e2e`, `package`), with the `test-report` (JUnit XML) and `e2e-logs` artifacts kept with the run. **Decisions:** `docs/decisions.md`, "Checkpoint 2 corrections R1 to R3". **Evidence package:** `docs/design/evidence/checkpoint2-corrections.md` (commands, committed-view and integrity-view output, residual risks).

### Corrections and their proof

| ID | Correction | Code | Proving tests (`tests/test_evidence_integrity.py` unless named) |
| --- | --- | --- | --- |
| R1 | Consistency model documented (spec section 5, item 6) | spec section 5 | (documentation) |
| R1 | The publisher-less cause commit moved out of the cancellable thread | `runner.explain_failures` | `test_f01_a_cancelled_investigation_that_finishes_late_never_publishes` |
| R1 | Results preserved when evidence is lost later; evidence states VALID, INVALID, EXPIRED, UNKNOWN; `healthy` | `store/ddl.sql` (`v_commit_state`, committed views, `v_latest_result`) | `test_f01_evidence_changed_after_verification_is_invalid_and_never_healthy`, `test_f02_evidence_lost_after_commit_keeps_the_result_and_marks_it_invalid`, `test_f09_commit_metadata_outliving_its_evidence_is_expired_not_lost`, `test_a_failed_sweep_is_unknown_not_lost_until_a_sweep_completes` |
| R1 | Compromised or unverified evidence never healthy, and never a baseline | views; `runner.without_pass`, `rebaselines`, forced full sweep; `integrity.compromised_baselines` | `test_compromised_baselines_until_rebaselined`, `test_partition_drift_cannot_pass_on_a_compromised_baseline`, `test_a_compromised_hop_runs_as_a_full_sweep_and_is_rebaselined`, `test_the_digest_never_counts_unverified_evidence_as_healthy` |
| R1 | Explicit lifetime (13 months partition states, 84 results); retention deadlines; investigation holds | `store/commits.py` (`with_lifetime`), `store/retention.py` (`droppable`), `config/models.py` (`RetentionHold`) | `test_a_commit_records_how_long_its_evidence_is_kept`, `test_retention_keeps_promised_and_held_partitions` |
| R1 | Confirmed loss (EVIDENCE\_MISMATCH, INVALID) against transient failure (SWEEP FAILED, UNKNOWN) | `store/integrity.py` (`sweep`) | `test_f02_...`, `test_a_failed_sweep_is_unknown_not_lost_until_a_sweep_completes` |
| R1 | Integrity-check freshness reported | `notify/digest.py`, `watchdog.integrity_problems` | `test_the_digest_never_counts_unverified_evidence_as_healthy`, `test_the_watchdog_reports_integrity_freshness_and_incidents`, the digest golden files |
| R2 | Readback after repair; only CONFIRMED units published; absent, conflicting, unavailable fail closed; conflicts recorded | `runner.Publisher._repaired`, `store/commits.stored_commits` | `test_f04_a_repair_whose_readback_fails_is_uncertain_and_not_published`, `test_identical_duplicate_commits_are_confirmed`, `test_a_conflicting_commit_fails_closed_and_is_recorded`; `tests/test_commit_protocol.py::test_a_failed_commit_write_is_read_back_and_completed[none, some, all]`, `::test_a_unit_left_split_by_a_failed_repair_does_not_count` |
| R3 | `result_run_date` on cause commits; join on run, run\_date, evaluation id | `store/ddl.sql` (`v_committed_cause`), `runner.cause_commits` | `test_f07_f08_causes_attach_by_run_run_date_and_evaluation_id`; `tests/test_trace.py::test_a_retried_investigation_s_cause_is_shown`; `tests/test_commit_protocol.py::test_a_cause_attaches_to_a_result_by_identity_never_by_time` |

### The failure-injection matrix

| ID | Scenario | Required outcome | Test | What it asserts |
| --- | --- | --- | --- | --- |
| F01 | Evidence changes between verification and commit | No false committed PASS | `test_f01_evidence_changed_after_verification_is_invalid_and_never_healthy`; late threads: `test_f01_a_cancelled_investigation_that_finishes_late_never_publishes`, `test_f01_a_cancelled_evidence_write_that_lands_late_stays_invisible` | The added key event is found by the sweep (`key events: 2 declared, 3 stored`); the PASSED result stays, INVALID, `healthy` false, its key events hidden. A late cause row has no commit and no visible cause; a late key event under a marker stays invisible |
| F02 | Evidence missing after commit | Integrity invalidation and visibility policy enforced | `test_f02_evidence_lost_after_commit_keeps_the_result_and_marks_it_invalid` | `key events: 3 declared, 0 stored` recorded; the FAILED result stays visible, INVALID |
| F03 | Commit append partly succeeds, then raises | Readback reconciles exact identities | `tests/test_commit_protocol.py::test_a_failed_commit_write_is_read_back_and_completed[none, some, all]`, `::test_a_unit_left_split_by_a_failed_repair_does_not_count` | Every unit published once after the repair; a unit left split by a failed repair is not counted |
| F04 | Repair append succeeds, then the readback fails | Uncertain; no success asserted | `test_f04_a_repair_whose_readback_fails_is_uncertain_and_not_published` | 0 published, 2 unconfirmed, although the 3 commit rows landed |
| F05 | Retry under the same deterministic evaluation id | Idempotent; no conflicting published rows | `test_f05_f06_one_current_result_when_runs_publish_the_same_evaluation`; the commit-protocol boundary tests (published once) | One current result |
| F06 | Two publishers race on the same evaluation | Single authoritative outcome | `test_f05_f06_one_current_result_when_runs_publish_the_same_evaluation` | Exactly one `v_latest_result` row: the later evaluation, then the higher run id |
| F07 | Cause date differs from the result's | Correct association | `test_f07_f08_causes_attach_by_run_run_date_and_evaluation_id`; `tests/test_trace.py::test_a_retried_investigation_s_cause_is_shown` | The date-B cause for the date-A result is shown, with `result_run_date` 2026-02-18 |
| F08 | Orphan or conflicting cause commit | Not visible as a valid cause | `test_f07_f08_causes_attach_by_run_run_date_and_evaluation_id` | Orphan, collision (same run and id, other date), unrelated and INVALID cause sets are not shown |
| F09 | Commit metadata survives evidence retention | No misleading complete-evidence claim | `test_f09_commit_metadata_outliving_its_evidence_is_expired_not_lost`; `test_retention_keeps_promised_and_held_partitions` | EXPIRED, not INVALID, not healthy; retention keeps promised and held partitions |
| F10 | Prior checkpoint 2 regression suite | All previously passing tests green | `tests/test_commit_protocol.py`, the full suite | Green (CI row in the handoff) |

Precedence (the acceptance list): `test_normal_rerun_and_replay_precedence` (a RERUN of the current window replaces it; a REPLAY or RERUN of an older window never becomes current), with `tests/test_execution_types.py`.

### Invariants re-verified

- **Hard constraints.** Writes through `store/writer.py`, append only, to the dq store; partition drops only in `store/retention.py`; no change to any pipeline; guard tests pass.
- **Three result states.** No new state: evidence states describe a committed unit, not a result. One new DID\_NOT\_RUN reason, `evidence_invalid` (PLATFORM).
- **D1 to D6 and E1 to E6.** Unchanged, with E3 as revised at the first review.
- **Tests restated, none weakened:** the synthetic key-event commit (one commit per evaluation, as a run writes it), `test_views.py`'s key-event ids, the digest golden files' new integrity line.

### Residual risks

1. **Outside the model.** Rows or commits forged by a writer with access to the dq database are not detectable when they match each other; the sweep detects only mismatches.
2. **Sweep cost.** The weekly full sweep counts every unexpired commit's rows (key events are the largest table); the daily sweep reads 35 days. Not load-tested (ADR-08).
3. **A compromised hop's full sweep** may exceed its budget; the window is then held and retried as a full sweep until `max_window_hours`, then becomes a WINDOW\_GAP.
4. **UNKNOWN is store-wide by scope.** A failed daily sweep makes every result of the last 35 days UNKNOWN (not healthy) until a sweep completes: noisy, but fail closed.
5. **Wall-clock ordering of sweeps.** Which sweep is latest is decided by `recorded_at` (DRE's own records); a clock step backwards could pick an older sweep. It affects UNKNOWN only, never which cause attaches to which result.
6. **An unconfirmed unit may be published twice**: if its commit landed but could not be confirmed, its window is evaluated again and published under the next run; the precedence rule picks one current result.

### Gate to step 3

1. Independent verification of R1 to R3 and F01 to F10 (each Verified or Reopened).
2. ~~CI green on the corrections commit, with the test report kept.~~ Run #85, green, both artifacts kept.
3. The user signs off checkpoint 2.


## Checkpoint 3: HOP_LEDGER and dq_reconciliation (2026-10-18)

**Status:** built; **reviewed 2026-10-19: changes required** (see "Checkpoint 3, revised" below). **CI:** run #88 on `1f30227`, green (`test`, `e2e`, `package`), with the `test-report` (JUnit XML) and `e2e-logs` artifacts kept with the run. **Decisions:** `docs/decisions.md`, "Phase 1 build step 3". **Implementation:** `docs/design/implementation.md`, "Step 3". **Evidence package:** `docs/design/evidence/checkpoint3-ledger.md` (committed-view and integrity-view output from the end-to-end store).

### Planned and delivered

| Planned (the authorisation of 2026-10-18) | Delivered |
| --- | --- |
| The ledger: populations, the eleven dispositions in precedence, the asserted identity | HOP\_LEDGER, one result per hop; due, pending and untimed populations; eleven dispositions (`superseded` never assigned before step 5); identity asserted, `ledger_unbalanced` otherwise |
| Tolerance on `missing`, `stale`, `divergent` only; volume guards; the reject policy; orphans; key collisions | All built; rejects need a declared `rejects` source (new contract field); orphans where the rows can be attributed (L6) |
| The reconciliation contract and the D6 conditions | Claims `contract-proven` only under a declared contract; an inferred aggregate is never reconciled; unsupported kinds are DID\_NOT\_RUN / unsupported\_mapping with their scope |
| The evidence scope: scope on every result, evidence levels, claim ceiling, blind spots | Scope JSON in `detail` and on every `dq_reconciliation` row; one line in the digest |
| The commit protocol and evidence states of checkpoint 2 | `dq_reconciliation` is evidence: written before the result, declared in `dq_commit.reconciliation_rows`, counted before the commit, gated by `v_committed_reconciliation` (not INVALID), in `v_commit_integrity` and the sweep, kept by retention while promised |

### Requirements and their proof

| Requirement | Code | Proving tests (`tests/checks/test_hop_ledger.py` unless named) |
| --- | --- | --- |
| Every due record in exactly one disposition; the identity asserted | `hop_ledger.sql.j2` (`placed`), `ledger.counted` | `test_every_due_record_delivered_passes_and_the_ledger_balances`, `test_an_unbalanced_ledger_is_did_not_run_never_a_verdict`, `test_counted_finds_a_record_in_no_bucket_or_in_two`; `tests/store/test_reconciliation.py::test_a_run_publishes_each_hop_ledger_with_its_result` |
| Due, pending, untimed; the version as of the cutoff | `up_cut`, `due_keys` | `test_the_latest_upstream_version_is_judged_and_a_later_one_waits`, `test_a_key_with_no_load_time_is_untimed_then_unassessable_on_the_full_sweep`, `test_the_full_sweep_judges_every_key_past_its_deadline`, `test_nothing_due_is_did_not_run_and_still_records_its_lines` |
| Deficits fail; tolerance on named deficits only | `ledger.judge` | `test_missing_stale_and_divergent_records_fail`, `test_a_tolerance_lets_named_deficits_pass_up_to_its_count`; validation: `config/test_contracts.py::...[tolerance-on-unknown]` |
| Unassessable: no record time, no key, key collisions | `judged.unknown_reason` | `test_a_key_collision_after_normalisation_is_unassessable`, `test_no_record_time_and_no_key_are_unassessable` |
| Exclusions by rule and version, effective dates, breaches, volume guards, overlaps | `exclusion_terms`, `judge` | `test_an_approved_exclusion_accounts_for_an_absent_record`, `test_an_excluded_record_present_downstream_is_a_breach`, `test_an_exclusion_over_its_max_share_fails_its_volume_guard`, `test_overlapping_rules_are_recorded_as_diagnostics` |
| Rejects under the policy (default defect), guards, missing evidence | `rejects`, `judge`, `ledger_params` | `test_a_reject_accounts_for_a_record_only_under_an_authorising_policy`, `test_without_rejects_evidence_a_rejected_record_is_missing_and_said_so`, `test_a_declared_rejects_table_that_is_gone_is_insufficient_evidence`; `config/test_contracts.py::test_a_reject_policy_without_rejects_evidence_is_a_warning`, `::...[rejects-need-a-key]`, `::...[rejects-need-a-table]` |
| Deletions, soft and hard | `ledger` CTE | `test_a_soft_deletion_applied_is_deleted_and_one_not_applied_is_a_breach`, `test_deletions_pass_when_applied_soft_or_hard` |
| Orphans, and where they cannot be checked | orphan lines, `orphans_unchecked` | `test_a_row_here_no_upstream_row_maps_to_is_an_orphan`, `test_orphans_are_not_checked_where_rows_here_cannot_be_attributed` |
| Unsupported kinds visible; no reconciliation from an inferred aggregate (D6 condition 3) | `unsupported` | `test_kinds_the_ledger_does_not_judge_are_unsupported[aggregate, join_contribution, survivorship, fan_out]`, `test_an_inferred_aggregate_is_never_reconciled` |
| The claim: contract-proven only under a declared contract (D6 conditions 1 and 2); the contract hash; the scope's fields | `scope_of`, `contract_hash` | `test_only_a_declared_contract_claims_contract_proven_completeness` |
| Evidence levels and the claim ceiling | `LEVEL`, `ceiling` | `test_an_approved_exclusion_accounts_for_an_absent_record` (reconstructed), `test_a_reject_accounts_for_a_record_only_under_an_authorising_policy` (reported), `test_every_due_record_delivered_passes_and_the_ledger_balances` (observed) |
| Three states; an exception is DID\_NOT\_RUN | `per_hop`, `run_check` | `test_a_missing_upstream_or_configuration_is_did_not_run`, `test_a_failure_inside_the_hop_is_did_not_run_with_a_reason` |
| No key value outside the store | counts only in lines and scope | `test_no_key_value_leaves_the_check`; `notify/test_digest.py::test_the_digest_never_carries_a_key_value` |
| Ledger lines are commit-gated evidence | `store/reconciliation.py`, `store/ddl.sql`, `store/commits.py` | `tests/store/test_reconciliation.py::test_ledger_lines_are_visible_only_under_a_commit_that_vouches_for_them`, `::test_a_unit_whose_ledger_lines_differ_from_its_commit_is_withheld`, `::test_lost_ledger_lines_are_listed_by_the_integrity_view_and_hidden_once_invalid` |
| The scope in the digest | `notify/digest.scope_line` | `notify/test_digest.py::test_a_reconciliation_result_prints_its_scope_in_one_line`, the golden file |
| End to end | `tests/e2e/verify.py` | Both feeds' HOP\_LEDGER PASSED, lines committed and balanced |

### Conformance by ADR

- **ADR-01 (scoped claims).** Each result stores its scope; tolerance only on named deficits; the identity asserted.
- **ADR-02 (mapping kinds).** `identity`, `dedup`, `merge` and `union_partition` judged; the others unsupported, visibly. Collisions detected at run time. Transforms are identity only (L12).
- **ADR-03 (multi-source).** A union target is read through its `feed_filter` (D5); attribution itself is proven in step 5. `superseded` waits for step 5.
- **ADR-04 (exclusions, rejects, deletions).** Exclusions by rule and version within their dates and guards; rejects by policy, default defect, guarded; soft and hard deletions. Reject lineage by key and time (L3).
- **ADR-05 (time).** The version as of the cutoff; evaluation modes are step 4.
- **ADR-06 (evidence levels).** A level per disposition, the claim ceiling on the result.
- **ADR-07 (commits).** `dq_reconciliation` follows the protocol and the evidence states of checkpoint 2 unchanged.
- **ADR-08 (isolation).** Nothing new runs outside the check's compute budget; the ledger reads only what the hop checks read, plus the declared rejects table.

### Deviations to ratify

L1 to L15, each with its reason, in `docs/decisions.md`, "Phase 1 build step 3". In short: the due population by deadline in the window (L1), the cutoff at the window end (L2), reject lineage by key and time (L3), `identity` judged like `dedup` (L4), `ledger_unbalanced` under PLATFORM (L5), orphans only where attributable (L6), overlaps among explaining rules only (L7), untimed records and the full sweep (L8), a soft-deleted record absent is missing (L9), `deleted` at reconstructed (L10), the claim with a legacy exclusion or an unproven union (L11), identity transforms only (L12), no cause investigation for HOP\_LEDGER (L13), the release 1 hop checks unchanged (L14), `insufficient_evidence` under DEPENDENCY (L15).

### Invariants re-verified

- **Hard constraints.** Writes only through `store/writer.py`, append only, to the dq store; no pipeline change; nothing source-specific (the engine branches on the contract's kind, never on a dataset); guard tests pass.
- **Three result states.** No new state; three new DID\_NOT\_RUN reasons (`ledger_unbalanced` PLATFORM, `unsupported_mapping` CONFIGURATION, `insufficient_evidence` DEPENDENCY). An empty population is DID\_NOT\_RUN.
- **No member data outside the store.** Keys stay in a session view dropped with the evaluation; lines, scope, `observed` and the digest carry counts, codes and labels.
- **Earlier decisions.** D1 to D6, E1 to E6 and the checkpoint 2 corrections unchanged. Tests restated, none weakened: the registry's list of built-in checks and the digest golden file gained HOP\_LEDGER; four result counts gained it (R09, R11a, R11b, `test_runner.py`), each asserting it is there; the failed-evidence-write test now expects two markers (HOP\_KEY\_CURRENCY and HOP\_LEDGER both carry evidence); `tests/e2e/verify.py` now requires HOP\_LEDGER.
- **A pre-existing gap closed.** `runner.pending_investigations` listed FAILED results of checks that are never investigated (no failure type); it now lists only checks with one (`docs/decisions.md`, "Phase 1 build step 3").

### What changed for existing configuration

- Nothing is required. HOP\_LEDGER runs on every dataset with `key_map` or `contracts` in a feed pattern. An inferred hop is judged and scoped "inferred contract"; a hop of an unsupported kind reports DID\_NOT\_RUN / unsupported\_mapping on every run until its contract changes.
- Optional: `contracts.<upstream>.rejects`, once the rejects table's layout is confirmed (`docs/evidence-contract.md`). A `reject_policy` without it is now a warning.
- `dre install --apply` creates `dq_reconciliation` and replaces the views. It does not add `dq_commit.reconciliation_rows` to a `dq_commit` created before step 3 (`CREATE TABLE IF NOT EXISTS` leaves an existing table as it is); `dre install --check` reports that table as different. No store is deployed yet, so nothing is affected today; a deployed store will need a migration step (as for the checkpoint 2 columns).

### Blind spots that remain

- Reject rule id and version not read (L3); `{from, to}` transforms not compared (L12); attribution and `superseded` (step 5); evaluation modes and AS\_OF (step 4); the chain (step 6); aggregate reconciliation under a declared contract.
- Proof scenarios are proven at check level; the replay tests R14 to R23 are still to add.

### Risks

1. **Cost.** The ledger reads every upstream row of each candidate key, as HOP\_KEY\_CURRENCY does, plus this dataset's rows for the due keys and, for orphans, this dataset's rows loaded in the window and the upstream's keys. On the full sweep it reads both tables whole. Not load-tested (ADR-08).
2. **Two judgements of one hop.** HOP\_KEY\_CURRENCY and HOP\_LEDGER can disagree in wording (for example a soft void not applied is STALE for one and `deleted_not_applied` for the other); both fail, and the ledger's label is the more specific.
3. **Rejects matched by key and time** attribute a reject of an earlier version to the version under judgement when it was recorded after that version loaded. Until the rejects table carries the version or batch (L3), this cannot be told apart; a reject is reported evidence, so a PASS resting on one has the ceiling `reported`.
4. **Schema migration.** An existing store keeps its old `dq_commit` (see "What changed"); `dre install --check` finds it, but nothing migrates it yet.

### Gate to step 4

1. ~~CI green on the step 3 commit, with the test report kept.~~ Run #88, green, both artifacts kept.
2. The user reviews checkpoint 3 and ratifies or changes L1 to L15.

## Checkpoint 3, revised: the corrections C3-01 to C3-04 and the conditions on L1 to L15 (2026-10-19)

**Status:** built; **awaits independent review and ratification.** Step 4 stays blocked until then. **CI:** pending (the CI line is added once the run finishes). **Decisions:** `docs/decisions.md`, "Checkpoint 3 reviewed: changes required, then the correction plan ratified". **Implementation:** `docs/design/implementation.md`, "Corrections after the checkpoint 3 review". **Model amendments:** `docs/design/reconciliation-model.md`, section 12. **Evidence package:** `docs/design/evidence/checkpoint3-corrections.md`.

### The ratified decisions and their proof

| Item | Decision | Code | Proving tests |
| --- | --- | --- | --- |
| C3-01 | Rejects need record-version lineage and, where the policy names rules, a versioned rule; unattributable rejects never authorise | `RejectsSource.lineage`, rule columns, `RejectReason.rules`; `hop_ledger.sql.j2`; `docs/evidence-contract.md` | `checks/test_hop_ledger.py::test_a_reject_of_an_earlier_version_explains_nothing_about_the_latest` (same key, two versions, reject of the earlier one), `::test_a_reject_without_lineage_cannot_explain_a_record`, `::test_a_reason_that_approves_rules_needs_an_approved_rule` (missing rule and an unapproved version), `::test_a_reject_accounts_for_a_record_only_under_an_authorising_policy` (an unauthorised reason; the share limit), `::test_a_reject_can_name_its_batch_instead_of_its_version`; `config/test_contracts.py` lineage and rules cases |
| C3-02 | Union attribution states and claim caps now; the proof in step 5 | `ledger.attribution_of`, `qualifications_of`, `claim_of` | `::test_a_union_is_never_contract_proven_until_its_attribution_is` (overlapping filters, an uncovered filter, rows of another feed, no filter); e2e |
| C3-03 | Persisted qualifications, claim-aware health; partial value coverage disclosed | `dq_check_result.qualifications`; `v_latest_result.healthy`; `ledger.value_coverage` | `store/test_reconciliation.py::test_a_pass_with_qualifications_is_never_healthy`; `::test_the_scope_names_verified_and_unverified_columns` (identity-only mapping, renamed owned column, none, complete, a missing owned column); `::test_kinds_the_ledger_does_not_judge_are_unsupported` (aggregate, fan_out) |
| C3-04 | Collisions, unchecked orphans, ledger-only failures, missing evidence visible; no unqualified healthy PASS | the qualifications, `ledger.triage`, the digest | `::test_a_key_collision_after_normalisation_is_unassessable`, `::test_orphans_are_not_checked_where_rows_here_cannot_be_attributed`, `::test_a_failed_ledger_says_what_to_look_at`, `::test_a_declared_rejects_table_that_is_gone_is_insufficient_evidence`; `notify/test_digest.py::test_a_reconciliation_result_prints_its_scope_triage_and_blind_spots`, `::test_a_qualified_pass_is_listed_and_never_counted_as_healthy`; `test_runner.py::test_a_failed_key_event_write_makes_the_run_partial_and_the_result_is_still_explained` |
| L1 | Coverage continuity for HOP_LEDGER and the existing hop checks, one shared committed-coverage calculation | `checks/hop/coverage.py`; `runner.covers`; `dq_check_result.coverage_from`, `coverage_to` | `checks/test_hop_coverage.py`: `test_a_shorter_sla_leaves_no_gap`, `test_a_longer_sla_judges_nothing_twice`, `test_a_longer_sla_with_nothing_due_yet_never_moves_coverage_back`, `test_a_window_that_covered_nothing_is_judged_by_the_next`, `test_a_held_window_judged_again_starts_where_its_first_attempt_did`, `test_invalid_results_cover_nothing`, `test_file_completeness_continues_from_its_coverage_too`, `test_the_range_bounds_are_exact`, `test_a_late_arriving_older_version_does_not_change_the_version_judged`, `test_only_covering_results_store_coverage_and_a_marker_never_does` |
| L4 | Identity uniqueness enforced | `hop_ledger.sql.j2` (`one_to_one`) | `::test_an_identity_hop_requires_one_record_on_each_side` |
| L5 | Unbalanced ledger fails closed and is an integrity incident | `integrity.unbalanced_records`, `runner.record_unbalanced`, watchdog | `::test_an_unbalanced_ledger_is_did_not_run_never_a_verdict`; `store/test_reconciliation.py::test_an_unbalanced_ledger_is_recorded_as_an_integrity_incident` |
| L13 | Actionable triage for ledger-only failures | `ledger.triage`; the digest | `::test_a_failed_ledger_says_what_to_look_at`; the digest test above |
| L14 | How the release 1 checks' states read against the ledger | spec section 6 (table) | (documentation) |
| L15 | Insufficient evidence holds the window | `events.BLOCKING_CODES`, `blocking_sql` | `checks/test_windows.py::test_insufficient_evidence_holds_the_window` |
| Exit | Every due record in one disposition, all at once | the ledger | `::test_the_ledger_balances_with_every_disposition_at_once` |

### The acceptance criteria

| Criterion | How it holds | Evidence |
| --- | --- | --- |
| No source record unassessably lost during SLA changes | The range starts where committed coverage ended | `test_a_shorter_sla_leaves_no_gap` (a key in the old gap is judged by every hop check) |
| Every due ledger record has exactly one disposition | The identity is asserted on every evaluation; an unbalanced ledger fails closed and is an incident | `test_the_ledger_balances_with_every_disposition_at_once`, `test_an_unbalanced_ledger_is_did_not_run_never_a_verdict`, the e2e check |
| Missing or insufficient evidence never advances coverage as successful | Only covering outcomes record coverage; markers and INVALID results never do; insufficient_evidence holds the window | `test_only_covering_results_store_coverage_and_a_marker_never_does`, `test_invalid_results_cover_nothing`, `test_insufficient_evidence_holds_the_window` |
| Unsupported or unproven contracts cannot generate an unqualified healthy PASS | Unsupported kinds are DID_NOT_RUN; every unproven premise is a stored qualification, and `healthy` needs none | `test_a_pass_with_qualifications_is_never_healthy`, `test_kinds_the_ledger_does_not_judge_are_unsupported`, the e2e check (no qualified PASS healthy) |
| The scope distinguishes verified from unverified claims | `claim`, `qualifications`, `attribution`, `values`, `blind_spots` | `test_the_scope_names_verified_and_unverified_columns`, `test_only_a_declared_contract_with_nothing_unproven_claims_contract_proven_completeness` |
| The existing reconciliation checks are consistent with the ledger's temporal model | They share `judged_range`, and the cause engine through `hop_params` | `checks/test_hop_coverage.py` (HOP_KEY_CURRENCY, HOP_FILE_COMPLETENESS, HOP_LEDGER alike) |
| All regression and failure-injection tests pass | The full suite and e2e | The CI line above |

### Invariants re-verified

- **Hard constraints.** Writes only through `store/writer.py`, to the dq store; no pipeline change; nothing source-specific; guard tests pass. The coverage read is a read of DRE's own committed results.
- **Three result states.** No new state or reason code; `insufficient_evidence` now holds its window.
- **Tests restated, none weakened:** the reject tests gained lineage (a reject without it no longer authorises); the claim test now expects a qualification wherever orphans cannot be checked; `store/test_reconciliation.py::test_a_run_publishes_each_hop_ledger_with_its_result` expected the declared union to claim contract-proven (the step 3 reading of L11, now revised) and now expects the capped claim and an unhealthy result; the digest golden files gained the qualified-pass section, triage and blind spots.

### Interpretations for the reviewer

In `docs/decisions.md` ("Interpretations, for the reviewer to confirm or correct"): only HOP_LEDGER writes qualifications; a qualified pass does not change the digest's subject; the coverage read is bounded by `history_lookback_days`; an unattributable reject affects only a record absent here.

### Residual risks

1. **The audit must write lineage.** Until the team's rejects table carries `record_version` (or a batch id the upstream also carries) and, where needed, `rule_id` and `rule_version`, every reject leaves its record unassessable: a hop with rejects fails rather than passes on them.
2. **Every ledger pass is qualified today** on the synthetic configuration (an inferred contract, or an unproven union attribution), so none is healthy until contracts are declared and step 5 proves attribution.
3. **Coverage beyond the lookback.** A hop that covered nothing for longer than `history_lookback_days` restarts from the window's own range; the weekly full sweep judges what lies before it.
4. **Catch-up cost.** After a long run of non-covering windows, the next covering window judges the whole span at once, within the check's budget.
5. **Schema migration** (as before): an existing store keeps its older `dq_commit` and `dq_check_result`; `dre install --check` reports them, nothing migrates them yet. No store is deployed.

### Gate to step 4

1. CI green on the corrections commit, with the test report kept.
2. Independent review of the corrections; the user ratifies the revised checkpoint 3.
