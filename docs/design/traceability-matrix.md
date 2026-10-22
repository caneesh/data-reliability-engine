# Traceability matrix: requirement → implementation → test

Status: first version, 2026-10-16. This answers the Phase 1 assessment's finding P1-12 and exit criterion 6. For every requirement the engine claims, or that the approved design adds, it says how far the requirement has got and where the evidence is. Keep it current: a step that builds or tests a requirement updates its row in the same commit.

## Levels

Each level needs the ones before it.

| Level | Meaning |
| --- | --- |
| **D** documented | Stated in `docs/spec.md`, `docs/architecture.md` or an approved design document |
| **C** coded | Implemented under `src/` |
| **U** unit-tested | A test under `tests/` (local-mode Spark, synthetic Hive tables, run by CI's `test` job) proves it, including the FAIL, PASS and DID_NOT_RUN paths where they apply |
| **R** replay-tested (build step 7) | Also proven end to end through `dre run` by a replay under `tests/replay/` (R14 onwards, one per proof scenario), asserting on the committed store views |
| **I** integration-tested | CI's `e2e` job proves it: `dre install` and `dre run` as separate processes on the two synthetic feeds, checked by `tests/e2e/verify.py`; or CI's `package` job, through `spark-submit` |
| **O** operationally proven | Shown on a representative cluster, with production-like volume and real tables |

**No row is at level O.** Every test runs in local-mode Spark on synthetic data (`docs/architecture.md`, "Operational safeguards"). A row marked "partly" names its gap.

Test references are `file::test`, with paths under `tests/`. Code paths are under `src/hcsc/datalake/dre/`.

## 1. Hard constraints (CLAUDE.md)

| # | Requirement | Level | Code | Tests | Gap |
| --- | --- | --- | --- | --- | --- |
| HC1 | Read-only outside the `dq` store | U | `store/writer.py` is the only write path | `guard/test_read_only.py::test_src_is_read_only_and_append_only`, `::test_scanner_catches`; `guard/test_ddl_only_through_install.py::test_only_install_path_applies_ddl` | The guard is static (it scans the source for write statements). There is no runtime check against cluster permissions; that is O |
| HC2 | No pipeline changes or hooks | D, by construction | No code touches a job or schedule | `guard/test_read_only.py` (no file moves or deletes) | Nothing to test beyond the guard: the repository contains no pipeline code |
| HC3 | No code per source | U | Configuration-only onboarding | `guard/test_generic_engine.py::test_engine_has_no_source_specific_words`, `::test_denylist_catches` | |
| HC4 | Three result states; an exception becomes DID_NOT_RUN | U, I | `checks/base.py` (`from_counts`, `did_not_run`, `run_check`) | `checks/test_base.py::test_exceptions_become_did_not_run_without_the_message`, `::test_run_check_never_raises`, `::test_run_check_with_no_rows_is_empty_population`; `e2e/verify.py` (48 results, every one in a valid state) | |
| HC5 | Append-only evidence; the writer exposes only `append` | U | `store/writer.py` | `test_writer.py::test_writer_exposes_only_append`; `guard/test_read_only.py` | |
| HC6 | No member data outside the store; the HMAC secret comes from a file | U | `notify/`, `checks/keys.py` | `guard/test_email_no_key_value.py::test_notify_package_never_references_key_value`; `notify/test_digest.py::test_the_digest_never_carries_a_key_value`; `checks/test_hop.py::test_key_events_keep_key_value_in_the_store_only`; `config/test_validate.py::test_hmac_secret_inside_repo_rejected`; `test_runner.py::test_run_refuses_to_start_with_an_unreadable_or_empty_secret` | Logs are not scanned at run time; only the code is guarded |
| HC7 | Synthetic data only | D | n/a | Review only | No automated scan of fixtures and docs for production names |

## 2. Architecture acceptance criteria (`docs/architecture.md`), at the scope they really have

Every criterion below is met **per hop**: one dataset against one upstream, for the keys due in a window, as the data stands at the run. None is an end-to-end claim (section 4, ADR-01).

| # | Criterion | Level | Tests | Real scope, and gap |
| --- | --- | --- | --- | --- |
| AC1 | Onboard without editing or restarting jobs | U, I | `guard/*`; `replay/test_second_feed.py` (a second feed onboarded by YAML alone); the `e2e` job runs two feeds from configuration | O: needs onboarding a real feed |
| AC2 | Missing data at the earliest observable layer, over three or more stages | U | `replay/test_acceptance_three_layers.py::test_missing_data_is_found_at_the_earliest_layer_across_three_stages`; `test_trace.py::test_the_cause_line_is_the_gap_hop_s_own` | It names the **first observed** gap, not the origin. There is no chain result: that is CHAIN_COMPLETENESS (section 5) |
| AC3 | Two upstreams feeding one gold target | U | `checks/test_hop.py::test_open_keys_are_kept_per_hop`, `::test_a_synthetic_pipeline_with_two_upstreams_exclusions_late_data_and_a_missing_key`, `::test_one_broken_hop_does_not_hide_the_others` | Each hop is judged alone. Union attribution (no row unattributed or double-counted) is not checked: T1_SOURCE_ATTRIBUTION |
| AC4 | Legitimate exclusions without false data-loss alerts | U | The same scenario test; `checks/test_hop.py::test_file_completeness_reports_what_it_excluded_and_what_is_pending`; `checks/test_derived.py::test_an_inner_join_makes_unmatched_rows_not_expected`; `checks/test_hop_contracts.py` (approved, versioned exclusions with effective dates) | The volume guard (`max_share`) is configured but not applied until build step 3, so an over-broad condition still passes; `hop_exclusions` is a warning |
| AC5 | Silent success, skipped processing and delayed loads, independent of job status | U | `replay/test_r02_r03_r08.py::test_r02_no_loads_for_longer_than_the_sla_is_on_time_failed`, `::test_r03_gold_wrote_zero_rows_while_upstream_had_rows_is_zero_rows_failed`; `replay/test_r01_file_not_loaded.py::test_r01_landed_file_without_raw_rows_while_later_files_loaded`; `replay/test_r11a_empty_no_load_due.py`, `replay/test_r11b_empty_load_due.py` | The expected volume comes from the cadence and `min_rows_per_load`, not from the source. A source file with records against zero rows loaded needs release 4 step 2 |
| AC6 | Recovery from partial DRE writes without duplicate active findings | U | `test_commit_protocol.py::test_a_run_that_dies_after_the_evidence_publishes_nothing`, `::test_a_run_that_dies_before_the_commit_publishes_nothing`, `::test_a_commit_that_cannot_be_stored_leaves_the_run_partial_and_nothing_visible`, `::test_partly_stored_evidence_is_never_published_and_the_marker_says_why`; `test_recovery.py`; `test_runner.py::test_a_failed_key_event_write_makes_the_run_partial_and_the_result_is_still_explained` | Closed by commit rows (step 2, 2026-10-17): a process that dies between writes publishes nothing, and the next run publishes the held window once. Revised after the checkpoint 2 review: verification before commit, repair after a failed commit write, causes by identity (`test_commit_protocol.py`, revision tests). Corrections R1 to R3: evidence states, integrity sweep, confirmed repair, cause dates (`test_evidence_integrity.py`, F01 to F09). O still needs a representative cluster
| AC7 | A DRE outage or heavy load does not affect source processing | D, C partly | `checks/test_budget.py::test_a_slow_check_is_cancelled_as_budget_exceeded`, `::test_a_spent_dataset_budget_or_a_too_large_scan_is_not_run` | Per-check budgets only. No run budget, queue, kill switch or load test (ADR-08). O needed |
| AC8 | Uncertainty reported clearly | U | `causes/test_causes.py::test_a_failure_with_nothing_to_explain_is_recorded_as_insufficient_evidence`, `::test_a_parameter_set_not_configured_is_not_ready_never_ruled_out`; `checks/test_hop.py::test_a_key_whose_upstream_record_times_are_all_null_is_unassessable_not_hidden` | Evidence levels exist for causes, but not for dispositions or results (ADR-06) |

## 3. Invariants (`docs/design/target-architecture.md`, section 2)

| # | Invariant | Level | Evidence | Gap |
| --- | --- | --- | --- | --- |
| IN1 | No production writes, hooks, schedule dependencies or job blocking | U | HC1, HC2 | O |
| IN2 | Every PASS is scoped to a versioned contract, population, grain, cutoff and coverage | **U** for HOP_LEDGER (step 3); D elsewhere | HOP_LEDGER stores its scope (edge, contract hash, claim, population, cutoff, grain, evidence levels, blind spots) in `detail` and on every `dq_reconciliation` row: `checks/test_hop_ledger.py::test_only_a_declared_contract_claims_contract_proven_completeness`, `store/test_reconciliation.py::test_a_run_publishes_each_hop_ledger_with_its_result` | The other checks carry no scope; evaluation modes are step 4 |
| IN3 | Every candidate record is accounted for exactly once, or reported unknown | **U** (step 3, HOP_LEDGER) | The identity is asserted: `checks/test_hop_ledger.py::test_an_unbalanced_ledger_is_did_not_run_never_a_verdict`, `::test_counted_finds_a_record_in_no_bucket_or_in_two`, `::test_every_due_record_delivered_passes_and_the_ledger_balances`; unknown records: `::test_a_key_collision_after_normalisation_is_unassessable`, `::test_no_record_time_and_no_key_are_unassessable`, `::test_a_key_with_no_load_time_is_untimed_then_unassessable_on_the_full_sweep` | Kinds the ledger does not judge yet are DID_NOT_RUN / unsupported_mapping, never accounted |
| IN4 | Transforms, merges, deletes, rejects, exclusions and late arrivals have distinct semantics | **U** (step 3), transforms partly | Distinct dispositions: deletions `checks/test_hop_ledger.py::test_a_soft_deletion_applied_is_deleted_and_one_not_applied_is_a_breach`, `::test_deletions_pass_when_applied_soft_or_hard`; rejects `::test_a_reject_accounts_for_a_record_only_under_an_authorising_policy`; exclusions `::test_an_approved_exclusion_accounts_for_an_absent_record`; late arrivals `::test_the_latest_upstream_version_is_judged_and_a_later_one_waits` | Transforms are identity (`owned_columns`) only (L12) |
| IN5 | An unobservable cause is never labelled confirmed | U | Causes are CONFIRMED only by a cause check, and suspected by elimination (`causes/test_causes.py`) | Ceilings for reported evidence (ADR-06) |
| IN6 | The earliest observed gap is not necessarily the originating defect | D, U | `dre trace` names the first hop with a gap (`test_trace.py`) | Its wording says "gap", but the digest has no explicit statement that this is not the origin |
| IN7 | Every result is reproducible against its evidence snapshot, or labelled a current recheck | **U** (step 4) | `evaluation_mode` stored on every result; RERUN and REPLAY stored and labelled CURRENT\_RECHECK: `test_evaluation_modes.py::test_a_rerun_is_stored_and_labelled_as_a_current_recheck`; AS\_OF\_REPLAY only with proof: `::test_an_as_of_replay_reads_append_only_data_as_it_stood_at_the_cutoff`, `::test_an_as_of_replay_without_proof_is_did_not_run` | Only HOP\_LEDGER reads as of a cutoff; no snapshot reader for `snapshot_partitioned` |
| IN8 | An incomplete evidence write never becomes a visible complete PASS | **U** (ADR-07, built in step 2) | See AC6 and PS8 | Interrupted rows stay in the tables, invisible, until retention. Stored rows are counted against the commit before it is written (checkpoint 2 revision); evidence changed afterwards makes the unit INVALID, never healthy (corrections R1) |

## 4. Proof scenarios (`docs/design/reconciliation-model.md`, section 10)

Each will become a replay test, R14 to R23. The "today" column says what the current engine does with the scenario.

| # | Scenario | Level | Today | Test to add |
| --- | --- | --- | --- | --- |
| PS1 | Equal counts with a substituted key must fail | **U, R** (steps 3, 7) | Check level: `checks/test_hop_ledger.py::test_missing_stale_and_divergent_records_fail`, `::test_a_row_here_no_upstream_row_maps_to_is_an_orphan`; replay: `replay/test_r14_substituted_key.py` (both tests, through `dre run`) | |
| PS2 | An approved filter is accounted for; an unapproved one fails | **U, R** | Check level as before; replay: `replay/test_r15_exclusions.py` (approved `excluded[rule@version]`, unapproved `missing` with the reported rule in a cause line; PASS contract-proven) | |
| PS3 | A rejected record with a defect reason must not pass | **U, R** | Check level as before; replay: `replay/test_r16_defect_reject.py` (defect and unlisted reasons FAILED; an authorised reason PASSED with `evidence_reported`, never healthy) | |
| PS4 | Two sources into one gold row, without double counting | **U, R** (step 5) | Union: `checks/test_source_attribution.py` (model example 9.2), `checks/test_hop_ledger_sources.py::test_a_union_whose_siblings_partition_the_table_is_verified_and_contract_proven`, `::test_an_unattributed_or_overlapping_table_keeps_the_qualification_and_is_never_healthy`; join: `checks/test_hop_contribution.py` (9.2), `::test_the_contributor_never_counts_toward_completeness`; survivorship: `checks/test_hop_ledger_sources.py::test_the_winner_delivers_and_the_loser_is_superseded`; replay: `replay/test_r17_two_sources.py` (union both ways, join both ways, through `dre run`) | Survivorship has no replay (unused by the member pipeline) |
| PS5 | Aggregation: group and contribution level, or a declared limit | U (group level; the chain's limit) | `checks/test_derived.py::test_an_aggregate_upstream_is_judged_at_its_grain`, `::test_an_aggregate_upstream_passes_when_gold_agrees`; a chain through an unjudged edge is `partial_chain`: `checks/test_chain.py::test_an_edge_the_ledger_does_not_judge_makes_a_partial_chain` | R18 **NOT_STARTED** (not counted as passed): no applicable aggregate fixture or contract; `docs/known-issues.md` KI-02 |
| PS6 | A CDC update, a tombstone, an out-of-order event and a late arrival have deterministic expected states | **U, R** | Check level as before; replay: `replay/test_r19_cdc_versions.py` (seven keys, one per case) | |
| PS7 | A historical rerun after a later load never claims a historical PASS | **U, R** (step 4) | `test_evaluation_modes.py` (all); replay: `replay/test_r20_historical_rerun.py` (NORMAL FAILED kept, RERUN labelled CURRENT\_RECHECK, AS\_OF\_REPLAY on mutable gold DID\_NOT\_RUN / no\_as\_of\_evidence) | |
| PS8 | A crash between the result and evidence writes never publishes a complete PASS | **U** (step 2) | Commit rows: evidence, then results, then the commit; views show committed rows only | Done as `test_commit_protocol.py` (the planned R21): four write boundaries, each followed by recovery |
| PS9 | A legitimate empty batch is told apart from a silent zero | U partly | `replay/test_r11a_empty_no_load_due.py` (no load due: DID_NOT_RUN), `replay/test_r11b_empty_load_due.py` (load due, nothing loaded: FAILED); calendars (`checks/test_calendars.py`) | R22 **BLOCKED** (not counted as passed): the source's file record count is release 4 step 2, not built; the cadence half is R11a/R11b; KI-03 |
| PS10 | An unsupported one-to-many mapping is visibly unsupported | **U, R** | Check level as before; replay: `replay/test_r23_fan_out_unsupported.py` (two runs, DID_NOT_RUN / unsupported_mapping each, listed by `dre validate`) | |

## 5. Approved design elements (ADR-01 to ADR-05)

Level D unless the row says otherwise. Each row names the build step (`docs/design/target-architecture.md`, section 7) that takes it further. Build step 1 (2026-10-16) took the contract schema to C and U.

| Element | ADR | Level | Evidence | Build step |
| --- | --- | --- | --- | --- |
| Candidate, due and pending populations; eleven dispositions; the asserted identity; `ledger_unbalanced` | 01 | D | | 3 (HOP_LEDGER) |
| Tolerance on deficits only, as an absolute count | 01 | **U** (validation) | `config/test_contracts.py::test_contract_errors_name_file_line_field_and_fix[tolerance-on-unknown]`, `[tolerance-negative]` | 3 (applied by the ledger) |
| Scope columns on results; `dq_reconciliation` | 01 | D | | 3 |
| No chain claim before CHAIN_COMPLETENESS | 01 | D | `docs/architecture.md` scope note | 6 |
| The `grain` block; mapping kinds and their rules; aliases for today's fields; each hop's blind spots in `dre validate` | 02 | **U** | `config/test_contracts.py` (every case), `::test_a_hop_without_a_contract_resolves_from_the_release_1_fields`, `::test_validate_prints_each_hop_its_kind_and_its_blind_spots`; `checks/test_hop_contracts.py::test_the_alias_and_an_equivalent_contract_judge_alike`; the `e2e` job (sample with a declared contract, results unchanged) | 1 (done) |
| Inferred kinds told apart from declared contracts; never contract-proven (D6 conditions 1 to 3, 2026-10-17) | 02 | **U** (validation and report) | `config/test_contracts.py::test_validate_prints_each_hop_its_kind_and_its_blind_spots`, `::test_a_hop_without_a_contract_resolves_from_the_release_1_fields` | 3 (result scope), 6 (chain) |
| DID_NOT_RUN / `unsupported_mapping` at run time | 02 | **U** (step 3) | `checks/test_hop_ledger.py::test_kinds_the_ledger_does_not_judge_are_unsupported`, `::test_an_inferred_aggregate_is_never_reconciled` | done |
| Run-time key collision detection | 02 | **U** (step 3) | `checks/test_hop_ledger.py::test_a_key_collision_after_normalisation_is_unassessable` | done |
| One-directional transforms with expressions | 02 | D (step 3 compares `owned_columns` only, L12; the scope says so) | | later step |
| Union attribution (T1_SOURCE_ATTRIBUTION) | 03 | **U, R** (step 5) | `checks/test_source_attribution.py` (all), `checks/test_hop_ledger_sources.py`; `replay/test_r17_two_sources.py` | 5 (done) |
| Join contribution (HOP_CONTRIBUTION); survivorship (unused by the member pipeline) | 03 | **U** (step 5); join also R | `checks/test_hop_contribution.py` (all); `checks/test_hop_ledger_sources.py` (survivorship); `replay/test_r17_two_sources.py` (join) | 5 (done) |
| CHAIN_COMPLETENESS, `partial_chain`, exception keys only; records pending at a later edge carried (final review P1-03) | 03 | **U** (step 6) | `checks/test_chain.py` (all) | 6 (done); no replay through `dre run` |
| Approved, versioned exclusions: id, version, approval, effective dates | 04 | **U** | `checks/test_hop_contracts.py::test_a_contract_exclusion_counts_only_rows_loaded_while_it_is_effective`, `::test_file_completeness_reads_the_contract_exclusions_too`; `config/test_contracts.py::...[exclusion-*]` | 1 (done) |
| The exclusion volume guard (`max_share`) | 04 | **U** (step 3) | `checks/test_hop_ledger.py::test_an_exclusion_over_its_max_share_fails_its_volume_guard`; `config/test_contracts.py::...[exclusion-needs-volume-guard]` | done |
| `reject_policy`, default defect; reject key lineage | 04 | **U** (step 3; lineage by key and time, L3) | `checks/test_hop_ledger.py::test_a_reject_accounts_for_a_record_only_under_an_authorising_policy`, `::test_a_declared_rejects_table_that_is_gone_is_insufficient_evidence`; `config/test_contracts.py::test_a_reject_policy_without_rejects_evidence_is_a_warning`, `::...[rejects-need-a-key]` | Rule id and version once the rejects layout is confirmed |
| Soft-void deletion (end date = effective date), `deleted_not_applied` | 04 | **U** (step 3) | `checks/test_hop_ledger.py::test_a_soft_deletion_applied_is_deleted_and_one_not_applied_is_a_breach`, `::test_deletions_pass_when_applied_soft_or_hard`; `config/test_contracts.py::...[soft-without-soft-when]` | done |
| The `history` class | 05 | **U** (declared; reported as a blind spot) | `config/test_contracts.py::...[bad-history]`, `::test_validate_prints_each_hop_its_kind_and_its_blind_spots` | 1 (done) |
| The `evaluation_mode` column; CURRENT_RECHECK labels; AS_OF_REPLAY | 05 | **U** (step 4) | `test_evaluation_modes.py` (all) | 4 (done) |
| Versioned additive migrations; guard rule 11 | (step 4, M1, M2) | **U** | `test_evaluation_modes.py::test_a_checkpoint_3_store_is_migrated_without_rewriting_a_row`, `::test_a_new_store_has_nothing_to_migrate`; `guard/test_read_only.py` (migration cases) | 4 (done) |

## 6. Approved, not built (ADR-06 to ADR-08)

| Element | ADR | Level |
| --- | --- | --- |
| Five evidence levels and claim ceilings on dispositions and results | 06 | **U** for HOP_LEDGER (step 3): `checks/test_hop_ledger.py::test_an_approved_exclusion_accounts_for_an_absent_record` (reconstructed), `::test_a_reject_accounts_for_a_record_only_under_an_authorising_policy` (reported). Cause lines and the chain: step 6 |
| `dq_commit` (per batch of evaluations) and committed-only views | 07 | **U, I** | `test_commit_protocol.py` (eight tests); `store/test_install.py::test_apply_replaces_views_left_by_an_older_release`; `config/test_validate.py::...[commit-retention-short]`; the `e2e` job (`dre install`, then `dre run`, through the committed views) | 2 (done) |
| The protected-queue rule (`yarn_queue` never in `protected_queues`) | 08 | **U** (static and at run start) | `config/test_contracts.py::...[queue-is-protected]`; `test_operations_runner.py::test_a_protected_or_foreign_queue_cannot_start_a_run` | 8 (done) |
| Run budget (40 minutes), kill switch, backpressure | 08 | **U** (step 8) | `test_operations.py`, `test_operations_runner.py` (all), `config/test_operations_config.py` | The load test is O (needs a representative cluster; KI-01) |
| Concurrency cap N, including after cancellation | 08 (final review O6) | **U** | `test_concurrency.py` (all); `config/test_operations_config.py` | Jobs inside one Spark action are not counted (KI-04) |

## 6b. Final review corrections (2026-10-20)

| Requirement | Ref | Level | Tests |
| --- | --- | --- | --- |
| A qualified PASS never makes a digest all clear | P1-01 | **U** | `notify/test_digest.py::test_a_digest_whose_passes_are_all_qualified_is_never_all_clear`, `::test_a_digest_with_failures_and_qualified_passes_counts_both`, `::test_only_a_fully_healthy_digest_is_all_clear` |
| Coverage continues past `history_lookback_days`; beyond retention, an explicit gap | P1-02 | **U** | `checks/test_hop_coverage.py::test_coverage_older_than_the_lookback_is_found_and_the_idle_span_is_judged`, `::test_invalid_coverage_inside_the_lookback_falls_back_to_trusted_coverage_before_it`, `::test_once_caught_up_the_range_continues_from_the_catch_up`, `::test_coverage_not_retained_is_an_explicit_gap_never_a_silent_restart` |
| A chain record pending at a later edge is judged once due, counted once, never a false PASS | P1-03 | **U** | `checks/test_chain.py::test_a_record_pending_at_a_later_edge_is_judged_when_it_becomes_due`, `::test_a_carried_record_that_never_arrives_is_lost_never_a_false_pass`, `::test_without_key_events_a_pending_record_is_not_carried_and_the_scope_says_so` |
| At most N units of Spark work, a cancelled one counting until it stops | O6 | **U** | `test_concurrency.py` (all) |

## 6a. Checkpoint 3 corrections (ratified 2026-10-19)

| Requirement | Ref | Level | Tests |
| --- | --- | --- | --- |
| No record lost or judged twice when `sla_hours` changes; a window that covered nothing is judged by the next; one shared, committed-coverage calculation for every hop check | L1 | **U, I** | `checks/test_hop_coverage.py` (all); e2e: every covering hop result has `coverage_to` |
| Missing or insufficient evidence never advances coverage | L1, L15 | **U** | `checks/test_hop_coverage.py::test_only_covering_results_store_coverage_and_a_marker_never_does`, `::test_invalid_results_cover_nothing`; `checks/test_windows.py::test_insufficient_evidence_holds_the_window` |
| A reject explains a record only through version or batch lineage and, where the policy names rules, an approved rule | C3-01 | **U** | `checks/test_hop_ledger.py::test_a_reject_of_an_earlier_version_explains_nothing_about_the_latest`, `::test_a_reject_without_lineage_cannot_explain_a_record`, `::test_a_reason_that_approves_rules_needs_an_approved_rule`, `::test_a_reject_can_name_its_batch_instead_of_its_version`, `::test_a_reject_accounts_for_a_record_only_under_an_authorising_policy`; `config/test_contracts.py::...[rejects-need-lineage, lineage-is-one-of, rule-needs-its-version, policy-rules-need-rule-columns, policy-rule-format, defect-takes-no-rules]` |
| Union attribution states; a union never contract-proven before step 5 | C3-02 | **U, I** | `checks/test_hop_ledger.py::test_a_union_is_never_contract_proven_until_its_attribution_is`; e2e (gold: `attribution_unverified`) |
| Persisted qualifications; a qualified PASS never healthy | C3-03, C3-04 | **U, I** | `store/test_reconciliation.py::test_a_pass_with_qualifications_is_never_healthy`; `checks/test_hop_ledger.py::test_only_a_declared_contract_with_nothing_unproven_claims_contract_proven_completeness`; `notify/test_digest.py::test_a_qualified_pass_is_listed_and_never_counted_as_healthy`; e2e (no qualified PASS healthy) |
| Verified and unverified columns named; partial coverage disclosed | C3-03 | **U** | `checks/test_hop_ledger.py::test_the_scope_names_verified_and_unverified_columns` |
| Collisions, unchecked orphans, ledger-only failures, missing evidence visible in status, scope, digest | C3-04 | **U** | `::test_a_key_collision_after_normalisation_is_unassessable`, `::test_orphans_are_not_checked_where_rows_here_cannot_be_attributed`, `::test_a_failed_ledger_says_what_to_look_at`, `::test_a_declared_rejects_table_that_is_gone_is_insufficient_evidence`; `notify/test_digest.py::test_a_reconciliation_result_prints_its_scope_triage_and_blind_spots`; `test_runner.py::test_a_failed_key_event_write_makes_the_run_partial_and_the_result_is_still_explained` |
| Identity uniqueness | L4 | **U** | `checks/test_hop_ledger.py::test_an_identity_hop_requires_one_record_on_each_side` |
| Unbalanced ledger fails closed and is an integrity incident | L5 | **U** | `checks/test_hop_ledger.py::test_an_unbalanced_ledger_is_did_not_run_never_a_verdict`; `store/test_reconciliation.py::test_an_unbalanced_ledger_is_recorded_as_an_integrity_incident` |
| Every due record in exactly one disposition, all dispositions at once | Exit criterion | **U** | `checks/test_hop_ledger.py::test_the_ledger_balances_with_every_disposition_at_once` |

## 7. What the matrix says

- **Every built guarantee is tested, at hop scope** (sections 1 and 2). The CI jobs cover unit, replay, end-to-end and packaging, all in local-mode Spark.
- **AC6 / PS8, recovery from a process that dies mid-evaluation**, was weaker than its "Met" label. Commit rows closed it in build step 2 (2026-10-17), proven by failure injection at each write boundary.
- **No row is operationally proven.** AC1, AC7 and IN1 need a representative cluster, which is a platform request.
- **PS1, PS3 and PS10 are no longer correct by accident** (step 3): orphans, the reject disposition and DID_NOT_RUN / unsupported_mapping are built and proven at check level. Their replay tests (R14, R16, R23) are still to add.
