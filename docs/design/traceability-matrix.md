# Traceability matrix: requirement → implementation → test

Status: first version, 2026-10-16. This answers the Phase 1 assessment's finding P1-12 and exit criterion 6. For every requirement the engine claims, or that the approved design adds, it says how far the requirement has got and where the evidence is. Keep it current: a step that builds or tests a requirement updates its row in the same commit.

## Levels

Each level needs the ones before it.

| Level | Meaning |
| --- | --- |
| **D** documented | Stated in `docs/spec.md`, `docs/architecture.md` or an approved design document |
| **C** coded | Implemented under `src/` |
| **U** unit-tested | A test under `tests/` (local-mode Spark, synthetic Hive tables, run by CI's `test` job) proves it, including the FAIL, PASS and DID_NOT_RUN paths where they apply |
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
| AC6 | Recovery from partial DRE writes without duplicate active findings | U, partly | `test_recovery.py::test_a_result_stored_without_its_key_events_is_recovered_by_the_next_run`, `::test_every_row_of_an_evaluation_is_marked_incomplete_together`; `test_runner.py::test_a_failed_key_event_write_makes_the_run_partial_and_the_result_is_still_explained` | **The tests cover a failed write in a live process, not a process that dies.** If the process dies after the result row and before its key events, the result is visible without them until the next run (P1-09). ADR-07 is not built |
| AC7 | A DRE outage or heavy load does not affect source processing | D, C partly | `checks/test_budget.py::test_a_slow_check_is_cancelled_as_budget_exceeded`, `::test_a_spent_dataset_budget_or_a_too_large_scan_is_not_run` | Per-check budgets only. No run budget, queue, kill switch or load test (ADR-08). O needed |
| AC8 | Uncertainty reported clearly | U | `causes/test_causes.py::test_a_failure_with_nothing_to_explain_is_recorded_as_insufficient_evidence`, `::test_a_parameter_set_not_configured_is_not_ready_never_ruled_out`; `checks/test_hop.py::test_a_key_whose_upstream_record_times_are_all_null_is_unassessable_not_hidden` | Evidence levels exist for causes, but not for dispositions or results (ADR-06) |

## 3. Invariants (`docs/design/target-architecture.md`, section 2)

| # | Invariant | Level | Evidence | Gap |
| --- | --- | --- | --- | --- |
| IN1 | No production writes, hooks, schedule dependencies or job blocking | U | HC1, HC2 | O |
| IN2 | Every PASS is scoped to a versioned contract, population, grain, cutoff and coverage | D (ADR-01, approved) | `expectation_version` and the window are stored; `observed` lists what was left out | No scope columns; no contract hash; no cutoff column |
| IN3 | Every candidate record is accounted for exactly once, or reported unknown | D (ADR-01, approved); C partly | HOP_KEY_CURRENCY classes due keys as CURRENT, STALE, MISSING or UNASSESSABLE, and counts excluded and pending keys | The identity is not asserted; there is no `deleted` or `rejected`, and no orphans |
| IN4 | Transforms, merges, deletes, rejects, exclusions and late arrivals have distinct semantics | D (ADR-02, ADR-04, approved); C partly | Merges (`winner_rule`), exclusions, late arrivals (`checks/test_hop.py::test_key_currency_takes_the_latest_upstream_version`) | Deletes and rejects are not dispositions |
| IN5 | An unobservable cause is never labelled confirmed | U | Causes are CONFIRMED only by a cause check, and suspected by elimination (`causes/test_causes.py`) | Ceilings for reported evidence (ADR-06) |
| IN6 | The earliest observed gap is not necessarily the originating defect | D, U | `dre trace` names the first hop with a gap (`test_trace.py`) | Its wording says "gap", but the digest has no explicit statement that this is not the origin |
| IN7 | Every result is reproducible against its evidence snapshot, or labelled a current recheck | D (ADR-05, approved); C partly | RERUN and REPLAY print that they are not a snapshot (`runner.py`); `test_execution_types.py::test_a_rerun_of_an_older_window_does_not_become_the_current_result` | No `evaluation_mode` column, no digest label, no AS_OF_REPLAY |
| IN8 | An incomplete evidence write never becomes a visible complete PASS | D (ADR-07, approved) | See AC6 | A process death between writes. Commit rows are not built |

## 4. Proof scenarios (`docs/design/reconciliation-model.md`, section 10)

Each will become a replay test, R14 to R23. The "today" column says what the current engine does with the scenario.

| # | Scenario | Level | Today | Test to add |
| --- | --- | --- | --- | --- |
| PS1 | Equal counts with a substituted key must fail | C, untested as stated | HOP_KEY_CURRENCY compares keys, not counts, so the dropped key is MISSING. The substitute is not reported (no orphans) | R14: equal counts, one key swapped, so FAILED with 1 missing and 1 orphan |
| PS2 | An approved filter is accounted for; an unapproved one fails | U partly | An approved, versioned exclusion is honoured only from its effective date; a record it does not cover is MISSING (`checks/test_hop_contracts.py::test_a_contract_exclusion_counts_only_rows_loaded_while_it_is_effective`). The cause line naming an unapproved, reported rule waits for the ledger | R15 |
| PS3 | A rejected record with a defect reason must not pass | C partly | The key is MISSING (FAILED), with the REJECTED cause when a rejects table is configured. That is correct by accident: there is no reject disposition yet | R16 |
| PS4 | Two sources into one gold row, without double counting | U partly | Per-hop key state (AC3) | R17: union attribution; join contribution |
| PS5 | Aggregation: group and contribution level, or a declared limit | U at group level | `checks/test_derived.py::test_an_aggregate_upstream_is_judged_at_its_grain`, `::test_an_aggregate_upstream_passes_when_gold_agrees` | R18: the contribution limit in the scope. Not needed by the member pipeline (no gold aggregates) |
| PS6 | A CDC update, a tombstone, an out-of-order event and a late arrival have deterministic expected states | U partly | Update and late arrival: `checks/test_hop.py::test_key_currency_takes_the_latest_upstream_version`, `::test_a_reload_here_does_not_judge_a_key_before_its_upstream_deadline`. A soft void is judged as a newer version (STALE when not applied) | R19: a soft void applied and not applied; an out-of-order version |
| PS7 | A historical rerun after a later load never claims a historical PASS | C partly | A printed note only | R20: the evaluation mode label; AS_OF_REPLAY refused on a mutable dataset |
| PS8 | A crash between the result and evidence writes never publishes a complete PASS | **Not met** | See AC6 | R21: failure injection at each write boundary (ADR-07) |
| PS9 | A legitimate empty batch is told apart from a silent zero | U partly | `replay/test_r11a_empty_no_load_due.py` (no load due: DID_NOT_RUN), `replay/test_r11b_empty_load_due.py` (load due, nothing loaded: FAILED); calendars (`checks/test_calendars.py`) | R22: the source's file record count (release 4 step 2) |
| PS10 | An unsupported one-to-many mapping is visibly unsupported | U partly | `mapping: fan_out` is a `dre validate` warning and a listed blind spot, and an error if the hop is keyed (`config/test_contracts.py::test_kinds_judged_later_or_never_are_warnings_never_silent`) | R23: DID_NOT_RUN / `unsupported_mapping` at run time (build step 3) |

## 5. Approved design elements (ADR-01 to ADR-05)

Level D unless the row says otherwise. Each row names the build step (`docs/design/target-architecture.md`, section 7) that takes it further. Build step 1 (2026-10-16) took the contract schema to C and U.

| Element | ADR | Level | Evidence | Build step |
| --- | --- | --- | --- | --- |
| Candidate, due and pending populations; eleven dispositions; the asserted identity; `ledger_unbalanced` | 01 | D | | 3 (HOP_LEDGER) |
| Tolerance on deficits only, as an absolute count | 01 | **U** (validation) | `config/test_contracts.py::test_contract_errors_name_file_line_field_and_fix[tolerance-on-unknown]`, `[tolerance-negative]` | 3 (applied by the ledger) |
| Scope columns on results; `dq_reconciliation` | 01 | D | | 3 |
| No chain claim before CHAIN_COMPLETENESS | 01 | D | `docs/architecture.md` scope note | 6 |
| The `grain` block; mapping kinds and their rules; aliases for today's fields; each hop's blind spots in `dre validate` | 02 | **U** | `config/test_contracts.py` (every case), `::test_a_hop_without_a_contract_resolves_from_the_release_1_fields`, `::test_validate_prints_each_hop_its_kind_and_its_blind_spots`; `checks/test_hop_contracts.py::test_the_alias_and_an_equivalent_contract_judge_alike`; the `e2e` job (sample with a declared contract, results unchanged) | 1 (done) |
| DID_NOT_RUN / `unsupported_mapping` at run time | 02 | D (today a `dre validate` warning, and an error if such a hop is keyed) | `config/test_contracts.py::test_kinds_judged_later_or_never_are_warnings_never_silent` | 3 |
| Run-time key collision detection | 02 | D | | 3 |
| One-directional transforms with expressions | 02 | D | | 3 |
| Union attribution (T1_SOURCE_ATTRIBUTION) | 03 | D (the attribution, `feed_filter`, is required by validation) | `config/test_contracts.py::...[union-needs-attribution]` | 5 |
| Join contribution (HOP_CONTRIBUTION); survivorship (unused by the member pipeline) | 03 | D (schema validated) | `config/test_contracts.py::test_kinds_judged_later_or_never_are_warnings_never_silent`, `::test_survivorship_hops_share_one_precedence` | 5 |
| CHAIN_COMPLETENESS, `partial_chain`, exception keys only | 03 | D | | 6 |
| Approved, versioned exclusions: id, version, approval, effective dates | 04 | **U** | `checks/test_hop_contracts.py::test_a_contract_exclusion_counts_only_rows_loaded_while_it_is_effective`, `::test_file_completeness_reads_the_contract_exclusions_too`; `config/test_contracts.py::...[exclusion-*]` | 1 (done) |
| The exclusion volume guard (`max_share`) | 04 | C (schema, required) | `config/test_contracts.py::...[exclusion-needs-volume-guard]` | 3 |
| `reject_policy`, default defect; reject key lineage | 04 | C (schema; validated) | `config/test_contracts.py::...[authorised-reject-needs-guard]`, `::test_the_sample_contract_resolves` | 3 |
| Soft-void deletion (end date = effective date), `deleted_not_applied` | 04 | C (schema; conditions parsed by `dre dry-run`) | `config/test_contracts.py::...[soft-without-soft-when]`; `test_runner.py::test_fragment_checks_resolve_a_contract_s_conditions` | 3 |
| The `history` class | 05 | **U** (declared; reported as a blind spot) | `config/test_contracts.py::...[bad-history]`, `::test_validate_prints_each_hop_its_kind_and_its_blind_spots` | 1 (done) |
| The `evaluation_mode` column; CURRENT_RECHECK labels; AS_OF_REPLAY | 05 | D | | 4 |

## 6. Approved, not built (ADR-06 to ADR-08)

| Element | ADR | Level |
| --- | --- | --- |
| Five evidence levels and claim ceilings on dispositions and results | 06 | D. Build steps 3 and 6 |
| `dq_commit` (per batch of evaluations) and committed-only views | 07 | D. Build step 2 |
| The protected-queue rule (`yarn_queue` never in `protected_queues`) | 08 | U (static) | `config/test_contracts.py::...[queue-is-protected]` | 8 (checked at run start) |
| Run budget (40 minutes), concurrency cap, kill switch, backpressure, load test | 08 | D. Build step 8; the load test is O |

## 7. What the matrix says

- **Every built guarantee is tested, at hop scope** (sections 1 and 2). The CI jobs cover unit, replay, end-to-end and packaging, all in local-mode Spark.
- **One stated guarantee is weaker than its "Met" label:** AC6 / PS8, recovery from a process that dies mid-evaluation. ADR-07 closes it, and it should be the first store change built (build step 2).
- **No row is operationally proven.** AC1, AC7 and IN1 need a representative cluster, which is a platform request.
- **Three proof scenarios are only correct by accident today:** PS1 (no orphans), PS3 (no reject disposition) and PS10 (unsupported means unconfigured, silently). Each has a named replay test to add.
