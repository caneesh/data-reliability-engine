# Implementation guide: Phase 1 build steps

How each built step of the Phase 1 design is implemented: the modules and functions, the data flow, the tables and views, the configuration, and the tests that prove it. Use it to read or change the code.

Each step builds one part of the design. The other documents cover the rest:

| Document | What it holds |
| --- | --- |
| `docs/design/target-architecture.md`, `docs/design/reconciliation-model.md` | What is built, and why |
| `docs/spec.md` | The rules the code follows |
| `docs/decisions.md` | Every decision taken while building |
| `docs/design/checkpoints.md` | Each step's review |
| `docs/design/traceability-matrix.md` | Which tests prove which requirement |

This guide is updated by each step, in the same commit as the code.

Module paths are under `src/hcsc/datalake/dre/`. Test paths are under `tests/`.

## Step 1: the reconciliation contract schema and its validation

Built 2026-10-16 (`f9d86d4`, CI #76). Checkpoint 1 ratified 2026-10-17, with conditions on D6 (`5e7830f`).

### What it adds

A hop is a dataset and one upstream in its `key_map`. Step 1 adds:

- a declared reconciliation contract per hop;
- a history class per dataset;
- validation of both;
- a report of every hop's kind and blind spots.

Only one check behaviour changes, and it is opt-in: hop checks honour exclusions written in the new form.

### Configuration models (`config/models.py`)

| Model | Fields | Rules enforced in the model |
| --- | --- | --- |
| `Grain` | `history`: `append_only`, `mutable`, `snapshot_partitioned`, or null | none |
| `Effective` | `from` (alias of `from_`), `to`: dates | `to` after `from` |
| `Exclusion` | `id`, `version` (≥ 1), `condition` (SQL fragment), `effective`, `approved_by`, `code_ref`, `max_share` in (0, 1] | `approved_by` and `max_share` required. `label` is `id@version` |
| `RejectReason` | `disposition`: `authorised` or `defect`; `max_share` | `authorised` needs `max_share`; `defect` takes none |
| `Deletion` | `tombstone` (SQL over the upstream), `applied_as`: `hard` or `soft`, `soft_when` (SQL over this dataset) | `soft_when` goes with `soft` and only with it |
| `Tolerance` | `missing`, `stale`, `divergent`: non-negative counts | any other key is refused, with the ADR-01 message |
| `EdgeContract` | `mapping` (eight kinds, `MAPPING_KINDS`), `exclusions`, `reject_policy`, `deletion`, `tolerance`, `reference`, `precedence` | per-field rules above |
| `Dataset` | gains `grain` and `contracts` (upstream id → `EdgeContract`) | |
| `Defaults` | gains `yarn_queue` and `protected_queues` | |

### Resolving a hop's contract (`config/contracts.py`)

**`hop_contract(datasets, ds, up_id) -> Hop`** is the single entry point.

- **A declared contract** is used as written (`declared=True`). If it lists no exclusions, a `hop_exclusions` entry for the hop is kept as the legacy exclusion, so declaring a kind never drops an exclusion.
- **No contract.** The kind is inferred (`declared=False`, decision D6): `aggregate` when the upstream is derived, `dedup` otherwise. `hop_exclusions` becomes `legacy_exclusion`.

**`hops(config)`** lists every hop: each `key_map` entry, then each contract entry outside `key_map`.

**`blind_spots(config, hop)`** lists in plain words what cannot be seen or proven yet:

- an inferred kind, which never supports a contract-proven claim (D6 conditions 1 to 3);
- unsupported or later kinds;
- an unproven union attribution;
- aggregate contribution;
- deletions not modelled;
- a legacy exclusion;
- guards that wait for the ledger;
- no as-of replay, for an undeclared or mutable history.

**`contract_report(config)`** is the report `dre validate` prints after a valid configuration. It labels each hop "declared" or "inferred from key_map".

**The kind tables:**
- `JUDGED`: kinds today's hop checks judge.
- `LATER`: `join_contribution` and `survivorship`, judged from step 5.
- `UNSUPPORTED`: `fan_out`.
- `KEYED`: kinds compared through `key_map`.

### Validation (`config/validate.py`)

**`_check_contracts(r, ds_id)`** runs for every table dataset. Each problem it reports carries file, line, field and fix (`tests/config/test_contracts.py`):

- a contract for an upstream that is not in `upstream`;
- a keyed kind without a `key_map` entry;
- `join_contribution` or `fan_out` with one;
- `aggregate` on a table upstream, or another kind on a derived upstream;
- `union_partition` without the dataset's `feed_filter` (its attribution);
- `reference` missing, or not covering the contributor's key;
- `precedence` missing the hop, or differing between survivorship hops;
- `reference` or `precedence` on the wrong kind;
- an exclusion `id@version` listed twice;
- two versions of one id effective on the same day (`_overlap`);
- exclusions given both under `contracts` and in `hop_exclusions`.

Warnings, not errors:
- `hop_exclusions` alone (no id, version, approval or volume guard);
- a kind judged later;
- `fan_out`.

Elsewhere:
- **`_check_derived`:** a derived dataset may not carry `contracts`.
- **`_check_defaults`:** `yarn_queue` may not be in `protected_queues`.

### What the checks use (`checks/hop/common.py`)

**`exclusion_condition(ds, up_id, up, default_timezone)`** builds the SQL condition the hop checks leave out:

- A legacy `hop_exclusions` condition is returned exactly as written, so existing configurations produce the same SQL.
- A contract exclusion becomes `(condition) AND load >= from AND load < to`. `load` is the upstream's parsed load time in UTC; `from` and `to` are midnight in the default time zone, converted to UTC.
- Several exclusions are joined with OR.

`hop_params` uses this condition for both the filter and the excluded count.

**`exclusion_label`** names the exclusions in `observed`: "excluded by hop_exclusions", or "excluded by exclusions drop_x@1, …". `key_currency.py` and `file_completeness.py` pass it to `not_judged`.

### Dry-run (`config/fragments.py`)

`check_fragments` resolves each contract condition:
- the exclusion and tombstone conditions against the upstream's table;
- `soft_when` against the dataset's own table.

### Sample configuration (`conf/`)

- **`gold_member_coverage`** declares:
  - `grain.history: mutable`;
  - a contract to `example_curated_enrollment` with `mapping: union_partition`;
  - the soft-void deletion: tombstone `to_date(enddate) = to_date(effectivedate)`, `soft_when` on gold's end and effective dates.
- **`example_curated_enrollment`:** `grain.history` stays null, a platform fact not yet known.
- **The provider feed:** raw `append_only`, gold `mutable`.

### Tests

| Test | Proves |
| --- | --- |
| `config/test_contracts.py` (26 tests) | Every validation rule, with file, line and field; the aliases; the report; the D6 labels |
| `checks/test_hop_contracts.py` (5 tests) | Effective dates change what the hop expects; the alias and an equivalent contract judge alike; the legacy SQL is unchanged; a kind-only contract keeps `hop_exclusions`; file completeness reads contract exclusions |
| `test_runner.py::test_fragment_checks_resolve_a_contract_s_conditions` | `dre dry-run` resolves the sample's conditions and reports a misspelt column |

## Step 2: commit rows and committed-only views (ADR-07)

Built 2026-10-17 (`d8db1ad`), revised after the checkpoint 2 review the same day (cause commits, verification before commit, repair after a failed commit write, the REPLAY list from committed results, `commit_batch`). Corrected after the second review (R1 to R3: evidence states, the integrity sweep, evidence lifetimes and holds, confirmed repair, cause dates). Checkpoint 2 awaits independent verification and sign-off.

### What it adds

An evaluation, or a cause investigation, becomes visible only once its commit rows are stored, and the rows they vouch for were counted first. Hive has no transaction across tables, so publication is a row written last, and every reader filters on it.

### Write order (`runner.py`)

For each evaluation in a run:

```
evaluate(...)                         # the check runs; nothing is written
write_evaluation(...)                 # 1. evidence (key events, partition states), only if the evaluation carries any
                                      # 2. result rows
                                      # -> (rows stored, complete 0/1, [Commit, ...])
publisher.add(commits, complete)      # 3. buffered; appended every commit_batch (default 25) units
explain_failures(..., publisher)      # causes for stored FAILED rows: cause rows, then a cause commit
                                      #   (publisher.add(cause_commits, 0, investigated=1))
...
publisher.flush()                     # the run's remaining commits, before retries
retry_investigations(..., publisher)  # NORMAL runs: causes for older committed results, same way
publisher.flush()                     # the retries' commits, before the digest
written += publisher.published        # checks_written counts committed evaluations only
                                      # investigations_done counts committed investigations only
```

- **`write_key_events`** appends each result's key events (`store/key_events.append_key_events`) and partition states (`store/partitions.record_state`). It returns, per evaluation id, the counts written.
- **`write_evaluation`** skips the evidence step when the evaluation carries no evidence.
  - **Evidence write fails.** It does not write the results. It writes the evidence_incomplete marker (`incomplete_rows`) and returns the marker's commits, which vouch for no evidence; `complete` is 0.
  - **Result write fails.** The same marker is tried.
  - **Marker write fails too.** Nothing is returned, and nothing is published.
- **`commits_for(rows, run, evidence)`** makes one result `Commit` per result row: each row is its own evaluation id, because group values are part of the id.
- **`cause_commits(cause_rows, row, run)`** makes one cause `Commit` per investigation: `kind` cause, `result_run_id` the stored row's run (this run's, or the original run's for a retry), `cause_rows`. None without cause rows.
- **`explain_failures`** investigates only rows stored as FAILED; an evidence_incomplete marker stored in place of a FAILED result is not investigated. Without a publisher (tests), it appends the cause commit at once.
- **`Publisher`** buffers units (`Unit`: the commits of one evaluation or investigation, with what it counts).
  - `add` buffers a unit and flushes when `batch` units are waiting. A unit with no commits counts at once.
  - `flush`:
    1. **Verify** (`_verified`): `store/commits.stored_counts` counts, in one query over the run's own rows, what the batch's commits vouch for; `mismatches` compares. A unit with any mismatch is withheld whole (`withheld`), printed as `integrity: ... withheld`. A batch that cannot be counted is withheld.
    2. **Write** the verified units' commits in one append of one file.
    3. **Repair** (`_repaired`) if the write raised: read back the run's commit rows (`stored_commits`: identity -> the distinct contents stored), append the missing ones once, read back again. A unit is CONFIRMED when every commit is stored with exactly its own contents (`Commit.contents`); absent and conflicting units, and every unit when a readback fails, are not published (`unconfirmed`). Conflicts are recorded as COMMIT\_CONFLICT incidents.
    4. **Count** the confirmed units: `published` (checks written) and `tally.done` (investigations), then append their `after` rows (REBASELINED) to `dq_integrity`.
  - With `retention` (the run passes `retention_months`), `add` sets each commit's `evidence_kept_until` (`Commit.with_lifetime`).
- **WINDOW_GAP rows** are committed like results.
- **When the run fails,** the `except` path flushes what was stored completely before reporting FAILED.

### The commit table (`store/commits.py`, `store/ddl.sql`)

`dq_commit`: `run_id`, `evaluation_id`, `dataset`, `check_id`, `kind` (result or cause), `result_run_id`, `result_run_date`, `result_rows`, `key_event_rows`, `partition_state_rows`, `cause_rows`, `evidence_kept_until`, `committed_at`, partitioned by `run_date`. `append_commits` writes a batch in one append of one file (`coalesce(1)`) through `store/writer.py`, so a batch is stored whole or not at all. A commit's identity is (run, evaluation id, kind); a duplicate row publishes nothing more.

Retention:
- `conf/defaults.yaml` keeps it 84 months.
- `Defaults._check` (`config/models.py`) refuses less than any table it gates (`GATED_BY_COMMIT`).

### Committed views (`store/ddl.sql`)

| View | Rows shown |
| --- | --- |
| `v_commit_state` | One row per commit identity (run, evaluation id, kind, run\_date) with its declared counts, `evidence_kept_until`, `versions` (distinct contents stored) and `evidence_state` (INVALID, EXPIRED, UNKNOWN, VALID; spec section 5, item 7) |
| `v_committed_result` | `dq_check_result` rows with a result commit of the same `run_id`, `evaluation_id` and `run_date`, plus `evidence_state` |
| `v_committed_key_event` | `dq_key_event` rows of such a commit whose `key_event_rows > 0` and whose state is not INVALID |
| `v_committed_partition_state` | `dq_partition_state` rows with a result commit of the same run, dataset and `run_date`, for `T1_PARTITION_DRIFT`, with `partition_state_rows > 0`, not INVALID |
| `v_committed_cause` | `dq_cause_result` rows with a cause commit of the same run, evaluation id and `run_date` (`cause_rows > 0`, not INVALID) whose named result (`result_run_id`, `result_run_date`, evaluation id) has a result commit; adds `result_run_id`, `result_run_date`, `evidence_state`. No timestamps |
| `v_commit_integrity` | Commits whose stored rows now differ from their declaration, with `evidence_kept_until` and `evidence_state`; read by the integrity sweep; query with a `run_date` bound |

These views are rebuilt on top of them:
- `v_latest_result` on `v_committed_result`, with `evidence_state` and `healthy` (PASSED and VALID: the one definition of healthy);
- `v_open_keys` on `v_committed_key_event`;
- `v_latest_partition_state` on `v_committed_partition_state`.

Every view is `CREATE OR REPLACE`, so `dre install --apply` upgrades an installed store. Tables stay `CREATE TABLE IF NOT EXISTS`.

Each view is a `LEFT SEMI JOIN` on `dq_commit`. A `run_date` predicate on the view prunes both scans, which `checks/test_hop.py::test_open_keys_are_read_from_recent_partitions_only` asserts.

### Readers

| Reader | Reads |
| --- | --- |
| Digest (`notify/digest.py`): this run's results, earlier results, causes, key-hash samples | committed views |
| `dre trace` (`trace.py`) | `v_committed_cause` joined to `v_committed_result` on evaluation id and `result_run_id` |
| Digest: current results whose evidence is INVALID or UNKNOWN, and the integrity line | `v_committed_result` (ranked as `v_latest_result`, within the history lookback); `store/integrity.freshness` |
| Watchdog: integrity freshness and the hour's incidents | `dq_integrity`, `dq_run` (its own SQL) |
| Compromised baselines (`store/integrity.compromised_baselines`) | `v_commit_state` anti joined to REBASELINED |
| Integrity sweep (`store/integrity.sweep`) | `v_commit_integrity`, `v_commit_state` |
| Retention (`store/retention.droppable`) | `dq_commit.evidence_kept_until`, `retention_holds` |
| The hop's open keys (`checks/sql/hop_common.sql.j2`) | `v_committed_key_event` |
| T1_VOLUME history, T1_SCHEMA_DRIFT history | `v_committed_result` |
| Rule schedule (`checks/rules/schedule.py`) | `v_committed_result` |
| RERUN idempotency (`runner.stored_outcomes`) | `v_committed_result` |
| Retry of investigations (`runner.pending_investigations`) | `v_committed_result`, anti joined to `v_committed_cause` on evaluation id and `result_run_id` |
| REPLAY window list (`checks/events.recorded_windows`) | `v_committed_result` (E1's condition) |
| T1_PARTITION_DRIFT baseline (`store/partitions.latest_state`) | `v_latest_partition_state` |
| **Window history** (`checks/events.py`) | **`dq_check_result` itself** (decision E1): it holds every event of a run that did not finish COMPLETED, which also covers uncommitted rows, and keeps the `max_window_hours` limit |

### Failure behaviour

| Interrupted at | Tables | Views | Next run |
| --- | --- | --- | --- |
| After the evidence, before the results (process dies) | Key events, plus earlier evaluations' results | Nothing | The run is STARTED, so its windows are held and evaluated again |
| After the results, before the commit (process dies) | Evidence and results | Nothing | The same |
| The commit is refused | Evidence and results | Nothing | The run is PARTIAL, so its windows are held |
| The evidence is partly stored, then fails | Partial key events, and a marker | The marker only (evidence_incomplete) | The run is PARTIAL, so the window is held |
| The results are stored, then the write fails | The results, and a marker under the same ids | Nothing: two rows where the marker's commit declares one, so verification withholds it | The run is PARTIAL, so the window is held |
| Stored rows differ from a commit's declaration | Everything the unit wrote | Nothing of that unit | The same |
| The commit write fails after its file landed | Evidence, results, commits | Everything, once the read-back finds the commits (or the repair appends them) | Nothing to redo |
| Cause rows partly stored, then the write fails | Partial cause rows | Nothing of them (no cause commit) | The next NORMAL run retries the investigation |
| A commit write fails and the readback fails | Commits possibly stored | Whatever landed is complete, but nothing is counted (uncertain) | The run is PARTIAL; the window is evaluated again, and the precedence rule gives one current result |
| A conflicting commit identity | Two contents of one identity | The unit is INVALID in every view; a COMMIT\_CONFLICT incident is recorded | Not counted; evaluated again |
| Evidence changed or lost after publication (outside DRE) | The changed rows | The result stays, INVALID, not healthy; its evidence rows hidden | The sweep records EVIDENCE\_MISMATCH; the hop runs full sweeps, drift cannot pass, until REBASELINED; a RERUN republishes |
| The integrity sweep fails | A SWEEP FAILED row | Results in its scope UNKNOWN, not healthy | The next run sweeps again |

A dead run's STARTED row blocks NORMAL runs for `watchdog_grace_minutes`, as before.

### Tests

| Test | Proves |
| --- | --- |
| `test_commit_protocol.py`, four boundary tests | Each interruption publishes nothing in all seven views, and the next run publishes the window once |
| `test_commit_protocol.py`, unit tests | An unpublished result does not make a RERUN skip; an unpublished partition state is not a baseline; causes attach by identity, never by time; batching and counting |
| `test_commit_protocol.py`, revision tests | A partly stored cause set and its retry; a marker over partly stored results; a unit withheld whole; a batch that cannot be counted; a failed commit write after none, some or all rows; a failed repair; the integrity view; the REPLAY list |
| `test_trace.py::test_a_retried_investigation_s_cause_is_shown` | Trace joins causes through `result_run_id` and `result_run_date` |
| `test_evidence_integrity.py` | Corrections R1 to R3, one test or more per failure-injection row F01 to F09: evidence states, late threads, lifetimes and holds, compromised baselines, confirmed repair, cause dates, precedence, the digest, the watchdog |
| `store/test_install.py::test_apply_replaces_views_left_by_an_older_release` | The upgrade path |
| `config/test_validate.py` (`commit-retention-short`, `commit-batch-zero`) | The retention rule; the batch size's range |
| `tests/store/conftest.append_rows(commit=True)` | Synthetic store rows in tests are committed as a run would commit them. `commit=False` leaves them unpublished |

### Changing this code

- **Writing a new kind of evidence:**
  1. Write it inside the evidence step of `write_evaluation`.
  2. Add its count to `Commit` and `dq_commit`, and count it in `stored_counts` and `mismatches`.
  3. Read it only through a committed view whose join requires that count to be above 0.
- **Publishing something new** (another kind of commit): give it its own `kind`, verify its rows before the commit, give it a lifetime in `with_lifetime` and a `_VOUCHED` condition in `store/retention.py`, count it in `v_commit_integrity`, and join its readers by identity, never by time.
- **Building a check on earlier evidence:** add it to `integrity.BASELINE_CHECKS` and say how it behaves on a compromised baseline (run without the baseline, or refuse to pass) and when it re-establishes the baseline (`runner.rebaselines`).
- **Reporting status:** use `v_latest_result.healthy`; never treat PASSED alone as healthy.
- **Reading results, events, partition states or causes:** use the committed views, never the tables. The guard tests do not enforce this yet; review does.
- **Changing a view:** edit `store/ddl.sql`, then run `dre install --apply` on each store. `dre install --check` shows which views differ.

## Step 3: HOP_LEDGER and dq_reconciliation

Built 2026-10-18 under the step 3 authorisation. Reviewed at checkpoint 3. Rules: spec section 6 ("HOP\_LEDGER"); deviations L1 to L15 in `docs/decisions.md`.

### What it adds

One new check, HOP\_LEDGER, which accounts for every due upstream record of a hop in exactly one disposition, and one new evidence table, `dq_reconciliation`, which holds each ledger under its result's commit. Nothing existing changes behaviour: the release 1 hop checks keep running beside it.

### The check (`checks/hop/ledger.py`)

- `HopLedger.evaluate` runs `_hop` once per upstream in `key_map`, then once per contract-only upstream (a `join_contribution` or `fan_out` hop has no `key_map` entry). Each hop is wrapped in `per_hop`, so an exception is DID\_NOT\_RUN for that hop only.
- `_hop`:
  1. Resolves the hop's contract (`config/contracts.hop_contract`). A kind outside `contracts.LEDGER_KINDS` (`identity`, `dedup`, `merge`, `union_partition`), or a hop without a `key_map` entry, returns `unsupported` (DID\_NOT\_RUN / unsupported\_mapping, with its scope).
  2. `ledger_params` checks the preconditions (upstream configured, `load_time`, `record_time` on both sides, tables present, the `rejects` table present: `insufficient_evidence` otherwise) and builds the template parameters from validated config only.
  3. Runs `hop_ledger.sql.j2` in mode `assessed`, materialises it with `localCheckpoint`, and registers it as the session view `ledger_view(dataset, upstream)`. The view holds keys; it never leaves the session, and `runner.drop_judged_views` drops it (and releases the checkpoint) with the evaluation, also when the budget cancelled the hop.
  4. Runs mode `summary` over the view and collects a few rows: per line, disposition and label, how many rows and distinct keys; and the overlap pairs.
  5. `counted` turns them into the ledger and the identity check; `judge` applies the contract (classes, the reject policy, tolerance, volume guards, orphans); `ceiling` gives the claim ceiling; `scope_of` builds the scope; `ledger_lines` builds the `dq_reconciliation` rows.
  6. Returns DID\_NOT\_RUN / ledger\_unbalanced or empty\_population (both still carrying their lines), or PASSED / FAILED with `population` = due records, `violations`, `observed` (the ledger line), `detail` (the scope JSON) and `reconciliation` (the lines).
- `orphans_unchecked` decides whether orphans can be counted (L6).

### The SQL (`checks/sql/hop_ledger.sql.j2`)

| CTE | What it holds |
| --- | --- |
| `up_rows` | Every upstream row (after its `feed_filter`): normalised key, raw key, whether it has a key, record and load time in UTC, the tombstone flag, one flag per exclusion term (`checks/hop/common.exclusion_terms`, effective dates included), the owned columns as strings |
| `up_cut` | Rows loaded before the window end (the cutoff), or with no load time |
| `up_cand` | Keyed rows of keys with a row loaded in the judged range (every keyed row on the full sweep). This narrows the work, not the answer: each candidate key's every row is kept |
| `up_ranked`, `up_top`, `up_keys` | Each key's version under judgement (rows tied at the latest record time), its load time, how many raw keys share the normalised key, its flags |
| `due_keys` | Keys whose version's deadline falls in the judged range (L1); on the full sweep, at or before it, and keys with no load time (L8) |
| `dn_rows` … `dn_keys` | This dataset's rows for the due keys: version, raw-key variants, untimed rows, the `soft_when` flag |
| `up_sets`, `dn_sets` | The owned-column value sets at each side's version (as HOP\_VALUE\_AGREEMENT) |
| `rejects`, `rejected` | Rejects before the cutoff, matched by key and rejected at or after the version's load time; the latest names the reason (L3) |
| `judged` | One row per due key: present here, tombstone, unknown reason, exclusion label, reject reason, divergence, soft delete applied |
| `ledger`, `placed` | `matched`: the explaining rules that hold, in precedence; `residual`: delivered, divergent, stale or missing; the disposition is the first match, else the residual |

The output unions the due keys, keyless due rows, pending and untimed keys, and orphans (when checked). A due key in no bucket has a null disposition, which `counted` reports as unbalanced.

### The table and the commit (`store/reconciliation.py`, `store/ddl.sql`, `store/commits.py`)

- `append_ledger` writes the lines through `store/writer.py`. Counts, codes and labels only.
- `runner.write_key_events` writes them with the other evidence, before the result, and returns (key events, partition states, ledger lines) per evaluation; `commits_for` declares the count in `Commit.reconciliation_rows`.
- `commits.stored_counts` counts them and `commits.mismatches` compares ("ledger rows"), so a unit with a lost line is withheld whole.
- `v_committed_reconciliation`: rows with a result commit of the same run, evaluation id and `run_date`, declaring `reconciliation_rows > 0`, not INVALID.
- `v_commit_integrity` compares the declared and stored counts, so the integrity sweep finds lost or added lines and records them as EVIDENCE\_MISMATCH (`integrity._mismatch_detail`: "ledger rows"); the unit is then INVALID and its lines are hidden. The ledger builds on no earlier evidence, so it has no compromised-baseline rule.
- `with_lifetime` and `retention._VOUCHED` keep the lines while a commit promises them (`retention_months.dq_reconciliation`, 84).

### Configuration

- `contracts.<upstream>.rejects` (`config/models.RejectsSource`): `table`, `condition`, `reason_column` (`reason_code`), `time_column` (`rejected_at`, UTC). Validated in `config/validate._check_contracts`.
- `config/contracts.blind_spots`: the ledger's limits per hop (no rejects evidence, values compared on `owned_columns` only, aggregate not reconciled).
- HOP\_LEDGER is listed in `patterns/file_cyclic.yaml`, `file_periodic.yaml` and `table_merge.yaml`. It has no failure type (L13).

### Reporting

`notify/digest.scope_line` prints a result's scope in one line, for any result whose `detail` is a scope (it has `edge` and `claim`), under "Changed since the last run" and "Did not run".

### Tests

| Test | Proves |
| --- | --- |
| `checks/test_hop_ledger.py` (31 tests) | Every disposition, PASS and FAIL; the identity (an unbalanced ledger is DID\_NOT\_RUN, never a verdict); empty population; collisions, missing record times and keyless rows unassessable; untimed keys, and unassessable on the full sweep; exclusions with effective dates, present excluded records, volume guards, overlaps; rejects authorised, defect, too old, over their guard; the missing rejects table; soft and hard deletions applied and not; orphans, and where they are not checked; every unsupported kind and the inferred aggregate; the claim (inferred, declared, legacy) and the contract hash; the full sweep; no key value in any output |
| `store/test_reconciliation.py` | Lines visible only under a commit that declares them; a unit with a lost line withheld; lost lines in the integrity view, and the sweep recording changed lines as EVIDENCE\_MISMATCH ("ledger rows: 2 declared, 3 stored"), after which the unit's lines are hidden; through `dre run`, the ledger committed with its result and balanced against its population |
| `config/test_contracts.py` (`rejects-need-a-key`, `rejects-need-a-table`, `test_a_reject_policy_without_rejects_evidence_is_a_warning`) | The `rejects` validation and blind spots |
| `notify/test_digest.py::test_a_reconciliation_result_prints_its_scope_in_one_line` and the golden file | The scope line |
| `store/test_ddl.py` | `dq_reconciliation` and `dq_commit.reconciliation_rows` match spec section 5 |

### Changing this code

- **A new disposition:** add it to `DISPOSITIONS` (in precedence, with class and level), to the `matched` array or the residual in the SQL, and to `judge` if it fails. The identity check needs nothing.
- **A new kind for the ledger:** add it to `contracts.LEDGER_KINDS` only once the SQL reads it correctly; until then it stays unsupported, with a reason in `unsupported`.
- **A new evidence source:** declare it on the contract, check it in `ledger_params` (`insufficient_evidence` when it is gone), and give its disposition the right evidence level.
