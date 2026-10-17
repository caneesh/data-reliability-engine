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

Built 2026-10-17 (`d8db1ad`), revised after the checkpoint 2 review the same day (cause commits, verification before commit, repair after a failed commit write, the REPLAY list from committed results, `commit_batch`). The revised checkpoint 2 awaits review.

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
    3. **Repair** (`_repaired`) if the write raised: read back the run's commit identities (`stored_commits`), append the missing ones once; if that fails, keep only units whose commits are all stored.
    4. **Count** the stored units: `published` (checks written) and `tally.done` (investigations).
- **WINDOW_GAP rows** are committed like results.
- **When the run fails,** the `except` path flushes what was stored completely before reporting FAILED.

### The commit table (`store/commits.py`, `store/ddl.sql`)

`dq_commit`: `run_id`, `evaluation_id`, `dataset`, `check_id`, `kind` (result or cause), `result_run_id`, `result_rows`, `key_event_rows`, `partition_state_rows`, `cause_rows`, `committed_at`, partitioned by `run_date`. `append_commits` writes a batch in one append of one file (`coalesce(1)`) through `store/writer.py`, so a batch is stored whole or not at all. A commit's identity is (run, evaluation id, kind); a duplicate row publishes nothing more.

Retention:
- `conf/defaults.yaml` keeps it 84 months.
- `Defaults._check` (`config/models.py`) refuses less than any table it gates (`GATED_BY_COMMIT`).

### Committed views (`store/ddl.sql`)

| View | Rows shown |
| --- | --- |
| `v_committed_result` | `dq_check_result` rows with a result commit of the same `run_id`, `evaluation_id` and `run_date` |
| `v_committed_key_event` | `dq_key_event` rows with such a commit whose `key_event_rows > 0` |
| `v_committed_partition_state` | `dq_partition_state` rows with a result commit of the same run, dataset and `run_date`, for `T1_PARTITION_DRIFT`, with `partition_state_rows > 0` |
| `v_committed_cause` | `dq_cause_result` rows with a cause commit of the same run, evaluation id and `run_date` (`cause_rows > 0`) whose named result (`result_run_id`, evaluation id) has a result commit; adds `result_run_id`. No timestamps |
| `v_commit_integrity` | Audit only: commits whose stored rows now differ from their declaration (expired evidence included); query with a `run_date` bound |

These views are rebuilt on top of them:
- `v_latest_result` on `v_committed_result`;
- `v_open_keys` on `v_committed_key_event`;
- `v_latest_partition_state` on `v_committed_partition_state`.

Every view is `CREATE OR REPLACE`, so `dre install --apply` upgrades an installed store. Tables stay `CREATE TABLE IF NOT EXISTS`.

Each view is a `LEFT SEMI JOIN` on `dq_commit`. A `run_date` predicate on the view prunes both scans, which `checks/test_hop.py::test_open_keys_are_read_from_recent_partitions_only` asserts.

### Readers

| Reader | Reads |
| --- | --- |
| Digest (`notify/digest.py`): this run's results, earlier results, causes, key-hash samples | committed views |
| `dre trace` (`trace.py`) | `v_committed_cause` joined to `v_committed_result` on evaluation id and `result_run_id` |
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

A dead run's STARTED row blocks NORMAL runs for `watchdog_grace_minutes`, as before.

### Tests

| Test | Proves |
| --- | --- |
| `test_commit_protocol.py`, four boundary tests | Each interruption publishes nothing in all seven views, and the next run publishes the window once |
| `test_commit_protocol.py`, unit tests | An unpublished result does not make a RERUN skip; an unpublished partition state is not a baseline; causes attach by identity, never by time; batching and counting |
| `test_commit_protocol.py`, revision tests | A partly stored cause set and its retry; a marker over partly stored results; a unit withheld whole; a batch that cannot be counted; a failed commit write after none, some or all rows; a failed repair; the integrity view; the REPLAY list |
| `test_trace.py::test_a_retried_investigation_s_cause_is_shown` | Trace joins causes through `result_run_id` |
| `store/test_install.py::test_apply_replaces_views_left_by_an_older_release` | The upgrade path |
| `config/test_validate.py` (`commit-retention-short`, `commit-batch-zero`) | The retention rule; the batch size's range |
| `tests/store/conftest.append_rows(commit=True)` | Synthetic store rows in tests are committed as a run would commit them. `commit=False` leaves them unpublished |

### Changing this code

- **Writing a new kind of evidence:**
  1. Write it inside the evidence step of `write_evaluation`.
  2. Add its count to `Commit` and `dq_commit`, and count it in `stored_counts` and `mismatches`.
  3. Read it only through a committed view whose join requires that count to be above 0.
- **Publishing something new** (another kind of commit): give it its own `kind`, verify its rows before the commit, and join its readers by identity, never by time.
- **Reading results, events, partition states or causes:** use the committed views, never the tables. The guard tests do not enforce this yet; review does.
- **Changing a view:** edit `store/ddl.sql`, then run `dre install --apply` on each store. `dre install --check` shows which views differ.
