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

Built 2026-10-17 (`d8db1ad`). Checkpoint 2 awaits review.

### What it adds

An evaluation becomes visible only once its commit row is stored. Hive has no transaction across tables, so publication is a row written last, and every reader filters on it.

### Write order (`runner.py`)

For each evaluation in a run:

```
evaluate(...)                         # the check runs; nothing is written
write_evaluation(...)                 # 1. evidence (key events, partition states), only if the evaluation carries any
                                      # 2. result rows
                                      # -> (rows stored, complete 0/1, [Commit, ...])
publisher.add(commits, complete)      # 3. buffered; appended every COMMIT_BATCH (25) evaluations
explain_failures(...)                 # causes for FAILED rows (published once their evaluation is committed)
...
publisher.flush()                     # the run's remaining commits, before retries and the digest
written += publisher.published        # checks_written counts committed evaluations only
```

- **`write_key_events`** appends each result's key events (`store/key_events.append_key_events`) and partition states (`store/partitions.record_state`). It returns, per evaluation id, the counts written.
- **`write_evaluation`** skips the evidence step when the evaluation carries no evidence.
  - **Evidence write fails.** It does not write the results. It writes the evidence_incomplete marker (`incomplete_rows`) and returns the marker's commits, which vouch for no evidence; `complete` is 0.
  - **Result write fails.** The same marker is tried.
  - **Marker write fails too.** Nothing is returned, and nothing is published.
- **`commits_for(rows, run, evidence)`** makes one `Commit` per result row: each row is its own evaluation id, because group values are part of the id.
- **`Publisher`** buffers commits.
  - `add` appends them when the batch is full.
  - `flush` appends them through the writer. `published` counts evaluations whose commits were stored, and a failed append leaves them unpublished (the run is PARTIAL).
  - An evaluation with no result rows has nothing to publish and counts at once.
- **WINDOW_GAP rows** are committed like results.
- **When the run fails,** the `except` path flushes what was stored completely before reporting FAILED.

### The commit table (`store/commits.py`, `store/ddl.sql`)

`dq_commit`: `run_id`, `evaluation_id`, `dataset`, `check_id`, `result_rows`, `key_event_rows`, `partition_state_rows`, `committed_at`, partitioned by `run_date`. `append_commits` writes a batch in one append through `store/writer.py`.

Retention:
- `conf/defaults.yaml` keeps it 84 months.
- `Defaults._check` (`config/models.py`) refuses less than any table it gates (`GATED_BY_COMMIT`).

### Committed views (`store/ddl.sql`)

| View | Rows shown |
| --- | --- |
| `v_committed_result` | `dq_check_result` rows with a commit of the same `run_id`, `evaluation_id` and `run_date` |
| `v_committed_key_event` | `dq_key_event` rows with such a commit whose `key_event_rows > 0` |
| `v_committed_partition_state` | `dq_partition_state` rows with a commit of the same run, dataset and `run_date`, for `T1_PARTITION_DRIFT`, with `partition_state_rows > 0` |
| `v_committed_cause` | `dq_cause_result` rows whose evaluation id was committed by the investigating run, or before a later run's retry investigated it (`committed_at <= evaluated_at`) |

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
| `dre trace` (`trace.py`) | `v_committed_cause` joined to `v_committed_result` |
| The hop's open keys (`checks/sql/hop_common.sql.j2`) | `v_committed_key_event` |
| T1_VOLUME history, T1_SCHEMA_DRIFT history | `v_committed_result` |
| Rule schedule (`checks/rules/schedule.py`) | `v_committed_result` |
| RERUN idempotency (`runner.stored_outcomes`) | `v_committed_result` |
| Retry of investigations (`runner.pending_investigations`) | `v_committed_result`, `v_committed_cause` |
| T1_PARTITION_DRIFT baseline (`store/partitions.latest_state`) | `v_latest_partition_state` |
| **Window history** (`checks/events.py`) | **`dq_check_result` itself** (decision E1): it holds every event of a run that did not finish COMPLETED, which also covers uncommitted rows, and keeps the `max_window_hours` limit |

### Failure behaviour

| Interrupted at | Tables | Views | Next run |
| --- | --- | --- | --- |
| After the evidence, before the results (process dies) | Key events, plus earlier evaluations' results | Nothing | The run is STARTED, so its windows are held and evaluated again |
| After the results, before the commit (process dies) | Evidence and results | Nothing | The same |
| The commit is refused | Evidence and results | Nothing | The run is PARTIAL, so its windows are held |
| The evidence is partly stored, then fails | Partial key events, and a marker | The marker only (evidence_incomplete) | The run is PARTIAL, so the window is held |

A dead run's STARTED row blocks NORMAL runs for `watchdog_grace_minutes`, as before.

### Tests

| Test | Proves |
| --- | --- |
| `test_commit_protocol.py`, four boundary tests | Each interruption publishes nothing in all seven views, and the next run publishes the window once |
| `test_commit_protocol.py`, unit tests | An unpublished result does not make a RERUN skip; an unpublished partition state is not a baseline; an orphan run's causes stay hidden; batching and counting |
| `store/test_install.py::test_apply_replaces_views_left_by_an_older_release` | The upgrade path |
| `config/test_validate.py` (`commit-retention-short`) | The retention rule |
| `tests/store/conftest.append_rows(commit=True)` | Synthetic store rows in tests are committed as a run would commit them. `commit=False` leaves them unpublished |

### Changing this code

- **Writing a new kind of evidence:**
  1. Write it inside the evidence step of `write_evaluation`.
  2. Add its count to `Commit` and `dq_commit`.
  3. Read it only through a committed view whose join requires that count to be above 0.
- **Reading results, events, partition states or causes:** use the committed views, never the tables. The guard tests do not enforce this yet; review does.
- **Changing a view:** edit `store/ddl.sql`, then run `dre install --apply` on each store. `dre install --check` shows which views differ.
