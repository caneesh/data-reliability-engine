# Evidence package: checkpoint 2 corrections R1 to R3

For the reviewer of checkpoint 2 (the user's "Checkpoint 2 — Architecture Ratification of R1–R3", 2026-10-17). It answers the review's evidence request item by item. Everything here is synthetic: the store is the end-to-end test's, built by `tests/e2e/build_synthetic.py`.

## (a) Source changes

Commit `ce7a283` on branch `claude/claude-md-documentation-a2lxt0`, CI run #85 (green: `test`, `e2e`, `package`; artifacts `test-report` and `e2e-logs`). The diff against `2c0f3f7` (the revised checkpoint 2) is the change set: `git diff 2c0f3f7 ce7a283`.

| Area | Files |
| --- | --- |
| Store schema and views | `src/hcsc/datalake/dre/store/ddl.sql` (`dq_commit` columns, `dq_integrity`, `v_commit_state`, committed views, `v_commit_integrity`, `v_latest_result.healthy`), `store/schema.py`, `store/names.py` |
| Commits | `store/commits.py` (`result_run_date`, `evidence_kept_until`, `with_lifetime`, `contents`, `stored_commits` with contents) |
| Integrity | `store/integrity.py` (records, sweep, compromised baselines, freshness) |
| Retention | `store/retention.py` (deadlines, holds), `config/models.py` (`RetentionHold`, `integrity_sweep_days`, `integrity_full_sweep_day`, `retention_holds`), `cli.py`, `conf/defaults.yaml` |
| Run | `runner.py` (`Publisher` readback and lifetimes, the late-thread fix in `explain_failures`, `cause_commits`, forced full sweep, `without_pass`, `rebaselines`, `integrity_sweep`, `stored_outcomes`), `checks/base.py` (`evidence_invalid`) |
| Reporting | `notify/digest.py`, `notify/digest.txt.j2`, `watchdog.py` |
| Spec and design | `docs/spec.md` sections 1, 3, 4, 5, 8; `docs/decisions.md`; `docs/design/checkpoints.md`; `docs/design/implementation.md`; `docs/architecture.md`; `docs/design/traceability-matrix.md` |

## (b) ADR-07 and the checkpoint notes

ADR-07 as built is spec section 5, "Commit protocol" (items 6 to 10 are new: the consistency model, evidence states, the integrity sweep, compromised baselines, the guarantee). The checkpoint report is `docs/design/checkpoints.md`, "Checkpoint 2 corrections R1 to R3".

## (c) Tests and assertions

The tables "Corrections and their proof" and "The failure-injection matrix" in `docs/design/checkpoints.md` name each test and what it asserts. The tests are in `tests/test_evidence_integrity.py` (new), `tests/test_commit_protocol.py`, `tests/test_trace.py`, `tests/notify/test_digest.py`.

## (d) CI and local commands

CI: the run named in the handoff row; the `test-report` artifact (JUnit XML) lists every test by name with its outcome, and `e2e-logs` holds the end-to-end run and its check.

Local, from the repository root (Python 3.11 and Java 21 here; CI uses Python 3.10 and Java 17):

```bash
pip install -e ".[dev]"
export PYSPARK_PYTHON=$PWD/.venv/bin/python
pytest tests/test_evidence_integrity.py tests/test_commit_protocol.py    # the correction and protocol tests
pytest                                                                   # everything
mkdir e2e && cd e2e && python ../tests/e2e/build_synthetic.py && dre install --conf conf --apply   && dre run --conf conf && python ../tests/e2e/verify.py                # the end-to-end check
```

## (e) Committed-view and integrity-view output

Produced on the end-to-end store after `dre run` (sections 1 to 4), after removing one evaluation's evidence by hand and running the integrity sweep (sections 5 to 8), and after the next `dre run --feed provider_directory_merge` (sections 10 to 12). The simulated loss stands for a change made outside DRE: it is a test action, not something the engine can do.

What it shows:

- Sections 1 and 2: every current result has VALID evidence and `healthy` is true exactly for PASSED results; commits record their evidence lifetime (13 months for the commit carrying partition states, 84 for the others).
- Sections 3 and 4: the integrity view is empty; the run's first sweep (FULL, the store being new) completed.
- Sections 5 to 8: the sweep records `partition states: 39 declared, 0 stored`; the integrity view lists the commit as INVALID; the current T1_PARTITION_DRIFT result is kept but not healthy.
- Sections 10 to 12: the next run did not judge drift on the lost baseline (DID_NOT_RUN / BASELINE / insufficient_history, since the INVALID states are hidden), stored new partition states, and recorded REBASELINED; only the new states are shown.

```text

-- 1. Current results with their evidence state and health (v_latest_result)
SELECT dataset, check_id, state, evidence_state, healthy, count(*) AS rows FROM dq.v_latest_result GROUP BY dataset, check_id, state, evidence_state, healthy ORDER BY dataset, check_id
+--------------------------+--------------------------+-----------+--------------+-------+----+
|dataset                   |check_id                  |state      |evidence_state|healthy|rows|
+--------------------------+--------------------------+-----------+--------------+-------+----+
|example_curated_enrollment|T1_KEY_NULLS              |PASSED     |VALID         |true   |1   |
|example_curated_enrollment|T1_ON_TIME                |PASSED     |VALID         |true   |1   |
|example_curated_enrollment|T1_SCHEMA_DRIFT           |DID_NOT_RUN|VALID         |false  |1   |
|example_curated_enrollment|T1_VOLUME                 |DID_NOT_RUN|VALID         |false  |6   |
|example_curated_enrollment|T1_ZERO_ROWS              |PASSED     |VALID         |true   |1   |
|gold_member_coverage      |HOP_KEY_CURRENCY          |PASSED     |VALID         |true   |1   |
|gold_member_coverage      |HOP_VALUE_AGREEMENT       |PASSED     |VALID         |true   |1   |
|gold_member_coverage      |T1_FRESHNESS              |PASSED     |VALID         |true   |1   |
|gold_member_coverage      |T1_KEY_DUPLICATES         |PASSED     |VALID         |true   |1   |
|gold_member_coverage      |T1_KEY_NULLS              |PASSED     |VALID         |true   |1   |
|gold_member_coverage      |T1_ON_TIME                |PASSED     |VALID         |true   |1   |
|gold_member_coverage      |T1_VOLUME                 |DID_NOT_RUN|VALID         |false  |6   |
|gold_member_coverage      |T1_ZERO_ROWS              |PASSED     |VALID         |true   |1   |
|gold_member_coverage      |end_not_before_start      |PASSED     |VALID         |true   |1   |
|gold_member_coverage      |older_coverage_still_open |PASSED     |VALID         |true   |1   |
|gold_member_coverage      |one_open_row_per_coverage |PASSED     |VALID         |true   |1   |
|gold_member_coverage      |one_row_per_coverage      |PASSED     |VALID         |true   |1   |
|gold_member_coverage_all  |T1_KEY_DUPLICATES         |PASSED     |VALID         |true   |1   |
|gold_member_coverage_all  |T1_KEY_NULLS              |PASSED     |VALID         |true   |1   |
|gold_member_coverage_all  |T1_SCHEMA_DRIFT           |DID_NOT_RUN|VALID         |false  |1   |
|gold_member_coverage_all  |coverage_end_date_format  |PASSED     |VALID         |true   |1   |
|provider_directory        |HOP_KEY_CURRENCY          |PASSED     |VALID         |true   |1   |
|provider_directory        |HOP_VALUE_AGREEMENT       |PASSED     |VALID         |true   |1   |
|provider_directory        |T1_KEY_DUPLICATES         |PASSED     |VALID         |true   |1   |
|provider_directory        |T1_KEY_NULLS              |PASSED     |VALID         |true   |1   |
|provider_directory        |T1_ON_TIME                |PASSED     |VALID         |true   |1   |
|provider_directory        |T1_PARTITION_DRIFT        |DID_NOT_RUN|VALID         |false  |1   |
|provider_directory        |T1_SCHEMA_DRIFT           |DID_NOT_RUN|VALID         |false  |1   |
|provider_directory        |T1_VOLUME                 |DID_NOT_RUN|VALID         |false  |1   |
|provider_directory        |T1_ZERO_ROWS              |PASSED     |VALID         |true   |1   |
|provider_directory        |provider_specialty_present|PASSED     |VALID         |true   |1   |
|provider_roster_raw       |T1_FILES_NOT_LOADED       |DID_NOT_RUN|VALID         |false  |1   |
|provider_roster_raw       |T1_KEY_DUPLICATES         |PASSED     |VALID         |true   |1   |
|provider_roster_raw       |T1_KEY_NULLS              |PASSED     |VALID         |true   |1   |
|provider_roster_raw       |T1_ON_TIME                |PASSED     |VALID         |true   |1   |
|provider_roster_raw       |T1_SCHEMA_DRIFT           |DID_NOT_RUN|VALID         |false  |1   |
|provider_roster_raw       |T1_VOLUME                 |DID_NOT_RUN|VALID         |false  |1   |
|provider_roster_raw       |T1_ZERO_ROWS              |PASSED     |VALID         |true   |1   |
+--------------------------+--------------------------+-----------+--------------+-------+----+


-- 2. Commit identities by kind and evidence state (v_commit_state)
SELECT kind, evidence_state, count(*) AS commits, sum(key_event_rows) AS key_events, sum(partition_state_rows) AS partition_states, min(evidence_kept_until) AS kept_until_min, max(evidence_kept_until) AS kept_until_max FROM dq.v_commit_state GROUP BY kind, evidence_state ORDER BY kind
+------+--------------+-------+----------+----------------+--------------+--------------+
|kind  |evidence_state|commits|key_events|partition_states|kept_until_min|kept_until_max|
+------+--------------+-------+----------+----------------+--------------+--------------+
|result|VALID         |48     |0         |39              |2027-11-17    |2033-10-17    |
+------+--------------+-------+----------+----------------+--------------+--------------+


-- 3. Integrity view before any change (v_commit_integrity)
SELECT * FROM dq.v_commit_integrity
+------+-------------+-------+--------+----+--------+-----------+------------------+--------------+---------------------+--------------------+---------------------------+----------+-----------------+-------------------+--------------+
|run_id|evaluation_id|dataset|check_id|kind|run_date|result_rows|stored_result_rows|key_event_rows|stored_key_event_rows|partition_state_rows|stored_partition_state_rows|cause_rows|stored_cause_rows|evidence_kept_until|evidence_state|
+------+-------------+-------+--------+----+--------+-----------+------------------+--------------+---------------------+--------------------+---------------------------+----------+-----------------+-------------------+--------------+
+------+-------------+-------+--------+----+--------+-----------+------------------+--------------+---------------------+--------------------+---------------------------+----------+-----------------+-------------------+--------------+


-- 4. Integrity records (dq_integrity)
SELECT record, status, scope, scope_from, scope_to, detail FROM dq.dq_integrity ORDER BY recorded_at
+------+---------+-----+----------+----------+-------------------------------------+
|record|status   |scope|scope_from|scope_to  |detail                               |
+------+---------+-----+----------+----------+-------------------------------------+
|SWEEP |COMPLETED|FULL |2019-10-17|2026-10-17|0 new mismatch(es), 0 new conflict(s)|
+------+---------+-----+----------+----------+-------------------------------------+


-- 5. Simulated loss outside DRE: drop dq_partition_state partition run_date=2026-10-17 (provider_directory T1_PARTITION_DRIFT, 39 rows declared)
sweep: SweepOutcome(mismatches=1, conflicts=0)

-- 6. Integrity view after the loss
SELECT run_id, evaluation_id, dataset, check_id, kind, key_event_rows, stored_key_event_rows, partition_state_rows, stored_partition_state_rows, evidence_kept_until, evidence_state FROM dq.v_commit_integrity
+------------------------------------+----------------------------------------------------------------+------------------+------------------+------+--------------+---------------------+--------------------+---------------------------+-------------------+--------------+
|run_id                              |evaluation_id                                                   |dataset           |check_id          |kind  |key_event_rows|stored_key_event_rows|partition_state_rows|stored_partition_state_rows|evidence_kept_until|evidence_state|
+------------------------------------+----------------------------------------------------------------+------------------+------------------+------+--------------+---------------------+--------------------+---------------------------+-------------------+--------------+
|5943a751-a44f-40c7-9791-e93ce5442cdc|518759bf7f643552321b531ca1b64456fb52891d80de07197c12f6c8ed511105|provider_directory|T1_PARTITION_DRIFT|result|0             |0                    |39                  |0                          |2027-11-17         |INVALID       |
+------------------------------------+----------------------------------------------------------------+------------------+------------------+------+--------------+---------------------+--------------------+---------------------------+-------------------+--------------+


-- 7. Integrity records after the sweep
SELECT record, status, scope, dataset, check_id, detail FROM dq.dq_integrity ORDER BY recorded_at
+-----------------+---------+-----+------------------+------------------+---------------------------------------+
|record           |status   |scope|dataset           |check_id          |detail                                 |
+-----------------+---------+-----+------------------+------------------+---------------------------------------+
|SWEEP            |COMPLETED|FULL |NULL              |NULL              |0 new mismatch(es), 0 new conflict(s)  |
|EVIDENCE_MISMATCH|NULL     |NULL |provider_directory|T1_PARTITION_DRIFT|partition states: 39 declared, 0 stored|
|SWEEP            |COMPLETED|DAILY|NULL              |NULL              |1 new mismatch(es), 0 new conflict(s)  |
+-----------------+---------+-----+------------------+------------------+---------------------------------------+


-- 8. The affected current results: kept, not healthy
SELECT dataset, check_id, state, evidence_state, healthy FROM dq.v_latest_result WHERE dataset = 'provider_directory' AND check_id = 'T1_PARTITION_DRIFT'
+------------------+------------------+-----------+--------------+-------+
|dataset           |check_id          |state      |evidence_state|healthy|
+------------------+------------------+-----------+--------------+-------+
|provider_directory|T1_PARTITION_DRIFT|DID_NOT_RUN|INVALID       |false  |
+------------------+------------------+-----------+--------------+-------+

-- 10. After the next run: integrity records
SELECT record, status, scope, dataset, check_id, detail FROM dq.dq_integrity ORDER BY recorded_at
+-----------------+---------+-----+------------------+------------------+-----------------------------------------------------------+
|record           |status   |scope|dataset           |check_id          |detail                                                     |
+-----------------+---------+-----+------------------+------------------+-----------------------------------------------------------+
|SWEEP            |COMPLETED|FULL |NULL              |NULL              |0 new mismatch(es), 0 new conflict(s)                      |
|EVIDENCE_MISMATCH|NULL     |NULL |provider_directory|T1_PARTITION_DRIFT|partition states: 39 declared, 0 stored                    |
|SWEEP            |COMPLETED|DAILY|NULL              |NULL              |1 new mismatch(es), 0 new conflict(s)                      |
|REBASELINED      |NULL     |NULL |provider_directory|T1_PARTITION_DRIFT|baseline re-established by a later evaluation of this check|
+-----------------+---------+-----+------------------+------------------+-----------------------------------------------------------+


-- 11. T1_PARTITION_DRIFT now (v_latest_result)
SELECT run_id, state, reason_code, evidence_state, healthy FROM dq.v_latest_result WHERE check_id = 'T1_PARTITION_DRIFT'
+------------------------------------+-----------+--------------------+--------------+-------+
|run_id                              |state      |reason_code         |evidence_state|healthy|
+------------------------------------+-----------+--------------------+--------------+-------+
|be259821-f691-4e82-8ab7-ef6c808051a8|DID_NOT_RUN|insufficient_history|VALID         |false  |
+------------------------------------+-----------+--------------------+--------------+-------+


-- 12. Its committed partition states (the INVALID unit's are hidden, the new ones shown)
SELECT run_id, count(*) AS states FROM dq.v_committed_partition_state GROUP BY run_id
+------------------------------------+------+
|run_id                              |states|
+------------------------------------+------+
|be259821-f691-4e82-8ab7-ef6c808051a8|39    |
+------------------------------------+------+
```

## (f) Residual risks

`docs/design/checkpoints.md`, "Checkpoint 2 corrections R1 to R3", "Residual risks".

## (g) Decision log

`docs/decisions.md`, "Checkpoint 2 corrections R1 to R3" (the review, my assessment, the ratification, what was built), with the earlier "Checkpoint 2 reviewed" and "Checkpoint 2 revision" entries.
