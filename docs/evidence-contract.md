# Evidence contract between the pipeline audit and DRE

The in-pipeline audit (the temporary solution the team is building) writes four artefacts that both the pipeline and DRE read: a control table, a rejects table, filter rule ids and job log lines. This contract fixes their shape so the audit can be built to it now and DRE's cause probes can be configured against it the same day. It answers developer questions 12 to 14 in `docs/handoff.md` (the "Run logs location" and "Filter rules and rejects table" rows of spec section 11).

Every name in this file is a placeholder (CLAUDE.md, hard constraint 7). The real database, table and path names live in the deployed configuration only.

## Rules for every artefact

1. **One owner.** The team owns these schemas. A column is added, never renamed or removed, and every change is announced before it ships, because DRE's configuration names the columns.
2. **Written by the job, read by anyone.** Only the pipeline writes them. The DRE service account needs SELECT on the two tables and read access to the log path, nothing more.
3. **Keyed by batch and stage.** Every row carries `batch_id` and `stage`. A rerun of a batch replaces its own rows for that stage, so reruns never double count.
4. **Times in one shape.** Every timestamp column is a TIMESTAMP in UTC, or a string in one declared format and one declared zone. Never a mix within a column.
5. **Keys as the layer writes them.** Key columns in the rejects table keep the names and the exact values of the input table they came from (unpadded if the input is unpadded), so DRE can match them with the same normalisation it applies to that layer.
6. **No member values outside the lake.** Keys and values may sit in the two tables, which live inside the lake. Logs, alerts and the control table carry counts, file names, batch ids and reason codes only.
7. **Fixed vocabularies.** `stage`, `status`, `status_reason` and `reason_code` are closed lists kept in one place, listed below. A new value is added to the list first.
8. **Placeholders in this document.** Table and path names here (`ops_db.dq_control`, `ops_db.dq_rejects`, `/data/logs/<job>/`) are placeholders; the real names stay in the deployed configuration.

## Control table (`ops_db.dq_control`)

One row per batch, stage and, where the stage works file by file, per file. Primary key: `batch_id`, `stage`, `file_name` (empty string when the stage has no file). Partitioned by `run_date`.

| Column | Type | Meaning |
| --- | --- | --- |
| `batch_id` | STRING | The pipeline's batch identifier, the same value in every stage of that batch |
| `stage` | STRING | One of the closed list below |
| `file_name` | STRING | Base name of the source file (no path), or empty |
| `partition_value` | STRING | The partition the stage wrote, as written in the table (empty if none) |
| `started_at`, `ended_at` | TIMESTAMP (UTC) | The stage's run span for this batch |
| `source_count` | BIGINT | Records read: records inside the file at the first stage, rows of the input table after |
| `target_count` | BIGINT | Rows written or present in the target for this batch |
| `filtered_count` | BIGINT | Rows kept out by filter rules, total (per-rule counts go in `filtered_by_rule`) |
| `filtered_by_rule` | MAP<STRING, BIGINT> | Filter rule id to rows filtered |
| `rejected_count` | BIGINT | Rows routed to the rejects table |
| `inserted_count`, `updated_count`, `unchanged_count` | BIGINT | Merge stages only; null elsewhere |
| `before_count`, `after_count` | BIGINT | Overwrite stages only: rows in the target partition before and after the write |
| `variance_pct` | DECIMAL(9,4) | (`target_count` + `filtered_count` + `rejected_count` - `source_count`) / `source_count` × 100, or the stage's own definition, named in `status_reason` |
| `status` | STRING | PASSED, FAILED or SKIPPED |
| `status_reason` | STRING | Why not PASSED, from the list below; empty when PASSED |
| `run_date` | DATE | UTC date of `started_at` |

**Stage list** (fixed, in pipeline order): `source_to_raw`, `raw_to_incr`, `incr_to_merge`, `incr_to_cdc`, `merge_to_gcf`, `gcf_to_gold_temp`, `gold_temp_to_gold_main`. The team may rename these before the first row is written; after that they are frozen.

**Invariants the audit checks and records:** `source_count = target_count + filtered_count + rejected_count` for copy stages; `before_count - after_count` within the stage's threshold for overwrite stages; `inserted + updated + unchanged = distinct input keys` for merge stages. A breached invariant sets `status` FAILED and names the invariant in `status_reason`.

**Status values:** PASSED (every invariant held), FAILED (an invariant did not hold, named in `status_reason`), SKIPPED (the stage did not process the batch on purpose, named in `status_reason`). A stage that never ran has no row; that absence is DRE's evidence (T1_ON_TIME, the NOT_RUN cause, the watchdog), not the pipeline's.

**Status reasons** (closed list; empty when PASSED):

| Value | With | Meaning |
| --- | --- | --- |
| COUNT_MISMATCH | FAILED, copy stages | `source_count` differs from `target_count` + `filtered_count` + `rejected_count` |
| MERGE_KEYS_SHORT | FAILED, merge stages | inserted + updated + unchanged is less than the distinct input keys |
| MERGE_KEYS_EXTRA | FAILED, merge stages | More rows written than distinct input keys (fan-out) |
| OVERWRITE_DROP | FAILED, overwrite stages | `before_count` minus `after_count` beyond the stage's threshold |
| OVERWRITE_OUT_OF_SCOPE | FAILED, overwrite stages | A partition other than the batch's changed |
| VARIANCE_EXCEEDED | FAILED, any | `variance_pct` beyond the stage's threshold while the counts still add up |
| REJECT_RATE_EXCEEDED | FAILED, any | `rejected_count` / `source_count` beyond the stage's threshold |
| SEQUENCE_GAP | FAILED, source_to_raw | A sequence number missing before this file |
| NO_INPUT | SKIPPED | The upstream stage produced zero rows or no file arrived |
| HELD | SKIPPED | A hold marker or operator stop kept the stage from running |
| UPSTREAM_FAILED | SKIPPED | The previous stage's row is FAILED, so this one did not run |

The rejects table's `reason_code` list is separate and never appears in `status_reason`; a batch with rejects still passes unless its reject rate breaches the threshold.

## Rejects table (`ops_db.dq_rejects`)

One row per record a stage refused to carry forward, written in the same job that refused it, never dropped silently. Partitioned by `run_date`.

| Column | Type | Meaning |
| --- | --- | --- |
| `batch_id`, `stage` | STRING | As in the control table |
| `file_name` | STRING | Base name of the source file the record came from, or empty |
| key columns of the input table | as the input | The record's key, column names and values exactly as the input table holds them (the subscriber id, member number, effective date and plan id for the enrollment chain) |
| `reason_code` | STRING | From the closed list below |
| `reason_detail` | STRING | A short technical note (the failing column, the parser message), no member values beyond the key columns |
| `rejected_at` | TIMESTAMP (UTC) | When the stage refused it |
| `run_date` | DATE | UTC date of `rejected_at` |

**Reason codes** (closed list): `PARSE_ERROR` (the record could not be read), `KEY_NULL` (a key column null or blank), `MANDATORY_NULL` (a required non-key column null), `INVALID_CODE` (a value outside its allowed set), `INVALID_FORMAT` (a value not matching its pattern), `DUPLICATE_DROPPED` (a duplicate the de-dup stage discarded, with the winner's key in `reason_detail`), `JOIN_UNMATCHED` (a lookup that found no match), `RULE_FAILED` (a business rule, named in `reason_detail`).

A record rejected at one stage never appears in a later stage's rejects. A record filtered by a filter rule is not a reject: it is counted in the control table under its rule id and stays in the input table, where the rule can be re-applied.

## Filter rules

A filter rule is a condition a job applies to keep rows out of the next layer on purpose. Each one is registered once, in a small YAML or table the team keeps beside the jobs, and referenced by id everywhere else.

| Field | Meaning |
| --- | --- |
| `id` | Short, stable, unique: `drop_test_members`, `drop_void_claims` |
| `stage` | The stage that applies it |
| `input_table` | The table the condition is evaluated on |
| `condition` | The rule as a SQL condition over `input_table`'s columns, exactly as the job applies it: `status = 'TEST'` |
| `code_ref` | Where it lives in the job: file and line, or a commit |
| `since` | The batch id or date it first applied |

Three obligations follow. The job counts the rows each rule removed and writes them to `filtered_by_rule` in the control table. The condition must be re-runnable as plain SQL on the input table, because DRE re-applies it to explain a missing key (a rule that needs Scala code to express is not a filter rule; it rejects, with `RULE_FAILED`). And a changed condition is a new id, so counts before and after the change are never compared as the same rule.

## Job logs

Logs stay free-form except for one line shape, written once per failed file or failed stage, which a reader can match with a single pattern:

```
<ISO-8601 UTC> ERROR DRE_AUDIT stage=<stage> batch_id=<id> file=<base name> reason=<reason code> count=<n>
```

- `file=` carries the base name only, no path; `file=-` when the stage has no file.
- `reason=` is a reason code from the rejects list or a status reason from the control table.
- Nothing after `count=` and no member value anywhere on the line. A stack trace may follow on later lines; the pattern line itself is complete on its own.
- The logs for one job live under one path with a stable glob, for example `/data/logs/<job>/*.log`, retained at least as long as DRE's investigation retry window (`investigation_retry_days`, default 7) plus the SLA.

A stage that fails before it knows the file still writes the line with `file=-`, so a missing load is explained by the log and not only by its absence.

## How DRE reads each artefact

Each artefact fills one probe parameter in the feed's configuration (`probes:` in the feed file; spec section 7), and no engine change is needed for any of them.

| Artefact | DRE probe and configuration key | Cause it confirms | Closes |
| --- | --- | --- | --- |
| Rejects table | `table_contains` under `probes.rejects`: `{ table: ops_db.dq_rejects, condition: "reason_code IS NOT NULL" }`, matched on the failing file's base name or the failing key's columns | REJECTED, for a missing row or key | Developer question 14 |
| Filter rules | `table_contains` under `probes.filter_rules`, one entry per rule: `{ table: <input_table>, condition: <condition>, id: <id>, code_ref: <code_ref> }` | FILTERED, naming the rule id in the digest | Developer question 13 |
| Job logs | `log_contains` under `probes.load_log`: `{ path_glob: /data/logs/<job>/*.log, pattern: "ERROR DRE_AUDIT" }`; the probe keeps only lines naming a failing file | LOAD_ERROR | Developer question 12 |
| Control table | Not read by a packaged probe today. Two later uses, both generic capabilities: a probe that confirms NOT_RUN or PIPELINE_STALLED from a missing or FAILED stage row, and a hop that compares `source_count` for a file with the rows raw holds for it, which is the record-count-inside-the-file check DRE lacks | NOT_RUN, PIPELINE_STALLED (later) | Part of question 3 |

The `table_contains` match uses the same key normalisation DRE applies to the input layer, which is why the rejects table must keep the input table's key column names and raw values (rule 5). Each DRE cause line then reads, for example, `cause: FILTERED (confirmed): drop_test_members (load.sql:120)`, with no member value.

## Decisions still open

- [ ] **Effect of a breached invariant, per stage.** Stop the load, or write the row as FAILED and continue. The first stages (source to raw, raw to incr) can stop safely; a merge into gold main cannot be half-applied, so its check must run before the merge or only alert after it.
- [ ] **Rollback and replay of a failed batch.** Which stages can be re-run from their input for one `batch_id`, and what the control row of the re-run looks like (it replaces the earlier row, rule 3).
- [ ] **Threshold values.** The allowed variance per stage, kept in one configuration file beside the filter rules, not in job code.
- [ ] **Retention.** How long `dq_rejects` and `dq_control` keep rows; the control table is small and can keep years, the rejects table holds keys and should follow the lake's retention for that layer.
- [ ] **Final stage names.** Confirm or rename the seven stage values before the first row is written.
- [ ] **Real names.** The real database, table and log path names replace the placeholders in the deployed DRE configuration only, never in the repository.
