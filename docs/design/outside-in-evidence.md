# Design: outside-in evidence (release 4)

Status: proposed on 2026-10-16, waiting for the user's go-ahead on the order and on the file read cap in "Decide before step 1"; the partition-drift evidence question was decided the same day (file metadata plus a bounded count). Nothing in it is built.

## Why

The team is building a temporary in-pipeline audit (`docs/evidence-contract.md` fixes what it writes). It can see three things from inside a job that DRE cannot see from outside: the records inside a landed file, the merge's insert, update and unchanged split, and which records the job rejected and why. Everything else on its checklist DRE already does, or can do, without touching a job.

Hard constraints 1 and 2 (read-only; no pipeline changes) stay as they are. This plan closes the gap the other way: five generic, read-only capabilities that read what the pipeline already produces without being asked, namely the landed files themselves, the file registry DRE already keeps, Hive partition statistics, and the tables on both sides of a hop. After it, the only checklist item DRE cannot observe is the reason a job itself dropped a record, which it reports by elimination (a MISSING key that no configured rule explains).

## Where the engine stands

| Checklist use case | Today | After this plan |
| --- | --- | --- |
| Records inside the file versus rows loaded to raw | Not observed; UNREADABLE reads a file's header only | Step 2, T1_FILE_RECORD_COUNT |
| Sequence-number gap per host | T1_ON_TIME catches a missing load, not a gap in a counter | Step 3, T1_SEQUENCE_GAP |
| Partition scope of an overwrite; before and after counts | Symptoms only (T1_VOLUME drop, MISSING keys on the full sweep) | Step 1, T1_PARTITION_DRIFT |
| Raw and incr are identical copies (row parity) | `owned_columns` listing every column compares the latest version per key | Step 4, HOP_ROW_PARITY |
| Join fan-out or row loss | T1_KEY_DUPLICATES downstream; derived joins | Step 5, HOP_CARDINALITY |
| Merge insert, update, unchanged | HOP_KEY_CURRENCY classifies every key MISSING, STALE or CURRENT | Not changed: the split between inserted and updated needs a snapshot of the target before the merge, which only the job has. The hop's counts are the observable part |
| End-to-end source keys versus gold main | Configuration: gold lists raw as a second upstream in `key_map` | Not changed |
| Reject routing with reason codes | Elimination: "cause not proven", ruled-out list | Not changed, by nature |
| Everything else on the checklist | Built (`docs/verification-2026-10-13.md` and the comparison of 2026-10-16) | Not changed |

## Principles

- **Read hooks only.** A new capability reads a file, a listing, a table or metastore statistics. It never asks a job to write anything, and it never writes outside the `dq` store. Reading a landed file through Spark or the Hadoop FileSystem API is a read like any other.
- **Evidence DRE keeps is append-only in its own store.** Two new tables (file record counts, partition counts), written only through `store/writer.py`, created only by `dre install --apply`. Current state comes from views.
- **Every read is bounded before it starts.** File reads are capped by count and bytes; partition counts come from statistics by default and from a scan only when configuration says so; both run inside the dataset's compute budget and release what they cache.
- **Three result states, unchanged.** Each new check writes PASSED, FAILED or DID_NOT_RUN through `from_counts` or `did_not_run`, with an empty population as DID_NOT_RUN / empty_population and a missing input as DID_NOT_RUN with a CONFIGURATION or DATA_UNAVAILABLE reason.
- **Configuration describes kinds, never sources.** A record tag, a sequence regex, a parity column list are settings of a dataset or feed; the checks are generic and registered like any other (`docs/extending.md`).
- **No member data outside the store.** A record count, a partition value, a sequence number and a file base name are not member data. Row hashes stay in the Spark job and in `detail` only as counts.

## The plan, in steps

Each step is one PR-sized change: spec amendment first, then the check, its SQL, its tests (FAIL, PASS and the DID_NOT_RUN paths), and a decisions entry. Every step keeps the suite and the replay scenarios green and the end-to-end check COMPLETED.

### Step 1. T1_PARTITION_DRIFT: a partition changed outside the window

The strongest addition: it catches an insert-overwrite that reached the wrong partition, or re-wrote an old one, which nothing observes today.

- **Why not statistics.** Hive's partition `numRows` is written only when something computes it (`ANALYZE TABLE`, or Hive-side auto-gathering); Spark writes through `insertInto` leave it stale, and an overwrite that lands in the wrong partition is the write most likely to leave the old number behind. DRE cannot refresh it (`ANALYZE TABLE` is a metastore write). A full `COUNT(*)` of every partition every hour is exact but is one table scan per dataset per run. So the check detects by file metadata and confirms by counting only what changed.
- **Configuration.** A dataset with `partition_column` and `partition_format` (already needed for pruning) may set `partition_drift: { tolerance_rows: 0 }`; the check applies only where it is set.
- **Store.** New table `dq_partition_state` (dataset, table, partition_value, file_count, total_bytes, latest_modified_at, row_count, observed_at, run_id, run_date), append-only, partitioned by `run_date`; `row_count` is null unless the partition was counted this run. View `v_latest_partition_state`, the latest row per dataset and partition. DDL in `store/ddl.sql`, created by `dre install --apply`.
- **Logic.** Each NORMAL run lists the dataset's partitions (`SHOW PARTITIONS`), reads each one's location (`DESCRIBE FORMATTED ... PARTITION`) and lists its directory through the Hadoop FileSystem API: file count, total bytes, latest modification time, no data read. The partitions the window could touch (`partitions.partition_values`, widened by one granule each side, as pruning does) are *expected* to change. A partition outside that set whose three values differ from the previous run's, or that disappeared, was rewritten: it is counted with one `COUNT(*)` on that partition alone and compared with its last stored `row_count` (counted when it was last flagged, or on its first sighting). A count that differs by more than `tolerance_rows` is a violation; an equal count is reported in `observed` as "rewritten, same rows" (a compaction, or an idempotent re-run). Population: partitions outside the window with an earlier state. `detail` lists up to 100 changed partitions with before and after counts; `observed` carries the counts of expected, unchanged, rewritten-same and changed partitions.
- **DID_NOT_RUN.** No earlier state: BASELINE / insufficient_history (the first run records state and counts every partition once, inside the compute budget; a table too large to count on its first run is DID_NOT_RUN / budget_exceeded and tries again). More than 1000 partitions: the same bound as pruning, DID_NOT_RUN / invalid_config naming the count. A partition location that cannot be listed: DATA_UNAVAILABLE / hdfs_unavailable. A table that is not file-backed (a view): CONFIGURATION / invalid_config. RERUN and REPLAY: not run (it reads the table as it is now).
- **Cost.** One listing per partition per run (metadata only) and one count per rewritten partition outside the window, which on a healthy day is none. The first run counts every partition once.
- **Tests.** A partition outside the window rewritten with fewer rows: FAILED, detail names it with both counts. Rewritten with the same rows: PASSED, `observed` says rewritten-same. Only the window's partitions change: PASSED. First run: insufficient_history, and the state and counts are stored. A location that cannot be listed: hdfs_unavailable. Replay R12-style: the rows in `dq_partition_state` are append-only (guard).
- **Done when:** the e2e synthetic gold table, partitioned by a load date in the next step's fixture, reports PASSED on the second run, and the FAILED test's detail names the partition and both counts.

### Step 2. T1_FILE_RECORD_COUNT: records in the file versus rows in raw

- **Configuration.** On the feed's `landing`: `record_count: { kind: lines | delimited | xml_tag | records, tag: <element name>, header_lines: 0, max_bytes: 1073741824 }`. `lines` counts non-empty lines (text); `delimited` counts rows after `header_lines` (csv); `xml_tag` counts occurrences of `<tag` at the start of an element (XML written one record per element, counted by a line scan with a regex, never parsed as a document); `records` counts records of a sequence, parquet or orc file through Spark's reader. A kind that does not fit `file_format` is a config error naming both.
- **Store.** New table `dq_file_count` (path, feed, record_count, kind, counted_at, size_bytes, run_id, run_date), append-only; a file is counted once per (path, size, modified time), so a changed file is counted again and the registry's first-seen time still never moves.
- **Logic.** For files first seen in the window (from `dq_file`) that have no count yet, read and count, within `max_bytes` per file and `landing.max_files` overall, inside the compute budget. Then compare each counted file's `record_count` with the raw dataset's rows per `file_name_column` (base name), after `feed_filter`. Violation: fewer rows than records. Population: files counted and old enough to have loaded (first seen more than `sla_hours` ago, the same rule as T1_FILES_NOT_LOADED). `detail` lists up to 100 short files with both counts; a file with more rows than records is counted in `observed`, not failed (a loader that splits records is reported, not alarmed, as HOP_FILE_COMPLETENESS does).
- **DID_NOT_RUN.** No `record_count` configured: the check does not apply (not DID_NOT_RUN, like a parametrised check). No `file_name_column` on the raw dataset: CONFIGURATION / invalid_config. A file over `max_bytes`: skipped and counted in `observed`. The landing unreadable: DATA_UNAVAILABLE / landing_unreadable.
- **Cost.** Each file is read once in its life. The budget decision for this step is `max_bytes`; the default of 1 GiB is a proposal.
- **Tests.** A file with 3 records and 2 raw rows: FAILED. Equal: PASSED. No file old enough: empty_population. A file over the byte cap: PASSED with the skip in `observed`. Each `kind` on a synthetic file of its format. The count is stored once and reused on the next run (store read in the test).
- **Done when:** the e2e synthetic landing (sequence files with a known record count) gives PASSED, and the FAILED test's detail names the file and both counts.

### Step 3. T1_SEQUENCE_GAP: a missing number in a file series

- **Configuration.** On the feed's `landing`: `sequence: { regex: "<regex with named groups>", number_group: seq, series_group: host }`. The regex is applied to the base name; `series_group` is optional (one series). `dre validate` compiles the regex and checks the groups exist.
- **Logic.** From `dq_file` (no new read): for files first seen in the window, parse the number and the series; for each series, the expected numbers run from the highest number seen before the window to the highest seen in it; every missing number is a violation. Population: files first seen in the window that parse. `detail` lists up to 100 missing numbers per series; files that do not match the regex are counted in `observed`.
- **DID_NOT_RUN.** No files first seen in the window: empty_population. No earlier file in a series: that series is skipped and counted in `observed` (there is no gap to measure).
- **Semantics to state in the spec.** A gap is judged in the window where the later file arrived. If the missing file lands later, the later window reports no gap and the earlier FAILED stays as history; the cause engine's PIPELINE_STALLED and PASSED_OVER then explain the missing file as they do today.
- **Cost.** None beyond reading `dq_file`, already bounded by `run_date`.
- **Tests.** Numbers 7, 8, 10 across two windows: FAILED naming 9. Contiguous: PASSED. Two series with a gap in one: FAILED with the series named. No earlier file: observed, not failed. A base name the regex does not match: observed.
- **Done when:** the e2e synthetic landing, renamed to carry a sequence, reports PASSED.

### Step 4. HOP_ROW_PARITY: every upstream row reached the copy

For hops the configuration declares as copies ("raw and incr are identical copies"), where HOP_VALUE_AGREEMENT's latest-version comparison is too weak.

- **Configuration.** On the dataset, per upstream in `key_map`: `parity: { <upstream>: { columns: { <upstream column>: <this column>, ... }, normalise: { trim: true, upper: false } } }`. Columns are compared as strings after `key_normalise` on the key and the listed normalisers on the values; null and empty differ, as in HOP_VALUE_AGREEMENT.
- **Logic.** For upstream rows loaded in the window and past the SLA, a hash (`sha2` over `concat_ws` of the normalised values, computed in SQL) per row, and the multiset of hashes per key on both sides. Violation: an upstream row hash with no match here. Population: upstream rows judged. `hop_exclusions` apply. `detail` carries counts per key for up to 100 keys (hashes are never written).
- **DID_NOT_RUN.** The usual hop preconditions (both record times, the upstream load time, the tables and columns). No upstream row past the SLA: empty_population.
- **Cost.** One pass over each side's window rows; candidate keys only, as HOP_KEY_CURRENCY does since the verification batch.
- **Tests.** A row changed in one column: FAILED. A row missing entirely: FAILED. Identical: PASSED. Trailing space with `trim`: PASSED. Excluded rows: not expected.
- **Done when:** the three-layer acceptance test gains a parity hop between its raw and curated fixtures and passes.

### Step 5. HOP_CARDINALITY: rows per key across a hop

- **Configuration.** On the dataset, per upstream: `cardinality: { <upstream>: equal | at_most }`. `equal`: the same number of rows per key on both sides. `at_most`: no more rows here than upstream (fan-out is the failure; fewer is HOP_KEY_CURRENCY's business).
- **Logic.** Rows per key on both sides for the keys the window judges; violation per key by the declared rule. Population: keys judged. `detail`: up to 100 keys with both counts.
- **DID_NOT_RUN.** The hop preconditions; empty_population.
- **Tests.** Fan-out 1 to 3 under `at_most`: FAILED. 2 to 1 under `equal`: FAILED. Equal counts: PASSED.
- **Done when:** a derived join fixture that fans out is FAILED by this check and the join without fan-out PASSED.

### Step 6. Documentation

`docs/extending.md` gains the five checks as worked examples of read hooks; `docs/architecture.md` ("Transformation-aware reconciliation") lists the new dispositions; the feature catalogue and the comparison with the temporary solution are updated. README's settings table gets the new knobs.

## Spec changes this needs

- Section 1: a "Release 4: outside-in evidence" paragraph, and the gap it leaves (in-job reject reasons; the merge split).
- Section 3: `partition_drift`, `landing.record_count`, `landing.sequence`, `parity`, `cardinality`, with their validation rules.
- Section 4: no new reason code (the drift check uses insufficient_history, budget_exceeded, hdfs_unavailable and invalid_config).
- Section 5: the two tables and the view; retention for both (`dq_partition_state` 13 months like the file registry; `dq_file_count` the same).
- Section 6: five rows in the check catalogue; the pattern YAMLs list the three Tier 1 checks for file patterns and all datasets with the configuration, like the parametrised checks.
- Section 7: no new causes. A FAILED partition drift gets the "load wrote nothing" cause list's WRONG_PARTITION first, since that is the most likely explanation; the other three new checks get the hop list.
- Section 8: the compute budget paragraph covers file reads and partition scans.
- Section 9: rule 1 says explicitly that reading a landed file's contents is a read; the guard scanner allows `spark.read` on landing paths only in the new reader module.

## Decide before step 1

| Decision | Proposal | Who confirms |
| --- | --- | --- |
| Partition drift evidence | Decided 2026-10-16: file metadata detects a rewrite, a count of the rewritten partition alone confirms it; no metastore statistics, no full scan (step 1) | User |
| File read cap | `max_bytes` 1 GiB per file, `landing.max_files` overall; a sequence file's record count through Spark's reader | User |
| Order | 1 partition drift, 2 file record count, 3 sequence gap, 4 row parity, 5 cardinality | User |

## What this plan does not do, and why

- **No in-job hooks of any kind.** No listener, no wrapper, no library a job imports. The moment DRE runs inside a job it stops being able to report that the job did not run.
- **No merge split (inserted versus updated).** It needs the target as it was before the merge; DRE would have to snapshot every key of every target each hour. The hop's MISSING, STALE and CURRENT counts are the observable part and stay.
- **No reject reasons.** Only the job knows why it refused a record. DRE names the key, the hop and the window and says what it ruled out; if the team's audit writes a rejects table, DRE reads it through `probes.rejects` with no engine change.
- **No learned baselines, no XML parsing.** The record count is a line scan with a regex, by design: parsing documents is slow and a format of its own.
- **No change to evaluation ids, result states or the writer.**

## Order and size

| Step | Size | Why this order |
| --- | --- | --- |
| 1. T1_PARTITION_DRIFT | M | The only check that sees an overwrite hit the wrong partition, even when the row count is unchanged; needs a new store table, so it settles the pattern for step 2 |
| 2. T1_FILE_RECORD_COUNT | M | The largest in-job fact DRE lacks; the one real cost decision |
| 3. T1_SEQUENCE_GAP | S | `dq_file` already holds the evidence |
| 4. HOP_ROW_PARITY | S | A hop check in the existing shape |
| 5. HOP_CARDINALITY | S | Same shape; least urgent, duplicates and derived joins cover most of it |
| 6. Documentation | S | After the behaviour is settled |
