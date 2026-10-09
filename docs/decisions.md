# Decisions

Answers to the open decisions in spec section 11. Fill in **Answer** before build step 1, or write "spec default". Claude Code adds a line here whenever it takes a default.

| Decision | Spec default | Answer | Decided by, date |
| --- | --- | --- | --- |
| Language | PySpark 3.5.1 | | |
| Python version and packages on the cluster | Python 3.10+, dependencies shipped in a zip | | |
| Where it runs and is scheduled | Control-M for the main run; a separate folder or cron for the watchdog | | |
| Database name and service account | `dq`; a read-only account with write access only to `dq` | | |
| Use of AI coding tools on HCSC code | Only through HCSC-approved access | | |
| Code repository location | (not in spec) | Approved by HCSC: the repo is hosted on Aneesh Chan's GitHub (`caneesh/data-reliability-engine`) | HCSC, 2026-10-09 |

## Defaults taken during the build

- 2026-10-09, step 1: Language: no answer recorded; took the spec default, PySpark 3.5.1.
- 2026-10-09, step 1: Python version and packages on the cluster: no answer recorded; took the spec default, Python 3.10+ with dependencies shipped in a zip. CI runs on Python 3.10.
- 2026-10-09, step 1: Where it runs and is scheduled: no answer recorded; took the spec default (Control-M for the main run, a separate folder or cron for the watchdog). Nothing built in step 1 depends on it.
- 2026-10-09, step 1: Database name and service account: no answer recorded; took the spec default, `dq`. The guard test accepts `dq` or the placeholder `dq_database` (see tests/guard/scan.py), since the real name comes from config.
- 2026-10-09, step 1: Use of AI coding tools on HCSC code: handled with HCSC separately, outside this build. Not a build blocker; no default recorded here.
- 2026-10-09, step 2: Settings defaults from the spec applied when defaults.yaml omits them: `volume_tolerance_pct` 50 (section 6), `email_sample_keys` false (section 1), `full_sweep_day` SUNDAY (section 8). Sample `retention_months`: 84 for results, events and runs, 13 for the file registry (section 5).

## Configuration assumptions (step 2, reviewed 2026-10-09)

The engine field names chosen in step 2 are in spec section 3. The other assumptions:

1. **Settings levels.** `sla_hours`, `email_sample_keys`, `min_rows_per_load`, `volume_tolerance_pct`, `compute_budget_minutes` and `full_sweep_day` may be set in `defaults.yaml`, a feed or a dataset. A dataset takes the value from the lowest level that sets it (dataset, then the feed that lists it, then defaults); null means inherit. Table-wide datasets skip the feed level.
2. **`defaults.yaml` fields.** `dq_database`, `environment`, `hmac_secret_file` (null until set; an absolute path outside the repository and the conf directory), `retention_months` (one entry per dq table), `recipients` (owner to addresses), and the settings above.
3. **Which nulls are allowed.** Fields the spec shows as null are optional: `partition_column`, `file_name_column`, `cause_inputs.*`, `owner` on feed datasets. `load_time` is optional with a warning. `record_time` is optional unless the dataset has a `key_map` or is mapped to by one. Every other field is required, so a null there is an error.
4. **Time formats.** `record_time` and `load_time` share the shape `{column, format, granularity}`; `format: null` means the column is already DATE or TIMESTAMP. The sample leaves `format` null where the real format is not confirmed.
5. **Load-time checks.** The warning names T1\_ON\_TIME, T1\_ZERO\_ROWS, T1\_VOLUME and T1\_KEY\_NULLS (and, with a key_map, cause checks NOT\_RUN and OLDER\_VERSION\_WRITTEN\_LATER). Step 5 must keep this list (`config/validate.py`, `LOAD_TIME_CHECKS`) in line with the checks it builds, including any fallback to `partition_column`.
6. **Regex dialect.** `value_format` patterns are compiled with Python's `re`; Spark uses Java regex. A pattern valid in one and not the other is caught by dry-run.
7. **SQL fragments.** `dre validate` only rejects `;` in `feed_filter`, `open_when` and filter conditions. **Step 4 must parse them in `dre dry-run`** (Spark `EXPLAIN` or equivalent) and report failures with file, line and field.
8. **Sample config.** No raw dataset until its columns are confirmed. The `older_coverage_still_open` grouping and order are placeholders until the rule baselines are run (spec section 11, queries 5 to 7).

## Store assumptions (step 3, 2026-10-09)

1. **Two dq_run rows per run** (changed in the step 3 review). A STARTED row when the run starts and a final row (COMPLETED, PARTIAL or FAILED) when it ends; the run's state is its latest row (`v_latest_run`). A run that dies stays STARTED, which the watchdog reports. Both rows carry `run_date` = the UTC date the run started. Status of the final row: COMPLETED when every expected check was written, PARTIAL when fewer, FAILED when the run says it failed.
2. **v_latest_result with groups.** The spec says one row per (dataset, check\_id). For a check split by `group_by`, the view returns every group row of the latest evaluation (latest by `evaluated_at`, then `run_id`), so an ungrouped check still gives one row.
3. **v_open_keys** gives the first flagged time of the key's current open spell (after its last CLEARED). It does not expose `key_value`, which stays restricted in `dq_key_event`.
4. **v_file_status** gives the earliest `first_seen_at`, and size, `modified_at` and `observed_at` from the latest observation.
5. **`dre install` applies the DDL** (decided in the step 3 review, 2026-10-09). The platform team only creates the empty dq database and grants the service account access. `dre install --print` renders the DDL for the configured database, `--apply` runs it, `--check` compares the existing tables and views with it. Every statement is IF NOT EXISTS, so `--apply` is safe to rerun and never replaces an existing table or view: a changed definition shows up as DIFFERS in `--check` and needs a person to migrate it. `dre run` checks the store tables and views exist and exits 3 naming `dre install` if not. Reason: the engine owns its schema, while creating databases and granting access stays with the platform team. Enforced by guard rule 8 (spec section 9).

## Check framework assumptions (step 4, 2026-10-09)

1. **T1_KEY_DUPLICATES built in step 4.** R09 and R11 need a real check or they pass vacuously. T1_KEY_DUPLICATES needs no load-time window and step 5 needs it for R08; step 5 adds the other six Tier 1 checks.
2. **Event windows** (replaced in the step 4 review, 2026-10-09; spec section 4). Windows are UTC, half-open, and recorded on each result row (`window_start`, `window_end`). The end is the run's start minus `settle_minutes`; the start is the latest `window_end` of the dataset's NORMAL events, skipping an event when **any** of its checks was DID\_NOT\_RUN for PLATFORM, BUDGET or CONFIGURATION, so a blocked check never silently loses a window. The cost: one check stuck on a configuration error holds the dataset's window open until it is fixed, and the window grows. First run: end minus `initial_lookback_hours`. Bounds truncate to `load_time.granularity` in the load time's zone. `event_id` and `evaluation_id` hash their parts joined with `|` (the spec writes `+`); `evaluation_id` includes the sorted group values.
3. **`evaluate` returns a list,** one result per group, rather than the single `CheckResult` in the spec's interface. A query that returns no rows (an empty table with `group_by`) is one DID\_NOT\_RUN / `empty_population` result.
4. **Error mapping.** Spark error classes map to reason codes: `UNRESOLVED_COLUMN*` to `column_missing`, `TABLE_OR_VIEW_NOT_FOUND` to `table_missing`, `DATATYPE_MISMATCH*` and `CAST_INVALID_INPUT` to `incompatible_type`, and everything else (including Python errors) to PLATFORM / `query_failed`. A failing catalogue call in the preconditions is `metastore_unavailable`.
5. **Run accounting and exit codes.** `checks_expected` is the number of (dataset, check) evaluations planned; `checks_written` the number whose rows were appended. A failed write is logged (check id and table only) and leaves the run PARTIAL (exit 2). An error outside a check ends the run FAILED, also exit 2, since spec section 8 has no separate code for it.
6. **Execution type.** `--execution-type` is recorded on each result row. Only NORMAL events move the window forward. Reusing the earlier window for a RERUN, so that it gets the same `evaluation_id`, is left to step 8, where the full run comes together.
8. **Time zones** (step 4 review). Time columns carry a `timezone` (IANA; default `timezone` in `defaults.yaml`). Checks convert with `to_utc_timestamp` after parsing, in a UTC Spark session; a column without `format` is read as a TIMESTAMP in its zone.
9. **Table-wide datasets** (step 4 review) get T1\_SCHEMA\_DRIFT, T1\_KEY\_NULLS and T1\_KEY\_DUPLICATES (`patterns/table_wide.yaml`), carry their own `expectation_version`, and run only in a full `dre run` (not with `--feed`). T1\_SCHEMA\_DRIFT runs once per physical table (step 5).
7. **dry-run** exits 0 even when SQL fragments do not resolve (spec section 8: "0 always, unless the command itself fails"); it exits 3 only when the config is invalid or the feed unknown. It reads the window from the store when there is one and writes nothing.

## Notes for later steps

- **Step 4:** done: `dre dry-run` parses SQL fragments (`config/fragments.py`).
- **Step 5:** keep `LOAD_TIME_CHECKS` in `config/validate.py` in line with the Tier 1 checks (configuration assumption 5).
- **Step 7:** during the full sweep (`full_sweep_day`), append CLEARED with `state_detail` "no longer upstream" for open keys (`v_open_keys`) that are no longer found upstream, so they do not stay open forever.
- **Step 9:** the digest reports groups (`group_values`) that were present in the previous run and are missing now, since `v_latest_result` shows only the latest evaluation's groups.
- **Step 10:** `dre retention` is a separate command, scheduled weekly. By default it lists the partitions it would drop under each table's `retention_months`; `--apply` drops them through `store/retention.py`.

## Guard rules: reasons (decided after step 1, 2026-10-09)

The rules themselves are in spec section 9 (Guard tests). Reasons, by rule:

1. **Naming the dq store.** The real database name comes from config, so code cannot hard-code `dq`; a fixed placeholder keeps the target visible to a static scan. Validating the name as a plain identifier stops it carrying dots, quotes or SQL into statements. Step 2 must call `validate_dq_database` when loading config.
2. **Read-only outside dq.** Hard rule 1. Path writes, FileSystem and os/shutil calls are other ways to change production data than SQL, so they are flagged the same way.
3. **Append-only.** Hard rule 5. Overwrite modes and `saveAsTable` are the DataFrame forms of INSERT OVERWRITE and of creating tables outside `store/ddl.sql`.
4. **One write path** (added before step 2). A static scan cannot follow a write chain split across statements; allowing the write APIs in one module closes that gap. Column order: `insertInto` matches by position, so a frame in a different order would silently write values into the wrong columns.
5. **Retention.** Retention needs old partitions dropped, which append-only otherwise bans; keeping it in one module, with a refusal for anything inside retention, limits the exception.
6. **Database creation** (added before step 2). On the cluster the platform team owns the database and the service account; the engine must not create it. Tests and local runs still need it.
7. **Email.** Hard rule 6. The digest does not exist yet, so the guard is static until step 9, which must add a test on the rendered email text.
8. **Store DDL only through `dre install`** (added in the step 3 review). The platform team creates the database and grants access; the engine creates its own tables and views, but only through one deliberate command, never as a side effect of `dre run`.

CI runs on Python 3.10 with Java 17 (`.github/workflows/ci.yml`). Nothing may depend on Java 21.
