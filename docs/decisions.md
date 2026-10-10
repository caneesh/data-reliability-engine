# Decisions

Answers to the open decisions in spec section 11. Fill in **Answer** before build step 1, or write "spec default". Claude Code adds a line here whenever it takes a default.

| Decision | Spec default | Answer | Decided by, date |
| --- | --- | --- | --- |
| Language | PySpark 3.5.1 | | |
| Python version and packages on the cluster | Python 3.10+, dependencies shipped in a zip | | |
| Where it runs and is scheduled | `dre run` hourly (Control-M or cron); the watchdog hourly from a separate folder or host | Hourly run, due feeds only (genericity review) | Aneesh Chan, 2026-10-10 |
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

## Tier 1 checks (step 5, 2026-10-09)

1. **T1\_FILES\_NOT\_LOADED is built in step 6** (decided by Aneesh Chan, 2026-10-09), with the `dq_file` registry it needs. Spec section 10 now gives step 5 as the six table-based checks. Until step 6 the check is not in any pattern file, and there is no stub that could PASS.
2. **Presence checks count expected loads** (decided by Aneesh Chan, 2026-10-09). T1\_ON\_TIME and T1\_ZERO\_ROWS take loads due as their population: a load due with nothing loaded is FAILED, no load due is DID\_NOT\_RUN / empty\_population, and neither can PASS on an empty table. R11 is split into R11a (no load due: everything DID\_NOT\_RUN) and R11b (a load due: those two FAILED, row-based checks DID\_NOT\_RUN). Each check's population is in spec section 6.
3. **Which slots** (revised in the step 5 review). T1\_ON\_TIME and T1\_ZERO\_ROWS both judge the slots whose deadline (slot + `sla_hours`) falls in the window, so each slot is judged once and never before its SLA has run out. They count rows loaded in [slot, slot + SLA): T1\_ON\_TIME needs one, T1\_ZERO\_ROWS needs `min_rows_per_load`, sharing one SQL template. With `min_rows_per_load: 1` the two agree on every slot. Named calendar files are rejected by `dre validate` (release 1 supports EVERYDAY and WEEKDAYS; `kind: calendar_dates` lists dates).
4. **T1\_VOLUME per slot** (revised in the step 5 review). One result per slot whose load period [slot, next slot) ends in the window, with group values `slot` (local HH:MM) and `slot_time` (local date and time, so two slots with the same time of day in one long window stay apart). History is the row counts of the dataset's earlier NORMAL results with the same `slot`, including DID\_NOT\_RUN / insufficient\_history ones, which record their count so the baseline builds up. A slot with no rows gives no T1\_VOLUME result; T1\_ZERO\_ROWS reports it.
5. **T1\_SCHEMA\_DRIFT reads columns with `DESCRIBE TABLE` and compares them in Python: an exception to "all check logic is SQL in Jinja2 templates"** (recorded in the step 5 review). The comparison is of catalogue metadata (names and types in order), not of rows, and needs the previous schema from `detail`. The first evaluation of a table is DID\_NOT\_RUN / insufficient\_history and records the baseline. `observed` holds the hash, `expected` the previous hash, and `detail` a JSON object with the table and columns (and the added, removed and retyped columns when FAILED). History is looked up by table, so it survives the dataset that records it changing.
6. **Grouping.** T1\_ON\_TIME and T1\_ZERO\_ROWS are per dataset (after `feed_filter`), not per `group_by` group; T1\_VOLUME is per slot; T1\_KEY\_NULLS and T1\_KEY\_DUPLICATES are per `group_by` group. T1\_KEY\_NULLS looks at raw key values (before `key_normalise`).
7. **No load_time.** Checks that need it are DID\_NOT\_RUN / invalid\_config. That is a CONFIGURATION reason, so the dataset's window does not move until `load_time` is set; only checks that cannot run anyway depend on it.
8. **One window per dataset per run.** The runner fixes every dataset's event before writing any result. Otherwise a dataset's later checks saw its earlier checks' rows from the same run and got an empty window (found and fixed in step 5).

## Landing (step 6, 2026-10-09)

1. **Listing** goes through the Hadoop FileSystem API from the Spark session (`listFiles`, recursive), which also reads local `file://` paths in tests. Only listing calls are used. Hidden files (`.` or `_` prefix, such as `_SUCCESS` and `.crc`) are skipped. A root that does not exist or cannot be read gives DID\_NOT\_RUN / landing\_unreadable; any other filesystem error gives hdfs\_unavailable.
2. **Registry rows.** A row is appended only for a new path, or a known path whose size or modified time changed since its latest row, not for every file on every run. `first_seen_at` is kept from the first row, so `v_file_status` shows both when the file was first seen and its latest size, which INCOMPLETE\_AT\_LOAD needs in step 8.
3. **Matching raw rows.** Files are matched to raw rows on the base file name: the last path component on both sides. This assumes the raw `file_name_column` holds the landed file's name (with or without a path). **To confirm with the developer.**
4. **Which files are judged.** Every file in the feed's registry first seen more than `sla_hours` before the window end, so a file that never loads keeps failing until it does. That assumes raw keeps rows for at least as long as the registry keeps files (13 months by default); otherwise files whose raw rows were purged would show as not loaded. **To confirm with the developer.**
5. **Validation.** `dre validate` warns (does not fail) when a file-pattern feed has no RAW dataset with a `file_name_column`, since T1\_FILES\_NOT\_LOADED cannot run for it. The sample config gets this warning until the raw dataset's columns are confirmed. Tests use a synthetic raw dataset.

## Genericity review (after step 6, 2026-10-10)

1. **Hourly run, due feeds only** (replaces the spec default of a scheduled main run per feed). `dre run` is scheduled every hour. Each run evaluates only feeds with something due: a cadence slot's deadline passed in the feed's window since its last evaluation, or new or changed landed files were registered. A skipped feed writes nothing, so its window carries over. `--feed F` forces F. `check_delay_minutes` (feed, default 0) moves the feed's window end back by that much, so slots are judged only once deadline plus delay has passed. Table-wide datasets run when a due feed's dataset shares their table, otherwise every run (**my choice; flag if hourly table-wide checks are too heavy**). The watchdog expects a run every hour plus grace. R11a now forces its feed with `--feed`, since a plain run correctly skips a feed with no load due.
2. **Generic cause probes** replace the named cause inputs. Probe types: `file_exists`, `file_value_compare`, `log_contains`, `table_contains`, `size_changed`, plus `builtin` for engine logic over DRE's own evidence. Pattern YAML lists, per failure type, `{code, probe, params}`; the feed's `probes:` supplies parameters by key; missing or null parameters mean NOT\_READY. Filter rules are `table_contains` parameter sets (`table`, `condition`, optional `id`, `code_ref`); the old `owner` and `expected_daily_volume` fields are dropped. Probe execution comes with the cause engine in step 8.
3. **Second synthetic feed** in `conf/`: `provider_roster_monthly` (FILE\_PERIODIC, monthly CSV, flat folder, no partitions, UTC, key `provider_id`) and `provider_directory_merge` (TABLE\_MERGE gold table). It needed one new generic capability: cadence `kind: monthly` with `days_of_month`. Its `initial_lookback_hours` is 840 (35 days) so a first window holds a monthly load. Replays R01, R02, R03, R08, R09, R11a and R11b run against it (`tests/replay/test_second_feed.py`).
4. **Genericity guard** (`tests/guard/test_generic_engine.py`): a denylist of source-specific words that must not appear under `src/`. Spec section 9, rule 9.
5. **End-to-end CI job** (`e2e` in `.github/workflows/ci.yml`): builds synthetic tables and landing folders for both feeds, then runs `dre install --apply`, `dre run` and a verifier as separate processes against one local metastore.

## Hop checks and gold rules (step 7, 2026-10-10)

1. **Keys judged at their deadline.** The spec says "for each upstream key" and "older than `sla_hours`". A hop check judges an upstream key once, in the window where its latest upstream load time plus `sla_hours` falls, plus keys still open in `v_open_keys`; the full sweep judges every key past its deadline. This keeps each run's work to the keys that changed, and never judges a key the downstream layer still has time to load. HOP\_FILE\_COMPLETENESS judges each file the same way, by its first upstream load time.
2. **Ties.** Every upstream row at a key's latest record time is kept; HOP\_VALUE\_AGREEMENT needs each of them to match this dataset's latest row (null-safe, compared as strings). So R06 (an open row and a termination tied) always fails agreement, whichever row gold kept. STALE means this dataset's latest record time for the key is older than upstream's, or null.
3. **One result per upstream** (group `upstream`), so a dataset mapped to two upstreams gets a result for each hop.
4. **The HMAC secret** is read once per run from `hmac_secret_file` (surrounding whitespace stripped). It is used in a Spark SQL function registered with the secret in its closure, never put in SQL text. Not set: no key events, and a FAILED HOP\_KEY\_CURRENCY says so in `detail`. Set but unreadable or empty: `dre run` exits 3 before writing anything, so key events are never lost quietly.
5. **Key normaliser `parse_date`.** R04 needs raw dates (MM/dd/yyyy) to match curated ones (yyyy-MM-dd). `key_normalise` now also takes `{ parse_date: <format> }`, which reads the string with that format and writes it as yyyy-MM-dd. A generic capability, not per-source code.
6. **Rule `format`.** `column_order`, `superseded_still_open` and `child_within_parent` take an optional `format` so string dates are parsed, not compared as strings (CLAUDE.md). The sample gold rules set `format: yyyy-MM-dd` (gold dates are YYYY-MM-DD, spec section 9).
7. **Rules run over the whole table** (after `feed_filter`) every time their dataset is evaluated, not only over the event window: the rules describe the table's state. proposed and approved rules both run; the email (step 9) marks proposed ones report only.
8. **Sample config: gold's window is held back until curated's `load_time` is confirmed.** Without it, gold's hop checks are DID\_NOT\_RUN / invalid\_config, a CONFIGURATION reason, so gold's window does not move on (spec section 4) and each run re-reads the same window from the first run's start. Correct by the spec, but worth knowing: **curated's load-time column is needed (spec section 11)**. Tests confirm it in their copy of the config.
9. **R04 at check level** uses a synthetic raw dataset (test-only columns, with `msg_ts` as its record and load time), since the sample config has no raw dataset until its columns are confirmed.

## Notes for later steps

- **Step 4:** done: `dre dry-run` parses SQL fragments (`config/fragments.py`).
- **Step 5:** done: `LOAD_TIME_CHECKS` matches the Tier 1 checks that need `load_time`. No fallback to `partition_column` yet.
- **Step 6:** done: T1\_FILES\_NOT\_LOADED is in `patterns/file_cyclic.yaml` and `patterns/file_periodic.yaml`.
- **Step 6 (open):** a dataset with `partition_column` but no `load_time` may use the partition value as its load time, at the partition's granularity. Waiting on the developer to confirm whether the raw table has a load-time column; until then such a dataset's load-time checks stay DID\_NOT\_RUN / invalid\_config.
- **Step 7:** done: on the full sweep, open keys no longer found upstream get CLEARED with `state_detail` "no longer upstream".
- **Step 9:** when T1\_ON\_TIME and T1\_ZERO\_ROWS both fail for the same slot, the digest shows one alert, not two.
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
