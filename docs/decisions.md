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
2. **Event windows** (replaced in the step 4 review, 2026-10-09; spec section 4). Windows are UTC, half-open, and recorded on each result row (`window_start`, `window_end`). The end is the run's start minus `settle_minutes`; the start is the latest `window_end` of the dataset's NORMAL events, skipping an event when **any** of its checks was DID\_NOT\_RUN for PLATFORM or BUDGET, so a blocked check never silently loses a window (a CONFIGURATION reason never holds: those checks report DID\_NOT\_RUN every run until the configuration is fixed; corrected 2026-10-14, the code and the spec always said so). The cost: one check stuck on a configuration error holds the dataset's window open until it is fixed, and the window grows. First run: end minus `initial_lookback_hours`. Bounds truncate to `load_time.granularity` in the load time's zone. `event_id` and `evaluation_id` hash their parts joined with `|` (the spec writes `+`); `evaluation_id` includes the sorted group values.
3. **`evaluate` returns a list,** one result per group, rather than the single `CheckResult` in the spec's interface. A query that returns no rows (an empty table with `group_by`) is one DID\_NOT\_RUN / `empty_population` result.
4. **Error mapping.** Spark error classes map to reason codes: `UNRESOLVED_COLUMN*` to `column_missing`, `TABLE_OR_VIEW_NOT_FOUND` to `table_missing`, `DATATYPE_MISMATCH*` and `CAST_INVALID_INPUT` to `incompatible_type`, and everything else (including Python errors) to PLATFORM / `query_failed`. A failing catalogue call in the preconditions is `metastore_unavailable`.
5. **Run accounting and exit codes.** `checks_expected` is the number of (dataset, check) evaluations planned; `checks_written` the number whose rows were appended. A failed write is logged (check id and table only) and leaves the run PARTIAL (exit 2). An error outside a check ends the run FAILED, also exit 2, since spec section 8 has no separate code for it.
6. **Execution type.** `--execution-type` is recorded on each result row. Only NORMAL events move the window forward. Reusing the earlier window for a RERUN, so that it gets the same `evaluation_id`, is left to step 8, where the full run comes together. (Built after the code review, 2026-10-12: see "Review items 6 and 7, answered".)
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
7. **Rules run over the whole table** (after `feed_filter`), not only over the event window: the rules describe the table's state. proposed and approved rules both run; the email (step 9) marks proposed ones report only. How often they run: see the step 7 review, item 2.
8. **Sample config: curated's `load_time` is not confirmed**, so gold's hop checks are DID\_NOT\_RUN / invalid\_config on every run until it is (spec section 11). Confirmed on 2026-10-15: see "Curated load time column" below. Since the step 7 review this no longer holds gold's window.
9. **R04 at check level** uses a synthetic raw dataset (test-only columns, with `msg_ts` as its record and load time), since the sample config has no raw dataset until its columns are confirmed.

## Step 7 review (2026-10-10)

1. **Hop keys reloaded downstream.** The hop key population also includes keys with a row in this dataset loaded in the window, so a key that regresses without an upstream change (for example gold overwritten with an older version) is judged again. Such a key is judged only once its upstream deadline (latest upstream load + `sla_hours`) has passed, so a reload never judges a key the downstream still has time to catch up on (my choice: otherwise a reload inside the SLA would flag a key early). Needs the downstream `load_time`; without one, only the other two terms apply. The weekly full sweep (`full_sweep_day`) already existed; it is tested at check level (`tests/checks/test_hop.py`, full-sweep tests) and for the day in the default time zone (`tests/test_runner.py`).
2. **Rule frequency.** `frequency: every_run | daily | weekly`, default daily, and `rule_run_at` (default `"06:00"`, in the defaults.yaml timezone) in `defaults.yaml`. **My choices, flag if wrong:** the key is named `rule_run_at` (a bare `run_at` read as the main run's schedule); weekly rules run on the dataset's `full_sweep_day`, since the spec names no other weekly day; a rule is due when it has no NORMAL result since its latest due moment, not counting a result held by a PLATFORM or BUDGET reason; rules are planned on their own schedule, not only when their feed is due (a daily rule on a monthly feed would otherwise run monthly); and rule results neither move nor hold their dataset's window.
3. **Window rule corrected.** Only PLATFORM and BUDGET reasons hold a window; CONFIGURATION reasons never do, and those checks report DID\_NOT\_RUN every run. A hold lasts at most `max_window_hours` (default 72, a setting overridable per feed or dataset). **My choices, flag if wrong:** the cap counts from the end of the first held event, not the window's length, so a monthly feed's naturally long window is not cut short by a single held run; on release the window starts where the last held event ended, and the gap is one result per dataset with `check_id` WINDOW\_GAP, DID\_NOT\_RUN / window\_gap, with reason category DATA\_UNAVAILABLE (a category that does not hold, so the gap's own end moves the window on), its window set to the unchecked span and `detail` listing the hours and the codes that held it. Gap rows count in the run's `checks_expected` and `checks_written`.

## Cause engine (step 8, 2026-10-10)

1. **Failures explained one by one** (`failure_ref`): files, slots, upstream files or key hashes, at most 100 per evaluation (as in check details). T1\_ON\_TIME and T1\_ZERO\_ROWS now list their failing slots in `detail` so they can be explained.
2. **Fallback not written as a row.** The spec's cause states are CONFIRMED, RULED\_OUT, NOT\_READY and ERROR; the fallback is not checked, so it gets no row. The cause is resolved from the rows plus the pattern's fallback (`causes/engine.py`, `resolve`); the email (step 9) and `dre trace` use the same.
3. **Keys without an HMAC secret.** Failing keys are referenced by `key_hash`. Without `hmac_secret_file` they cannot be, so each cause check is written once with no `failure_ref`, NOT\_READY ("hmac_secret_file is not set"). The replays configure a secret.
4. **`file_value_compare` takes a `format`** (a Spark datetime pattern), required like its other parameters. The spec's parameters (path, extract\_regex, compare\_to) would force comparing partition values as strings, which CLAUDE.md forbids. The failing file's partition is the same `extract_regex` applied to the file's folder name; a missed or short slot's partition is the slot's date in the feed's time zone, written with `format` and read back. **To confirm with the developer: the handoff file's date format.**
5. **WRONG\_PARTITION (spec unclear, flag).** The spec says "Rows were written to a partition other than the one named in the cursor file", but the probe it names (`file_value_compare`, `compare_to: partition`) confirms when the cursor's partition is later than the failure's partition. For a short slot, I compare the cursor with the slot's partition: CONFIRMED when the cursor is ahead (today's load went to a later partition). It does not look for the rows in the other partition (gold has no partition column in the sample). **Please confirm this reading, or say how the rows in the other partition should be found.** R03's WRONG\_PARTITION replay is built on it.
6. **Missed slots on file patterns** (T1\_ON\_TIME): RAW\_LOAD\_HELD and the cursor apply as for files; PIPELINE\_STALLED, size\_changed, UNREADABLE and LOAD\_ERROR look at the files first seen since the slot that have no raw rows. Without a raw dataset those are NOT\_READY.
7. **PIPELINE\_STALLED** counts files first seen with (in the same listing) or after the failing file, excluding it: if any of them loaded, the pipeline ran. With no such file it is RULED\_OUT (nothing shows the pipeline stopped). R02's replay lands files after the missed loads.
8. **NO\_UPSTREAM\_DATA** counts upstream rows loaded in the event window (the spec's "in the window"), not in the slot's own load period: a merge runs after its upstream loads.
9. **Load times compared at this dataset's granularity** (NOT\_RUN, OLDER\_VERSION\_WRITTEN\_LATER): the upstream load time is truncated to this dataset's `load_time.granularity` first. NOT\_RUN is CONFIRMED only when this dataset's latest load is strictly before it; the same minute counts as "ran".
10. **TIE\_RESOLVED\_BY\_RULE.** CONFIRMED for a CURRENT key when the upstream rows tied at the latest record time carry different owned-column values and this dataset's row matches the row `winner_rule` ranks first: the disagreement is the rule's choice (R06). STALE and MISSING keys: RULED\_OUT.
11. **UNREADABLE** reads only the first record (or header) with the reader for `file_format` (Spark's sequence, text and CSV readers; parquet and orc by schema; xml as text starting with `<`). An empty file is unreadable; a file that has gone is ERROR, not UNREADABLE. Reading files and logs uses Spark readers, not `spark.sql` templates (a recorded exception, like DESCRIBE in step 5). Synthetic landing files in tests are now real sequence files (`tests/fixtures/landing.py`), as spec section 9 asks.
12. **`log_contains`** matches the pattern with Spark's (Java) regex; `dre validate` compiles it with Python's. Simple patterns behave the same; flag if the real pattern uses syntax where they differ.
13. **Cause checks never stop the run.** A probe that raises is ERROR; if the failing things cannot be listed, every cause check is written as ERROR; a failed cause write is logged (exception type only) and the run continues.

## Email and trace (step 9, 2026-10-10)

1. **Sending.** The spec names a sender but no mail relay. `defaults.yaml` gets `email: {smtp_host, smtp_port, sender}` (standard-library SMTP, no service of ours); null until the platform team confirms the relay (added to spec section 11). Until then each run prints, per owner, the subject and "not sent".
2. **One digest per owner per run**, holding only that owner's results (the feed's owner; a table-wide dataset's owner; a rule's owner for its results). An owner with nothing evaluated in a run gets no digest. **Flag:** with the hourly run (genericity review) this is up to 24 digests a day per owner, and a DID\_NOT\_RUN for a configuration problem appears in each. Say if you want, for example, all-clear digests only once a day.
3. **What counts.** New failures: results FAILED now whose previous result was not FAILED (a first result that FAILED counts). Did not run: every DID\_NOT\_RUN this run, including WINDOW\_GAP. `all clear` only when nothing is newly failing, still failing or did not run. Proposed rules are report only and never counted; approved rules count like checks.
4. **Cause line** follows the spec's form and also names the fallback: `cause not proven (DROPPED): ruled out …`. `dre run` prints the same lines.
5. **One alert for T1\_ON\_TIME and T1\_ZERO\_ROWS** when every short load of T1\_ZERO\_ROWS is also a missed load of T1\_ON\_TIME (a missed load always writes zero rows); shown under T1\_ON\_TIME with a note. A T1\_ZERO\_ROWS with other short loads keeps its own alert.
6. **Missing groups** are judged only on configured group columns (the dataset's or rule's `group_by`, and `upstream`): T1\_VOLUME's groups name the slot's date, so last run's slots are never "missing".
7. **Trace** prints layers most upstream first and finds the first hop with the key missing or at an older record time. Its cause line is the latest HOP\_KEY\_CURRENCY cause for the key's `key_hash` at that hop, so it needs `hmac_secret_file`. Key values on the command line are split on commas (a value containing a comma cannot be traced). Bad input exits 3, like `dre run` when it cannot start.

## Watchdog and budgets (step 10, 2026-10-10)

1. **Budget per check.** "If a check exceeds its budget" read as each check getting its dataset's whole `compute_budget_minutes` (not one budget shared by the dataset's checks in a run). **Flag if a shared budget per dataset is meant.**
2. **Cancelling.** Each check runs in its own Spark job group in a worker thread; at the budget the group is cancelled with interrupt, which stops its running Spark jobs. Python work inside the check cannot be interrupted; it is abandoned and its result ignored.
3. **Scan estimate.** The spec asks to estimate the scan size before running, but turning bytes into minutes needs the cluster's throughput, which nobody has given. The estimate (Spark's table statistics, present when the metastore has them, e.g. after ANALYZE TABLE) is recorded in a budget\_exceeded result's detail; checks are not refused up front. **To confirm with the platform team:** a throughput figure, if checks should be refused before running.
4. **Watchdog hour and grace.** One hour judged per watchdog run: (now − `watchdog_grace_minutes`), floored to the hour; grace 30 minutes by default. A run that started late and is still within its grace is not an alert; if it is still STARTED at the next watchdog run it falls outside the judged hour and is not reported again (a known gap, to keep the watchdog stateless).
5. **Watchdog alerts** go to every address in `recipients` (no separate operations owner is configured; **flag if there should be one**), through the digest's mail relay; with the relay unset the alert is printed and the exit code is still 1. A FAILED final row is an alert as well as `checks_written < checks_expected`.
6. **Independence.** `watchdog.py` imports only the config loader, the store's name validation and the mail sender; a test checks its imports.

## Packaging (step 11, 2026-10-10)

1. **Artifacts** (`python scripts/package.py`): the wheel; `dre-pyfiles-<version>.zip` with the package for `--py-files`; `dre-deps-<version>-<platform>.zip`, wheels of pydantic, PyYAML and Jinja2 for the cluster's Python and platform (default 3.10, manylinux2014\_x86\_64); `dre_main.py`, the entry point. PySpark is the cluster's.
2. **Dependencies are installed, not shipped in `--py-files`.** pydantic's core is compiled, and Python cannot import compiled modules from a zip. They are installed once on the driver host from the wheel zip (offline), and runs use `--deploy-mode client`. Executors need only the package itself (for the key-hash function), which the `--py-files` zip carries. **To confirm with the platform team:** client mode on an edge node, and its Python version.
3. **Namespace packages in the zip.** Python's zip importer cannot resolve native namespace packages, so the `--py-files` zip alone gets pkgutil-style `hcsc/__init__.py` and `hcsc/datalake/__init__.py`, which still merge with other HCSC tools' portions. The source keeps native namespace packages (CLAUDE.md).
4. **CI** gains a `package` job: it builds the artifacts, installs only PySpark and the dependency wheels (the `dre` package itself is not installed), and runs `validate`, `install --print` and `dry-run` through `spark-submit` from `dist/`.

## Configurable assumptions (2026-10-10)

Asked to make the engine work on its assumptions and make them configurable, until answers come. Each choice where the spec was open is now a setting whose default is the assumption (README, "Settings that carry an assumption"): `email.all_clear_digest` (step 9, item 2), `watchdog_owner` (step 10, item 5), `compute_budget_scope` (step 10, item 1), `scan_mb_per_minute` (step 10, item 3), `rule_weekly_day` (step 7 review, item 2) and the probe parameter `confirm_when` (step 8, item 5: `different` confirms WRONG\_PARTITION for any partition other than the cursor's). Unknown production values (table, column and path names, formats) are not given assumed values: CLAUDE.md forbids inventing them, and they stay null so the dependent checks and causes say DID\_NOT\_RUN or NOT\_READY.

## Code review (2026-10-11)

A full review of `src/` found 29 issues. The fixes below change behaviour; each has a test that fails without it. Items 6 and 7 waited on answers (end of this section); both were answered and built on 2026-10-12.

**Changes to the spec or the store** (spec sections 4 to 8 updated):

1. **The driver runs in UTC.** PySpark returns TIMESTAMP values as naive datetimes in the driver process's local time zone, not `spark.sql.session.timeZone`. `session.get_spark` now sets the process time zone to UTC, so a host whose clock is not UTC no longer shifts every time read back from Spark: window starts, `dq_file` comparisons, slot times and the watchdog's hour.
2. **Key events are per hop.** `dq_key_event` gains `hop` (`<upstream>-><dataset>`, as in `dq_cause_result`), and `v_open_keys` is per (dataset, check, hop, key). Before, a dataset with two upstreams had each hop clear the keys the other flagged. A store installed before this change needs the column and the view recreated; no store has been deployed yet.
3. **Key events are computed inside the check.** HOP_KEY_CURRENCY materialises the hop's judged keys once (a local checkpoint) and reads the summary, the key events and the cause engine's failing keys from them. All of its Spark work now runs within the compute budget, and an error in it becomes DID_NOT_RUN. Before, the events query ran later, in the runner's write path, outside both.
4. **A check counts as written only when its key events are too.** A result already stored is still explained, and a failed key-event write leaves the run PARTIAL.
5. **The full sweep runs once on `full_sweep_day`.** It is the dataset's first window ending that day (in the `defaults.yaml` time zone). Before, every hourly run that day did a full sweep.
6. **`dre retention [--apply]`** is built, as decided at step 10: it lists, or drops, each dq table's partitions older than its `retention_months`.
7. **`winner_rule.formats`** (optional, column -> Spark datetime pattern): the winner rule now orders time columns as times. The upstream's `record_time` and `load_time` columns use their own configured reading; any other time column stored as a string needs a format.
8. **Parameter sets.** A cause whose ready parameter sets are all ruled out, but which has a set that is not configured, is NOT_READY, not RULED_OUT (spec section 7: missing parameters are NOT_READY, never RULED_OUT).

**Fixes that change no stated rule:**

- **T1_VOLUME:**
  - Monthly and sparse cadences now form load periods, because the previous slot is looked up however far back it is. Before, a monthly feed was `empty_population` on every run.
  - A load judged again after a held window counts once in the 14-load history.
  - The median and the tolerance band are computed in SQL.
- **Window history.** Rule results are left out by their row (only rule results carry a severity), not by today's rule ids, so a renamed rule cannot move a window. Every dataset's history is read in one query instead of one query per dataset.
- **Choosing the T1_SCHEMA_DRIFT dataset.** The one per table is chosen after dropping feeds that are not due, so a shared table is still checked when only one of its feeds is due.
- **Run start.** A planning error after the STARTED row now ends the run FAILED instead of leaving it STARTED.
- **Hop checks.** An error in one upstream is DID_NOT_RUN for that upstream only; the other upstreams' results stand.
- **value_format** tests the stored value, not its `key_normalise` form.
- **null_rate_max** compares the rate in SQL.
- **dre trace:**
  - The cause line is the gap hop's own; it is filtered on `hop`.
  - The key hash is computed with the hop checks' own `key_string` and `dre_key_hash`.
- **Cause checks and null load times.** FILE_SKIPPED, OLDER_VERSION_WRITTEN_LATER and KEY_MISMATCH no longer crash on a null upstream load time, or on a single-column key whose only column is in `mismatch_probe_drop`.
- **Efficiency:**
  - Probes read a log glob, a value file and a parsed value once per evaluation, not once per failure.
  - T1_FILES_NOT_LOADED and HOP_FILE_COMPLETENESS count and list in one pass.
  - The digest reads its run's rows from the run's `run_date` partition only.
- **Clean-up:**
  - The duplicate `as_utc` and regex validator are removed.
  - Dead code is removed from `cli.py`: the "not built" path and the unused `EXECUTION_TYPES`.
  - Missing type hints are added.

**Not fixed in the review: they waited on an answer** (both answered and built on 2026-10-12; next section).

- **Review item 6: NO_UPSTREAM_DATA and the event window** (answered 2026-10-12, below). Spec section 7 says the cause is CONFIRMED when every upstream "loaded no rows in the event window". With hourly runs, T1_ZERO_ROWS judges a slot in the window around its deadline (slot + `sla_hours`), long after the upstream load. So in steady state the upstream count in that window is usually 0: NO_UPSTREAM_DATA is CONFIRMED and hides the later causes (R03's WRONG_PARTITION would rarely win). Proposed change: count upstream rows loaded in the failing slot's load period, `[slot, next slot)`, or from the previous slot up to this slot's deadline. That needed a spec change, so it was left as the spec said.
- **Review item 7: what `--execution-type RERUN|REPLAY` evaluates** (answered 2026-10-12, below). Today it only labels the rows. The window is still the next new span after the last NORMAL event, so a RERUN cannot reproduce an earlier `evaluation_id` (spec section 4). It also appends `dq_file` rows and key events that the next NORMAL run then repeats. A rerun needs a way to name the window it re-evaluates, for example `--event-id`, or `--window-start` and `--window-end`. It also needs a rule for what it may write: probably results and causes but no key events or `dq_file` rows. Step 4 decision 6 deferred this to step 8, and it was not done.

## Review items 6 and 7, answered (2026-10-12)

The user's answers:

- **Item 6:** "use the expected business event window rather than the current execution window. NO_UPSTREAM_DATA should only be confirmed when the upstream data is genuinely absent. Preserve stronger evidence of downstream failures."
- **Item 7:** "NORMAL should process the next eligible window, RERUN should explicitly select a historical window, and REPLAY should support a historical range. Ensure idempotent results and key-event handling."

**Item 6, as built:**

1. **Business window.** The failing load (a T1_ZERO_ROWS slot) is judged over its business window: from the previous cadence slot, the load before it, to this slot's deadline (slot + `sla_hours`). That is the span in which the load could have taken upstream data. The previous slot is found however far back it is (monthly cadences).
2. **When it confirms.** CONFIRMED only when every upstream loaded zero rows in that window. Any upstream rows rule it out, and the business window goes in the evidence. It is NOT_READY when an upstream has no `load_time`, when the failure names no slot, or when the feed has no earlier slot: absence that cannot be measured is never confirmed.
3. **Order.** WRONG_PARTITION now comes before NO_UPSTREAM_DATA in all three patterns. When both are confirmed, the direct downstream evidence is the cause. Both rows stay in `dq_cause_result`.

**Item 7, as built** (spec section 4, "Execution types"):

1. **CLI.** `dre run --execution-type RERUN --window-start T --window-end T` and `--execution-type REPLAY --from T --to T`, in ISO 8601 (UTC without an offset). NORMAL takes neither. The window or range must be historical (ending by now) and start before it ends; otherwise exit 3.
2. **Which windows.** RERUN truncates the bounds the way NORMAL does, so naming a NORMAL window reproduces its `event_id` and `evaluation_id`s. REPLAY replays the recorded NORMAL windows in the range exactly, a held-then-extended window once, and evaluates the spans between them as windows of their own. A feed is evaluated in a window only if something was due in it, unless `--feed` names it.
3. **Idempotent results.** Before writing, a RERUN or REPLAY reads the latest stored result of each evaluation. An identical outcome is not written again, but still counts as written. A changed outcome is appended under the same `evaluation_id`. `v_latest_result` now orders by `window_end` before `evaluated_at`, so a rerun of the current window replaces it but a rerun of an older window never becomes the current state.
4. **Key events.** A RERUN or REPLAY neither reads `v_open_keys` nor writes `dq_key_event`. The open-key state is today's, not the historical window's, so a rerun judges the keys its window owns (at their deadline, or reloaded in it). Repeating a rerun therefore cannot flag, clear or re-flag a key. The judged keys are still kept for the cause engine, so failing keys are explained.
5. **What else they leave alone.** No `dq_file` registration, no WINDOW_GAP rows, no window moved or held. No gold rules or T1_SCHEMA_DRIFT, which read the table as it is now. No email: the digest reports scheduled runs, and RERUN and REPLAY print their results.
6. **The watchdog.** `dq_run` gains `execution_type`, and the watchdog counts only NORMAL runs (or rows without the column), so a manual rerun never stands in for a missed scheduled run.

## Code review comments (October 2026 document)

A second review, in an uploaded document, raised four findings and six architecture concerns. Each finding was first reproduced with a test that failed on the code as it was, then fixed.

1. **[HIGH] Latest-time ties gave false value mismatches** (reproduced).
   - The cause: HOP_VALUE_AGREEMENT compared upstream and downstream rows pairwise, so two identical tied rows on each side mismatched one another.
   - The fix: the comparison grain is now the key's latest version on each side, as the set of distinct owned-column values among the rows tied at that side's latest record time (as strings; null differs from empty). Identical multi-row versions pass; one changed row fails.
   - R06 still fails, as it should: gold kept one of two tied upstream versions.
2. **[HIGH] Null upstream record times hid keys** (reproduced: such a key fell out of the population and the check PASSED).
   - Such keys are now kept, with a new key state, UNASSESSABLE, which also applies when the downstream side has no record time.
   - UNASSESSABLE is a violation, written as a key event (`state_detail` UNASSESSABLE), and never given a cause: there is no evidence to explain it with. The observed text counts these keys.
   - Before, a missing downstream record time was called STALE, which claimed more than the evidence shows.
3. **[HIGH] A result and its key events were not committed together.** The store is Hive/ORC, append-only, with no transactions across tables. Recovery therefore uses deterministic ids, newest-row-wins views and holding windows:
   - **Explicit marker.** A result stored without its key events gets a newer row under the same `evaluation_id`: DID_NOT_RUN / PLATFORM / `evidence_incomplete` (a new reason code). The run ends PARTIAL.
   - **Held windows.** A NORMAL event evaluated by a run that did not finish COMPLETED is held: evaluated again next run, for up to `max_window_hours`. This covers a run that died before even the marker could be written. A result with no run row is not held.
   - **Tested:** inject the failure after the result write, then retry. The retry gives exactly one current result, on the same held window, with complete key events.
   - **Bug found by this test:** a dataset's first window, when held, was retried from the initial lookback before the new end, so its start slid forward and that slice was never checked. `release_hold` now starts a held span at its first event's start when there is no good event yet.
4. **[MEDIUM] Failed cause investigations were invisible.** They are now tracked apart from the checks.
   - **Recorded:** `dq_run` gains `investigations_expected` and `investigations_done`. The run status stays about checks, so a failed investigation never relabels a check.
   - **Reported:** the watchdog alerts on an incomplete investigation, and the digest shows `cause: investigation incomplete (the next run retries it)`.
   - **Retried:** each NORMAL run retries FAILED results from the last `investigation_retry_days` (new setting, default 7) that have no cause rows and are still their check's latest result. Both scans are bounded by `run_date`.
   - **Never silent:** a FAILED result with nothing to investigate (only UNASSESSABLE keys) gets NOT_READY rows, so it is never pending.
   - **Tested:** inject a cause-store failure, then retry.

Architecture concerns:

- **Evidence quality.** Every cause line now names its level: confirmed; suspected (every cause check ruled out, so the fallback stands by elimination); or insufficient evidence (a cause check was NOT_READY or ERROR).
- **Allowed exclusions.** `hop_exclusions` (dataset; per upstream, a SQL condition on the upstream's rows) is the contract for legitimate business-rule exclusions: hop checks do not expect those rows.
- **End-to-end scenario.** A synthetic test covers two upstreams, an allowed exclusion, a late upstream row and a missing gold key.
- **Replay semantics.** RERUN and REPLAY are best-effort re-evaluations against today's data, not snapshots; the run prints this.
- **The rest** is answered in `docs/architecture.md`: zero-touch isolation, multi-source lineage and operational safeguards. That page also lists what is still open: join and aggregate contracts, snapshot reads, and benchmarking the unbounded store reads.

## Architecture principles document (October 2026)

The user asked for a critical review of an uploaded principles document ("don't blindly follow them"). The review checked each point against the code. Taken up, then built after the user said yes:

1. **Counts of what a hop result left out.** HOP_KEY_CURRENCY and HOP_FILE_COMPLETENESS report in `observed` the keys or rows excluded by `hop_exclusions` and the keys or files pending (within `sla_hours`), so a raw-to-gold difference is accounted for. A DID_NOT_RUN with nothing judged says the same in `detail`. The count CTEs are added only to the queries that use them: Spark analyses even unused CTEs, and with a missing upstream table their error (`column_missing`) masked the real one (`table_missing`).
2. **A three-layer acceptance pipeline** (`tests/replay/test_acceptance_three_layers.py`, through `dre run` and `dre trace`): loss between raw and curated, loss between curated and gold, an excluded test member and a late key. Each loss is flagged at its earliest hop only.

Already met, no change: zero pipeline changes and read-only access (CLAUDE.md hard constraints 1 to 3, guard tests); verification independent of job status; allowed lateness (`sla_hours`, `check_delay_minutes`, `settle_minutes`); evidence levels; check status separate from investigation status; recoverable persistence; replay limits; bounded scans and retention.

Not taken, with reasons:

- **Six dispositions as result states.** They map onto what exists (CURRENT, excluded, pending, MISSING or STALE with an unconfirmed cause, UNASSESSABLE), now with counts in `observed`. New result states would break hard constraint 4 (exactly PASSED, FAILED or DID_NOT_RUN). The mapping is in `docs/architecture.md`.
- **An adapter framework for DB2, Kafka and cloud stores.** Kafka is out of release 1, and every read goes through Spark SQL or the Hadoop FileSystem API, which already is the abstraction. Building adapters before a source needs one is building ahead.
- **Approval history, effective dates and validation status inside contracts.** Each run records the config's git commit, rules carry proposed, approved or retired, and git review is the approval history. Duplicating it in YAML adds fields nothing reads.
- **Sampling.** Sampling would break the guarantee that every missing record is tied to a hop. Scans are bounded by windows and compute budgets instead.
- **"Evidence confidence requirements" per contract.** The evidence level is reported; nothing would consume a required level.
- **Table snapshots by the observers.** The store keeps judgements and key hashes, not copies of production data; the replay limit is documented instead.
- **A control plane with access controls.** There is no service or UI; access is the `dq` database's grants, owned by the platform team.

Deferred:

- **Join and aggregate hops** ("transformed/aggregated"): a new contract kind (group-by and expected measure), designed before it is built; release 2.
- **Scheduler history as cause evidence:** waits on the platform team saying where it can be read.
- **Criterion 7** (an outage and heavy load don't affect sources): by design, but it can only be demonstrated in a representative cluster.

## Derived datasets (release 2, 2026-10-12)

Planned in a design discussion, agreed by the user (structured contract, upstream only, a synthetic example until real keys arrive, and spec section 1 amended for release 2), then built. The reasoning and the choices are in `docs/design/derived-datasets.md`; the contract is in spec section 3. What changed:

1. **Config.** A dataset is either a `table` or `derived` (from, joins, where, group_by, columns). A derived dataset's `record_time` and `load_time` are the engine's `record_ts` and `load_ts` (UTC); it sets no filter, files, partitions, upstream or key_map; no feed lists it; `dre validate` checks all of this and warns when nothing uses it.
2. **Rendering.** `checks/derived.py` renders the relation from `checks/sql/derived_relation.sql.j2`. Hop parameters, `dre trace`, NO_UPSTREAM_DATA and `dre dry-run` read a derived dataset through it. The checks get the derived dataset's base datasets through `CheckContext.upstreams`.
3. **Planning.** Derived datasets are never planned: they are read as upstreams only.
4. **Cause.** JOIN_UNMATCHED (builtin) is order 3 in every pattern's key_missing_or_stale: NOT_READY unless the upstream is derived with joins, CONFIRMED when a joined dataset had no match for the MISSING key's group. The existing tests that encode the cause order were updated for the new entry.

## Extensibility, step 1: self-registering capabilities (2026-10-12)

The user asked for the framework to be made extendable and agreed to the eight-step plan in `docs/design/extensibility.md`, in order. Step 1, as built:

1. **One registry class** (`registry.py`). Each kind of capability has a `Registry`: register, look up, reject a duplicate id, and find the built-ins by importing the engine's own packages on first use. `temporary` registers for one test.
2. **Checks** register themselves at the end of their module (`register_check(OnTime())`); the import list in `checks/registry.py` is gone. `PATTERNS` is read from the packaged `patterns/*.yaml` files, not typed by hand.
3. **Probes** are defined once: the parameter model and the function together in `causes/probes.py`, with `@probe(name, model)`. The two dicts (`PROBE_PARAMS`, `PROBES`) are gone.
4. **Builtin causes** are `@builtin(code)` on the function in `causes/landing.py` or `causes/merge.py`; the `BUILTINS` dict in the engine is gone. A pattern naming a builtin code that is not registered is rejected when the pattern loads (before, it would have been an ERROR row at run time).
5. **Rule templates** are `TemplateSpec` records (`checks/rules/template.py`), one module per template under `checks/rules/templates/`, each with its params model. `RuleCheck` is one generic path: the two `if` chains (`evaluate`, `required_columns`) are gone. A template declares the other datasets it reads (`related`, e.g. `parent_dataset`), so `dre validate` and the planner handle any such template, not only `child_within_parent`.
6. **Key normalisers** are a registry too, with four generic ones added because sources will need them without code: `trim`, `upper`, `lower` and `{regex_replace: {pattern, replacement}}`. Values are escaped through `sql_string`.
7. **Config validation.** `Pattern` and `Template` were `Literal`s; they are now validated against the registries, with the same "is not allowed; use one of [...]" error, file and line. `key_normalise` errors come from the registry and name the known forms.

Not changed: evaluation ids (check ids and cause codes are the same strings), the guard rules, the store, and any test's expectations other than three that encoded the closed sets (`trim` as an invalid normaliser, `A` as a builtin cause code, the parent passed as a positional argument).

Deferred to step 2: `causes/failures.py` still lists the failing things of a FAILED result by `check_id`, so a new check gets causes only through that list. Step 2 (check parameters) gives a check a `failures` hook along with its params model.

## Extensibility, step 2: check parameters from configuration (2026-10-12)

1. **`check_params`.** A feed and a dataset may carry `check_params: {CHECK_ID: {...}}`; a pattern may give defaults (`{check: ID, params: {...}}` in its `checks:` list). Resolution: pattern, then feed, then dataset, field by field. A check with a params model (`Check.params_model`) runs only where something configures it, so listing T1_FRESHNESS and T1_COLUMN_RANGE in every pattern changes nothing for existing configurations.
2. **Validation.** `dre validate`: unknown check id, parameters on a check that takes none, bad values (file and line of where they are written), a field missing at every level (reported on the dataset), a check the pattern does not list (a warning). At run time the merged set is validated again; a bad set is DID_NOT_RUN / invalid_config, with the field names and never the values in `detail`.
3. **The failures hook.** `Check.failures(cx, result)` lets a check list its own failing things; `causes/failures.py` keeps the packaged lists. This closes the gap left in step 1.
4. **Two generic checks**, both grouped by the dataset's `group_by` and filtered by `feed_filter`:
   - **T1_FRESHNESS** (`max_age_hours`, `time: record | load`): the latest time against the event's window end, not the clock, so a RERUN answers for its window. No usable time in any row is FAILED, never PASSED (the evidence principle from the code-review document).
   - **T1_COLUMN_RANGE** (`column`, numeric `min`/`max`, `allowed` strings, `nulls_fail`): rows loaded in the window, so it needs `load_time` like T1_KEY_NULLS. Nulls are left out unless `nulls_fail`.
   - Neither has cause checks: like T1_VOLUME and T1_KEY_NULLS, a failure has no failing thing to probe; the retry of investigations skips checks without a failure type, so these never show as pending.
5. **Sample config.** `gold_member_coverage` configures T1_FRESHNESS on load time (72 hours), so the e2e run has a parametrised check in it: 42 results. Four tests count the sample's results (R09, R11a, R11b, `test_run_writes_results_and_windows_follow_on`); each count went up by one, with its derivation comment updated. The new result fits each scenario (an empty table gives DID_NOT_RUN / empty_population).

## Extensibility, step 3: patterns from configuration (2026-10-12)

1. **`conf/patterns/*.yaml`.** The config loader points the pattern registry at the conf directory; a file there adds a pattern or, with a packaged name, replaces it. Nothing else changes: `pattern_check_ids`, `pattern_causes` and `probe_keys` read whichever file applies.
2. **`kind: file | table`** on every pattern, packaged ones included. "File pattern" was inferred from the name (`FILE_*`) in three places (the landing rule on `Feed`, `dre validate`'s raw-dataset warning, and T1_FILES_NOT_LOADED's `applies_to`); all three now read the kind, so a conf pattern can be a file pattern under any name.
3. **Validation.** `dre validate` reads every conf pattern (kind, checks, causes) and reports a problem as a config error naming the pattern file. A feed on an unknown pattern gets the known names, conf ones included, in its fix. A feed's `probes:` keys are still checked against its pattern's causes, so a feed moved to a pattern that uses none of them must drop them (the tests do).
4. **One new pattern rule:** a failure type needs at least one cause check before its fallback. A fallback-only list would write no cause rows, and the investigation retry would then treat every such FAILED result as unfinished, every run.
5. **`dre validate` prints** which patterns are packaged and which come from `conf/patterns`.

## Extensibility, step 4: rule templates from configuration (2026-10-12)

1. **`conf/templates/*.yaml`** becomes a `TemplateSpec` (`checks/rules/conf_template.py`) and is registered before the rules load, so `rule.template` validates against it. The params model is built from the declared types (`pydantic.create_model`); `column` and `columns` params are the rule's required columns.
2. **Sandbox and helpers.** The SQL renders in Jinja's `SandboxedEnvironment` with `StrictUndefined` and sees only `table`, `feed_filter`, `group_by`, `params`, `column`, `time`, `key_expr`, `literal` and `number`. Nothing from the engine's Python is reachable from a template.
3. **Guard rule 10** (spec section 9): the rendered statement always runs as `SELECT * FROM (...) dre_rule`, so a command cannot parse; `dre validate` additionally rejects INSERT, UPDATE, DELETE, MERGE, DROP, ALTER, TRUNCATE, CREATE, LOAD, GRANT, REVOKE, MSCK, REFRESH, CACHE, UNCACHE, ANALYZE and ADD JAR/FILE as whole words in the text. `replace(...)` and column names such as `delete_flag` are fine.
4. **No overriding packaged templates.** A conf template with a packaged name is a config error. Unlike patterns, a packaged template is Python logic that existing rules depend on; replacing it from YAML could change their meaning silently.
5. **`dre dry-run`** renders each rule on a conf template against its table (`LIMIT 0`), so a column or expression error shows before a run.
6. **No sample template.** A template without a rule shows nothing, and a sample rule would change the result counts again; the spec example and the tests carry it.

## Extensibility, step 5: named calendars (2026-10-12)

1. **`conf/calendars/*.yaml`** (`weekdays_only`, `holidays`) registers a calendar before the feeds load; `cadence.calendar` validates against the calendar registry (`checks/calendars.py`), which also holds EVERYDAY and WEEKDAYS. A packaged name is refused.
2. **What a calendar is.** Only "which days the feed runs": weekends off or not, plus listed holidays. Nothing else was asked for; business-day counting, half days or regional sets can be added to the file's model when a real feed needs them.
3. **Runtime guard kept.** A cadence with an unknown calendar (only possible by bypassing config validation) still raises `UnsupportedCalendar`, which the checks turn into DID_NOT_RUN / invalid_config as before.
4. **Two tests changed:** `test_named_calendar_is_rejected_by_config_and_guarded_here` and the `named-calendar-file` validate case encoded "named calendars are not supported"; both now check that an *unknown* calendar is rejected, which is the rule that remains.
5. **A loader bug found on the way:** PyYAML builds date literals itself, so `2026-13-01` raised a bare `ValueError` out of `dre validate` instead of a config error. `read_yaml` now reports it with the file.

## Extensibility, step 6: notification sinks (2026-10-12)

1. **The seam.** `notify/sinks.py`: a `Message` (owner, subject, body, recipients), a `Sink` with `send(messages) -> status lines`, and a registry of sink kinds, each with its config model. `deliver` sends through every configured sink and turns an exception into a status line. The runner's digests and the watchdog's alert both go through it.
2. **Configuration.** `notify:` lists sinks in order; each entry is validated by its kind's model, with the kind's name in the error. With no `notify:`, the `email` block is one email sink, so existing configuration behaves as before (the email sink's status lines are unchanged).
3. **Two sinks, not three.** The plan had a `file` sink. Dropped: the guard rules forbid plain file `.write(...)` outside `store/writer.py`, the digest already goes to the run's stdout (which the scheduler captures), and a guard exception for a sink of little use is the wrong trade. `stdout` makes today's "built but not sent" path an explicit sink. A webhook sink is a module away when someone asks.
4. **Hard rule 6 for every sink:** the guard that nothing in `notify/` references the key column covers the sinks module and any sink added there.

## Extensibility, step 7: extension packages (2026-10-12)

1. **Entry points, allowlisted.** An extension declares one entry point in the group `dre.extensions`; it loads only when `defaults.yaml` lists its name under `extensions:`. The loader loads extensions right after `defaults.yaml`, before anything validates against the registries. A missing or failing extension, or a colliding id, is a config error on the `extensions` line, and the engine runs with what loaded. `dre validate` prints what each extension registered.
2. **`dre guard <path>`.** The scanner moved from `tests/guard/scan.py` into the package (`hcsc.datalake.dre.guard`); the test module re-exports it. Two exemptions, both tested: the scan skips the scanner module (its text is the patterns), and the "never references local_setup" guard allows it (it names the file as an allowance, not an import). Scanned from `src/`, the engine is clean under its own scanner.
3. **The testing kit** (`hcsc.datalake.dre.testing`): `check_contract` (FAIL, PASS, DID_NOT_RUN / empty_population, well-formed results), `probe_contract` (NOT_READY unconfigured, one of the four states, no key values in evidence) and `template_contract` (a query returning population and violations). No table builders: the engine's source may not create or drop tables, so an extension copies `tests/fixtures/layers.py`.
4. **Tested without pip.** `tests/test_extensions.py` installs the example package (`tests/extensions/dre_ext_example`, one capability of each kind) through a `.dist-info` with an `entry_points.txt` on `sys.path`, which is what pip would produce. `extensions.unload` exists for tests only, so the example's ids never leak into other tests.

## Extensibility, step 8: documentation (2026-10-12)

`docs/extending.md`: one section per extension point, by configuration first (feeds, check parameters, patterns, templates, calendars, sinks) then by code (the registries, the contracts, extension packages), and what is not extensible on purpose. The README links it. Each step's spec amendments are in sections 2, 3, 7, 8 and 9.

## Code review of release 3 (2026-10-13)

Five reviews in parallel (functional; constraints, leakage and performance; SQL from configuration; design; tests and docs), consolidated in `docs/code-review-2026-10-13.md`. The user asked for everything fixable to be fixed without breaking existing functionality. The eleven defects (A1 to A11) and the two lower items were fixed in four commits, each with the test the review named; the design pass (section B of the review) follows as its own commits. Decisions taken while fixing:

1. **The SQL scan is defence in depth, in every fragment.** The wrap makes commands unparseable; `TRANSFORM` and `java_method`/`reflect` are refused by word because the wrap cannot stop them. `USING` is not refused (it is a join clause); the scan blanks string literals and comments first. Configuration stays a trusted, reviewed artefact; the scan catches mistakes and makes the guarantee honest, it is not a sandbox.
2. **`dre guard` grants nothing.** The engine's own path allowances apply only to the engine's own scan from `src/`; any other tree is held to every rule, whatever its files are called.
3. **The conf's `extensions:` list is the whole truth.** Extensions it does not list are unloaded on `load()`, so a long-lived process never validates against capabilities the current conf did not enable. Extensions load before `defaults.yaml` validates, so a sink an extension registers can be named under `notify:`.
4. **`check_params` are validated for every check that will be planned**, whichever level configures it; a field missing at every level is reported on each dataset, naming the feed.
5. **T1_FRESHNESS reads only rows timed before the window end.** The earlier docstring claimed a RERUN answered for its window; it did not. Now it does, and a window before every row is DID_NOT_RUN / empty_population.
6. **T1_COLUMN_RANGE: `allowed` or bounds, never both**, and bounds compare `CAST(column AS DOUBLE)`, so a value that is not a number is a violation. The spec said "or"; the SQL did "and" when both were given; refusing the combination is clearer than defining it.
7. **The stdout sink prints counts.** The run's stdout is the scheduler's log; hard rule 6 reserves logs for counts, check ids and table names. `full_body: true` is the operator's explicit choice.
8. **Not changed on purpose:** a parametrised check configured where its pattern does not list it stays a warning (the spec says so); the watchdog still runs the full `load()` (a lighter `load_defaults` is a design-pass item, B10).

### Design pass (section B of the review)

Done as five commits, no behaviour change, each followed by the full suite and the e2e run before the push: one conf lifecycle in `Registry.replace_conf` and one loader path for templates and calendars (B1); `checks/patterns.py` split out of the check registry, with parameter resolution beside it (B2); function-level imports hoisted except the deliberate ones in `config/models.py` and `cli.py` (B3); `cross_check`, `run()` and the loader's object loop split into named steps (B4); the `params`/`kind` renames (B5); dead code and stale docstrings removed (B6); `runner.load_config` public for `trace.py`, and tests use `Registry.unregister` instead of the item dict (B7). Decisions:

1. **Hoisted imports change what tests patch.** A test that stubs a store write patches the name on the module that calls it (`runner.append_cause_results`), not on the store module; the recovery test was the one that still patched the origin.
2. **Not done, on purpose:** B8 (replacing `Any` where the type is at hand) and B10 (a `load_defaults` for the watchdog) are left for a later pass; neither affects behaviour, and B10 changes what the watchdog imports, which the R12 dependency test pins.
3. **The R12 watchdog test starts its stuck run on the hour.** It started it one minute in, so in the first minute after each half hour the run was inside the grace and the test failed (CI run #40). On the hour, the judged hour is always at least the grace old.

A test-ordering quirk found on the way: pytest 9 matches conftest fixtures by collector object, so giving the same package directory as two separate command-line arguments with other arguments between them can lose a directory's fixtures. The full suite runs as one tree, so CI is unaffected; targeted runs are grouped by directory.

## Independent verification, batch 1 (2026-10-14)

The verification brief's review (report kept outside the repository, at the user's request that the review itself change nothing) found seven items worth fixing before any production run. Decisions taken while fixing them:

1. **The rendered template SQL is scanned too (N3).** Spec section 9 rule 10 promised the word scan "in every SQL fragment and template from configuration"; the scan ran on the template's text and on `sql` defaults, so a `string` parameter rendered raw escaped it. `conf_template.spec_from` now scans the rendered text and the rule is DID_NOT_RUN / invalid_config, naming the word and never the value. The wrap still makes a non-query a parse error (DID_NOT_RUN / query_failed), tested with a statement the word scan does not know.
2. **Group values stay out of stdout (N1).** `describe` printed `group_by` values (`[src_sys_nm=SRC_A]`); the run's stdout is the scheduler's log and hard rule 6 reserves it for counts, check ids and table names. A grouped result is now named by its column and its place in the evaluation (`[src_sys_nm #1]`); the hop's `upstream` group is a dataset id and is still printed. Values stay in the dq store and in the email.
3. **One NORMAL run at a time (N5).** `dre run` exits 3 when another NORMAL run's latest `dq_run` row is STARTED and younger than `watchdog_grace_minutes`; an older STARTED run is dead (the watchdog reports it) and does not block. RERUN and REPLAY never block: they write no key events or files. There is no lock: the check reads `v_latest_run` before the STARTED row is appended, so two runs started in the same second could still both proceed; that window is seconds, against the hour-long overlap the check prevents.
4. **A whole evaluation is marked incomplete together (F2).** A failed key-event write marked only the results that carried key events, with a newer `evaluated_at`; `v_latest_result` ranks a (dataset, check) by `evaluated_at`, so the evaluation's other groups vanished from the view until the next run (reproduced). Every row of an evaluation that carried key events is now marked, with one shared time. An evaluation with no key events at all is still not marked.
5. **A `table_contains` probe looks for the failing refs only (P5).** The cause engine records the evaluation's listed refs (at most 100) in the cause context and the probe's SQL filters on them, so the rows collected are bounded by the failures, not by the configured table.
6. **Key-hash samples are bounded in SQL and read only when wanted (P2).** At most `SAMPLE_KEYS` per evaluation (`ROW_NUMBER`), and the query runs only when some dataset sets `email_sample_keys`.
7. **The hourly store reads look back `history_lookback_days` (P14).** Default 400 days (the file registry's 13 months, rounded up), a recorded default rather than a spec value: the window history, the rule schedule and the digest's earlier results read `run_date >= today - lookback`, so the hourly scans of `dq_check_result` stop growing with retention. A dataset whose last NORMAL event is older windows as on a first run (the first-run rule). The value must cover `max_window_hours` and `investigation_retry_days`, which the model checks. The rule schedule also reads rule rows only (`severity IS NOT NULL`), as its result was only ever used for rules.

## Independent verification, batch 2 (2026-10-14)

The scale items of the verification report (P4, P6, P8, P1), with P3 (candidate keys before the hop's window functions, one judged view shared by the three hop checks) left for its own batch. Decisions:

1. **Partition pruning is a second predicate, never a substitute (P4).** `partition_column` plus a new `partition_format` (a Spark date pattern of `yyyy`, `MM`, `dd`, `HH` and separators, at least to the day) render `partition_column IN (...)` beside the parsed load-time predicate on every windowed scan: every partition value the span could touch in the load time's zone, widened by one granule each side, as an IN list rather than BETWEEN so any pattern works, not only sortable ones. The parsed predicate stays, so a partition value that does not match its rows' load times costs speed, never correctness. More than 1000 values, a pattern the engine cannot write, or a column without a format: no predicate (`dre validate` warns on the last). Both fields stay `null` in the sample until the platform confirms them (spec section 11, the step-6 decision).
2. **Cause investigations run under the budget (P6).** Each FAILED result's investigation runs in its own budgeted job group with the dataset's remaining budget; past it, it is cancelled and counted as not done, the outcome the engine already tolerates and retries. A spent budget skips the investigation with the same outcome. The key-event write has a fixed 15-minute bound of its own (`runner.WRITE_BUDGET_MINUTES`), apart from the compute budget, which is about reading production tables: a tiny compute budget must not fail the write. The scan estimate for a hop check sums its upstream tables.
3. **A hop's materialisations are released (P8).** The checkpointed judgement and events frames ride on the result (`CheckResult.checkpoints`) and the runner unpersists them when it drops the views; the key-event write releases its own checkpoint. Every hop's view is dropped by name from the `key_map`, so a hop cancelled by the budget after creating its view (its result carries none) is released too.
5. **Candidate keys before the window functions (P3, first half).** On a normal day the hop CTEs pre-select the keys the window can judge or count (loaded upstream since the judged range began, loaded here in the window, or open, by `key_value` from the bounded open-key read) and run the per-key window functions and the key-hash UDF over those only; the `judged` conditions are unchanged, so the answer is the same and the tests prove it, including the regressed-here and pending cases. The full sweep keeps every key. The candidate join lists its columns rather than `u.*`, and the hop checks the upstream table exists up front: with a table gone, Spark reports an invalid star before the missing table, which would have turned `table_missing` into `query_failed`. The second half of P3 (one judged view shared by the three hop checks and their causes) is not done: the judged view carries no owned columns, so sharing it would mean materialising `up_top` and `dn_top` too, which costs more than the three reads it saves now that the window functions run over candidates only.
4. **The open-key read is bounded by the history lookback (P1).** The hop reads open keys from `dq_key_event` within `history_lookback_days` rather than through `v_open_keys` over the whole table; the view stays for operators. The bound is sound because an open key is re-emitted on every evaluation (STILL_FLAGGED), so its latest event is never older than the hop's last evaluation, and 400 days covers any gap between evaluations. The result's `observed` counts judged keys that were open from earlier runs. Whether STILL_FLAGGED should be written per evaluation at all is a spec question (verification report, section 9) and is not changed here.

## Independent verification, batch 3a (2026-10-14)

The smaller items of the verification report that need no spec answer. Decisions:

1. **The script transform is refused by its `USING` (N4).** `TRANSFORM` alone is also Spark's higher-order array function, which a configured expression may legitimately use, and `MAP ... USING` and `REDUCE ... USING` are script forms the word list never caught. A `USING` not followed by a parenthesised column list is the script form (or a statement refused anyway), so that is what the scan refuses; `reflect2` joins `reflect`.
2. **A failing retry of pending investigations does not fail the run (N6).** By then every check is written; the retry is reported and tried again next run.
3. **Logs and the run's framing lines (N8).** `logging.basicConfig` once per process (stderr, time, level) unless the host configured logging; a `dre run: started <run_id>` line first, so a run killed midway leaves its id in the log; a `dre run: FAILED ... <exception type>` summary; the final row's write is guarded, so a store failure there leaves the run STARTED for the watchdog rather than a traceback.
4. **`dre install` prints the exception type and class only (N9).** Like every other path.
5. **Historical outcomes are read by dataset and span (H2).** One predicate however many events; the event ids are matched in Python.
6. **`dre trace` reads the latest evaluation's cause rows only (H4).** Ranked in SQL.
7. **The digest groups cause rows once (P18).**
8. **`dre validate` warns about a winner-rule term ordered as stored (F8)** and the sample gives `enddate` its date pattern; and once, on `defaults.timezone`, about a zone that observes daylight saving time (F3), since no configuration can resolve the repeated hour.
9. **A file that grew between layers is counted and said, not passed silently (F5).** The row-count assumption of HOP_FILE_COMPLETENESS is now in the spec.
10. **A key whose every column is null is not judged at a hop (P17).** It is not a version of anything; T1_KEY_NULLS counts it. Such keys also collapsed into one hot partition in the window functions. A partly null key is still a key.
11. **The slot and volume scans read only the span their slots cover (P11, first half).** Before the range join, so the nested loop runs over the window's rows. ON_TIME and ZERO_ROWS still scan separately: sharing a scan across two checks needs state across evaluations the runner does not have; with the pre-filter and partition pruning each scan is small.
12. **The decisions entry on event windows (D1)** no longer says CONFIGURATION holds a window.

## Independent verification, batch 3b (2026-10-14)

1. **A retried investigation judges the hop's open keys (F6).** The failing evaluation judged them, so the retry must list them too; `CheckContext.judge_open_keys` says so without writing key events. Tested end to end: a key judged only because it was open, its investigation unstorable, then retried with a real cause.
2. **The landing causes read from the evaluation's earliest failure (H1).** Every landing cause looks at files first seen with or after its own failure, so the read is bounded to files first seen since the earliest missed slot or the earliest first-seen time of the named files (one bounded read of `dq_file`), by `run_date` and `first_seen_at`, and the raw dataset is counted for those files only. A named file the registry does not know leaves the read unbounded.
3. **`landing.max_files` (P7b).** A listing that passes it is DID_NOT_RUN / landing_unreadable naming the limit; the alternative, skipping files older than the registry retention, is the spec question on first-seen (verification report, section 9) and is not done.
4. **The log probe collects only lines naming a failing file, at most 500 (P12).** The cause engine records the evaluation's failing files (base names) beside its refs; a probe used on its own, without that list, reads as before.
5. **Older pending investigations are retried once a day (P15).** Today's every run (a store down for an hour), older ones in the hour from `rule_run_at`, so a failure that repeats is not retried hourly for seven days.
6. **The watchdog's own failure is exit 3 (N7).** A store that cannot be read stays an alert (exit 1); configuration it cannot read, or an error before the store read, is exit 3, so a scheduler keyed on exit codes tells them apart. Its `dq_run` read is bounded by `run_date`.
7. **`dre trace` takes the key from a file or stdin and carries it as data (N2).** `--key-file FILE` (first line; `-` for stdin) keeps the key off the command line; `--key` stays for interactive use with a warning in its help. The key parts go into a one-row temp view (a local relation, whose rows no plan prints) that the layer queries cross-join, so no SQL text, plan or history-server page carries them; the normalisers apply to the view's columns in SQL as before.

Not done from the report's batch-3 list: P16 (a derived upstream materialised once per hop; P3's candidate keys took most of its cost), the second half of P11 (one scan for ON_TIME and ZERO_ROWS), and everything waiting on a spec answer (F1, F4, F7, F9, P9, P13, the STILL_FLAGGED question).

## Curated load time column (2026-10-15)

Developer question 1 answered: the curated table's load time is `cdc_upd_ts`. The other two timestamp columns (`sourcelastupdatets`, the source's last update, already curated's `record_time`; and `src_upd_ts`, the RMS record time) are record times, not load times.

1. **Sample config.** `example_curated_enrollment.yaml` sets `load_time: { column: cdc_upd_ts, format: "yyyy-MM-dd HH:mm:ss:SSSSSS", granularity: minute }` (the format from the developer's example value, confirmed as a string by the user on 2026-10-15; the granularity chosen by the user, see 2). Gold's hop checks (HOP\_KEY\_CURRENCY, HOP\_VALUE\_AGREEMENT) now run on the unedited sample instead of DID\_NOT\_RUN / invalid\_config, and `dre validate` no longer warns on it.
2. **Format and granularity.** The column is a string with microseconds after a colon, like the gold load time's pattern. Granularity is `minute`, the same as gold's, decided by the user: a RERUN names one window for every dataset of the feed, and the named bounds match a dataset's NORMAL window only at that dataset's granularity, so with curated at no granularity a RERUN of gold's window wrote curated's windowed results again under a new span (the rerun idempotency test caught it). Rounding window edges to the minute loses nothing: consecutive windows stay contiguous, so a row stamped after the rounded edge falls in the next window.
3. **Fixtures.** `CURATED_COLUMNS` carries `cdc_upd_ts` as a STRING; `curated_row(loaded=...)` writes it in the confirmed format, defaulting to the record time so every existing scenario keeps its meaning. The replay, trace, R11, recovery and rerun tests stop patching `load_time` into the sample, because the sample now has it; the runner test removes it in its own copy to keep testing a repeated CONFIGURATION reason.
4. **End-to-end.** The synthetic gold rows carry the curated version's record time, so the member feed's hops are healthy like the provider feed's, and `verify.py` asserts them.
5. **Proving test.** `tests/replay/test_r04_to_r07_hops.py::test_curated_keys_are_judged_by_cdc_upd_ts_not_by_their_record_time`: a version updated 60 hours ago but written 12 hours ago is judged in this window; read by its record time it would not be.

## Evidence contract (2026-10-16)

The team is building a temporary in-pipeline audit (a control table, a rejects table, filter rules and audit log lines, written by each job). It is not part of DRE and nothing of it is built here; `docs/evidence-contract.md` fixes the shape of what it writes so that DRE's packaged probes read it by configuration alone:

1. The rejects table fills `probes.rejects` (`table_contains`, cause REJECTED), the filter rule registry fills `probes.filter_rules` (`table_contains`, cause FILTERED, naming the rule id), the audit log line fills `probes.load_log` (`log_contains`, cause LOAD_ERROR). No engine change.
2. The control table is not read by a packaged probe. Two generic capabilities are noted for later, not built: a stage-status probe (NOT_RUN, PIPELINE_STALLED from a missing or FAILED stage row) and a file record-count hop (the control row's `source_count` against the rows raw holds for the file).
3. Names in the contract are placeholders (hard constraint 7); the deployed configuration carries the real ones. The sample feed's probe comments point at the contract.
4. DRE's own run, SLA and watchdog checks stay in place whatever the audit reports: a pipeline cannot report its own non-run.

## Release 4, step 1: T1_PARTITION_DRIFT (2026-10-16)

`docs/design/outside-in-evidence.md`, step 1, agreed by the user (order and the 1 GiB file cap for step 2 on 2026-10-16; the partition-drift evidence decided the same day: file metadata, not statistics).

1. **A parametrised check, not a dataset field.** The plan said `partition_drift: { tolerance_rows }` on the dataset; it is `check_params: { T1_PARTITION_DRIFT: { tolerance_rows } }` instead, the release 3 mechanism for a check that runs only where it is configured, validated by `dre validate` like T1\_FRESHNESS. `dre validate` warns when it is configured on a dataset without `partition_column` and `partition_format`.
2. **Every changed partition is counted, wherever it is.** The plan counted only rewritten partitions outside the window. A partition written inside the window and rewritten later outside it would then have been compared with a count from before its legitimate writes. So a partition whose files changed, or seen for the first time, is counted in or out of the window; only a change outside the window is a violation. On a healthy day that is the window's own partitions, a few.
3. **Detection by directory listing.** One `SHOW PARTITIONS`, one `DESCRIBE FORMATTED` for the table's location, and one Hadoop FileSystem listing per partition directory (file count, bytes, latest modification time). A partition whose directory is not under the table's location (added with its own LOCATION) is found through `DESCRIBE FORMATTED ... PARTITION`, whose first Location row is the partition's. Hive statistics are never read or written.
4. **State in `dq_partition_state`**, append-only through the writer, with `row_count` carried forward when the files did not change and `counted_at` saying when it was counted; `v_latest_partition_state` gives the latest row per dataset and partition. The rows ride on the CheckResult (`partition_state`) and the runner appends them with the run id after the result, like key events; a failed write marks the evaluation evidence\_incomplete like a failed key-event write.
5. **Once per physical table, never in RERUN or REPLAY**, like T1\_SCHEMA\_DRIFT (`runner.TABLE_CHECKS`).
6. **Sample config.** The synthetic provider gold table is partitioned by its merge date (`merge_dt`, a test-only column) and drift-checked, so the e2e run records a baseline (DID\_NOT\_RUN / insufficient\_history) and `verify.py` asserts it. The sample member-coverage gold stays unpartitioned: its real partition column is a platform value (spec section 11) and is not invented.
7. **Retention** for `dq_partition_state` is 13 months, like the file registry.

## Phase 1 ADRs approved: ADR-01, ADR-02, ADR-05 (2026-10-16)

The user approved the three decisions that resolve the assessment's critical findings (`docs/design/target-architecture.md`), as written:

1. **ADR-01 (P1-01, P1-04).**
   - Candidate, due and pending populations, with pending outside the identity.
   - Eleven dispositions in a fixed precedence, with an identity the engine asserts.
   - Tolerance as an absolute count, on `missing`, `stale` and `divergent` only.
   - A scope stored on every result.
   - No chain-level claim until CHAIN_COMPLETENESS is built and tested.
   - With it, section 3: **the result states stay at three** (PASSED, FAILED, DID_NOT_RUN). "Not assessable", "partial chain" and "unknown" are reasons or dispositions, not states, so hard constraint 4 stands unchanged.
2. **ADR-02 (P1-02).**
   - A `grain` block per dataset, and one mapping kind per edge.
   - An unsupported kind is DID_NOT_RUN / `unsupported_mapping` on every run; `fan_out` stays unsupported.
   - Transforms are one-directional.
   - Key collisions from normalisers are detected at run time.
   - Today's `key_map`, `winner_rule` and `owned_columns` become aliases.
3. **ADR-05 (P1-03).**
   - Every dataset declares its history class.
   - Three evaluation modes: NORMAL; CURRENT_RECHECK, which today's RERUN and REPLAY become (labelled in the digest and the CLI); AS_OF_REPLAY, only on append-only or snapshot datasets with no partition drift since the cutoff.
   - `execution_type` keeps its stored values, and a new `evaluation_mode` column carries the mode.

Not built yet. Exit criterion 1 is met.

**Then ADR-03 and ADR-04, approved the same day**, as written:

4. **ADR-03 (P1-06, P1-08).** Three multi-source shapes, never mixed on one edge:
   - **Union partition:** attribution must be disjoint and complete, proven by T1_SOURCE_ATTRIBUTION. This is the member pipeline's shape (the developer's answers below).
   - **Join contribution:** the primary source owns the population; contributors are checked by HOP_CONTRIBUTION for resolution and value agreement.
   - **Survivorship:** by precedence. It is not used here.

   Lineage rules:
   - Aggregate membership is reconstructed; contribution is unavailable without a member-grain table.
   - The chain reads the layers directly. A proven loss is FAILED, and an unobservable segment is `partial_chain`.
5. **ADR-04 (P1-05).**
   - **Exclusions** are approved, versioned rules with effective dates and a `max_share` volume guard.
   - **Rejects** count only with key lineage, under a `reject_policy` whose **default is defect**.
   - **Deletions** are a declared tombstone rule. For the member pipeline that is the soft void below.
   - The pipeline's own filter registry never authorises an exclusion.

The traceability matrix comes before code (exit criterion 6).

**Then ADR-06 to ADR-08, approved the same day** ("I approve all"). With them every exit criterion is met, and the Phase 1 design is frozen. A later change needs a new entry here.

6. **ADR-06.** Five evidence levels (observed, reconstructed, reported, inferred, unavailable) on every disposition count, cause line and result. A claim is capped by its weakest evidence. Reported evidence (the team's audit) counts only under policy. Blind spots are listed.
7. **ADR-07.** Evidence, then the result, then a commit row in `dq_commit`; the views show only committed evaluations. Amended on approval: **one commit row per batch of evaluations**, written after all their evidence and results, to avoid a small Hive file per evaluation. It is built first among the store changes (build step 2).
8. **ADR-08.**
   - **Run budget:** `run_budget_minutes` defaults to **40**, with on-time and completeness checks first when it runs out.
   - **Queue:** a dedicated YARN queue whose name the user will provide (left `null` until then). DRE **never runs in the cluster's high-priority queue**, which the production pipelines use. Its name is a production value, so it is kept only in deployed configuration (hard constraint 7), and `dre validate` will refuse a queue on the deployment's `protected_queues` list.
   - **Kill switch:** a file path in configuration. Its path and owner are deployment values, left `null`.
   - **Backpressure:** at 80% of the budget the next run defers the full sweep.
   - **Load test:** on a representative cluster, which needs the platform team.

## Gate A remediation: Step 0 specification approval (2026-10-22)

**Approval.** The user, as spec owner, gave the architecture review disposition "APPROVED, subject to the documented implementation verification gates" on 2026-10-22 for the package `docs/reviews/gate-a-step0/final-approval-package/` (committed `21e54f2`, CI #106 green). No individual signatory or organisational authorisation beyond that disposition is implied. This Gate A remediation (the reconciliation examination's F-02, F-05 and F-07) is distinct from the earlier build gate also called Gate A (steps 4 and 5, defaults M1 to M9).

The approved decision-log text (A-15r), as approved:

> **Gate A: key identity, untimed rows, ties (approved 2026-10-22).**
>
> Approved by the user: A-1r, A-2, A-3, A-4r, A-5r, A-6 to A-8, A-9r, A-10r, A-11 to A-14 and A-16 of `docs/reviews/gate-a-step0/final-approval-package/01-amendment-approval-matrix.md`.
>
> **Previously approved behaviour changed:**
> - the key string (spec §6, line 641), including:
>   - CHAR keys compared without pad;
>   - DECIMAL keys compared by numeric value;
> - HOP_KEY_CURRENCY's UNASSESSABLE (line 605);
> - the ledger's "no record time" (line 613);
> - the ledger's exclusion effectiveness for tied rows (per row).
>
> **Unchanged:** `winner_rule` semantics, R06, the value-set tie rule, ADR-01 to ADR-08 (ADR-02's collision rule implemented).
>
> **Defaults taken:**
> - every disorder bound `null`;
> - a refuted bound stays refuted until a new version;
> - TIMESTAMP_NTZ and BINARY key parts refused.
>
> **Deferred:** NF-1 (KI-20), outside Gate A.
>
> **Release split:**
> - N: A-3, A-9r, A-10r (reading);
> - N+1: the rest.

(The line numbers are those of `docs/spec.md` at `21e54f2`; the amended text is now in place, marked "Gate A remediation".)

Recorded with the approval:

- **Where the text went.** `docs/spec.md` sections 1, 3, 5, 6, 7, 8 and 9, and `docs/design/reconciliation-model.md` sections 4 and 8 (A-13; the model is frozen, so this entry is its record). Each approved text is marked with its release and "not built"; the text it amends is marked "as built" until that release. A-12 (value-tie wording) and A-13's two wording corrections describe the engine as built today and apply now; they change no behaviour.
- **Conditions carried to the dependent release** (each a test that must pass before that release merges):
  - release N+1: the existing STRING-argument hash test passes unchanged (A-2); the soft-delete tie test (A-6); the full-sweep target-count test (A-8); the UTC start test (A-11); the TIMESTAMP rendering test (A-1r); the refutation-persistence tests (A-4r); the end-to-end N+1 to N rollback demonstration;
  - release N: equivalence against existing stores; the migration, writer, view, reader, rollback-reading, overlapping-run and trace tests of `docs/reviews/gate-a-step0/final-approval-package/04-release-n-compatibility-matrix.md`.
- **Release boundary.** Release N is compatibility infrastructure only (migration 2, writer tolerance of migration-added columns, `key_encoding = 1`, the final key-event views, encoding-aware readers including keys written by N+1 after a rollback, trace labels). It activates no new key encoding and changes no F-02, F-05 or F-07 semantics. Release N+1 carries every behavioural change. Rollback below N, once migration 2 is applied, is unsupported.
- **TIMESTAMP_NTZ** key parts stay refused until a canonical form is separately specified and approved (the conditions are in the package's 02, Q-7).
- **KI-20** (NF-1) is recorded in `docs/known-issues.md` and deferred outside Gate A remediation; it is not in the scope of release N or N+1.
- **Deployment prerequisites, not specification blockers:** O-1 (a non-NULL disorder bound needs dataset-owner evidence and approval; the default stays NULL), O-2 (production key-column types inventoried against the supported types), O-3 (acknowledgement that every key hash changes once when N+1 activates encoding 2). None has been given.
- **Specification approval is not implementation authorisation.** Neither release N nor release N+1 is authorised; each needs its own explicit authorisation, and deployment needs separate approval. Nothing is built.

## Final engineering acceptance: KI-18 ratified, KI-19 corrected (2026-10-21)

- **KI-18, ratified by the user (2026-10-21).** In a store older than its evidence retention, where a hop's first evaluation cannot be told from coverage that retention dropped, the result is qualified `coverage_gap` until a covering full sweep establishes coverage. A store younger than the retention evaluates a hop's first window without the qualification. No change; verified by `tests/checks/test_coverage_gap.py` and `tests/notify/test_digest.py::test_a_coverage_gap_is_reported_in_the_digest_and_is_never_all_clear`.
- **KI-19, the open-key read follows the hop's continuity.** HOP\_KEY\_CURRENCY reads its open keys within `history_lookback_days` while the hop's own coverage continues from inside the lookback. Otherwise it reads them back to `dq_key_event`'s retention (`key_events_since`, the CA-02 fallback).
  - CA-02's semantics carry over:
    - every NORMAL evaluation with a secret re-emits each open key it reads (STILL\_FLAGGED, or CLEARED once CURRENT), so continuity keeps open keys' latest events inside the lookback;
    - CLEARED ends a key's open state, so a terminal outcome is recorded once;
    - only committed, non-INVALID events are read (`v_committed_key_event`).
  - The disclosure differs from the chain's. HOP\_KEY\_CURRENCY has no scope JSON, so the horizon goes into `observed` (or `detail`, when nothing was judged). It is stated only when the store holds a `dq_key_event` partition at or before the horizon, so retention may have dropped events.
  - `coverage.may_have_dropped` now serves both CA-01 and KI-19.
  - The existing test `test_hop.py::test_open_keys_are_read_from_recent_partitions_only` encoded the old rule (open keys from the lookback only, without continuity). Its setup gains a committed covering result inside the lookback, ending where the range starts; its assertions are unchanged.

## Final conditional approval: conditions CA-01 and CA-02 (2026-10-20)

The user's conditional approval named two correctness conditions. Both were reproduced as defects on the current code (`docs/design/evidence/final-conditional-approval.md`). The smallest corrections, to ratify:

- **CA-01, an unresolved coverage gap is a qualification.** A hop range that does not continue from trusted committed coverage that may have existed is qualified `coverage_gap`. That is the case when only INVALID results are retained, or when nothing is retained in a store older than `dq_check_result`'s retention (a partition at or before the retention's first day, from the metastore's partition list, read only in that branch), where retention may have dropped the hop's coverage. It uses the existing qualification mechanism (C3-03), so the PASS is published as it is, never healthy, listed in the digest and counted in its subject. No new result state, column or view. A later window continuing from such a result's coverage carries the qualification, read from the covering row (on a tie of `coverage_to`, the gapped row wins). Only a full sweep that covers the hop clears it, because a full sweep judges every record due before its range end; a sweep that did not cover (DID\_NOT\_RUN) resolves nothing. This applies to every hop check (HOP\_LEDGER, HOP\_KEY\_CURRENCY, HOP\_VALUE\_AGREEMENT, HOP\_FILE\_COMPLETENESS) and to CHAIN\_COMPLETENESS, whose scope now also states `coverage_basis`.
  - A first evaluation in a store that has lost nothing to retention is not a gap: no history is missing, and its clean PASS stays healthy. This keeps the approved proof scenarios (PS2's healthy, contract-proven PASS on a fresh store).
    - A first version qualified every first evaluation. It failed seven runner-level tests (R14, R15, R16, R19, and the store's reconciliation test), so it was narrowed, not those tests restated.
    - In a store older than the retention, a first evaluation is indistinguishable from dropped coverage and is qualified until the hop's first full sweep (fail closed; KI-18).
  - A context without a history bound (`history_since` null, check-level tests only; the runner always sets it) keeps the earlier "first evaluation" basis, unqualified.
- **CA-02, the carry read follows the chain's continuity.** Each carrying evaluation records again every record it still finds pending, so while the chain's coverage continues from inside `history_lookback_days`, its outstanding records' latest events are inside the lookback too, and the hourly read stays bounded there. When the chain's coverage did not continue from inside the lookback (it was not evaluated for longer, or it has none), the read of its PENDING events reaches back to `dq_key_event`'s retention (`CheckContext.key_events_since`, from `retention_months`). This is the same bounded-read-with-fallback as P1-02, so the read stays cheap when continuity holds.
  - A PENDING event past that retention is gone by the approved retention policy (spec section 5). The chain's scope names the horizon, and the full sweep, which reads the layers rather than the store, still judges the record.
  - EXPIRED evidence only names a key to judge again: the outcome comes from this evaluation's reading of the layers. An INVALID evaluation's PENDING events are not trusted and its coverage covers nothing, so the next evaluation judges that range again from the layers.


The user's final consolidated review asked for four corrections, each with a reproducing test; the evidence is `docs/design/evidence/final-corrections.md` and the open items `docs/known-issues.md`. Choices made, for the reviewer:

- **P1-01.** A qualified pass keeps a digest from being all clear and is counted in its subject (`, <q> passed with qualifications`); a digest that is not all clear is always sent, whatever `all_clear_digest` says. Supersedes "a qualified pass does not change the digest's subject" (checkpoint 3 interpretations).
- **P1-02.** A safe fallback rather than only a declared gap: when the lookback holds no trusted coverage, a second read, bounded by `dq_check_result`'s retention (`CheckContext.coverage_since`), finds the latest trusted coverage before the lookback, and the range continues from it (never later than an INVALID result's start inside the lookback). When nothing is retained, the range starts at the window and the ledger's scope states the gap (`coverage_basis`). Only HOP\_LEDGER's scope carries `coverage_basis`; the other hop checks have no scope JSON, but share the same range.
- **P1-03.** Records pending at a later edge are carried by key events (PENDING, then RESOLVED, LOST, UNOBSERVABLE or PENDING again), not by holding the chain's range back: holding it back would judge again, and count twice, the records the range already judged. NORMAL runs with an HMAC secret only (a recheck neither reads nor writes key state); without, the scope keeps the blind spot. The carried keys are read within `history_lookback_days`.
- **O6.** N is enforced on DRE's units of Spark work (a check, an investigation, an evidence write), each in its own job group: at most `max_concurrent_jobs` (default 1, the runner's sequential design) live at once, a cancelled unit counting until its thread stops, its job group cancelled again every 0.5 seconds while it lingers; a unit not admitted within `cancel_grace_seconds` (default 60) is DID\_NOT\_RUN / BUDGET / concurrency\_limit. Not enforced by DRE: more than one Spark job inside one action (a broadcast); the YARN queue's share bounds that. N and the grace are defaults to ratify.

## Phase 1 build step 8: run budget, kill switch, queue, backpressure (2026-10-20)

Built under the master implementation prompt of 2026-10-20, not ratified. Defaults taken, for Gate B:

- **O1, the run budget** (`run_budget_minutes`, 40) applies to NORMAL runs only ("of the hourly run", ADR-08), measured from the run's start. A job reached at or past it is written and committed as DID\_NOT\_RUN / BUDGET / run\_budget\_exceeded (the budget and the minutes elapsed in `detail`); BUDGET holds its window. The run still ends COMPLETED: every result is written, as for the per-check budget\_exceeded; PARTIAL would hold every window of the run, not only the cut ones. A check already running may overrun by up to its own compute budget.
- **O2, priority:** on-time and completeness checks first (T1\_ON\_TIME, T1\_ZERO\_ROWS, T1\_FILES\_NOT\_LOADED, HOP\_FILE\_COMPLETENESS, HOP\_LEDGER, CHAIN\_COMPLETENESS), every other check after; stable within a class.
- **O3, the kill switch** (`kill_switch_file`, null) applies to every execution type, read at the start and before each check; once seen it stays on for the run. An unreadable location counts as on (fail closed). Remaining checks are DID\_NOT\_RUN / PLATFORM / disabled, written and committed but not counted as written, so the run is PARTIAL and the watchdog alerts: a forgotten switch is never silent. The path is never printed. A halted run skips the retried investigations and the integrity sweep (the next run does them).
- **O4, the queue** is read from `spark.yarn.queue`. `dre run` refuses to start when it is a `protected_queues` entry, or when `yarn_queue` is set and the application runs in another queue or names none. Local mode without a queue is not refused. Protected queue names are never printed.
- **O5, backpressure:** the latest finished NORMAL run took more than `backpressure_share` (0.8) of the run budget, so this run defers the calendar's full sweep. A deferral is recorded in `dq_integrity` as SWEEP\_DEFERRED (a new record kind, operational, needs ratification; the alternative is a dedicated table through a migration), and the first run not under backpressure forces the owed sweep. Records unreadable: every dataset counts as owed (fail closed). Integrity's forced sweeps are never deferred.
- **O6, concurrency (N): superseded by the final review correction O6 (below).** Was: not enforced and not measured. The runner evaluates one check at a time, but one SQL action can run several Spark jobs, and a check cancelled by its budget can keep a daemon thread starting jobs; the effective cap is the YARN queue's share, a platform setting.
- **O7, measurements:** `scripts/measure_run.py` prints a run's elapsed time against the budget and per-check duration statistics, read-only, locally or on the cluster. The load test (acceptance criterion 7) still needs a representative cluster.

## Phase 1 build step 6: CHAIN_COMPLETENESS (2026-10-20)

Built under the master implementation prompt of 2026-10-20, not ratified. Defaults taken, for Gate B:

- **K1, unassessable is lost.** A record unassessable at an edge (no load time at an intermediate layer, a key that cannot be mapped forward) counts as lost: fail closed, as HOP\_LEDGER fails on unassessable records.
- **K2, later edges.** A carried key is due at a later edge when its latest version's load time + that edge's `sla_hours` is at or before the cutoff, pending otherwise. **Corrected by the final review, P1-03 (below):** a record pending at a later edge is carried to the next evaluation. Was: not judged again before the full sweep.
- **K3, unjudged edges** after the first are not read: records reaching one are unobservable. An unjudged first edge is read only for the due population.
- **K4, edges come from `key_map` only** (the frozen interface): a contributor edge declared only in `contracts` is not on a path.
- **K5, one record to several keys** at an edge is unobservable there.
- **K6, guards and orphans** stay with each edge's HOP\_LEDGER.
- **K7, coverage.** partial\_chain covers its range (`runner.covers`): otherwise a chain with a permanently unobservable edge would widen its range on every run.
- **K8, evidence shape** (differs from the frozen text): `label` is the edge where a record ended and `class` its disposition's class; key events are LOST or UNOBSERVABLE (not FLAGGED), so they never enter `v_open_keys`, which nothing would clear; `key_value` is the first edge's canonical key.
- **K9, configuration context.** `CheckContext.config` carries the loaded configuration, so the chain can read datasets and settings beyond `ctx.upstreams`. `dre validate` does not yet check the terminal or the path's uniqueness: found at run time (invalid\_config).

## Phase 1 build step 5: source attribution, contribution, survivorship (2026-10-20)

Built under the master implementation prompt of 2026-10-20, not ratified. Defaults taken, for Gate A:

- **S1, where `superseded` ranks.** Directly after `deleted` (the model's table puts it after `rejected`). Ranked there, the losing source's own exclusion and tombstone rules never judge the winner's row as a breach (`excluded_but_present`, `deleted_not_applied`); such a pair is an overlap diagnostic instead. Moving it is one tuple and one SQL line.
- **S2, "the winner delivered it".** The losing edge recomputes the winner's latest version as of the cutoff and compares it with this dataset as `delivered` does; it does not read the winner's due set (the winner's version may have been due in another window), nor the winner's own tombstone or exclusion rules. A winner that cannot be read makes the losing hop DID\_NOT\_RUN (invalid\_config or table\_missing).
- **S3, the attribution proof** reads every row of the table, with no time bound, in T1\_SOURCE\_ATTRIBUTION and again in each union hop's ledger (one scan per sibling). A proof over every row also holds for the rows loaded before an AS\_OF\_REPLAY's cutoff.
- **S4, one attribution result per table per window** in rechecks too: a RERUN or REPLAY plans datasets one by one, so duplicates per table and window are dropped (`runner.once_per_table_window`).
- **S5, HOP\_CONTRIBUTION.** A row whose reference is entirely null is `unresolved` ("no reference"): the strict reading, so an optional reference fails. Tied contributor versions: a row agrees if it matches any. Contributor rows without record or load time are ignored. The contract's tolerance is not applied. Its scope's `claim` is free text.
- **S6, `CheckContext.datasets`** carries every configured dataset, so a check can find a union's siblings; `upstreams_of` also includes contract upstreams (a contributor has no `key_map` entry).

## Phase 1 build step 4: evaluation modes and AS_OF_REPLAY (2026-10-20)

Built under the master implementation prompt of 2026-10-20 (parallel development of checkpoints 3 to 8, reviewed at Gate A and Gate B). The revised checkpoint 3 still awaits its independent review; step 4 is built against it on that authority and is not ratified. Each item is a default taken, for ratification at Gate A:

- **M1, migrations.** There was no way to bring an existing store to a new DDL: `CREATE ... IF NOT EXISTS` keeps the old columns and `dre run` then fails in the writer. `store/migrations.py` adds versioned, additive migrations (`ADD COLUMNS` only; the column goes last among the data columns, where `ddl.sql` declares it too, so a migrated table equals a new one). Applied by `dre install --apply` between creating the tables and replacing the views. Pending = a column absent: no bookkeeping table, so `retention_months` (which must list every dq table) is unchanged. The base layout is checkpoint 3's; an older store is reported by `--check`, not migrated. `dre run` refuses to start while a migration is pending.
- **M2, guard rule 11.** ALTER TABLE `<dq>.<table>` ADD COLUMNS is allowed in `store/migrations.py` only; every other ALTER stays banned, ADD COLUMNS outside dq or outside that module is flagged, and `dre guard` (no allowances) flags it in an extension. This widens the guard, so it needs the user's ratification; the alternative (drop and recreate) is barred by the master prompt.
- **M3, mode of old rows.** A row stored before `evaluation_mode` existed reads NORMAL when its `execution_type` is NORMAL and CURRENT\_RECHECK otherwise; nothing stored is rewritten.
- **M4, the AS_OF cutoff** is the window end, the ledger's cutoff (L2). Reads: upstream rows loaded before it (as before), and now, under AS\_OF\_REPLAY, downstream rows loaded before it too.
- **M5, the drift proof.** "No drift since C" (ADR-05) is read from committed NORMAL T1\_PARTITION\_DRIFT results of the datasets whose partition states name the table: a baseline recorded at or before C (PASSED, FAILED or insufficient\_history), and since C at least one PASSED, none FAILED, none INVALID. The proof reaches only the latest drift evaluation; the scope says "drift checked through" it. No staleness threshold is set (none is specified).
- **M6, refusals beyond the ADR's text.** `snapshot_partitioned` is refused (no snapshot reader; step 4 is "AS_OF_REPLAY for append-only datasets"); a dataset without `load_time` is refused; a hop that reads a `rejects` table is refused (the rejects table declares no history). Only HOP\_LEDGER is `as_of_capable`; every other check is DID\_NOT\_RUN / no\_as\_of\_evidence in an AS\_OF\_REPLAY.
- **M7, reason category.** no\_as\_of\_evidence is DATA\_UNAVAILABLE: an AS\_OF\_REPLAY never holds a window.
- **M8, idempotency.** An AS\_OF\_REPLAY result is compared only with stored AS\_OF\_REPLAY results, and a current reading only with current readings: the same `evaluation_id` can carry both, as different claims.
- **M9, labels.** The CLI prints "[current recheck]" or "[as-of replay]" after each check line of such a run, and the digest's item label carries the mode for any result that is not NORMAL (digests are sent for NORMAL runs, so this shows only where a recheck is the current result of a window).

## Checkpoint 3 reviewed: changes required, then the correction plan ratified (2026-10-19)

The user's review ("DRE Checkpoint 3 Review Observations"): **changes required; step 4 blocked; checkpoint 3 not ratified.** Ratification of L1 to L15: approved L2, L5, L7, L8, L9, L10, L14; approved with conditions L1 (test deadline and window boundaries, late arrivals and SLA changes), L4 (enforce identity uniqueness independently), L6 (cap the claim when attribution is unverified), L13 (ledger-only failures need actionable triage), L15 (insufficient evidence must not silently clear or abandon windows); L5 also to be tracked as an integrity failure, L14 to document how conflicting results read; **revise L3** (key-and-time reject matching cannot prove a version or an authorised rule), **L11** (unproven union attribution cannot be contract-proven), **L12** (unimplemented transforms cannot imply full column verification). Corrections C3-01 (reject attribution), C3-02 (union attribution), C3-03 (transformation verification), C3-04 (operational visibility), each with tests.

My answer before building, point by point: agreed, with three things for the user to decide and one disagreement. (1) C3-01: the rejects layout we had agreed carries a batch id but no rule id or version, and the upstream tables no batch column, so every reject would be unassessable until the audit writes lineage; I proposed extending the evidence contract. (2) C3-02: I disagreed with building detection of overlapping or uncovered feed filters now: proving attribution is T1_SOURCE_ATTRIBUTION, build step 5; step 3 caps the claim. (3) "No unqualified healthy PASS" changes `healthy`; I proposed persisted qualifications and asked whether partial value coverage should be one. (4) The L1 SLA-change gap also affects the release 1 hop checks.

**The user's ratification ("Correction plan approved. Step 4 remains blocked pending verification"):**

1. C3-01: extend the reject evidence contract with record-version lineage and versioned business-rule identifiers where applicable; unattributable rejects cannot establish authorised exclusions.
2. C3-02: union attribution states and claim caps in step 3; full source-attribution proof deferred to step 5.
3. C3-03: persisted result qualifications and claim-aware health; partial value coverage does not block a completeness-only claim but must be disclosed.
4. L1: SLA coverage continuity for HOP_LEDGER **and the existing hop checks**, with a shared, committed-coverage calculation.
5. L4, L5, L13, L14, L15: the proposed identity, integrity, triage, documentation and window-hold corrections.

Acceptance: no source record unassessably lost during SLA changes; every due ledger record in exactly one disposition; missing or insufficient evidence never advances coverage as successful; unsupported or unproven contracts never generate an unqualified healthy PASS; the scope distinguishes verified from unverified claims; the existing reconciliation checks consistent with the ledger's temporal model; all regression and failure-injection tests pass; decisions, test evidence and checkpoint documentation updated. Step 4 only after the revised checkpoint 3 is independently reviewed and ratified.

What was built:

1. **L1, coverage continuity** (`checks/hop/coverage.py`). `dq_check_result` gains `coverage_from` and `coverage_to`, written by the runner on covering results only (PASSED, FAILED, DID_NOT_RUN / empty_population; never on an evidence_incomplete marker). A hop's deadline range starts at the latest committed covering NORMAL result of an earlier window (`window_end` at or before this window's start), INVALID ones excluded; without one, at the hop's first evaluated window's start (INVALID ones included) minus `sla_hours`; on a first evaluation, at the window start minus `sla_hours`. Used by HOP_FILE_COMPLETENESS, HOP_KEY_CURRENCY, HOP_VALUE_AGREEMENT, HOP_LEDGER and the cause engine (through `hop_params`). Pending counts start at the range's end or start, whichever is later, so a key judged under a shorter SLA is not pending again.
2. **C3-01, reject lineage.** `rejects.lineage` is required: `version` (the refused record's version, parsed, compared in UTC with the upstream record time) or `batch` (`column` against the upstream's `upstream_column`, as text). `rule_id_column` and `rule_version_column` (both or neither). A policy entry may list `rules` (id@version). A reject explains the version under judgement only when its lineage matches; one naming another version explains nothing; one with no lineage leaves an absent record unassessable ("reject not attributable to a version"); one whose reason's policy lists rules but which names none is unassessable ("reject names no rule"); a rule not listed is a defect. The class is computed in SQL from the policy. The evidence contract's rejects table gains `record_version`, `rule_id` and `rule_version` (`docs/evidence-contract.md`), for the team's audit to write. **L3 revised accordingly.**
3. **C3-02, union attribution.** Every ledger result's scope carries `attribution`: single_source, shared_target_unattributed, feed_filter_unverified, unknown (`verified` from step 5). A union is never contract-proven before step 5. **L11 revised.**
4. **C3-03 and C3-04, qualifications and health.** `dq_check_result.qualifications` (comma-separated codes): inferred_contract, legacy_exclusion, attribution_unverified, orphans_unchecked, evidence_reported. `v_latest_result.healthy` = PASSED, VALID evidence and no qualifications. The claim is contract-proven only without qualifications. The scope's `values` lists verified (owned_columns) and unverified columns (every other column but key, times, partition and file name, read from the table's schema) and the coverage; partial coverage is disclosed, not a qualification. **L12 revised.** The digest lists qualified passes under "Passed with qualifications (not counted as healthy)" with their blind spots, prints the blind spots and triage of a failed ledger, and counts only healthy passes.
5. **L4.** Under `mapping: identity`, more than one row at the version on either side is unassessable ("not one-to-one").
6. **L5.** An unbalanced ledger is recorded in `dq_integrity` as LEDGER_UNBALANCED when stored (`runner.record_unbalanced`), an incident in the digest's integrity line and the watchdog; it makes no unit INVALID.
7. **L13.** A FAILED ledger's scope carries triage lines (generic, counts and labels only), printed in the digest.
8. **L14.** Spec section 6 has the table of how the release 1 hop checks' states read against the ledger's dispositions.
9. **L15.** `insufficient_evidence` holds the window (`events.BLOCKING_CODES`, `blocking_sql`), up to `max_window_hours` and then a WINDOW_GAP.

Interpretations, for the reviewer to confirm or correct:

- **Only HOP_LEDGER writes qualifications.** The release 1 hop checks claim key presence and value agreement, not contract-proven completeness, so their passes stay healthy as before; the contract-based claim, and its qualifications, are the ledger's.
- **Superseded by the final review, P1-01 (2026-10-20):** a qualified pass keeps the digest from being all clear and is counted in its subject. **Was:** a qualified pass does not change the digest's subject. "all clear" still means nothing failed, did not run or has unverified evidence; qualified passes are listed in the body and not counted as passing. Every ledger pass on today's synthetic configuration is qualified (an inferred contract, or a union whose attribution is unproven), so this keeps the subject meaningful until step 5.
- **The coverage read is bounded by `history_lookback_days`.** Coverage older than that is not seen; the range then falls back as above, and the weekly full sweep judges every record past its deadline.
- **An unattributable reject affects only a record absent here.** A record present here is judged by what it shows, since a reject cannot be what explains it.

Tests: `tests/checks/test_hop_coverage.py` (L1, every hop check), `tests/checks/test_hop_ledger.py` (C3-01 to C3-04, L4, the balance invariant with every disposition at once), `tests/store/test_reconciliation.py` (claim-aware health, LEDGER_UNBALANCED), `tests/checks/test_windows.py::test_insufficient_evidence_holds_the_window`, `tests/config/test_contracts.py` (lineage and rules validation), `tests/notify/test_digest.py` (qualified passes, triage, blind spots).

## Phase 1 build step 3: HOP_LEDGER and dq_reconciliation (2026-10-18)

Built under the step 3 authorisation below. Waits for the user's review at checkpoint 3 (`docs/design/checkpoints.md`); step 4 stays blocked until then. Spec: section 6 ("HOP\_LEDGER"), section 5 (`dq_reconciliation`, `dq_commit.reconciliation_rows`, `v_committed_reconciliation`), section 3 (`rejects`), section 4 (reason codes), section 8 (the scope line).

What was built:

1. **HOP\_LEDGER** (`checks/hop/ledger.py`, `checks/sql/hop_ledger.sql.j2`), in the three feed patterns after HOP\_VALUE\_AGREEMENT. One result per hop. The SQL assesses every record once (one row per due record, pending, untimed and orphan key), materialised as a local checkpoint in a session view the runner drops; a summary query counts it per disposition and label. Only counts leave the session.
2. **The eleven dispositions** in the model's precedence, with classes and evidence levels; overlaps among the explaining rules are diagnostics. **The identity is asserted** (no record unassigned, the buckets add up to the due records, each key in one bucket); otherwise DID\_NOT\_RUN / ledger\_unbalanced.
3. **The outcome rules:** unassessable, breaches, defect rejects and orphans fail; tolerance only on `missing`, `stale`, `divergent`; volume guards (`max_share`) on each exclusion and each authorised reject reason; unsupported kinds DID\_NOT\_RUN / unsupported\_mapping, an inferred aggregate included (D6 condition 3).
4. **Key collisions** (distinct raw keys sharing a normalised key, on either side) are unassessable.
5. **Rejects:** a new optional contract field `rejects: {table, condition, reason_column, time_column}`, reported evidence. A declared table that is gone is DID\_NOT\_RUN / insufficient\_evidence. `dre validate`: an error on a kind not compared through `key_map`, a warning for a `reject_policy` without `rejects`.
6. **Scope** on every result (JSON in `detail`) and every `dq_reconciliation` row: edge, kind, declared or inferred, contract hash, claim (`contract-proven` only under a declared contract; D6 conditions 1 and 2), population rule, window, cutoff, full sweep, grain, evidence levels, the claim ceiling, blind spots, unverified columns, orphans checked or not. The digest prints it in one line.
7. **`dq_reconciliation`** (84 months), evidence of its result under ADR-07: written before the result, declared in `dq_commit.reconciliation_rows`, counted before the commit, visible only through `v_committed_reconciliation` (committed, not INVALID), checked by `v_commit_integrity` and the integrity sweep, kept by retention while a commit promises it.

**Deviations from the model, for ratification at checkpoint 3** (the model is frozen, so each needs the user's word):

- **L1. Due population: the deadline falls in the window.** The model's candidates are records loaded in W, and due those whose deadline is at or before C. With hourly windows and an 8-hour SLA, nothing loaded in W is ever due, and in the window where a record's deadline falls it is no longer a candidate, so read literally the population is always empty. Built: a key is due in the window where its version's deadline falls (at or before the window end on the full sweep), exactly as HOP\_KEY\_CURRENCY judges keys, so each record is judged once. Pending: the keys loaded in the judged range whose deadline is after the cutoff.
- **L2. The cutoff is the window end.** A version loaded after it belongs to a later window, so a RERUN of a window judges the versions that window had (as far as the tables still hold them; spec section 4, "best effort").
- **L3. Reject lineage.** The model wants the source key, the version or batch, the reason, the rule id and the rule version. The rejects table's layout is not confirmed (`docs/evidence-contract.md`), so a reject accounts for the version under judgement when its key matches (the upstream's key columns by name) and it was rejected at or after that version's load time and before the cutoff; the latest reject names the reason. No rule id or version is read. Rejects are reported evidence, so a PASS resting on one has the ceiling `reported`.
- **L4. `identity` is judged like `dedup`:** the latest version per key. One-to-one is not asserted here (T1\_KEY\_DUPLICATES checks uniqueness).
- **L5. `ledger_unbalanced` is filed under PLATFORM**, so it holds the window and the next run evaluates it again; there is no engine-defect category.
- **L6. Orphans only where the rows here can be attributed to the hop:** the dataset has a `load_time` and one upstream, and a `union_partition` target has its `feed_filter` (decision D5). Otherwise the scope says "orphans not checked" and why.
- **L7. Overlaps only among the explaining rules** (unassessable to rejected). `delivered` to `missing` describe what D shows when no rule explains the record, so "excluded and missing" is not an overlap.
- **L8. Untimed records.** A normal run counts as untimed the candidate keys whose version has no load time (a key whose rows all lack one cannot be placed in any window). The full sweep puts every such key, and every keyless row with no load time, in the ledger as unassessable ("no load time"), so the model's "judged by the full sweep" fails them rather than passing them.
- **L9. A soft-deleted record absent from D is `missing`,** not `deleted_not_applied` (the model's rule is "still shows the record as live").
- **L10. Evidence level of `deleted` is `reconstructed`** (the model says observed + reconstructed): a combination takes its weaker level.
- **L11. A declared contract with a `hop_exclusions` entry is not contract-proven** (the legacy exclusion is unapproved), and a declared `union_partition` is contract-proven while its attribution is unproven until step 5 (the scope lists it as a blind spot).
- **L12. Transforms:** values are compared on `owned_columns` only (identity transforms); `{from, to}` transforms are not built, so `unverified_columns` is always empty and the blind spots say "other columns are not compared".
- **L13. No cause investigation for HOP\_LEDGER:** no failure type in the packaged patterns. Its missing, stale and divergent records are the keys HOP\_KEY\_CURRENCY and HOP\_VALUE\_AGREEMENT flag and explain.
- **L14. The release 1 hop checks keep running** beside the ledger; nothing they report changed.
- **L15. `insufficient_evidence` is filed under DEPENDENCY**, which does not hold the window (like a missing table, which is CONFIGURATION).

**A fix found while building.** `runner.pending_investigations` listed every FAILED result without causes, also for checks that have no failure type and are never investigated; only the retry loop skipped them. HOP\_LEDGER is the first such check that fails in the tests, and it would have stayed "pending" for good. It now lists only results of checks with a failure type (`tests/test_recovery.py::test_a_cause_investigation_that_cannot_be_stored_is_tracked_and_retried` asserts HOP\_LEDGER's FAILED result is not pending).

**Tests restated, none weakened:** four result counts went up by one, the new check (R09, R11a, R11b, `test_runner.py::test_run_writes_results_and_windows_follow_on`), each now also asserting HOP\_LEDGER is among them, and the same test's list of hop checks reporting the configuration problem again; `test_runner.py::test_a_failed_key_event_write_makes_the_run_partial_and_the_result_is_still_explained` fails every evidence write, so HOP\_LEDGER's ledger write fails too and it publishes its own `evidence_incomplete` marker: two evaluations short instead of one, both asserted.

Not built in step 3 (later steps or not in scope): `superseded` (survivorship, step 5), T1\_SOURCE\_ATTRIBUTION (step 5), evaluation modes and AS\_OF (step 4), the chain (step 6), aggregate reconciliation under a declared contract.

## Checkpoint 2 signed off for controlled development; step 3 authorised (2026-10-18)

The user, after the corrections R1 to R3 (CI run #85 on `ce7a283`): **"This is authorization to proceed with controlled development, not production certification. The Step 3 implementation must still satisfy the previously approved reconciliation-contract, ledger, and evidence-scope requirements."**

1. **Checkpoint 2 is closed for development.** It is not a production certification, and nothing in this repository may cite it as one.
2. **Step 3 (HOP_LEDGER and `dq_reconciliation`) is authorised**, bound by:
   - the reconciliation contract (ADR-02, ADR-03, ADR-04; model sections 4 to 6) and the D6 conditions of checkpoint 1;
   - the ledger (ADR-01; model sections 2 and 3): the populations, the eleven dispositions in precedence, the asserted identity (`ledger_unbalanced` otherwise), tolerance on `missing`, `stale` and `divergent` only, the volume guards, orphans, key collisions;
   - the evidence scope (ADR-01, ADR-06): the scope stored with every result, evidence levels on every disposition, the claim ceiling, blind spots and unsupported mappings visible;
   - the commit protocol and evidence states of checkpoint 2.
3. Step 3 ends at checkpoint 3, which the user reviews before step 4.

## Checkpoint 2 corrections R1 to R3: evidence states, integrity sweep, confirmed repair, cause dates (2026-10-17)

The user's second checkpoint 2 review ("DRE Checkpoint 2 Correction Lifecycle", 2026-10-17) registered three corrections (CP2-R1 publication and integrity race, high; CP2-R2 repair without final readback, medium; CP2-R3 cross-date cause linkage, medium) and a failure-injection matrix F01 to F10. My assessment (given before building): R1's race cannot occur inside DRE (one writer, append only, run-scoped rows, a unit verified after its last write), except for one late-thread path; evidence lost after a commit is real (retention, changes outside DRE); R2 agreed; R3's identity was already unambiguous but should be explicit. The user then ratified **"Approved for implementation, subject to acceptance testing"**:

- **R1, under the documented single-writer, append-only consistency model:** move the publisher-less cause commit out of the cancellable thread; preserve published results when evidence is lost later; expose evidence states VALID, INVALID, EXPIRED, UNKNOWN; never treat compromised or unverified evidence as healthy; an explicit evidence lifetime (13 months for partition states, 84 for historical results); enforce retention deadlines and support investigation holds; distinguish confirmed loss from transient verification failures; report integrity-check freshness.
- **R2:** verify identities and declared counts after repair; publish only CONFIRMED units; fail closed on absent, conflicting or unavailable readbacks; keep conflicts as integrity incidents.
- **R3:** add `result_run_date` to cause commits; join on it with `result_run_id` and the evaluation id; no visible cause for an uncommitted or orphaned result; cross-date and collision tests.
- **Acceptance:** documented consistency assumptions and the limits of changes made outside DRE; F01 to F10 (ambiguous writes, late threads, duplicate commits, concurrent publishers); NORMAL/RERUN/REPLAY precedence; source changes, assertions, CI evidence, committed-view and integrity-view output; compromised evidence never produces a false healthy status. Checkpoint 2 stays open until independently verified; step 3 stays blocked.

What was built:

1. **Consistency model** (spec section 5, "Commit protocol", item 6): one writer, append only; run-scoped rows (a UUID run id, one `run_date` per run); verification after a unit's last write, in the run's thread; late threads (a cancelled evidence write lands under a marker that vouches for none of it; a cancelled investigation never publishes); read ordering (rows land before their commit); outside the model, changes made by hand are detected, not prevented, and a forged commit over forged rows is not detectable.
2. **The late-thread fix.** `explain_failures` without a publisher appended the cause commit inside the cancellable investigation thread; it now appends it in the run's thread after the investigation returned. The publisher path already did.
3. **Evidence states** (`v_commit_state`): INVALID (an identity stored with different contents, or a recorded EVIDENCE\_MISMATCH or COMMIT\_CONFLICT), EXPIRED (past `evidence_kept_until`), UNKNOWN (the latest sweep covering its `run_date` failed with no later completed sweep covering it), VALID. On `v_committed_result`, `v_latest_result` (with `healthy` = PASSED and VALID, the one definition) and `v_committed_cause`. Results are preserved whatever their state; evidence rows of INVALID units are never shown (no baseline on them); INVALID cause sets are not valid causes (the investigation is retried); a RERUN republishes an INVALID result's window (`stored_outcomes` ignores INVALID rows).
4. **Evidence lifetime.** Each commit records `evidence_kept_until` (run\_date plus the shortest retention of the tables holding what it vouches for: partition states 13 months, results, key events, causes 84). `dre retention` keeps any partition a commit promised past today, even after `retention_months` is shortened, and any partition under an investigation hold (`retention_holds`: `{from, to, reason, until}`).
5. **Integrity sweep** (`store/integrity.py`, `runner.integrity_sweep`): daily over `integrity_sweep_days` (35), weekly over every unexpired commit (`integrity_full_sweep_day`, SUNDAY), at the first NORMAL run at or after `rule_run_at`; a failed sweep is retried next run. It records new EVIDENCE\_MISMATCH (lost or added rows before the lifetime ends) and COMMIT\_CONFLICT rows, then SWEEP COMPLETED; on failure SWEEP FAILED (UNKNOWN, not INVALID). New table `dq_integrity` (observations, not commit-gated; 84 months).
6. **Compromised baselines.** An INVALID unit that carried key events or partition states, until REBASELINED: HOP\_KEY\_CURRENCY of that dataset runs as a full sweep (it judges every key, so it does not rest on the lost open keys); T1\_PARTITION\_DRIFT cannot pass (PASSED becomes DID\_NOT\_RUN / PLATFORM / evidence\_invalid, a new reason code, which holds the window). When such an evaluation is published whole, REBASELINED is recorded (after its commit is confirmed). Unreadable: every such check is compromised (fail closed).
7. **Freshness reporting.** Every digest carries the store integrity line (last completed sweep, stale after 26 hours, a failed latest sweep, the last day's incidents) and lists the owner's current results with INVALID or UNKNOWN evidence under "Evidence not verified (not counted as healthy)", which keeps it from being all clear; passing counts only VALID passes. The watchdog alerts on stale verification (once the store is older than 26 hours) and on incidents recorded in the judged hour.
8. **R2.** After a failed commit write: read back, append the missing once, read back again; per unit CONFIRMED (every commit stored with exactly its contents), absent, conflicting (recorded as COMMIT\_CONFLICT), or uncertain (readback failed). Only CONFIRMED units are published and counted (`Publisher.unconfirmed` counts the rest). A conflicting identity is INVALID in every view by itself (`versions` > 1), even if the incident record cannot be written.
9. **R3.** `dq_commit.result_run_date`; `v_committed_cause` requires a result commit with the same run id, `run_date` and evaluation id; it also prunes the result side by date.
10. **Test-data fix, not a weakening.** `tests/store/conftest.append_rows` used to commit a result and its key events with two different commits of one identity, which is now a conflict; key events now take one commit with their evaluation's uncommitted results (the helper refuses otherwise), and `test_views.py`'s key events got their own ids. The digest golden files gained the integrity line.
11. **Tests:** `tests/test_evidence_integrity.py` (F01 to F09, evidence states, retention, compromised baselines, R2, R3, precedence, digest, watchdog) and the earlier `tests/test_commit_protocol.py` (F03, F10).

## Checkpoint 2 revision: identity, verification and repair (2026-10-17)

Built after the checkpoint 2 review below, on the user's approval of the proposed changes. Revised checkpoint 2 (`docs/design/checkpoints.md`) waits for the user's review; step 3 stays blocked until then.

1. **Causes attach by identity (E3 revised; CP2-03).**
   - A cause investigation is published by its own `dq_commit` row: `kind` cause, the investigating run, the evaluation id, `result_run_id` (the run whose committed result it explains) and `cause_rows`. Result commits carry `kind` result and their own run as `result_run_id`.
   - `v_committed_cause` shows a cause row only with a cause commit of its own run and evaluation id, and only while the named result has a result commit. It exposes `result_run_id`. No timestamp is compared.
   - Readers join by it: `dre trace` (`r.run_id = c.result_run_id`; it used to require the cause's run to be the result's, so a retried cause was never shown, a defect older than step 2), the retry of investigations (an anti join on evaluation id and result run), the digest (this run's causes of this run's results).
   - An investigation counts in `investigations_done` only once its cause commit is stored. A partly stored cause set has no commit, stays invisible, and the next run retries it.
   - An evidence_incomplete marker stored in place of a FAILED result is not investigated: no FAILED result was published for a cause to explain.
   - `dq_commit` is new in step 2 and not yet released, so its columns changed without altering a released table.
2. **Stored rows are counted before a commit (CP2-01, E2's condition).**
   - Before each batch is written, one query over the run's own rows counts what the batch's commits vouch for: result rows; key events and partition states where a commit declares any; cause rows. A table no commit declares rows in is not read.
   - A unit (an evaluation with every row and group it stored, or an investigation) whose stored rows differ is withheld whole, said in the run's output (`integrity: ... withheld`) and the log, not counted, and evaluated again. A batch that cannot be counted is withheld.
   - This closes a hole of the first build: a result write that stored its rows and then failed was followed by a marker under the same evaluation id, and the semi join published both. Now two rows are stored where one is declared, and the marker is withheld.
   - Counting at read time was rejected: every reader would count `dq_key_event`, the largest table. The run pays one aggregate query per batch instead, over its own rows (ORC stripe statistics on `run_id` skip other runs' files).
   - The narrower guarantee is documented (spec section 5, "What is guaranteed"): a visible evaluation's rows were all stored and counted when it was published; afterwards they last as long as their tables' retention. `v_commit_integrity` is an audit view listing commits whose stored rows now differ, expired evidence included (partition states are kept 13 months, results 84).
3. **A failed commit write is read back and completed (CP2-02).**
   - A batch's commit rows are written as one file, so a write stores all of them or none. A write can still fail after its file landed.
   - After a failed write the run reads back this run's commit rows, counts the units whose commits are all stored, and appends the missing rows once more. A commit's identity is (run, evaluation id, kind); a duplicate row publishes nothing more, because every view only asks whether a matching commit exists.
   - If the repair fails too, only units whose commits are all stored count. Tested with failures after none, some and all of a batch's rows were stored, and with a repair that fails.
4. **The REPLAY window list reads committed results only (E1's condition).** The window history keeps reading the raw table: it must see uncommitted attempts to hold their windows, and it labels them by their run's status. A window that was attempted and never committed is not replayed: the next NORMAL run evaluated it again, usually with other bounds.
5. **`commit_batch` (E6).** A setting in `defaults.yaml`, default 25, 1 to 1000, instead of a constant.
6. **Observations are not evaluations (CP2-04).** `dq_run` and `dq_file` are not commit-gated, documented in spec section 5: a `dq_file` row records that a file was seen at a time, true whether or not the run finishes, and a first-seen time must survive a run that dies. What the checks conclude from it is published through the commit protocol.
7. **Run evidence a reviewer can inspect.** CI keeps the per-test report (`test-report`, JUnit XML) and the e2e logs (`e2e-logs`) with each run.
8. **Tests** (`tests/test_commit_protocol.py` unless named): causes by identity, never by time (an orphan cause stamped after the other run's commit stays out); a partly stored cause set and its retry; a marker over partly stored results; a unit withheld whole (a missing group, a key-event count short); a batch that cannot be counted; a failed commit write after none, some or all rows, and a failed repair; the integrity view; the REPLAY list; `tests/test_trace.py::test_a_retried_investigation_s_cause_is_shown`; `config/test_validate.py` case commit-batch-zero.

## Checkpoint 2 reviewed: changes required (2026-10-17)

The user's review of checkpoint 2 ("DRE Checkpoint 2 Architecture Implementation Review", 2026-10-17): **changes required before step 3**. The evidence-first, result-second, commit-last direction is accepted. The review is a static review; it does not claim to have run the failure-injection suite.

1. **Findings.**
   - **CP2-01 (high):** a commit's existence does not prove its evidence is complete. Verify persisted evidence against the declared counts, or document a narrower guarantee with integrity monitoring.
   - **CP2-02 (high):** a commit write may persist some rows and then raise. Define commit identities, read back what was stored, and test failures after none, some and all rows.
   - **CP2-03 (high):** associating causes with evaluations by timestamp is fragile. Use an explicit identity.
   - **CP2-04 (medium):** the file registry is not commit-gated; document its different publication semantics.
2. **E1 to E6.**
   - **E1 approved with conditions:** distinguish incomplete attempts from committed evaluations where raw history is used.
   - **E2 approved with conditions:** zero or positive counts are not proof of durable evidence.
   - **E3 not approved as written:** use an explicit committed evaluation or investigation identity.
   - **E4 approved:** validate replacement against existing readers.
   - **E5 approved:** commit metadata kept at least as long as what depends on it.
   - **E6 approved:** treat the batch size as tunable; test load, retry and partial writes.
3. **My assessment, given to the user before building:**
   - CP2-01 is partly right. On the error path a partial evidence write is never published (the marker commits counts of 0, already tested). What was unproven: a write that returns with fewer rows stored, rows lost afterwards (partition states outlive by retention, 13 months against 84), and a partly stored result followed by a marker, which the first build published. Verification at publication, not at read time.
   - CP2-02 can happen, but a stored subset of commits publishes only fully written evaluations; the harm is the count, a held window and a second publication. Fixed by single-file batches, a read-back and a repair.
   - CP2-03 is right, and more: clock skew between hosts, partly stored cause sets, and a `dre trace` defect older than step 2.
   - CP2-04: documentation only.
4. **Before step 3, the review requires:** cause identity (E3), recovery from ambiguous commit writes, verification or a narrowed guarantee with monitoring, the file registry's semantics, tests of ambiguous outcomes and evidence mismatch, and this record with CI results and implementation references. The user approved building them ("Yes"); the revision is the entry above.

## Phase 1 build step 2: commit rows and committed-only views (2026-10-17)

ADR-07, authorised by the checkpoint 1 ratification. Checkpoint 2 (`docs/design/checkpoints.md`) waits for the user's review.

1. **`dq_commit`.**
   - One row per committed evaluation id (each result row is its own id): run, evaluation, dataset, check, and the counts it vouches for (result rows, key events, partition states).
   - Appended through the one writer, in batches of 25 evaluations and at the end of the run, also after a failure.
   - Retention 84 months. `dre validate` refuses less than any table it gates, or results would disappear with their commits.
2. **Write order**: evidence (key events, partition states), then results, then the commit.
   - An evaluation counts in `checks_written` only once its commit is stored.
   - If the evidence cannot be stored, the results are not written. The release 1 evidence_incomplete marker is written and committed with evidence counts of 0, so the current state explains the gap and the partial evidence stays invisible.
   - If the results cannot be stored, the marker is tried the same way.
   - If the commit cannot be stored, the run is PARTIAL and nothing is published.
   - WINDOW_GAP rows are committed like any result.
3. **Committed views**: `v_committed_result`, `v_committed_key_event`, `v_committed_partition_state`, `v_committed_cause`. `v_latest_result`, `v_open_keys` and `v_latest_partition_state` read them. So do every other reader:
   - the digest;
   - `dre trace`;
   - the hop's open keys;
   - T1_VOLUME and T1_SCHEMA_DRIFT history;
   - the rule schedule;
   - RERUN idempotency (`stored_outcomes`), so an unpublished row never makes a RERUN skip its window;
   - the retry of investigations.
4. **Decisions beyond the ADR's text, for checkpoint 2:**
   1. **The window history reads `dq_check_result` itself.** It already holds every event of a run that did not finish COMPLETED, and a run is COMPLETED only once every evaluation is committed. Holding is therefore stricter than committed-only reading, and it keeps the `max_window_hours` limit and the WINDOW_GAP record. The REPLAY window list reads the table too, for the same reason. *(Changed at the checkpoint 2 revision: the REPLAY list reads committed results.)*
   2. **A commit's evidence counts gate its evidence.** Key events and partition states of a commit with a count of 0 stay invisible. This is what keeps a marker from publishing partial evidence.
   3. **Causes are gated by timing.** *(Not approved at the checkpoint 2 review; replaced by cause commits, see "Checkpoint 2 revision".)* A cause row counts once its evaluation id is committed, by the investigating run or before a later run's retry. A cause left by a run that never committed stays out, even after another run commits the same deterministic id.
   4. **Views are `CREATE OR REPLACE`.** Tables stay IF NOT EXISTS. Views hold no data, so `dre install --apply` replaces older definitions; that is the upgrade path for an installed store. The guard already allowed CREATE OR REPLACE VIEW in `store/ddl.sql` only.
   5. **Batch size 25**, a constant. The ADR said "per batch" without a size. *(Now the `commit_batch` setting, default 25.)*
5. **Tests.**
   - `tests/test_commit_protocol.py`, the acceptance condition: failure injection at four write boundaries, each followed by recovery, plus unit tests of the committed views.
   - `store/test_install.py`: replacing older views.
   - `config/test_validate.py`: the retention rule.
   - Tests that write synthetic store rows commit them, as a run would (`tests/store/conftest.append_rows(commit=True)`). `commit=False` leaves them unpublished, and the protocol tests use it.

## Checkpoint 1 ratified, with conditions (2026-10-17)

The user ratified checkpoint 1 (`docs/design/checkpoints.md`) as "approved with conditions".

1. **D1 to D5 approved** as documented: the grain without a repeated key or version; conditions that parse their own dates; `applied_as` with `soft_when`; reject reasons as `{disposition, max_share}`; the union attribution as the dataset's `feed_filter`.
2. **D6 approved for backward compatibility**, on four conditions that bind every later step:
   1. **Inferred is distinguishable from declared.** Done in step 1:
      - `Hop.declared`;
      - `dre validate` labels each hop "declared" or "inferred from key_map";
      - an inferred hop's blind spots say that its results can never claim contract-proven completeness.
   2. **An inferred dedup never establishes contract-proven completeness.**
      - The ledger (step 3) marks a result's claim as contract-proven only under a declared contract. Otherwise it is scoped "inferred contract".
      - CHAIN_COMPLETENESS (step 6) treats an inferred hop as below the claim level, so a chain through it is at best `partial_chain`.
   3. **An inferred aggregate never authorises aggregation reconciliation.**
      - The ledger reconciles an aggregate hop only under a declared `mapping: aggregate` whose derived definition passed validation.
      - Today's release 2 group-level hop checks keep running, as before.
      - In step 1, an inferred aggregate hop carries a blind-spot line saying so.
   4. **Unsupported mappings and evidence limits stay visible**, in validation (step 1: warnings and blind spots) and in result reporting (step 3: DID_NOT_RUN / `unsupported_mapping`, and blind spots on each result's scope).
3. **The remaining step 1 gaps** are accepted as temporary, each to be resolved at its assigned checkpoint (the checkpoint's "Left for later steps" column).
4. **Step 2 is authorised**: ADR-07 commit rows and committed-only views. **Its acceptance requires failure-injection and recovery tests** proving that no incomplete evaluation becomes visible as a committed result.
5. **The approval is not an independent verification** of CI run #76 or of the source code, and is not to be cited as one.

## Phase 1 build step 1: the contract schema and its validation (2026-10-16)

`docs/design/target-architecture.md` section 7, step 1. The user asked for it to be "an architectural checkpoint, a controlled implementation phase": `docs/design/checkpoints.md` is the checkpoint report, and step 2 waits for the user's review.

1. **What was built.**
   - On a dataset: `grain` (only `history`) and `contracts` (per upstream: `mapping`, `exclusions`, `reject_policy`, `deletion`, `tolerance`, `reference`, `precedence`).
   - In `defaults.yaml`: `yarn_queue` and `protected_queues`.
   - Validation of every rule in spec section 3, "The reconciliation contract".
   - The hop resolution in `config/contracts.py`.
   - `dre validate`'s per-hop report of kinds and blind spots.
   - `dre dry-run` resolving a contract's conditions.
2. **Hop checks read exclusions through the contract.** Otherwise a team that wrote its exclusions in the new form would get every excluded record reported MISSING. A legacy `hop_exclusions` entry is used exactly as written. A contract exclusion matches rows whose upstream load time falls in its effective span, and the conditions are joined with OR. The `observed` text names the source: "excluded by hop_exclusions", or "excluded by exclusions drop_x@1". Nothing else in the checks changed.
3. **Deviations from the reconciliation model as written.** The model document was aligned to what was built:
   - `grain` does not repeat the key or the version. They are the dataset's `key` and `record_time`, with a hop's `winner_rule` for ties: one source of truth.
   - `deletion` has no `formats` map. Conditions are SQL and parse their own dates with `to_date`, like every other configuration fragment.
   - `applied_as` is `hard` or `soft`, with `soft_when` beside it.
   - `reject_policy` values are `{disposition, max_share}`, not a bare word, so that an authorised reason carries its volume guard.
   - A union partition's attribution is the dataset's `feed_filter`, which already partitions a shared gold table among per-source datasets, not a second field.
   - The default kind of a hop with no contract is `dedup` (what HOP_KEY_CURRENCY judges: the latest upstream version per key), or `aggregate` for a derived upstream.
4. **What waits, and where.** Each item waits for the build step named:
   - `max_share`, the reject policy, the deletion state and the tolerance: the ledger (step 3). Until then the blind-spot report says so.
   - DID_NOT_RUN / `unsupported_mapping` at run time: step 3. Today a `fan_out` or `join_contribution` hop is a warning, and an error if keyed, so it is never judged one-to-one.
   - The protected-queue check at run start: step 8.
5. **Sample configuration.**
   - The synthetic gold member coverage declares its contract: `union_partition` (each source's rows side by side, equal priority) and the soft void as `deletion`. It declares `grain.history: mutable` (merged in place).
   - Curated's history stays null. Whether it is append-only is a platform fact, and it is not invented.
   - The synthetic provider feed declares raw `append_only` and gold `mutable`.

## Developer answers on the reconciliation model (2026-10-16)

Three facts for the proposed reconciliation model (`docs/design/reconciliation-model.md`), given by the user on 2026-10-16. Nothing is built from them yet; they settle the model's open points for the member pipeline.

1. **Deletions are soft voids.** RMS voids a coverage with a new version whose end date equals its effective date; the record (whose key includes the effective date) stays at every layer, and gold keeps it with the voided end date. In the model: `deletion.tombstone` is end date = effective date on the upstream, `applied_as` is `soft` (gold end date = gold effective date), dates parsed with their formats. Today's engine already sees an unapplied void as STALE or as an end-date disagreement, because the void is a newer version and the end date is an owned column; the model adds the `deleted` and `deleted_not_applied` labels. The gold rule `end_not_before_start` (`column_order`, failing only when upper < lower) passes a void. "Usually" was the developer's word; other void shapes are question 19.
2. **No gold aggregates.** No gold table holds counts or totals, so the `aggregate` mapping and its contribution limit do not apply to the member pipeline. The kind stays in the model for proof scenario 5 and other feeds.
3. **Equal priority, no survivorship.** A member with group and retail coverage appears in every associated table as separate records keyed by coverage (and by source system in a table shared across sources, as the sample `gold_member_coverage_all` is keyed). Multi-source gold tables are union partitions by source; `superseded` never occurs here. Whether any gold row joins columns from two sources is question 19.

## Notes for later steps

- **Step 4:** done: `dre dry-run` parses SQL fragments (`config/fragments.py`).
- **Step 5:** done: `LOAD_TIME_CHECKS` matches the Tier 1 checks that need `load_time`. No fallback to `partition_column` yet.
- **Step 6:** done: T1\_FILES\_NOT\_LOADED is in `patterns/file_cyclic.yaml` and `patterns/file_periodic.yaml`.
- **Step 6 (open):** a dataset with `partition_column` but no `load_time` may use the partition value as its load time, at the partition's granularity. Waiting on the developer to confirm whether the raw table has a load-time column; until then such a dataset's load-time checks stay DID\_NOT\_RUN / invalid\_config.
- **Step 7:** done: on the full sweep, open keys no longer found upstream get CLEARED with `state_detail` "no longer upstream".
- **Step 9:** done: one alert when T1\_ON\_TIME and T1\_ZERO\_ROWS fail for the same loads; groups missing since the previous run are listed.
- **Step 10:** `dre retention` is a separate command, scheduled weekly. By default it lists the partitions it would drop under each table's `retention_months`; `--apply` drops them through `store/retention.py`. Built in the code review (2026-10-11).

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
