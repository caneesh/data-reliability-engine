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

## Guard rules: reasons (decided after step 1, 2026-10-09)

The rules themselves are in spec section 9 (Guard tests). Reasons, by rule:

1. **Naming the dq store.** The real database name comes from config, so code cannot hard-code `dq`; a fixed placeholder keeps the target visible to a static scan. Validating the name as a plain identifier stops it carrying dots, quotes or SQL into statements. Step 2 must call `validate_dq_database` when loading config.
2. **Read-only outside dq.** Hard rule 1. Path writes, FileSystem and os/shutil calls are other ways to change production data than SQL, so they are flagged the same way.
3. **Append-only.** Hard rule 5. Overwrite modes and `saveAsTable` are the DataFrame forms of INSERT OVERWRITE and of creating tables outside `store/ddl.sql`.
4. **One write path** (added before step 2). A static scan cannot follow a write chain split across statements; allowing the write APIs in one module closes that gap. Column order: `insertInto` matches by position, so a frame in a different order would silently write values into the wrong columns.
5. **Retention.** Retention needs old partitions dropped, which append-only otherwise bans; keeping it in one module, with a refusal for anything inside retention, limits the exception.
6. **Database creation** (added before step 2). On the cluster the platform team owns the database and the service account; the engine must not create it. Tests and local runs still need it.
7. **Email.** Hard rule 6. The digest does not exist yet, so the guard is static until step 9, which must add a test on the rendered email text.

CI runs on Python 3.10 with Java 17 (`.github/workflows/ci.yml`). Nothing may depend on Java 21.
