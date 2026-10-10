# Data Reliability Engine — Build spec (release 1)

This is the single source of truth for building release 1 of the Data Reliability Engine (DRE). Where it disagrees with earlier design documents, this spec wins. Build the engine against synthetic data; production configuration comes later.

Repository copy: production paths, file names, and database and table names are replaced with placeholders. Column names in the examples are kept so that the fixtures match the real layers.

## 1. Scope and constraints

**Release 1 delivers:** configuration registry with validation; three ingestion patterns; Tier 1 checks; gold rule templates; cause checks; append-only results store; email digest; watchdog; record trace command.

**Hard constraints (never break these):**

1. **Read-only.** The engine reads production tables, HDFS listings and scheduler history. It writes only to its own database (`dq` in this spec; real name set by config). No INSERT, UPDATE, DELETE, MERGE, DROP or ALTER on anything else; no file moves or deletes in HDFS.
2. **No pipeline changes.** Nothing is added to existing jobs, scripts or schedules.
3. **No code per source.** Everything source-specific lives in YAML. If a source needs logic the engine lacks, add a generic capability, never an `if feed == ...` branch.
4. **Three result states.** Every check, every run, writes exactly one of PASSED, FAILED, DID\_NOT\_RUN. A check that evaluated zero rows of its population is DID\_NOT\_RUN with reason `empty_population`, never PASSED.
5. **Append-only evidence.** Result and event tables are only appended to. Current state is derived through views.
6. **No member data outside the `dq` store.** Logs and emails carry counts, check ids and table names. Sample keys go to emails only when a feed sets `email_sample_keys: true`; default false.
7. **Synthetic data only in development and tests.** No production extracts in the repository.

**Not in release 1:** learned baselines, Kafka checks, reconciliation across every layer (release 1 compares adjacent layers only where a dataset has a key\_map), UI, alert acknowledgement, integrations other than email, blocking or quarantine of any kind.

## 2. Stack and repository layout

**Default stack** (confirm against the cluster; see section 11):

- Python 3.10+, PySpark 3.5.1 (matches the cluster's Spark 3.5.1)
- PyYAML for config, Pydantic v2 for schema validation, Jinja2 for SQL templates
- pytest with local-mode Spark for all tests
- Packaged as a wheel plus a zip of dependencies for `spark-submit --py-files`
- No other runtime services: no database server, no web server, no message queue

**Names.** Repository `data-reliability-engine`. Python package `hcsc.datalake.dre` under `src/` (`hcsc` and `hcsc/datalake` are namespace packages with no `__init__.py`, so other HCSC data lake tools can share the prefix). Distribution `hcsc-datalake-dre`. Command `dre`.

All check logic is SQL rendered from templates and run through `spark.sql`, so the same logic runs on the cluster's Hive tables and on local test tables.

```text
data-reliability-engine/
  README.md
  CLAUDE.md                  # points to this spec; repeats the hard constraints
  pyproject.toml             # distribution hcsc-datalake-dre; console script `dre`
  src/
    hcsc/                    # namespace package: no __init__.py
      datalake/              # namespace package: no __init__.py
        dre/
          __init__.py
          cli.py             # validate, install, dry-run, run, trace, watchdog
          session.py         # the Spark session (active one, or a new one with Hive support)
          runner.py          # dre run and dre dry-run: plan, evaluate, append results, run rows
          config/
            models.py        # Pydantic models for feeds, datasets, rules, defaults
            loader.py        # load YAML, apply defaults and overrides
            validate.py      # static and runtime validation
            fragments.py     # dry-run: parse SQL fragments from config against Spark
          patterns/
            file_cyclic.yaml # checks and cause checks per pattern
            file_periodic.yaml
            table_merge.yaml
            table_wide.yaml  # checks for table-wide datasets (no feed)
          checks/
            base.py          # Check interface, result building, denominator rule, preconditions, run_check
            keys.py          # key expressions with key_normalise applied
            times.py         # time columns to UTC; window bounds truncated to granularity
            cadence.py       # cadence slots (expected loads) in UTC
            events.py        # batch load events, event_id and evaluation_id
            registry.py      # which checks run, from patterns/*.yaml
            tier1/           # one module per Tier 1 check
            hop/             # hop checks: common.py (the two sides of a hop, judged keys, key_hash), one module per check
            rules/           # rule_check.py: each gold rule as a check over its template
            sql/             # Jinja2 SQL templates
          causes/
            engine.py        # runs cause checks in order
            landing.py merge.py  # cause checks by hop type
          store/
            ddl.sql          # dq tables and views
            schema.py        # renders ddl.sql for the configured dq database; parses the objects it creates
            install.py       # dre install: print, apply or check the DDL; the only path that applies it
            runs.py          # dq_run bookkeeping: STARTED row at start, final row at end
            writer.py        # append-only writes; the only DataFrame write path
            results.py       # appends check results through the writer
            key_events.py    # appends hop key events to dq_key_event through the writer
            files.py         # dq_file registry: appends new and changed landed files
            retention.py     # drops expired run_date partitions; the only DROP PARTITION
            local_setup.py   # creates the dq database for tests and local runs only
            names.py         # identifier validation for dq names
          sources/
            hdfs.py          # listing landing folders, read-only, through the Hadoop FS API
            scheduler.py     # optional: scheduler history adapter (stub in release 1)
          notify/
            email.py         # digest builder and sender
          trace.py
  conf/
    defaults.yaml
    feeds/  datasets/  rules/
  tests/
    fixtures/                # synthetic data builders
    replay/                  # one test per replay scenario (section 9)
```

## 3. Configuration model

Three kinds of YAML file, one object per file, plus one defaults file. Values resolve in this order: `defaults.yaml`, then the feed, then the dataset. A field set lower overrides one set higher.

**Feed** (how data arrives):

```yaml
feed: rms_realtime                # unique id
expectation_version: 1            # bump when checks for this feed change
pattern: FILE_CYCLIC              # FILE_CYCLIC | FILE_PERIODIC | TABLE_MERGE
owner: membership-gold
landing:
  roots: [/data/landing/example_feed]
  file_format: sequence           # sequence | text | xml | csv | parquet | orc
  file_name_pattern: "*"
cadence:
  kind: times                     # times | interval | calendar_dates | monthly
  times: ["00:30", "04:00", "08:00", "12:00", "16:00", "20:00"]  # kind times: quoted HH:MM
  # kind interval:       interval_minutes: 60
  # kind calendar_dates: dates: [2026-01-05, 2026-02-02]   (times optional, default 00:00)
  # kind monthly:        days_of_month: [1, 15], times: ["06:00"]   (a day past the month's end is skipped)
  timezone: America/Chicago
  calendar: EVERYDAY              # EVERYDAY | WEEKDAYS (named calendar files: not in release 1; dre validate rejects them)
sla_hours: 8
datasets: [rms_raw_enrollment, rms_curated_enrollment, gold_member_coverage]
check_delay_minutes: 0            # evaluate a slot this long after its deadline (default 0)
probes:                           # cause probe parameters by key (section 7); missing or null: NOT_READY
  load_hold_marker: { path: null }
  partition_cursor: { path: null, extract_regex: null, compare_to: partition }
  load_log: { path_glob: null, pattern: null }
  filter_rules: null
  rejects: { table: null, condition: null }
email_sample_keys: false
```

**Dataset** (a table at any layer):

```yaml
dataset: gold_member_coverage
table: gold_db.member_coverage
layer: GOLD                       # RAW | CURATED | CDC | GOLD
upstream: [rms_curated_enrollment]
feed_filter: "src_sys_nm = 'RMS'" # rows belonging to this feed, when a table holds several
key: [sub_id, mem_nbr, mbr_mbrshp_covrg_eff_dt, covrg_agrmt_id]
key_normalise: { sub_id: strip_leading_zeros }   # strip_leading_zeros, or { parse_date: MM/dd/yyyy } (a date string read as yyyy-MM-dd)
record_time: { column: src_lcts, format: null }  # same shape as load_time; format null when the column is DATE or TIMESTAMP
load_time: { column: gld_lcts, format: "yyyy-MM-dd HH:mm:ss:SSSSSS", granularity: minute, timezone: America/Chicago }
                                  # timezone: IANA zone the values are written in; null uses defaults.yaml `timezone`
partition_column: null
file_name_column: null            # column holding the landed file name, if any
min_rows_per_load: 1
key_unique: true                  # table must hold one row per key
group_by: [src_sys_nm]            # results split by these columns
volume_tolerance_pct: 50
key_map:                          # how this table's key maps to its upstream's key
  rms_curated_enrollment:
    sub_id: subscriberidnumber
    mem_nbr: membernumber
    mbr_mbrshp_covrg_eff_dt: effectivedate
    covrg_agrmt_id: qualifiedhealthplanid
winner_rule:                      # optional; how the merge chooses among versions
  order_by: ["sourcelastupdatets DESC", "enddate DESC"]
compute_budget_minutes: 8
```

**Table-wide datasets.** A dataset that no feed lists covers a whole table, for example to run gold rules across every source system. It must set `owner` (whose recipients get its results) and `expectation_version` (it has no feed to take one from), has no `feed_filter`, and takes its settings from `defaults.yaml`. It gets the checks in `patterns/table_wide.yaml`: T1\_SCHEMA\_DRIFT, T1\_KEY\_NULLS and T1\_KEY\_DUPLICATES only, plus gold rules. A dataset that a feed lists takes its owner and expectation version from the feed and must set neither. Each dataset is listed by at most one feed.

**Defaults for time and windows** (in `defaults.yaml`, overridable per feed or dataset where noted): `timezone` (IANA name for time columns that do not set their own; not overridable), `settle_minutes` (default 15), `initial_lookback_hours` (default 24) and `max_window_hours` (default 72), all used by event windows (section 4), and `rule_run_at` (default `"06:00"`, local time in `timezone`; not overridable), when daily and weekly gold rules fall due.

**Rule** (a gold rule from a template):

```yaml
rule: one_open_row_per_coverage
template: max_open_rows_per_key
dataset: gold_member_coverage
params:
  open_when: "mbr_mbrshp_covrg_end_dt = '9999-12-31'"
  max: 1
group_by: [src_sys_nm]
owner: membership-gold
status: proposed                  # proposed | approved | retired
severity: high                    # high | medium | low
frequency: daily                  # every_run | daily | weekly (default daily)
```

Rule `params` shapes, where section 6 does not spell them out: `superseded_still_open.group_key` is a list of columns (a single column may be written as a string); `child_within_parent.join` maps each child column to its parent column, for example `{ sub_id: sub_id, mem_nbr: mem_nbr }`. `column_order`, `superseded_still_open` and `child_within_parent` take an optional `format` (a Spark date or timestamp format): when set, the time columns they compare are parsed with it first, so dates held as strings are never compared as strings. Join and key columns get the dataset's `key_normalise`.

**Validation**, in two stages:

- `dre validate` (static, run in CI): YAML parses; required fields present; enums valid; every referenced dataset, feed and template exists; every `key_map` covers the full key; no duplicate ids. A dataset with `key_map`, and each upstream it maps to, must set `record_time`. Every owner (feed, table-wide dataset, rule) has recipients. `value_format` patterns compile. `hmac_secret_file` is an absolute path outside the repository and the conf directory. Errors name the file, line and field, and say how to fix them. Warnings are reported without failing validation: a dataset without `load_time` gets a warning naming the checks that will be DID\_NOT\_RUN. SQL fragments (`feed_filter`, `open_when`, filter conditions) are parsed by `dre dry-run`, not here.
- Runtime preconditions (before each run): table exists; every configured column exists; landing roots are readable. A failure here makes the affected checks DID\_NOT\_RUN with reason category CONFIGURATION, and the run continues.

## 4. Core concepts

**Run.** One execution of `dre run`. Gets a `run_id` (UUID), records start and end time, the engine version and the git commit of the configuration.

**Evaluation event.** The thing a check evaluates. In release 1 there is one kind, a batch load event: one dataset, one load window, identified by `event_id = sha256(dataset + window_start + window_end)`. Streaming windows come later and must fit the same interface. Windows are UTC and half-open: they include `window_start` and exclude `window_end`. Each result row records its event's `window_start` and `window_end`.

- `window_end` = the run's start minus `settle_minutes` (default 15), so rows still being written are left for the next run.
- `window_start` = the latest `window_end` among the dataset's NORMAL events, skipping any event in which a check was DID\_NOT\_RUN for a PLATFORM or BUDGET reason (that window is held: evaluated again). On a dataset's first run, `window_end` minus `initial_lookback_hours` (default 24). CONFIGURATION reasons never hold a window: those checks report DID\_NOT\_RUN on every run until the configuration is fixed. Gold rule results neither move nor hold a window (rules run on their own schedule over the whole table).
- A hold lasts at most `max_window_hours` (default 72), counted from the end of the first held event. When a run finds the cap reached, the window advances: it starts where the last held event ended, and one result with `check_id` WINDOW\_GAP, DID\_NOT\_RUN / window\_gap, records the unchecked span as its window (`detail`: unchecked hours and the reason codes that held it). The gap is never evaluated again.
- Both bounds are truncated to `load_time.granularity`, counted in the load time's zone. A window never runs backwards: if the previous end is later than this end, the window is empty.

**Evaluation identity.** `evaluation_id = sha256(event_id + check_id + expectation_version + engine_version + group_values)`, with `group_values` written as `key=value` pairs sorted by key (empty for an ungrouped check), so each group row of a grouped check has its own id. A rerun of the same evaluation writes a new row with the same `evaluation_id` and `execution_type = RERUN`; it never overwrites. Values: NORMAL, RERUN, REPLAY.

**Result states.** Exactly one per check per event:

| State | Meaning |
| --- | --- |
| PASSED | The check evaluated a non-empty population and the condition held |
| FAILED | The check evaluated and the condition did not hold; `observed` and `expected` are filled |
| DID\_NOT\_RUN | The check could not evaluate; `reason_category` and `reason_code` are filled |

**Reason codes for DID\_NOT\_RUN** (category, then code):

| Category | Codes |
| --- | --- |
| DATA\_UNAVAILABLE | partition\_missing, landing\_unreadable, empty\_population, window\_gap |
| PLATFORM | metastore\_unavailable, hdfs\_unavailable, query\_failed |
| CONFIGURATION | table\_missing, column\_missing, incompatible\_type, invalid\_config |
| BUDGET | budget\_exceeded |
| DEPENDENCY | upstream\_check\_failed, premise\_unconfirmed |
| BASELINE | insufficient\_history |

**Denominator rule.** Every check records `population` (rows or keys it evaluated) and `violations`. `population = 0` means DID\_NOT\_RUN / empty\_population.

**Cause result states**, for cause checks only: CONFIRMED, RULED\_OUT, NOT\_READY (a required config input is empty), ERROR. The cause of a failure is the first CONFIRMED in the pattern's order; if none, "cause not proven".

**Times.** Three kinds, never compared with each other: record time (the source's version of a row), load time (when a layer wrote it), first-seen time (when DRE first saw a file or record). Every check declares which one it uses. Parse every time column with its configured format and convert it from its configured `timezone` (or the default in `defaults.yaml`) to UTC before comparing; never assume a source column is UTC, and never compare times as strings.

## 5. Data model

All tables in the `dq` database, ORC, partitioned by `run_date`. Append-only: the writer module exposes only `append`, which selects the frame's columns in the table's column order before `insertInto`. Retention is set per table in `defaults.yaml` (results and events default to 7 years, file registry to 13 months) and applied only by `store/retention.py`. The platform team creates the empty `dq` database and grants the service account access; `dre install --apply` creates the tables and views in it (every statement is IF NOT EXISTS, so it is safe to rerun), and `dre install --check` compares an existing store with the DDL. `dre run` never creates the database or the store: it checks the store tables and views exist and, if not, exits 3 naming `dre install`. `store/local_setup.py` creates the database and store for tests and local runs.

`dq_run` gets two rows per run: STARTED when the run starts (`ended_at` and the check counts null) and a final row (COMPLETED, PARTIAL or FAILED) when it ends. A run's state is its latest row. Both rows carry `run_date` = the UTC date the run started, so a run that ends after midnight UTC stays in one partition.

```sql
CREATE TABLE dq.dq_run (
  run_id STRING, started_at TIMESTAMP, ended_at TIMESTAMP,
  engine_version STRING, config_commit STRING, status STRING,  -- STARTED | COMPLETED | PARTIAL | FAILED
  checks_expected INT, checks_written INT
) PARTITIONED BY (run_date DATE) STORED AS ORC;

CREATE TABLE dq.dq_check_result (
  evaluation_id STRING, run_id STRING, event_id STRING,
  window_start TIMESTAMP, window_end TIMESTAMP,          -- the event's window, UTC: [start, end)
  execution_type STRING,
  feed STRING, dataset STRING, check_id STRING, expectation_version INT,
  state STRING,                         -- PASSED | FAILED | DID_NOT_RUN
  reason_category STRING, reason_code STRING,
  population BIGINT, violations BIGINT, observed STRING, expected STRING,
  group_values MAP<STRING,STRING>,      -- e.g. src_sys_nm for gold rules
  severity STRING, evaluated_at TIMESTAMP, duration_ms BIGINT, detail STRING
) PARTITIONED BY (run_date DATE) STORED AS ORC;

CREATE TABLE dq.dq_cause_result (
  run_id STRING, evaluation_id STRING, failure_ref STRING,  -- file path or normalised key
  hop STRING, cause_check_id STRING, order_no INT,
  state STRING,                         -- CONFIRMED | RULED_OUT | NOT_READY | ERROR
  cause_code STRING, evidence STRING, evaluated_at TIMESTAMP
) PARTITIONED BY (run_date DATE) STORED AS ORC;

CREATE TABLE dq.dq_key_event (
  run_id STRING, evaluation_id STRING, dataset STRING, check_id STRING,
  key_hash STRING, key_value STRING,    -- key_value restricted access
  event STRING,                         -- FLAGGED | STILL_FLAGGED | CLEARED
  state_detail STRING, observed_at TIMESTAMP
) PARTITIONED BY (run_date DATE) STORED AS ORC;

CREATE TABLE dq.dq_file (
  path STRING, feed STRING, first_seen_at TIMESTAMP, size_bytes BIGINT,
  modified_at TIMESTAMP, observed_at TIMESTAMP, run_id STRING
) PARTITIONED BY (run_date DATE) STORED AS ORC;
```

Views derive current state:

- `dq.v_latest_run`: each run's latest `dq_run` row (its final row once written, otherwise STARTED)
- `dq.v_latest_result`: the latest evaluation per (dataset, check\_id) by `evaluated_at`: one row for an ungrouped check, one row per group for a grouped one
- `dq.v_open_keys`: keys whose latest `dq_key_event` is FLAGGED or STILL\_FLAGGED, with first flagged date
- `dq.v_file_status`: one row per file path with first-seen time and latest size

`key_hash` is HMAC-SHA256 of the normalised key, with the secret read from a protected file named in config, never from the repository. Emails use counts and, only if enabled, `key_hash`; `key_value` is never written to logs or emails.

## 6. Check catalogue

Each check is a class implementing `applies_to(dataset, pattern)`, `required_columns(dataset)` (the configured columns its preconditions verify) and `evaluate(ctx, event) -> list[CheckResult]` (one result per `group_by` group, one in total for an ungrouped check). Logic lives in a Jinja2 SQL template; the class only renders, runs and builds the result. `run_check` wraps every check: preconditions first (table and columns exist), then `evaluate`; any exception becomes DID\_NOT\_RUN with a reason, and `detail` keeps only the exception type and Spark error class, never the message. The checks for each pattern are listed in `patterns/*.yaml`; table-wide datasets (no feed, so no pattern) get the checks in `patterns/table_wide.yaml`. T1\_SCHEMA\_DRIFT runs once per physical table, not once per dataset, even when several datasets share a table.

**Tier 1 checks** (applied automatically by pattern):

| Check id | Patterns | Logic | Population | FAILED when |
| --- | --- | --- | --- | --- |
| T1\_ON\_TIME | all | For each cadence slot due in the window, look for a load time later than the slot within `sla_hours` | Loads due: cadence slots whose deadline (slot + `sla_hours`) falls in the window | A due slot has no load within SLA |
| T1\_ZERO\_ROWS | all | For each cadence slot due in the window, count rows loaded within `sla_hours` of the slot | Loads due: cadence slots whose deadline (slot + `sla_hours`) falls in the window, the same slots as T1\_ON\_TIME | A due load wrote fewer than `min_rows_per_load` rows |
| T1\_VOLUME | all | For each slot whose load period (slot to next slot) ends in the window, compare its row count with the median of the last 14 loads for the same slot; one result per slot (group `slot` = local HH:MM, `slot_time`) | The slot's rows | Outside median ± `volume_tolerance_pct` (default 50). Fewer than 7 prior loads for the slot: DID\_NOT\_RUN / insufficient\_history |
| T1\_FILES\_NOT\_LOADED | FILE\_CYCLIC, FILE\_PERIODIC; RAW datasets with `file_name_column` | Files in `dq_file` first seen more than `sla_hours` ago with zero rows in the raw dataset (after `feed_filter`) matching on `file_name_column`, compared by base file name | The feed's files in `dq_file` first seen more than `sla_hours` before the window end | Any such file; `detail` lists the files (up to 100). Landing roots not listable this run: DID\_NOT\_RUN / landing\_unreadable or hdfs\_unavailable |
| T1\_SCHEMA\_DRIFT | all, table-wide | Hash the table's column names and types; compare with the previous run's hash, stored in `observed` | The table's columns | Hash changed; `detail` lists added, removed and retyped columns. No earlier hash: DID\_NOT\_RUN / insufficient\_history |
| T1\_KEY\_NULLS | all, table-wide | Rows in the event window with a null or empty key column, grouped by `group_by` | Rows loaded in the window (per group) | Any |
| T1\_KEY\_DUPLICATES | datasets with `key_unique: true`, table-wide | Keys with more than one row, after `key_normalise` | Distinct keys in the table (per group) | Any |

T1\_ON\_TIME and T1\_ZERO\_ROWS count expected loads, not rows: with a load due and nothing loaded they are FAILED, also on an empty table; with no load due they are DID\_NOT\_RUN / empty\_population. Neither can PASS on an empty table. Both judge a slot once, in the window where its deadline (slot + `sla_hours`) falls, never when the slot first appears: a load due at 08:00 with an 8-hour SLA is not judged by a run at 09:00. T1\_VOLUME likewise judges each slot once, in the window where its load period ends, so a load is never split across windows; slots with no rows give no T1\_VOLUME result (T1\_ZERO\_ROWS reports them). Checks that need `load_time` are DID\_NOT\_RUN / invalid\_config when it is not set.

**Landing registry.** At the start of `dre run`, the landing roots of every file-pattern feed being run are listed (recursively, matching `file_name_pattern`, skipping hidden files whose names start with `.` or `_`). A `dq_file` row is appended for each new file, and for each known file whose size or modified time changed; `first_seen_at` stays the time DRE first saw the path. `dre dry-run` lists but does not register.

**Hop checks** (datasets with `key_map` to an upstream dataset):

| Check id | Logic | FAILED when |
| --- | --- | --- |
| HOP\_FILE\_COMPLETENESS | For files loaded upstream in the window, compare row counts per `file_name_column` upstream and here | A file has rows upstream and none here, or fewer here than upstream after `feed_filter` |
| HOP\_KEY\_CURRENCY | For each upstream key, take the latest record time (keeping every row tied at it); left-join to this dataset on the mapped, normalised key. States: MISSING (no row here), STALE (record time here is older), CURRENT | Any MISSING or STALE key older than `sla_hours`; each written to `dq_key_event` |
| HOP\_VALUE\_AGREEMENT | For CURRENT keys, compare columns listed in `owned_columns` (upstream column to this column) with null-safe equality | Any mismatch |

Add `owned_columns` to the dataset config when HOP\_VALUE\_AGREEMENT is wanted, for example `{enddate: mbr_mbrshp_covrg_end_dt}`.

Hop details:

- **A hop** is this dataset and one upstream in its `key_map`; each hop check writes one result per upstream (group `upstream`). Keys on both sides are built from the mapped columns with each side's `key_normalise`, as one string (parts joined with `|`, null written as `<null>`). Each side's `feed_filter` applies. Both datasets need `record_time` and the upstream needs `load_time`; otherwise DID\_NOT\_RUN / invalid\_config.
- **Keys judged.** An upstream key is judged once, in the window where its latest load time plus `sla_hours` falls (so a key younger than the SLA is never judged); a key with a row in this dataset loaded in the window is judged again once its upstream deadline has passed, so a key that regresses here without any upstream change (for example overwritten with an older version) is caught; and every key still open in `v_open_keys`. On `full_sweep_day`, every upstream key past its deadline. HOP\_FILE\_COMPLETENESS judges files the same way, by each file's first upstream load time.
- **Ties.** Every upstream row at a key's latest record time is kept. HOP\_VALUE\_AGREEMENT requires each of them to match this dataset's latest row, so tied upstream rows that disagree with each other always fail.
- **Population.** HOP\_KEY\_CURRENCY: keys judged. HOP\_VALUE\_AGREEMENT: judged keys that are CURRENT. HOP\_FILE\_COMPLETENESS: files judged; `detail` lists the short files (up to 100).
- **Key events.** HOP\_KEY\_CURRENCY appends to `dq_key_event`: FLAGGED (newly MISSING or STALE), STILL\_FLAGGED (open and still not CURRENT), CLEARED (open and now CURRENT; on the full sweep also open keys no longer upstream, with `state_detail` "no longer upstream"). `state_detail` holds MISSING, STALE or CURRENT. `key_hash` is computed in SQL by a function registered with the secret in its closure, so the secret never appears in SQL text or query plans. Without `hmac_secret_file` no events are written and a FAILED result says so in `detail`; a configured secret file that cannot be read, or is empty, stops `dre run` before it starts (exit 3).

**Gold rule templates** (one rule file per use):

| Template | Params | Violation |
| --- | --- | --- |
| max\_rows\_per\_key | max | Key with more than `max` rows |
| max\_open\_rows\_per\_key | open\_when, max | Key with more than `max` rows matching `open_when` |
| column\_order | lower, upper, format | Row where `upper < lower` (nulls ignored unless `nulls_fail: true`) |
| superseded\_still\_open | group\_key, order\_column, open\_when, format | Row matching `open_when` while a row in the same group has a later `order_column` |
| child\_within\_parent | parent\_dataset, join, child\_start, parent\_start, parent\_end, format | Child row whose start falls outside its parent's window |
| value\_format | column, pattern | Non-null value not matching the regex |
| null\_rate\_max | column, max\_rate | Group whose null rate exceeds `max_rate` |

Each rule is one check: `check_id` is the rule id and results carry the rule's `severity`. It runs over the whole table after the dataset's `feed_filter`, one result per the rule's `group_by` group, when its `frequency` makes it due, whether or not its feed is due: `every_run` on every run; `daily` on the first run at or after `rule_run_at` each day; `weekly` on the first run at or after `rule_run_at` on the dataset's `full_sweep_day`. A rule whose last result was held by a PLATFORM or BUDGET reason is tried again on the next run. `retired` rules do not run. Population: keys (max\_rows\_per\_key, max\_open\_rows\_per\_key), rows with both values (column\_order; every row when `nulls_fail`), rows matching `open_when` (superseded\_still\_open), child rows with at least one matching parent (child\_within\_parent), non-null values (value\_format), rows (null\_rate\_max). Rules with `status: proposed` run and record results but are listed as "report only" in the email. Only `approved` rules count toward alerts. The first result of a rule is its baseline; after that the email shows change from the previous run.

Example template (`max_open_rows_per_key.sql.j2`):

```sql
SELECT {% for g in group_by %}{{ g }} AS g_{{ loop.index }},{% endfor %}
       COUNT(*) AS population,
       SUM(CASE WHEN open_rows > {{ params.max }} THEN 1 ELSE 0 END) AS violations
FROM (
  SELECT {{ key_expr }}{% for g in group_by %}, {{ g }}{% endfor %},
         SUM(CASE WHEN {{ params.open_when }} THEN 1 ELSE 0 END) AS open_rows
  FROM {{ table }}
  {% if feed_filter %}WHERE {{ feed_filter }}{% endif %}
  GROUP BY {{ key_expr }}{% for g in group_by %}, {{ g }}{% endfor %}
) k
{% if group_by %}GROUP BY {% for g in group_by %}{{ g }}{% if not loop.last %}, {% endif %}{% endfor %}{% endif %}
```

`key_expr` applies `key_normalise` (for example `regexp_replace(sub_id,'^0+','')`). Never use positional `GROUP BY 1, 2`; Hive rejects it.

## 7. Cause checks

When a check FAILS, the cause engine runs every cause check listed for that failure type, in order, and writes one `dq_cause_result` row per cause check. The cause is the first CONFIRMED. If none is confirmed, the cause is "not proven" and the email lists what was ruled out and what was NOT\_READY. A cause check whose parameters are missing or null returns NOT\_READY, never RULED\_OUT.

**Generic probes.** Every cause check is a probe. The engine knows a fixed set of generic probe types and nothing about any source:

| Probe | Parameters | Confirmed when |
| --- | --- | --- |
| `file_exists` | `path` | The file exists |
| `file_value_compare` | `path`, `extract_regex`, `compare_to: partition \| window` | The value read from the file (first regex group) is later than the failing file's partition, or outside the event window |
| `log_contains` | `path_glob`, `pattern` | A log file matching the glob has a line matching the pattern that names the failure (file or key) |
| `table_contains` | `table`, `condition` (optional `id`, `code_ref`) | The table has rows matching the condition for the failure's file or key |
| `size_changed` | none | The file's size changed after it was first seen (`dq_file`) |
| `builtin` | none | Engine logic over DRE's own evidence (tables, `dq_file`, dataset config) |

Each pattern's YAML lists, per failure type, the checks it covers and the cause entries in order: `{code, probe, params}`, where `params` names a key in the feed's `probes:` that supplies the parameters, and the last entry is the fallback. A feed configures its probes by key; a key may hold one parameter set or a list (any one confirming confirms the cause). `dre validate` checks every key against the feed's pattern and the probe type's parameters. Adding a cause never touches the engine; a new kind of evidence is a new generic probe type.

```yaml
# feed config
probes:
  load_hold_marker: { path: /data/ctl/example_feed/hold.flag }
  partition_cursor: { path: /data/ctl/example_feed/cursor.prm, extract_regex: "(\\d{8})", compare_to: partition }
  load_log: { path_glob: /data/logs/example_feed/*.log, pattern: "ERROR" }
  filter_rules:
    - { table: curated_db.enrollment, condition: "status = 'TEST'", id: drop_test_members, code_ref: "load.sql:120" }
  rejects: { table: ops_db.rejects, condition: "reason IS NOT NULL" }
```

**File not loaded** (T1\_FILES\_NOT\_LOADED, T1\_ON\_TIME on file patterns)

| Order | Cause code | Confirmed when | Probe (`params` key) |
| --- | --- | --- | --- |
| 1 | RAW\_LOAD\_HELD | A load-hold marker file exists | `file_exists` (`load_hold_marker`) |
| 2 | PIPELINE\_STALLED | No file first seen after this one has rows in raw either | `builtin` |
| 3 | SKIPPED\_BEHIND\_CURSOR | The partition named in the cursor file is later than this file's folder | `file_value_compare`, `compare_to: partition` (`partition_cursor`) |
| 4 | INCOMPLETE\_AT\_LOAD | The file's size changed after it was first seen | `size_changed` |
| 5 | UNREADABLE | The file's header can't be read with the reader for `file_format` | `builtin` |
| 6 | LOAD\_ERROR | The job log mentions the file with an error | `log_contains` (`load_log`) |
| fallback | PASSED\_OVER | Later files loaded; no cause confirmed |  |

**Rows missing between layers** (HOP\_FILE\_COMPLETENESS)

| Order | Cause code | Confirmed when | Probe (`params` key) |
| --- | --- | --- | --- |
| 1 | NOT\_RUN | Nothing loaded into this dataset after the upstream rows arrived | `builtin` |
| 2 | FILE\_SKIPPED | No rows from that file here at all, while later files did load (file patterns) | `builtin` |
| 3 | INVALID\_KEY | The upstream row has a null or unparseable key column | `builtin` |
| 4 | FILTERED | The upstream row matches a configured filter rule | `table_contains` (`filter_rules`) |
| 5 | REJECTED | The row is in the rejects table | `table_contains` (`rejects`) |
| fallback | DROPPED | No cause confirmed |  |

**Key missing or stale** (HOP\_KEY\_CURRENCY, HOP\_VALUE\_AGREEMENT)

| Order | Cause code | Confirmed when | Probe (`params` key) |
| --- | --- | --- | --- |
| 1 | NOT\_RUN | No row in this dataset has a load time after the upstream record's load time | `builtin` |
| 2 | KEY\_MISMATCH | The key matches once a column in `mismatch_probe_drop` is ignored | `builtin` (NOT\_READY without `mismatch_probe_drop` on the dataset) |
| 3 | TIE\_RESOLVED\_BY\_RULE | Several upstream rows share the latest record time, and applying `winner_rule` picks a row other than the one expected (for example the open row over a termination) | `builtin` (NOT\_READY without `winner_rule`) |
| 4 | OLDER\_VERSION\_WRITTEN\_LATER | STALE only: this row's load time is later than the newer upstream record's load time, compared at `load_time.granularity` | `builtin` |
| 5 | FILTERED | Matches a configured filter rule | `table_contains` (`filter_rules`) |
| 6 | REJECTED | In the rejects table | `table_contains` (`rejects`) |
| fallback | MERGE\_NOT\_APPLIED | No cause confirmed |  |

**Load wrote nothing** (T1\_ZERO\_ROWS)

| Order | Cause code | Confirmed when | Probe (`params` key) |
| --- | --- | --- | --- |
| 1 | NO\_UPSTREAM\_DATA | The upstream dataset also had zero rows in the window | `builtin` (needs `upstream`) |
| 2 | WRONG\_PARTITION | Rows were written to a partition other than the one named in the cursor file | `file_value_compare`, `compare_to: partition` (`partition_cursor`) |
| fallback | EMPTY\_LOAD | No cause confirmed |  |

A filter rule is one `table_contains` parameter set under `filter_rules`: `table` and `condition` (SQL), with optional `id` and `code_ref` (file and line, or commit).

## 8. Command line, email and watchdog

| Command | What it does | Exit code |
| --- | --- | --- |
| `dre validate [--conf DIR]` | Static validation of all config | 0 valid, 1 errors |
| `dre dry-run --feed F` | Runs preconditions and checks for one feed, prints results, writes nothing | 0 always, unless the command itself fails |
| `dre install --print\|--apply\|--check [--conf DIR]` | `--print` renders the store DDL for the configured dq database; `--apply` creates missing tables and views (all IF NOT EXISTS, safe to rerun); `--check` compares the existing tables and views with the DDL | 0 done or matching, 1 differences found or apply failed, 3 the database does not exist |
| `dre run [--feed F] [--execution-type NORMAL\|RERUN\|REPLAY]` | Scheduled every hour. Evaluates only feeds with something due: a cadence slot's deadline (slot + `sla_hours`) passed since the feed's last evaluated window, or new or changed landed files. `--feed F` evaluates F even when nothing is due. Then preconditions, checks, causes, append results, send email | 0 run completed (even with FAILED checks), 2 partial, 3 could not start (including: the store is not installed; the message names `dre install`) |
| `dre trace --dataset D --key "k1,k2,..."` | Prints one line per layer along the `upstream` chain: present or not, record time, load time, file; ends with the first layer where the key is absent or older, then that hop's cause line | 0 |
| `dre watchdog` | Checks the last expected run exists and wrote every expected check; emails if not | 0 healthy, 1 alert sent |

**Email digest**, one per run, plain text, in this order:

1. Subject: `DRE <env>: <n> new failures, <m> checks did not run` (or `all clear`)
2. Checks that changed state since the last run, then DID\_NOT\_RUN checks, each with dataset, check, observed versus expected, and the cause line: `cause: FILE_SKIPPED (confirmed)` or `cause not proven: ruled out NOT_RUN, INVALID_KEY; not ready FILTERED`
3. Still-failing checks, one line each with the date first failed
4. Proposed rules, under "report only"
5. Passing checks, as a single count

Recipients come from each owner (a feed's, a table-wide dataset's or a rule's): `recipients` in `defaults.yaml` maps each owner to its addresses.

**Scheduling.** `dre run` is scheduled hourly; there is no per-feed schedule. Each run decides which feeds are due (see the command table) and skips the rest, writing nothing for them; a skipped feed's window simply carries on to the next run that evaluates it. A feed's `check_delay_minutes` (default 0) moves its window end back, so a slot is judged only once its deadline plus the delay has passed. Table-wide datasets run when a due feed's dataset shares their table, and every run when no feed dataset covers their table.

**Watchdog.** A separate entry point with no dependency on the main run's code beyond the store schema. The expected run times are every hour. It alerts when no `dq_run` row exists for an expected hour plus grace, when a run's latest row is still STARTED after the grace, or when `checks_written < checks_expected`. Schedule it separately from the main run (a different scheduler folder or host), so one scheduling failure doesn't stop both.

**Compute budget.** Each dataset has `compute_budget_minutes`. Before running, estimate the scan size from partition statistics; if a check exceeds its budget, cancel it and record DID\_NOT\_RUN / budget\_exceeded. Hop checks read only keys changed in the window, plus a full sweep on the day set by `full_sweep_day` (default Sunday).

## 9. Testing

All tests run on local-mode Spark against synthetic tables built by fixtures in `tests/fixtures/`. Fixtures copy the quirks of the real layers so the logic is exercised honestly:

- subscriber ids zero-padded in gold, unpadded upstream
- raw dates as MM/DD/YYYY, curated and gold as YYYY-MM-DD
- curated keeps several versions per key, including ties at the same record time
- gold load times minute-granular, in the format `yyyy-MM-dd HH:mm:ss:000000`
- one raw table holding two feeds, told apart by file name
- landing files as small local files, including a sequence file

**Replay scenarios.** Each is a test that builds the situation, runs `dre run`, and asserts the check state and the cause. Each is the shape of a real past incident, with no real data. Every scenario that applies also runs against the second synthetic feed (`provider_roster_monthly` and `provider_directory_merge` in `conf/`), which looks nothing like the first: monthly CSV in a flat folder, no partitions, UTC times, a single-column key, and a TABLE\_MERGE gold table. CI also runs `dre install` and `dre run` end to end, as separate processes, against both synthetic feeds (`tests/e2e/`).

| ID | Situation built | Expected check | Expected cause |
| --- | --- | --- | --- |
| R01 | A landed file has no raw rows; later files did load | T1\_FILES\_NOT\_LOADED FAILED | PASSED\_OVER (not proven) without inputs; SKIPPED\_BEHIND\_CURSOR when the handoff file is configured |
| R02 | No files loaded for longer than the SLA | T1\_ON\_TIME FAILED | PIPELINE\_STALLED, or RAW\_LOAD\_HELD when the stopper file exists |
| R03 | Gold load wrote zero rows while upstream had rows | T1\_ZERO\_ROWS FAILED | WRONG\_PARTITION when rows landed in another partition |
| R04 | A message is in raw and not in curated; its file's other rows loaded | HOP\_FILE\_COMPLETENESS FAILED | DROPPED (not proven) with FILE\_SKIPPED and NOT\_RUN ruled out |
| R05 | Curated latest is a termination; gold is older | HOP\_KEY\_CURRENCY FAILED (STALE) | NOT\_RUN or MERGE\_NOT\_APPLIED, depending on gold load times |
| R06 | Curated has an open row and a termination tied at the latest record time; gold is open | HOP\_VALUE\_AGREEMENT FAILED | TIE\_RESOLVED\_BY\_RULE with winner rule `sourcelastupdatets DESC, enddate DESC` |
| R07 | An older version was loaded into gold after a newer one reached curated | HOP\_KEY\_CURRENCY FAILED (STALE) | OLDER\_VERSION\_WRITTEN\_LATER |
| R08 | Duplicate rows per key in gold | T1\_KEY\_DUPLICATES FAILED | none required |
| R09 | A configured column is renamed in the table | Checks using it are DID\_NOT\_RUN / column\_missing; run continues | n/a |
| R10 | An older coverage stays open while a newer one exists | Rule superseded\_still\_open FAILED, grouped by source | n/a |
| R11a | Empty tables, no load due in the window | Every check DID\_NOT\_RUN: empty\_population (T1\_SCHEMA\_DRIFT: insufficient\_history on its first run); never PASSED | n/a |
| R11b | Empty tables, a load due in the window | T1\_ON\_TIME and T1\_ZERO\_ROWS FAILED; row-based checks DID\_NOT\_RUN / empty\_population; never PASSED | n/a |
| R12 | Main run never happens | Watchdog alerts | n/a |

**Guard tests.** Tests in `tests/guard/` scan every file under `src/` (scanner: `tests/guard/scan.py`) and fail on any breach of these rules. Each rule has a snippet test that is caught and one that is allowed. The reasons are in `docs/decisions.md`.

1. **Naming the dq store.** SQL names the store as the literal `dq`, or as `{{ dq_database }}` (Jinja) or `{dq_database}` (Python f-string), because the real name comes from config. A write target the scanner cannot read (for example one built by string concatenation) fails. The configured dq database name must be a plain identifier (`store/names.py`, `validate_dq_database`).
2. **Read-only outside dq.** Flagged unless the target is the dq store: INSERT INTO; `insertInto` and `writeTo`; DataFrameWriter path writes (`.save`, `.orc`, `.parquet`, `.csv`, `.json`, `.text` on a `.write` chain); Hadoop FileSystem `delete`, `rename`, `mkdirs`; `os.remove`, `unlink`, `rmdir`, `removedirs`, `rename`, `replace`; `shutil.rmtree`, `shutil.move`. The dq store is addressed by table name, so a path target never qualifies. `hdfs dfs` / `hadoop fs` `-rm`, `-rmr`, `-rmdir`, `-mv` and `-mkdir` are always flagged.
3. **Append-only.** The only DML allowed on dq is INSERT INTO (or `insertInto` in append mode). Banned everywhere, dq included: UPDATE, DELETE, MERGE, TRUNCATE, INSERT OVERWRITE, `mode("overwrite")`, `overwrite=True`, `saveAsTable`, `writeTo(...)` followed by `overwrite`, `overwritePartitions`, `create`, `replace` or `createOrReplace`, DROP TABLE/VIEW/DATABASE, and every ALTER except rule 5.
4. **One write path.** The DataFrame write APIs (`.write`, `.writeTo`, `.writeStream`, `insertInto`) are allowed only in `store/writer.py`. Any `.write` elsewhere is flagged, including `w = df.write` split across statements and plain file `.write(...)` calls. `append` selects columns in the table's order before `insertInto`; a test fails if a column lands out of order.
5. **Retention.** ALTER TABLE `<dq>.<table>` DROP [IF EXISTS] PARTITION is allowed only in `store/retention.py`. That module refuses to drop any `run_date` partition on or after today minus the configured retention in months, and refuses a retention below one month; a test proves it.
6. **Database creation.** CREATE DATABASE is allowed only for the dq database and only in `store/local_setup.py`. No other module under `src/` may reference `local_setup`, so `dre run` never creates the database.
7. **Email.** Email text contains no `key_value`. Until the digest exists (step 9) the guard is static: nothing in `notify/` references `key_value`. Step 9 adds a test on the rendered email text.
8. **Store DDL only through `dre install`.** CREATE TABLE and CREATE [OR REPLACE] VIEW are allowed only on the dq store and only in `store/ddl.sql`. Only `store/install.py` (behind `dre install --apply`) and `store/local_setup.py` may reference `apply_ddl`, so `dre run` never creates the store.
9. **Generic engine.** Engine code, the config schema, pattern YAML and SQL templates (everything under `src/`) must not contain source-specific words. The denylist lives in `tests/guard/test_generic_engine.py` (stopper, handoff, file\_date, sub\_id, rms) and grows whenever a source-specific name is found; such words may appear only in `conf/` and `tests/`.

## 10. Build order

Build in this order. Each step ends with its tests passing and is usable before the next starts.

1. **Skeleton.** Repository layout, `CLAUDE.md` with the hard constraints, local Spark test setup, guard tests. Done when the guard tests run in CI.
2. **Configuration.** Pydantic models, loader with defaults and overrides, `dre validate`. Done when valid sample config passes and each error type gives a file, line and fix.
3. **Store.** DDL, append-only writer, views, `dq_run` bookkeeping. Done when a run with no checks writes a `dq_run` row and the views read back correctly.
4. **Check framework.** Base class, preconditions, denominator rule, result building, Jinja2 rendering. Done when R09 and R11 pass.
5. **Tier 1 checks.** The six table-based Tier 1 checks: T1\_ON\_TIME, T1\_ZERO\_ROWS, T1\_VOLUME, T1\_SCHEMA\_DRIFT, T1\_KEY\_NULLS, T1\_KEY\_DUPLICATES. T1\_FILES\_NOT\_LOADED comes in step 6 with the `dq_file` registry it needs. Done when R02, R03 and R08 pass at check level (causes come in step 8).
6. **Landing.** HDFS listing adapter (local filesystem in tests), `dq_file` registry, T1\_FILES\_NOT\_LOADED. Done when R01 passes at check level.
7. **Hop checks and gold rules.** HOP\_FILE\_COMPLETENESS, HOP\_KEY\_CURRENCY, HOP\_VALUE\_AGREEMENT, all seven rule templates. Done when R04 to R07 and R10 pass at check level.
8. **Cause engine.** Ordered cause checks per pattern from YAML, NOT\_READY handling. Done when every replay scenario passes with its expected cause.
9. **Email and trace.** Digest builder and `dre trace`. Done when a golden-file test of the email passes and trace output matches for R05.
10. **Watchdog and budgets.** Done when R12 passes and a deliberately slow check ends DID\_NOT\_RUN / budget\_exceeded.
11. **Packaging.** Wheel and dependency zip; a `spark-submit` command documented in the README.

**Release 1 is done when:** all replay scenarios pass; a second synthetic feed with a different pattern is added with configuration only; and, later in a test environment, the RMS configuration runs unattended for two weeks.

## 11. Open decisions and first configuration

**Decide before step 1**

| Decision | Default in this spec | Who confirms |
| --- | --- | --- |
| Language | PySpark 3.5.1 | Team: what you can maintain; platform: whether Python jobs can be submitted |
| Python version and packages on the cluster | Python 3.10+, dependencies shipped in a zip | Platform team |
| Where it runs and is scheduled | `dre run` hourly (Control-M or cron); the watchdog hourly from a separate folder or host | Scheduling owner |
| Database name and service account | `dq`, read-only account with write only to `dq` | Platform team |
| Use of AI coding tools on HCSC code | Only through HCSC-approved access | HCSC policy |

**Decide before RMS goes live** (do not block the engine build)

| Item | Status |
| --- | --- |
| Curated winner rule across loads | Within one load confirmed (`sourcelastupdatets DESC, enddate DESC`); across loads: developer question 2 |
| Gold merge clause and dedupe | Developer question 3 |
| CDC table and key | Not confirmed; developer question 4 |
| Run logs location | Developer question 5 |
| Filter rules and rejects table | Developer question 6 |
| Plan join for the gold key | Query 4 |
| Rule baselines | Queries 5 to 7 |

**First configuration (RMS)**

The real RMS values (landing path, stopper and handoff file names, database and table names) are kept with the deployed instance's configuration, not in this repository. The shape the engine must support:

- One feed, pattern FILE\_CYCLIC: sequence files; six loads a day on an every-day calendar. Its source-specific cause inputs are probe configs (section 7), with the real paths kept in the deployed config:
  - the stopper file that holds the raw load: `probes.load_hold_marker: { path: <stopper file> }` (`file_exists`, for RAW\_LOAD\_HELD);
  - the handoff file naming the partition passed between phases: `probes.partition_cursor: { path: <handoff file>, extract_regex: <partition pattern>, compare_to: partition }` (`file_value_compare`, for SKIPPED\_BEHIND\_CURSOR and WRONG\_PARTITION);
  - the job log: `probes.load_log: { path_glob: <log glob>, pattern: <error pattern> }` (`log_contains`, for LOAD\_ERROR);
  - filter rules and the rejects table: `probes.filter_rules` and `probes.rejects` (`table_contains`, for FILTERED and REJECTED), once developer question 6 is answered.
- Three datasets: raw (dates MM/DD/YYYY, partitioned by file date, real-time rows told apart by file name); curated (several versions per key, its own load time and record time); gold (four-part key with a zero-padded subscriber id, minute-granular load time, `feed_filter` on source system).
- Three rules: `one_row_per_coverage` (max\_rows\_per\_key), `end_not_before_start` (column\_order), `older_coverage_still_open` (superseded\_still\_open, grouped by source system).
