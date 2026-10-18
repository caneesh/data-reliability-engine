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

**Release 2 (added October 2026, after the architecture review):** derived datasets, so a hop can compare at a grain other than the upstream's rows: an aggregate (one gold row per group of upstream rows) or a join (upstream rows looked up against another dataset). Section 3 ("Derived dataset") and section 6 ("Derived upstreams"). Still out: one-to-many hops (one upstream row to several target rows), and a derived dataset as a downstream.

**Release 3 (extensibility, October 2026; `docs/design/extensibility.md`):** the engine's capabilities (checks, cause probes, builtin causes, gold rule templates, key normalisers, calendars, notification sinks) register themselves where they are defined, so adding one is adding a module (section 2, "Extension points"). Check parameters, patterns and rule templates from configuration (section 3), named calendars (section 3), notification sinks (section 8) and extension packages (section 2) are built; `docs/extending.md` is the how-to, and `docs/code-review-2026-10-13.md` the review that followed.

**Release 4 (outside-in evidence, October 2026; `docs/design/outside-in-evidence.md`):** read-only checks that see what an in-pipeline audit sees from inside the jobs, without touching a job. Built so far: T1\_PARTITION\_DRIFT (section 6; a partition rewritten outside the window, detected from each partition's files and confirmed by counting that partition alone; its state in `dq_partition_state`, section 5). Planned: T1\_FILE\_RECORD\_COUNT, T1\_SEQUENCE\_GAP, HOP\_ROW\_PARITY and HOP\_CARDINALITY. Still out, by nature: the reason a job itself rejected a record, and a merge's inserted-versus-updated split.

**Phase 1 design (approved and frozen 2026-10-16; `docs/design/target-architecture.md`, `docs/design/reconciliation-model.md`):** every hop gets a reconciliation contract, and the engine will account for every due record in exactly one disposition. Built in steps (the architecture's section 7). Built so far: step 1, the contract schema and its validation: `grain` and `contracts` on a dataset, `yarn_queue` and `protected_queues` in `defaults.yaml` (section 3); hop checks read a hop's exclusions through its contract (section 6); `dre validate` reports each hop's kind and blind spots (section 3, "Validation"). Step 2, commit rows: an evaluation or a cause investigation becomes visible only once its `dq_commit` rows are stored, after the rows they vouch for were counted, and every reader reads committed rows (section 5, "Commit protocol"; revised after the checkpoint 2 review). Corrections R1 to R3: evidence states (VALID, INVALID, EXPIRED, UNKNOWN; a result is healthy only when PASSED with VALID evidence), the integrity sweep and `dq_integrity`, evidence lifetimes and investigation holds, confirmed commit repair, causes joined on the result's run date. Step 3, the ledger: HOP\_LEDGER puts every due upstream record of a hop in exactly one disposition, applies the volume guards, the reject policy (with a declared `rejects` source), the deletion state and the tolerance, and records each ledger in `dq_reconciliation` under the result's commit; every result carries its scope (section 6, "Hop checks"; section 5). Not built yet: evaluation modes, attribution and contribution checks, the chain, run budgets.

## 2. Stack and repository layout

**Default stack** (confirm against the cluster; see section 11):

- Python 3.10+, PySpark 3.5.1 (matches the cluster's Spark 3.5.1)
- PyYAML for config, Pydantic v2 for schema validation, Jinja2 for SQL templates
- pytest with local-mode Spark for all tests
- Packaged as a wheel plus a zip of dependencies for `spark-submit --py-files` (as built: the package zip goes in `--py-files`; pydantic has a compiled core, so the dependencies ship as a zip of wheels installed on the driver host, and runs use client deploy mode; README)
- No other runtime services: no database server, no web server, no message queue

**Names.** Repository `data-reliability-engine`. Python package `hcsc.datalake.dre` under `src/` (`hcsc` and `hcsc/datalake` are namespace packages with no `__init__.py`, so other HCSC data lake tools can share the prefix). Distribution `hcsc-datalake-dre`. Command `dre`.

All check logic is SQL rendered from templates and run through `spark.sql`, so the same logic runs on the cluster's Hive tables and on local test tables.

**Extension points** (release 3). Seven kinds of capability each have one registry (`registry.py`, class `Registry`): checks (`checks/registry.py`, `register_check`), cause probes (`config/probes.py`, `@probe(name, params_model)`), builtin cause checks (`causes/base.py`, `@builtin(code)`), gold rule templates (`checks/rules/template.py`, `register_template(TemplateSpec)`), key normalisers (`checks/keys.py`, `register_normaliser`), calendars (`checks/calendars.py`) and notification sinks (`notify/sinks.py`, `@sink(kind, config_model)`). A capability registers itself in the module that defines it; nothing central lists them. The built-in ones are found by importing the engine's own packages on first use (`checks/tier1`, `checks/hop`, `causes`, `checks/rules/templates`). An id registered twice is an error, because check ids go into `evaluation_id` and cause codes into `dq_cause_result`. A feed's `pattern` must be a packaged pattern or one under `conf/patterns`, a rule's `template` a registered template, a `key_normalise` entry a registered normaliser, a `cadence.calendar` a registered calendar, a `notify` entry a registered sink kind; `dre validate` reports anything else with the known names in its fix. Pattern YAML is checked against the registries when loaded: an unknown check, probe type or builtin cause code is an error. A check may declare a params model (`params_model`, section 3) and may list its own failing things for the cause engine (`failures`, section 7). Every capability keeps the hard constraints: a check writes exactly PASSED, FAILED or DID\_NOT\_RUN through `from_counts` or `did_not_run`; a probe is NOT\_READY without its parameters and never raises out of the engine; a template's SQL returns `population` and `violations`.

**Extension packages** (release 3). A separately installed package may add capabilities of any of these kinds, and notification sinks (section 8) and calendars (section 3): it registers them the way the engine's modules do and declares one entry point in the group `dre.extensions` whose value is the module to import. Nothing loads unless `defaults.yaml` lists the entry point's name under `extensions:`; a name that is not installed, a module that fails to import, or an id that collides with an existing one is a config error naming `defaults.yaml`. `dre validate` prints what each extension registered. An extension is held to the hard constraints by `dre guard <path>` (section 9) in its own CI, and to the capability contracts by the testing kit `hcsc.datalake.dre.testing` (`check_contract`, `probe_contract`, `template_contract`). The kit holds no table builders, because the engine's source may not create or drop tables; an extension builds its synthetic tables itself. `docs/extending.md` is the how-to.

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
          cli.py             # validate, install, dry-run, run, trace, watchdog, retention, guard
          session.py         # the Spark session (active one, or a new one with Hive support)
          runner.py          # dre run and dre dry-run: plan, evaluate, append results, run rows
          registry.py        # the Registry class: capabilities register themselves; built-ins found by import
          extensions.py      # extension packages: entry points in the group dre.extensions, loaded when listed
          guard.py           # the read-only and append-only scanner, behind `dre guard` and the guard tests
          testing.py         # the testing kit: the contracts a check, probe or template must meet
          config/
            models.py        # Pydantic models for feeds, datasets, rules, defaults
            loader.py        # load YAML, apply defaults and overrides
            validate.py      # static and runtime validation
            fragments.py     # dry-run: parse SQL fragments from config against Spark
            sqlcheck.py      # the words configuration SQL may not use (section 9, rule 10)
            probes.py        # the probe registry (type name, params model, function) and readiness
          patterns/
            file_cyclic.yaml # checks and cause checks per pattern
            file_periodic.yaml
            table_merge.yaml
            table_wide.yaml  # checks for table-wide datasets (no feed)
          checks/
            rules/schedule.py  # when a gold rule is due (frequency)
            rules/template.py  # TemplateSpec and the template registry; rules/templates/: one module per template
            rules/conf_template.py  # templates from conf/templates: typed params, sandboxed SQL, the query-only guard
            base.py          # Check interface, result building, denominator rule, preconditions, run_check
            budget.py        # compute budget: a check in its own Spark job group, cancelled when over
            keys.py          # key expressions with key_normalise applied; the normaliser registry
            times.py         # time columns to UTC; window bounds truncated to granularity
            cadence.py       # cadence slots (expected loads) in UTC
            calendars.py     # EVERYDAY, WEEKDAYS and the named calendars from conf/calendars
            events.py        # batch load events, event_id and evaluation_id
            derived.py       # derived datasets (release 2): the relation read in place of a table
            registry.py      # the check registry; which checks run, from patterns/*.yaml
            tier1/           # one module per Tier 1 check
            hop/             # hop checks: common.py (the two sides of a hop, judged keys, key_hash), one module per check
            rules/           # rule_check.py: each gold rule as a check over its TemplateSpec
            sql/             # Jinja2 SQL templates
          causes/
            engine.py        # runs cause checks in order, writes dq_cause_result rows, resolves the cause
            base.py          # outcomes, failures, the cause context and the builtin-cause registry
            failures.py      # the failing things of a FAILED check (files, slots, keys)
            probes.py        # the generic probe types
            landing.py merge.py  # builtin cause checks by hop type
          store/
            ddl.sql          # dq tables and views
            schema.py        # renders ddl.sql for the configured dq database; parses the objects it creates
            install.py       # dre install: print, apply or check the DDL; the only path that applies it
            runs.py          # dq_run bookkeeping: STARTED row at start, final row at end
            writer.py        # append-only writes; the only DataFrame write path
            results.py       # appends check results through the writer
            key_events.py    # appends hop key events to dq_key_event through the writer
            causes.py        # appends cause results to dq_cause_result through the writer
            files.py         # dq_file registry: appends new and changed landed files
            retention.py     # drops expired run_date partitions; the only DROP PARTITION
            local_setup.py   # creates the dq database for tests and local runs only
            names.py         # identifier validation for dq names
          sources/
            hdfs.py          # listing landing folders, read-only, through the Hadoop FS API
          notify/
            digest.py        # digest builder: one digest per owner per run, from the dq store
            digest.txt.j2    # the digest's plain-text layout
            email.py         # sender (SMTP, standard library)
            sinks.py         # notification sinks: email, stdout; the sink registry and `notify:`
          trace.py           # dre trace: one key along the upstream chain
          watchdog.py        # dre watchdog: reads dq_run only; no dependency on the run's code
  scripts/
    package.py               # builds the wheel, the --py-files zip and the dependency wheels into dist/
    dre_main.py              # the spark-submit entry point
  conf/
    defaults.yaml
    feeds/  datasets/  rules/
    patterns/                # optional: patterns of this instance; a packaged name replaces that pattern (section 3)
    templates/               # optional: gold rule templates of this instance, SQL in YAML (section 3)
    calendars/               # optional: named calendars (weekdays only, holidays) for cadences (section 3)
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
  calendar: EVERYDAY              # EVERYDAY | WEEKDAYS | a named calendar from conf/calendars (release 3)
sla_hours: 8
datasets: [rms_raw_enrollment, rms_curated_enrollment, gold_member_coverage]
check_delay_minutes: 0            # evaluate a slot this long after its deadline (default 0)
probes:                           # cause probe parameters by key (section 7); missing or null: NOT_READY
  load_hold_marker: { path: null }
  partition_cursor: { path: null, extract_regex: null, format: null, compare_to: partition }
  load_log: { path_glob: null, pattern: null }
  filter_rules: null
  rejects: { table: null, condition: null }
email_sample_keys: false
check_params:                     # parameters of parametrised checks, for every dataset of the feed (optional)
  T1_FRESHNESS: { max_age_hours: 6 }
```

**Dataset** (a table at any layer):

```yaml
dataset: gold_member_coverage
table: gold_db.member_coverage
layer: GOLD                       # RAW | CURATED | CDC | GOLD
upstream: [rms_curated_enrollment]
feed_filter: "src_sys_nm = 'RMS'" # rows belonging to this feed, when a table holds several
key: [sub_id, mem_nbr, mbr_mbrshp_covrg_eff_dt, covrg_agrmt_id]
key_normalise: { sub_id: strip_leading_zeros }   # a registered normaliser: strip_leading_zeros, trim, upper, lower,
                                  # { parse_date: MM/dd/yyyy } (a date string read as yyyy-MM-dd), or
                                  # { regex_replace: { pattern: '-', replacement: '' } }
record_time: { column: src_lcts, format: null }  # same shape as load_time; format null when the column is DATE or TIMESTAMP
load_time: { column: gld_lcts, format: "yyyy-MM-dd HH:mm:ss:SSSSSS", granularity: minute, timezone: America/Chicago }
                                  # timezone: IANA zone the values are written in; null uses defaults.yaml `timezone`
partition_column: null
partition_format: null            # the partition value's date pattern (e.g. yyyy-MM-dd); with it, windowed scans prune to the window's partitions
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
hop_exclusions:                   # optional; per upstream, rows a business rule keeps out of this dataset:
  # rms_curated_enrollment: "status = 'TEST'"   # hop checks do not expect them here (never counted as lost)
winner_rule:                      # optional; how the merge chooses among versions
  order_by: ["sourcelastupdatets DESC", "enddate DESC"]
  # formats: { enddate: yyyy-MM-dd }  # optional: pattern of an upstream time column stored as a string;
  #                                     record_time and load_time columns use their own reading
grain:                            # optional (Phase 1 design): what one record is; its key is `key`, its version `record_time`
  history: mutable                # append_only | mutable | snapshot_partitioned; null: not declared (no as-of replay)
contracts:                        # optional (Phase 1 design): each hop's reconciliation contract, per upstream
  rms_curated_enrollment:
    mapping: union_partition      # identity | dedup | merge | union_partition | join_contribution | survivorship | aggregate | fan_out
    exclusions:                   # approved, versioned rules; replaces hop_exclusions for this upstream
      - { id: drop_test_members, version: 1, condition: "status = 'TEST'", effective: { from: 2026-09-01 },
          approved_by: CHG-0000, code_ref: null, max_share: 0.05 }
    reject_policy: { INVALID_CODE: { disposition: authorised, max_share: 0.02 } }   # unlisted reason codes are defects
    deletion:                     # how the upstream deletes a record, and what this dataset shows for it
      tombstone: "to_date(enddate, 'yyyy-MM-dd') = to_date(effectivedate, 'yyyy-MM-dd')"
      applied_as: soft            # hard: absent here | soft: present and matching soft_when
      soft_when: "to_date(mbr_mbrshp_covrg_end_dt, 'yyyy-MM-dd') = to_date(mbr_mbrshp_covrg_eff_dt, 'yyyy-MM-dd')"
    tolerance: { missing: 0, stale: 0, divergent: 0 }   # absolute counts; nothing else may be tolerated
    rejects:                      # optional (build step 3): where the pipeline's rejects for this hop are (reported evidence)
      table: example_db.enrollment_rejects     # placeholder; carries the upstream's key columns by name
      condition: "stage = 'curated'"            # optional SQL over the table: the stage that feeds this hop
      reason_column: reason_code                # default
      time_column: { column: rejected_at, timezone: UTC }   # default: when the record was rejected
    # reference: { <this column>: <contributor key column> }   join_contribution only
    # precedence: [<upstream>, ...]                            survivorship only, the winner first
compute_budget_minutes: 8
check_params:                     # optional; a parametrised check runs only where it is configured
  T1_FRESHNESS: { max_age_hours: 72, time: load }
  T1_COLUMN_RANGE: { column: mem_nbr, min: 1, max: 99 }
  T1_PARTITION_DRIFT: { tolerance_rows: 0 }   # needs partition_column and partition_format (release 4)
```

**The reconciliation contract** (Phase 1 design; `docs/design/reconciliation-model.md`). A hop (this dataset, one upstream) has one mapping kind. Without a `contracts` entry its kind comes from the release 1 fields, which stay valid as aliases: `aggregate` when the upstream is derived, `dedup` otherwise (the latest upstream version of each key), with `hop_exclusions` as one legacy exclusion (unversioned, unapproved, no volume guard: `dre validate` warns), `winner_rule` as the version rule and `owned_columns` as identity transforms. Rules:

- **Kinds compared through `key_map`** (`identity`, `dedup`, `merge`, `union_partition`, `survivorship`, `aggregate`) need a `key_map` entry; `join_contribution` and `fan_out` must not have one.
- **`aggregate`** is the kind for a derived upstream, and only for one.
- **`union_partition`** reads this dataset through its attribution, which is its `feed_filter`, so one must be set.
- **`join_contribution`** needs `reference`, mapping this dataset's columns to all of the contributor's key.
- **`survivorship`** needs `precedence`, the same list on every survivorship hop of the dataset.
- **Judged later.** `join_contribution` and `survivorship` are judged from build step 5, and until then `dre validate` warns that the hop is a blind spot and HOP\_LEDGER is DID\_NOT\_RUN / `unsupported_mapping`. `fan_out` is never judged: a warning, and DID\_NOT\_RUN / `unsupported_mapping`.
- **Exclusions** have an `id` and `version` (unique together), an `approved_by` and a `max_share` in (0, 1]. `effective.from` and `effective.to` are dates at midnight in `defaults.yaml` `timezone`, judged against the upstream row's load time. Two versions of one id may not be effective on the same day. An upstream may have exclusions under `contracts` or `hop_exclusions`, not both.
- **Reject policy.** An `authorised` reason needs `max_share`, and a `defect` takes none.
- **Deletion.** `soft_when` goes with `applied_as: soft` and only with it. Conditions are SQL fragments: the tombstone and the exclusions over the upstream's columns, `soft_when` over this dataset's. Dates held as strings are compared through `to_date(column, 'pattern')`.
- **Tolerance** may name only `missing`, `stale` and `divergent`.
- **Rejects** (build step 3) is the table of the pipeline's rejected records for the hop: reported evidence, never proof. It carries the upstream's key columns under their upstream names (normalised as the upstream's are), a reason code and when the record was rejected. Only kinds compared through `key_map` take it (an error otherwise); a `reject_policy` without `rejects` is a warning, since no record can be rejected without lineage.

Exclusions take effect in every hop check: they leave them out of what they expect (section 6). The volume guards, the reject policy, the deletion state and the tolerance take effect in HOP\_LEDGER (section 6).

**Parametrised checks** (release 3). A check may declare a parameter model (`Check.params_model`). It is listed by the patterns like any check, but runs only where something configures it: the pattern's own entry (`{check: T1_FRESHNESS, params: {max_age_hours: 6}}` in `patterns/*.yaml`), the feed's `check_params` or the dataset's, each overriding the one before field by field. `dre validate` rejects an unknown check id, parameters on a check that takes none, and bad values, with file and line; a field missing at every level is reported on the dataset. A parametrised check configured for a dataset whose pattern does not list it is a warning (it will not run). At run time the merged parameters are validated again, and a bad set makes the check DID\_NOT\_RUN / invalid\_config.

**Patterns from configuration** (release 3). A pattern is data: the checks in run order and the cause checks per failure type (section 7). The engine packages `FILE_CYCLIC`, `FILE_PERIODIC`, `TABLE_MERGE` and `TABLE_WIDE`; a deployed instance may add its own under `conf/patterns/<name>.yaml` with the same layout, and a file named like a packaged pattern replaces it. A pattern file declares:

```yaml
pattern: TABLE_RELOAD             # the file's name, upper case
kind: table                       # file: data lands as files (the feed needs `landing`, and gets T1_FILES_NOT_LOADED
                                  #   and the landing probes); table: data arrives in a table
checks: [T1_ON_TIME, T1_ZERO_ROWS, T1_VOLUME, T1_KEY_DUPLICATES, {check: T1_FRESHNESS, params: {max_age_hours: 30}}]
causes:                           # as in section 7; each failure type needs one cause check at least, then its fallback
  load_wrote_nothing: { checks: [T1_ZERO_ROWS], order: [{code: NO_UPSTREAM_DATA, probe: builtin}, {code: EMPTY_LOAD, fallback: true}] }
```

Every check, probe type and builtin cause code a pattern names must be registered (section 2, "Extension points"); `dre validate` reports a malformed pattern file by name, and says which patterns are packaged and which come from `conf/patterns`. A feed's `pattern` must be one of them; its `probes:` keys must be ones the pattern's causes use. Whether a feed lands files is the pattern's `kind`, never its name.

**Rule templates from configuration** (release 3). A gold rule template may be defined in `conf/templates/<name>.yaml` instead of code. It declares typed params, the text of its result and a Jinja SQL template:

```yaml
template: not_after               # the name rules use; a packaged template's name is refused
params:
  column: column                  # types: string, int, float, bool, column, columns, sql, format
  limit: { type: string }         # required unless `required: false` or a `default` is given
  format: { type: format, required: false }
population: rows with a value     # what the population counts
violation: rows have a value after the limit   # the observed text: "<n> of <population> rows have ..."
unit: rows                        # the expected text: "0 rows breaking rule <rule>"
sql: |                            # returns population and violations, and g_1.. for the rule's group_by
  SELECT {% for g in group_by %}{{ g }} AS g_{{ loop.index }}, {% endfor %}
         COUNT({{ params.column }}) AS population,
         SUM(CASE WHEN {{ time(params.column, params.format) }} > {{ time(literal(params.limit), params.format) }}
                  THEN 1 ELSE 0 END) AS violations
  FROM {{ table }}
  {% if feed_filter %}WHERE {{ feed_filter }}{% endif %}
  {% if group_by %}GROUP BY {{ group_by | join(', ') }}{% endif %}
```

The template sees `table`, `feed_filter`, `group_by`, `params` and five helpers: `column(name)` (with `key_normalise`), `time(name, format)` (parsed, never compared as a string), `key_expr`, `literal(text)` and `number(value)`. A param's `default` must fit its declared type, and a `sql` param's default is scanned like the template text; the rendered SQL is scanned again before it runs, so a rule's parameter value cannot smuggle a forbidden word in (the rule is DID\_NOT\_RUN / invalid\_config, naming the word). Params of type `column` and `columns` are the columns the rule reads (preconditions). The SQL renders in a sandboxed Jinja environment and always runs wrapped as `SELECT * FROM (...) dre_rule`, so a statement that is not a query is a parse error (section 9, rule 10); `dre validate` also rejects write and DDL keywords in the text, naming the file and line, and `dre dry-run` renders each such rule against its table. Rules on these templates follow the denominator rule like any other: an empty population is DID\_NOT\_RUN.

**Named calendars** (release 3). `conf/calendars/<name>.yaml` names the days a feed does not load on, so its slots are not expected (and not reported late) on them:

```yaml
calendar: US_BUSINESS             # the name a cadence's `calendar` uses
weekdays_only: true               # skip Saturdays and Sundays
holidays: [2026-01-01, 2026-01-19, 2026-07-03]
```

`EVERYDAY` and `WEEKDAYS` are packaged and cannot be redefined. A calendar applies to `times` and `interval` cadences; `calendar_dates` and `monthly` list their own days. An unknown calendar is a config error naming the known ones.

**Table-wide datasets.** A dataset that no feed lists covers a whole table, for example to run gold rules across every source system. It must set `owner` (whose recipients get its results) and `expectation_version` (it has no feed to take one from), has no `feed_filter`, and takes its settings from `defaults.yaml`. It gets the checks in `patterns/table_wide.yaml`: T1\_SCHEMA\_DRIFT, T1\_KEY\_NULLS and T1\_KEY\_DUPLICATES only, plus gold rules. A dataset that a feed lists takes its owner and expectation version from the feed and must set neither. Each dataset is listed by at most one feed.

**Derived dataset** (release 2). A dataset that has no table of its own but a query over other datasets, used only as an upstream. It declares the grain at which a downstream is compared: its `key` is the grain. The engine builds the query; expressions are SQL fragments, validated and parsed like `feed_filter`, and name columns as `<dataset id>.<column>`.

```yaml
dataset: claims_by_member_month   # virtual: never checked itself, only read as an upstream
layer: CURATED
key: [memberid, service_month]    # the grain: group_by outputs only
derived:
  from: curated_claims            # rows after its feed_filter
  joins:                          # optional; each after its own feed_filter
    - { dataset: curated_members, match: { memberid: memberid }, type: left }   # match: from column -> joined column; left | inner
  where: "curated_claims.status <> 'VOID'"                                   # optional
  group_by:                       # output name -> expression
    memberid: curated_claims.memberid
    service_month: "substr(curated_claims.servicedate, 1, 7)"
  columns:                        # output name -> aggregate expression
    claim_count: "count(*)"
    paid_total: "sum(cast(curated_claims.paidamount as decimal(18,2)))"
    plan: "max(curated_members.plan)"
```

The engine adds `record_ts` and `load_ts` (UTC): the latest record and load time of the `from` rows in each group, so `from` needs both. They are the derived dataset's `record_time` and `load_time`; it sets neither (nor `feed_filter`, `file_name_column`, `partition_column`, `upstream`, `key_map` or `owned_columns`). No feed lists it, no rule targets it, and it gets no checks: the dataset that names it in `upstream` and `key_map` is checked against it like any upstream. `from` and `joins` name table datasets (not derived ones). Join type `left` keeps rows the lookup did not match, so their absence downstream is MISSING, and JOIN\_UNMATCHED (section 7) explains it; `inner` drops them, so they are never expected. Measures are compared as strings by HOP\_VALUE\_AGREEMENT: cast sums to the target's DECIMAL scale.

**What the compute budget covers.** A check's SQL and the materialisation of its judgement run under the dataset's `compute_budget_minutes`; so does each cause investigation of a FAILED result, under what is left of the dataset's budget in that run (`compute_budget_scope: dataset`) or the full budget (`check`): an investigation that runs past it is cancelled, counted as not done and retried by the next run, exactly like one whose store write failed; a spent budget skips it the same way. The key-event write has its own fixed bound (15 minutes); a write that runs past it takes the `evidence_incomplete` path. The scan estimate a `scan_mb_per_minute` refusal uses sums a hop check's upstream tables with its own. Landing listing, store maintenance and the digest are not budgeted.

**Watchdog** (in `defaults.yaml`): `watchdog_grace_minutes` (default 30).

**Commit batches** (in `defaults.yaml`, Phase 1 design ADR-07): `commit_batch` (default 25, 1 to 1000), how many evaluations and investigations a run publishes per append of commit rows (section 5, "Commit protocol").

**Store integrity** (in `defaults.yaml`, checkpoint 2 corrections R1): `integrity_sweep_days` (default 35) and `integrity_full_sweep_day` (default SUNDAY; null: never), the integrity sweep's daily and weekly scope; `retention_holds`, investigation holds, each `{from, to, reason, until}` (dates; `until` null: until removed), which keep every dq partition with a `run_date` in `[from, to]` from `dre retention` (section 5, "Commit protocol").

**Isolation** (in `defaults.yaml`, Phase 1 design ADR-08): `yarn_queue`, DRE's own YARN queue (a platform value; null until confirmed), and `protected_queues`, the queues DRE must never run in (the production pipelines'). Both are production values, kept in the deployed configuration only. `dre validate` rejects a `yarn_queue` listed in `protected_queues`. The run budget, the kill switch and the check at run start come in build step 8.

**Partition pruning.** A windowed check filters on the parsed load time, which Spark cannot push down to a partition column. A dataset that sets `partition_column` and `partition_format` (a Spark date pattern of `yyyy`, `MM`, `dd`, `HH` and separators, at least to the day, such as `yyyy-MM-dd` or `yyyyMMdd`) gets a second predicate on every windowed scan (T1\_ON\_TIME, T1\_ZERO\_ROWS, T1\_VOLUME, T1\_KEY\_NULLS, T1\_COLUMN\_RANGE, and NO\_UPSTREAM\_DATA's upstream count): `partition_column IN (...)`, every partition value the window could touch in the load time's zone, widened by one granule on each side. It only narrows what is read; the parsed-time predicate stays, so a partition value that does not match its rows' load times costs speed, never correctness. A span listing more than 1000 partitions, a pattern the engine cannot write, or a column without a format gets no predicate (`dre validate` warns on the last). The real values are platform-confirmed (section 11); until then both stay `null`.

**Defaults for time and windows** (in `defaults.yaml`, overridable per feed or dataset where noted): `timezone` (IANA name for time columns that do not set their own; not overridable), `settle_minutes` (default 15), `initial_lookback_hours` (default 24) and `max_window_hours` (default 72), all used by event windows (section 4), `history_lookback_days` (default 400; not overridable), how far back by `run_date` the hourly reads of the `dq` store look (the window history, the rule schedule, the digest's earlier results), and `rule_run_at` (default `"06:00"`, local time in `timezone`; not overridable), when daily and weekly gold rules fall due.

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

- `dre validate` (static, run in CI): YAML parses; required fields present; enums valid; every referenced dataset, feed and template exists; every `key_map` covers the full key; no duplicate ids. A dataset with `key_map`, and each upstream it maps to, must set `record_time`. Every owner (feed, table-wide dataset, rule) has recipients. `value_format` patterns compile. `hmac_secret_file` is an absolute path outside the repository and the conf directory. Errors name the file, line and field, and say how to fix them. Warnings are reported without failing validation: a dataset without `load_time` gets a warning naming the checks that will be DID\_NOT\_RUN. SQL fragments (`feed_filter`, `open_when`, filter conditions, a contract's conditions) are parsed by `dre dry-run`, not here. The contract rules above are checked here. A valid configuration is followed by a report of every hop: its kind, whether it comes from `contracts` or `key_map`, and its blind spots. Blind spots are what the engine cannot yet see or prove on the hop: an unproven attribution, an aggregate's contribution, deletions not modelled, a legacy exclusion, guards that wait for the ledger, and no as-of replay for a dataset whose `grain.history` is undeclared or mutable.
- Runtime preconditions (before each run): table exists; every configured column exists; landing roots are readable. A failure here makes the affected checks DID\_NOT\_RUN with reason category CONFIGURATION, and the run continues.

## 4. Core concepts

**Run.** One execution of `dre run`. Gets a `run_id` (UUID), records start and end time, the engine version and the git commit of the configuration.

**Evaluation event.** The thing a check evaluates. In release 1 there is one kind, a batch load event: one dataset, one load window, identified by `event_id = sha256(dataset + window_start + window_end)`. Streaming windows come later and must fit the same interface. Windows are UTC and half-open: they include `window_start` and exclude `window_end`. Each result row records its event's `window_start` and `window_end`.

- `window_end` = the run's start minus `settle_minutes` (default 15), so rows still being written are left for the next run.
- `window_start` = the latest `window_end` among the dataset's NORMAL events, skipping any event in which a check was DID\_NOT\_RUN for a PLATFORM or BUDGET reason, or that was evaluated by a run that did not finish COMPLETED (PARTIAL, FAILED, or died while STARTED), since such a run may not have stored all its evidence (that window is held: evaluated again). A time column written in a zone that observes daylight saving time, without an offset in the value, is ambiguous for one hour a year: the repeated fall-back hour reads as the first offset, so rows stamped in the second 01:xx of that day read an hour early (`dre validate` says so once, on `defaults.timezone`). On a dataset's first run, `window_end` minus `initial_lookback_hours` (default 24); a dataset whose last NORMAL event is older than `history_lookback_days` windows the same way, since the hourly reads of the store look only at the `run_date` partitions inside that lookback (it must cover `max_window_hours` and `investigation_retry_days`, which `dre validate` checks). CONFIGURATION reasons never hold a window: those checks report DID\_NOT\_RUN on every run until the configuration is fixed. Gold rule results neither move nor hold a window (rules run on their own schedule over the whole table); they are told apart by the row (only rule results carry a `severity`), so a renamed or deleted rule's results stay out too.
- A hold lasts at most `max_window_hours` (default 72), counted from the end of the first held event. When a run finds the cap reached, the window advances: it starts where the last held event ended, and one result with `check_id` WINDOW\_GAP, DID\_NOT\_RUN / window\_gap, records the unchecked span as its window (`detail`: unchecked hours and the reason codes that held it). The gap is never evaluated again.
- Both bounds are truncated to `load_time.granularity`, counted in the load time's zone. A window never runs backwards: if the previous end is later than this end, the window is empty.

**Evaluation identity.** `evaluation_id = sha256(event_id + check_id + expectation_version + engine_version + group_values)`, with `group_values` written as `key=value` pairs sorted by key (empty for an ungrouped check), so each group row of a grouped check has its own id. A rerun of the same evaluation writes a new row with the same `evaluation_id` and `execution_type = RERUN`; it never overwrites. Values: NORMAL, RERUN, REPLAY.

**Execution types.**

- **NORMAL** (the hourly schedule) evaluates each dataset's next eligible window, as above.
- **RERUN** evaluates one historical window that the caller names (`--window-start`, `--window-end`, UTC), for every dataset planned. The bounds are truncated as a NORMAL window's are, so naming a NORMAL event's bounds gives the same `event_id` and `evaluation_id`s.
- **REPLAY** evaluates a historical range (`--from`, `--to`). Each dataset's recorded NORMAL windows inside the range are replayed exactly, keeping their ids; a window held and evaluated again is replayed once, as its longest evaluation. Each span no recorded window covers is evaluated as one window.

RERUN and REPLAY:

- **Historical only.** They must end by the run's start.
- **Windowed checks only.** They run only the checks that read a window: not gold rules or T1\_SCHEMA\_DRIFT, which read the table as it is now.
- **Idempotent.** A result whose outcome (state, reason, counts, observed, expected, detail, severity) equals the latest stored result of the same `evaluation_id` is not written again, and still counts as written.
- **No change to current state.** They neither read nor write the open-key state: HOP\_KEY\_CURRENCY judges the keys the window owns (at their deadline, or reloaded in it), writes no key events, and keeps the judged keys for the cause engine. They register no files in `dq_file`.
- **No effect on windows.** They never move or hold a NORMAL window.
- **No email.** They send no digest, because digests report scheduled runs.
- **Best effort, not a snapshot.** They read the source tables as they are now: Hive tables keep no versions, so rows changed, merged over or purged since the window are not seen. A RERUN answers "what does this window look like against today's data", not "what did the run see then". The run prints this.

`dq_run.execution_type` records the type.

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
| PLATFORM | metastore\_unavailable, hdfs\_unavailable, query\_failed, evidence\_incomplete (the result was stored but its key events were not; written as a newer row under the same `evaluation_id`), evidence\_invalid (T1\_PARTITION\_DRIFT would have passed against a baseline whose evidence was found INVALID after publication; section 5, "Commit protocol", item 9), ledger\_unbalanced (HOP\_LEDGER: the dispositions do not add up to the due population, an engine defect; it holds the window) |
| CONFIGURATION | table\_missing, column\_missing, incompatible\_type, invalid\_config, unsupported\_mapping (HOP\_LEDGER: the hop's kind is not one the ledger judges) |
| BUDGET | budget\_exceeded |
| DEPENDENCY | upstream\_check\_failed, premise\_unconfirmed, insufficient\_evidence (HOP\_LEDGER: the evidence the contract names, its `rejects` table, cannot be read) |
| BASELINE | insufficient\_history |

**Denominator rule.** Every check records `population` (rows or keys it evaluated) and `violations`. `population = 0` means DID\_NOT\_RUN / empty\_population.

**Cause result states**, for cause checks only: CONFIRMED, RULED\_OUT, NOT\_READY (a required config input is empty), ERROR. The cause of a failure is the first CONFIRMED in the pattern's order; if none, "cause not proven". Every cause line also says how far the evidence goes. **Confirmed**: a cause check proved it. **Suspected**: every cause check ran and was ruled out, so the fallback stands by elimination. **Insufficient evidence**: some cause check was NOT\_READY or ERROR. Root causes are never stated beyond the evidence. A FAILED result whose failing items carry nothing to investigate (only UNASSESSABLE keys) still gets one NOT\_READY row per cause check, so its investigation is complete and says so.

**Times.** Three kinds, never compared with each other: record time (the source's version of a row), load time (when a layer wrote it), first-seen time (when DRE first saw a file or record). Every check declares which one it uses. Parse every time column with its configured format and convert it from its configured `timezone` (or the default in `defaults.yaml`) to UTC before comparing; never assume a source column is UTC, and never compare times as strings.

## 5. Data model

All tables in the `dq` database, ORC, partitioned by `run_date`. Append-only: the writer module exposes only `append`, which selects the frame's columns in the table's column order before `insertInto`. Retention is set per table in `defaults.yaml` (results, events and commits default to 7 years, the file registry and partition states to 13 months) and applied only by `store/retention.py`. The platform team creates the empty `dq` database and grants the service account access; `dre install --apply` creates the tables and views in it (every statement is IF NOT EXISTS, so it is safe to rerun), and `dre install --check` compares an existing store with the DDL. `dre run` never creates the database or the store: it checks the store tables and views exist and, if not, exits 3 naming `dre install`. `store/local_setup.py` creates the database and store for tests and local runs.

`dq_run` gets two rows per run: STARTED when the run starts (`ended_at` and the check counts null) and a final row (COMPLETED, PARTIAL or FAILED) when it ends. A run's state is its latest row. Both rows carry `run_date` = the UTC date the run started, so a run that ends after midnight UTC stays in one partition.

```sql
CREATE TABLE dq.dq_run (
  run_id STRING, started_at TIMESTAMP, ended_at TIMESTAMP,
  engine_version STRING, config_commit STRING, status STRING,  -- STARTED | COMPLETED | PARTIAL | FAILED
  checks_expected INT, checks_written INT,
  investigations_expected INT, investigations_done INT,  -- cause investigations, tracked apart from checks
  execution_type STRING                 -- NORMAL | RERUN | REPLAY; the watchdog counts NORMAL runs only
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
  hop STRING,                           -- <upstream>-><dataset>: a key is open or cleared per hop
  key_hash STRING, key_value STRING,    -- key_value restricted access
  event STRING,                         -- FLAGGED | STILL_FLAGGED | CLEARED
  state_detail STRING, observed_at TIMESTAMP
) PARTITIONED BY (run_date DATE) STORED AS ORC;

CREATE TABLE dq.dq_file (
  path STRING, feed STRING, first_seen_at TIMESTAMP, size_bytes BIGINT,
  modified_at TIMESTAMP, observed_at TIMESTAMP, run_id STRING
) PARTITIONED BY (run_date DATE) STORED AS ORC;

-- Release 4: what each partition of a table looked like at a run (T1_PARTITION_DRIFT, section 6).
-- row_count is the latest known count: counted when the partition was first seen or when its
-- files changed, and carried forward otherwise (counted_at says when).
CREATE TABLE dq.dq_partition_state (
  dataset STRING, table_name STRING, partition_value STRING,
  file_count INT, total_bytes BIGINT, latest_modified_at TIMESTAMP,
  row_count BIGINT, counted_at TIMESTAMP,
  observed_at TIMESTAMP, run_id STRING
) PARTITIONED BY (run_date DATE) STORED AS ORC;

-- Phase 1 design, ADR-07: one row per committed evaluation id (each result row is its own id),
-- and one per committed cause investigation. The counts say what the commit vouches for, and were
-- checked against the stored rows before the commit was written; evidence rows of a commit with a
-- count of 0 stay invisible.
CREATE TABLE dq.dq_commit (
  run_id STRING, evaluation_id STRING, dataset STRING, check_id STRING,
  kind STRING,                          -- result | cause
  result_run_id STRING,                 -- the run whose committed result this publishes or explains
  result_run_date DATE,                 -- that run's run_date (with result_run_id and evaluation_id: the result's identity)
  result_rows INT, key_event_rows BIGINT, partition_state_rows INT, cause_rows INT,
  reconciliation_rows INT,              -- build step 3: HOP_LEDGER's dq_reconciliation rows
  evidence_kept_until DATE,             -- run_date plus the shortest retention of the tables holding what it vouches for
  committed_at TIMESTAMP
) PARTITIONED BY (run_date DATE) STORED AS ORC;

-- Phase 1 build step 3: HOP_LEDGER's accounting (section 6), one row per line of an evaluation's
-- ledger. Evidence of its result: written before it and committed with it. Counts, codes and
-- rule labels only, never a key.
CREATE TABLE dq.dq_reconciliation (
  run_id STRING, evaluation_id STRING, dataset STRING, check_id STRING, upstream STRING,
  line STRING,                          -- due | pending | untimed | a disposition | orphan | overlap | guard
  label STRING,                         -- the exclusion rule@version, the reject reason, why unassessable, an overlap pair
  class STRING,                         -- population | unknown | authorised | breach | defect | ok | deficit | diagnostic | guard
  records BIGINT,
  evidence_level STRING,                -- observed | reconstructed | reported | inferred | unavailable (ADR-06)
  scope STRING,                         -- the result's scope (JSON), the same on every row of the evaluation
  recorded_at TIMESTAMP
) PARTITIONED BY (run_date DATE) STORED AS ORC;

-- Checkpoint 2 corrections (R1, R2): what DRE learned about its own store after publishing.
CREATE TABLE dq.dq_integrity (
  run_id STRING,                        -- the run that recorded the row
  record STRING,                        -- SWEEP | EVIDENCE_MISMATCH | COMMIT_CONFLICT | REBASELINED
  status STRING,                        -- SWEEP: COMPLETED | FAILED
  subject_run_id STRING, subject_run_date DATE, evaluation_id STRING, commit_kind STRING,  -- the commit concerned
  dataset STRING, check_id STRING,
  scope STRING, scope_from DATE, scope_to DATE,  -- SWEEP: DAILY | FULL, over these run_dates
  detail STRING,                        -- counts only
  recorded_at TIMESTAMP
) PARTITIONED BY (run_date DATE) STORED AS ORC;
```

**Commit protocol** (Phase 1 design, ADR-07). Hive has no transaction across tables, so an evaluation is published by a commit row written last:

1. **Order.** The run writes an evaluation's evidence (key events, partition states, ledger lines), then its result rows, then, in a batch with other evaluations, one `dq_commit` row per evaluation id (`kind` result, `result_run_id` the run itself). A cause investigation writes its `dq_cause_result` rows, then one `dq_commit` row (`kind` cause, `result_run_id` the run whose committed result it explains, which differs from the run for a retried investigation), in the same batches. The commit rows are appended every `commit_batch` units (an evaluation or an investigation; `defaults.yaml`, default 25) and at the end of the run, also when the run fails, so the store gains one small file per batch, not one per evaluation. Each batch is written as one file.
2. **Verification.** Before a batch's commit rows are written, the rows they vouch for are counted in the store, in one query over the run's own rows: each result commit's result rows, and its key events, partition states and ledger lines when it declares any; each cause commit's cause rows. A unit (an evaluation, with every row and group it wrote, or an investigation) whose stored rows differ from what its commits declare is withheld whole: none of its commits is written, the run says so (`integrity: ... withheld`), it does not count as written or done, and its window is held and evaluated again (an investigation is retried). A batch that cannot be counted is withheld the same way.
3. **Visibility.** Every reader of results, key events, partition states, ledger lines and causes reads the committed views below, never the tables:
   - results, key events and ledger lines count only with a result commit of the same run and evaluation id;
   - partition states count only with a result commit of the same run, dataset and T1\_PARTITION\_DRIFT;
   - causes count only with a cause commit of the same run and evaluation id, and only while the result it names (`result_run_id`, evaluation id) has a result commit. No timestamp decides it, so a cause left by a run that never committed, or written on a host whose clock is off, never attaches to another run's result.

   The window history is the one exception. It reads `dq_check_result` itself and holds every event of a run that did not finish COMPLETED (section 4), which is stricter: a run is COMPLETED only once all its evaluations are committed. The REPLAY window list reads committed results only, so a window that was attempted and never committed is not replayed.
4. **Failures.** An evaluation interrupted before its commit is invisible. This covers a process that dies, a write that fails, and a commit that cannot be stored. Its run is not COMPLETED, so its window is held and evaluated again, and evaluation ids are deterministic (section 4), so the retry publishes the same ids under its own run.
   - **Evidence cannot be stored.** The results are not written. An evidence\_incomplete DID\_NOT\_RUN row for every row of the evaluation is written and committed with evidence counts of 0, so the current state says so, and the partial evidence stays invisible. No cause is investigated for it.
   - **Results cannot be stored.** The same marker is tried. If the failed result write left rows behind, the marker's stored count differs from its declaration and verification withholds it.
   - **A commit write fails.** The run reads back the batch's commit rows (a write can fail after its file landed), appends the missing ones once, and reads back again. Each unit is then CONFIRMED (every commit stored with exactly its own contents), absent, or conflicting (an identity stored with other contents, recorded as a COMMIT\_CONFLICT incident). Only CONFIRMED units are published and counted; the others, and every unit when a readback fails (uncertain), fail closed: their windows are held and evaluated again. Identical duplicate commit rows are harmless; a conflicting identity is INVALID (below).
   - **Counting.** An evaluation counts in `checks_written`, and an investigation in `investigations_done`, only once its commits are stored.
5. **Retention.** `dq_commit` must be kept at least as long as every table it gates (`dq_check_result`, `dq_cause_result`, `dq_key_event`, `dq_partition_state`, `dq_reconciliation`), or results would disappear with their commits; `dre validate` checks it. Each commit records `evidence_kept_until`: its `run_date` plus the shortest `retention_months` among the tables holding what it vouches for (partition states 13 months, results, key events, ledger lines and causes 84 by default). `dre retention` never drops a partition holding rows a commit promised to keep past today, even if `retention_months` was shortened since, nor a partition under an investigation hold (`retention_holds`).
6. **Consistency model.** The guarantee rests on these assumptions, which the guard tests and the code enforce:
   - **One writer, append only.** DRE is the only writer of the dq store, through `store/writer.py`, which appends and nothing else; only `store/retention.py` drops partitions, and only expired, unheld, unpromised ones.
   - **Run-scoped rows.** Every row carries the run id (a UUID) of the one process that wrote it, and a run writes every row under the `run_date` it started on, so (run id, run\_date, evaluation id) names one unit. No process writes rows under another run's id.
   - **Verify after the last write.** The publisher handles one unit at a time in the run's thread and counts a unit's rows only after every write of the unit returned. So no DRE write changes a unit between its verification and its commit.
   - **Late threads.** A write or investigation cancelled by its time budget may keep running in the background and store rows later. A cancelled evidence write lands only under an evidence\_incomplete marker whose commit vouches for no evidence, so its rows stay invisible; a cancelled investigation never publishes, because its commit is written by the run's thread after the investigation returned.
   - **Read ordering.** Hive has no snapshot isolation. A unit's rows land before its commit, so a reader that sees a commit sees the rows it vouches for; a reader that started before the commit landed sees neither.
   - **Outside the model.** A change made outside DRE (files deleted or added by hand, a writer with access to the dq database) is not prevented; such a writer could also forge commits. The integrity sweep detects rows that no longer match their commits, and a conflicting commit identity is INVALID by itself; a forged commit that matches forged rows is not detectable.
7. **Evidence states.** Each committed unit has one (`dq.v_commit_state`), carried on `v_committed_result`, `v_latest_result` and `v_committed_cause`:
   - **VALID:** verified when published, and not found changed since.
   - **INVALID:** the identity was stored with different contents, or a sweep recorded its rows changed or lost (EVIDENCE\_MISMATCH) or a conflict (COMMIT\_CONFLICT). The result stays visible (a published FAILED never disappears), is never healthy, and its evidence rows are never shown, so no baseline is built on them. An INVALID cause set is not a valid cause, so the result's investigation is retried. A RERUN of the window writes and publishes the evaluation again.
   - **EXPIRED:** past `evidence_kept_until`: its evidence may be gone by retention, as documented. Never healthy.
   - **UNKNOWN:** the latest integrity sweep covering its `run_date` failed and no later completed sweep covers it: a transient verification failure, not a loss. Never healthy; its evidence is still used, since it was verified when published.

   **A healthy result** is one that PASSED with VALID evidence (`v_latest_result.healthy`); nothing else is reported as healthy.
8. **Integrity sweep.** At the first NORMAL run at or after `rule_run_at` each day, the run checks the commits of the last `integrity_sweep_days` days (default 35) against the store (`v_commit_integrity`); on `integrity_full_sweep_day` (default SUNDAY; null: never) it checks every commit whose evidence is still kept. It records each new mismatch before `evidence_kept_until` (EVIDENCE\_MISMATCH) and each new conflicting identity (COMMIT\_CONFLICT), then a SWEEP row COMPLETED. A sweep that fails records a SWEEP row FAILED (what it covers is UNKNOWN) and is tried again at the next run; it never stops the run.
9. **Compromised baselines.** Two checks build on earlier evidence: HOP\_KEY\_CURRENCY judges the hop's open keys from its key events, and T1\_PARTITION\_DRIFT compares with the latest partition states. While an INVALID unit that carried such evidence is not REBASELINED, HOP\_KEY\_CURRENCY of that dataset runs as a full sweep (it judges every key, so it needs no open keys), and T1\_PARTITION\_DRIFT cannot pass: a PASSED result is written as DID\_NOT\_RUN / PLATFORM / evidence\_invalid, which holds the window (a FAILED stands). Once such an evaluation is published whole (the full sweep judged every group, or new partition states were stored), the run records REBASELINED for those units. If the INVALID units cannot be read, every such check is treated as compromised.
10. **What is guaranteed.** A visible evaluation's rows were all stored, and counted, when it was published, and it is reported healthy only while its evidence is VALID. Compromised or unverified evidence never yields a healthy status: the unit is INVALID or UNKNOWN, and a check whose baseline it fed cannot pass on it. `dq.v_commit_integrity` lists every commit whose stored rows now differ from its declaration (expired evidence included); query it with a `run_date` bound.

**Observations are not evaluations.** `dq_run`, `dq_file` and `dq_integrity` are not commit-gated. A `dq_run` row records a run's state as it happens, and a `dq_file` row records that DRE saw a file at a time, which stays true whether or not the run that saw it finishes; a first-seen time must survive a run that dies. Neither is a result or evidence of one: what the checks conclude from the file registry (T1\_FILES\_NOT\_LOADED, the landing causes) is published through the commit protocol like every other result. `dq.v_file_status` and `dq.v_latest_run` show observations, not committed outcomes.

Views derive current state. They are `CREATE OR REPLACE`, so `dre install --apply` replaces them after an upgrade; they hold no data:

- `dq.v_commit_state`: one row per commit identity with its evidence state (commit protocol, item 7)
- `dq.v_committed_result`, `dq.v_committed_key_event`, `dq.v_committed_partition_state`, `dq.v_committed_reconciliation`, `dq.v_committed_cause`: the committed rows of each table (commit protocol above); a committed result carries `evidence_state`, a committed cause `result_run_id`, `result_run_date` and `evidence_state`; evidence rows of an INVALID unit, and INVALID cause sets, are not shown
- `dq.v_commit_integrity`: commits whose stored rows differ from what they declare, with `evidence_kept_until` and `evidence_state` (commit protocol, item 10); read by the integrity sweep

- `dq.v_latest_run`: each run's latest `dq_run` row (its final row once written, otherwise STARTED)
- `dq.v_latest_result`: the latest committed evaluation per (dataset, check\_id), by `window_end`, then `evaluated_at`, then `run_id` (so a RERUN or REPLAY of an older window never becomes the current state, a RERUN of the current window replaces it, and two runs that published the same window give one current result): one row for an ungrouped check, one row per group for a grouped one, with `evidence_state` and `healthy` (PASSED with VALID evidence)
- `dq.v_open_keys`: keys whose latest committed `dq_key_event` at a hop is FLAGGED or STILL\_FLAGGED, with first flagged date (per dataset, check, hop and key)
- `dq.v_file_status`: one row per file path with first-seen time and latest size (observations, not commit-gated)
- `dq.v_latest_partition_state`: the latest committed `dq_partition_state` row per dataset and partition (release 4)

`key_hash` is HMAC-SHA256 of the normalised key, with the secret read from a protected file named in config, never from the repository. Emails use counts and, only if enabled, `key_hash`; `key_value` is never written to logs or emails.

## 6. Check catalogue

Each check is a class implementing `applies_to(dataset, pattern)`, `required_columns(dataset)` (the configured columns its preconditions verify) and `evaluate(ctx, event) -> list[CheckResult]` (one result per `group_by` group, one in total for an ungrouped check). Logic lives in a Jinja2 SQL template; the class only renders, runs and builds the result. `run_check` wraps every check: preconditions first (table and columns exist), then `evaluate`; any exception becomes DID\_NOT\_RUN with a reason, and `detail` keeps only the exception type and Spark error class, never the message. The checks for each pattern are listed in `patterns/*.yaml`; table-wide datasets (no feed, so no pattern) get the checks in `patterns/table_wide.yaml`. T1\_SCHEMA\_DRIFT runs once per physical table, not once per dataset, even when several datasets share a table.

**Tier 1 checks** (applied automatically by pattern):

| Check id | Patterns | Logic | Population | FAILED when |
| --- | --- | --- | --- | --- |
| T1\_ON\_TIME | all | For each cadence slot due in the window, look for a load time later than the slot within `sla_hours` | Loads due: cadence slots whose deadline (slot + `sla_hours`) falls in the window | A due slot has no load within SLA |
| T1\_ZERO\_ROWS | all | For each cadence slot due in the window, count rows loaded within `sla_hours` of the slot | Loads due: cadence slots whose deadline (slot + `sla_hours`) falls in the window, the same slots as T1\_ON\_TIME | A due load wrote fewer than `min_rows_per_load` rows |
| T1\_VOLUME | all | For each slot whose load period (slot to next slot) ends in the window, compare its row count with the median of the last 14 loads for the same slot (a load judged again after a held window counts once; the previous slot is found however far back, so monthly cadences form periods); one result per slot (group `slot` = local HH:MM, `slot_time`) | The slot's rows | Outside median ± `volume_tolerance_pct` (default 50). Fewer than 7 prior loads for the slot: DID\_NOT\_RUN / insufficient\_history |
| T1\_FILES\_NOT\_LOADED | FILE\_CYCLIC, FILE\_PERIODIC; RAW datasets with `file_name_column` | Files in `dq_file` first seen more than `sla_hours` ago with zero rows in the raw dataset (after `feed_filter`) matching on `file_name_column`, compared by base file name | The feed's files in `dq_file` first seen more than `sla_hours` before the window end | Any such file; `detail` lists the files (up to 100). Landing roots not listable this run: DID\_NOT\_RUN / landing\_unreadable or hdfs\_unavailable |
| T1\_SCHEMA\_DRIFT | all, table-wide | Hash the table's column names and types; compare with the previous run's hash, stored in `observed` | The table's columns | Hash changed; `detail` lists added, removed and retyped columns. No earlier hash: DID\_NOT\_RUN / insufficient\_history |
| T1\_KEY\_NULLS | all, table-wide | Rows in the event window with a null or empty key column, grouped by `group_by` | Rows loaded in the window (per group) | Any |
| T1\_KEY\_DUPLICATES | datasets with `key_unique: true`, table-wide | Keys with more than one row, after `key_normalise` | Distinct keys in the table (per group) | Any |
| T1\_FRESHNESS | all, table-wide; where `check_params` configures it | The latest record time (or load time, `time: load`) among rows timed before the window end, after `feed_filter`, per group, against the window end (so a RERUN or REPLAY answers as of its window) | Rows timed before the window end, and rows with no usable time (per group) | The latest time is more than `max_age_hours` before the window end, or no row has a usable time |
| T1\_COLUMN\_RANGE | all, table-wide; where `check_params` configures it | Values of `column` in rows loaded in the window: numeric `min` and `max` (compared on `CAST(column AS DOUBLE)`), or an `allowed` set of strings (compared with the value's string form), never both; nulls left out unless `nulls_fail` | Rows loaded in the window with a non-null value (every row when `nulls_fail`), per group | Any value out of range or not a number when bounds are set, not allowed, or null when `nulls_fail` |

| T1\_PARTITION\_DRIFT | all, table-wide; where `check_params` configures it (release 4) | Each run lists the table's partitions (`SHOW PARTITIONS`) and each partition's directory through the Hadoop FileSystem API: file count, total bytes, latest modification time; no data read. A partition whose files changed since the last run, or seen for the first time, is counted (one `COUNT(*)` on that partition), so its stored count always matches its last state. The partitions the window could touch (the pruning set, widened by one granule each side) are expected to change; any other partition whose count changed by more than `tolerance_rows`, or that disappeared, is a violation. Runs once per physical table, like T1\_SCHEMA\_DRIFT; not in RERUN or REPLAY (it reads the table as it is now). `detail` lists up to 100 changed or dropped partitions with both counts; `observed` counts expected, unchanged, rewritten-same (a compaction or an idempotent re-run), changed, dropped and new-outside-the-window partitions | Partitions outside the window with an earlier state | Any changed count or dropped partition. First sight of the table: DID\_NOT\_RUN / insufficient\_history (state and counts are recorded). More than 1000 partitions: invalid\_config. A table that is not file-backed, or without `partition_column` and `partition_format`: invalid\_config. A partition directory that cannot be listed: hdfs\_unavailable |

T1\_ON\_TIME and T1\_ZERO\_ROWS count expected loads, not rows: with a load due and nothing loaded they are FAILED, also on an empty table; with no load due they are DID\_NOT\_RUN / empty\_population. Neither can PASS on an empty table. Both judge a slot once, in the window where its deadline (slot + `sla_hours`) falls, never when the slot first appears: a load due at 08:00 with an 8-hour SLA is not judged by a run at 09:00. T1\_VOLUME likewise judges each slot once, in the window where its load period ends, so a load is never split across windows; slots with no rows give no T1\_VOLUME result (T1\_ZERO\_ROWS reports them). Checks that need `load_time` are DID\_NOT\_RUN / invalid\_config when it is not set.

**Landing registry.** At the start of `dre run`, the landing roots of every file-pattern feed being run are listed (recursively, matching `file_name_pattern`, skipping hidden files whose names start with `.` or `_`; a feed may set `landing.max_files`, above which the listing is DID\_NOT\_RUN / landing\_unreadable naming the limit, so a root that keeps its archive cannot stall the run). A `dq_file` row is appended for each new file, and for each known file whose size or modified time changed; `first_seen_at` stays the time DRE first saw the path. `dre dry-run` lists but does not register.

**Hop checks** (datasets with `key_map` to an upstream dataset):

| Check id | Logic | FAILED when |
| --- | --- | --- |
| HOP\_FILE\_COMPLETENESS | For files loaded upstream in the window, compare row counts per `file_name_column` upstream and here; upstream rows kept out by the hop's exclusions (`hop_exclusions`, or a contract's exclusions effective at the row's load time, section 3) are not expected, and `observed` counts them and the files pending (first loaded within `sla_hours`). The check presumes a file keeps its row count between the layers: a layer that deduplicates a file's rows fails every file, and one that duplicates them passes, which `observed` says (files with more rows here than upstream) | A file has rows upstream and none here, or fewer here than upstream after `feed_filter` |
| HOP\_KEY\_CURRENCY | For each upstream key, take the latest record time (keeping every row tied at it); left-join to this dataset on the mapped, normalised key. Keys not judged are counted in `observed` with the reason: excluded by the hop's exclusions (`hop_exclusions`, or the contract's), or pending (loaded upstream within `sla_hours`). States: MISSING (no row here), UNASSESSABLE (all of the key's upstream rows, or its rows here, have no usable record time: kept and reported, never passed), STALE (record time here is older), CURRENT | Any MISSING, STALE or UNASSESSABLE key older than `sla_hours`; each written to `dq_key_event`. UNASSESSABLE keys get no cause (the evidence to explain them is missing) |
| HOP\_VALUE\_AGREEMENT | For CURRENT keys, compare columns listed in `owned_columns` (upstream column to this column). Grain: the key's latest version on each side, as the set of distinct owned-column values among the rows tied at that side's latest record time (as strings; null differs from empty). Equal sets agree whatever the number of tied rows | The sets differ |

Add `owned_columns` to the dataset config when HOP\_VALUE\_AGREEMENT is wanted, for example `{enddate: mbr_mbrshp_covrg_end_dt}`.

**HOP\_LEDGER** (Phase 1 build step 3; `docs/design/reconciliation-model.md` sections 1 to 5). One result per hop (group `upstream`): every due upstream record of the hop is put in exactly one disposition, and the engine asserts that they add up.

- **Due population.** An upstream key is due when its version under judgement (its latest by record time among the upstream rows loaded before the window end, the cutoff; every row tied at it kept) has its deadline (that version's load time + `sla_hours`) in the window; on the full sweep, at or before the window end. Keys whose deadline is after the cutoff are `pending`, and candidate keys whose version has no usable load time `untimed`; both are counted beside the ledger and are not part of it. On the full sweep every untimed key is due instead, as unassessable. A row loaded after the cutoff belongs to a later window. An upstream row with no key, loaded in the judged range, is one due record (unassessable, "no key").
- **Dispositions,** first match wins, in this order: `unassessable` (no record time on either side, no load time on the full sweep, or a key collision: distinct raw keys on one side that share a normalised key), `deleted` (the version matches the contract's `deletion.tombstone`, and this dataset shows the delete: absent when `applied_as: hard`, present and matching `soft_when` when `soft`), `deleted_not_applied` (a tombstone, and the record is still live here), `excluded[rule@version]` (an approved exclusion effective at the version's load time, and absent here; `hop_exclusions` is labelled `hop_exclusions`), `excluded_but_present`, `rejected[reason]` (the contract's `rejects` table names the key, rejected at or after the version's load time and before the cutoff; the latest reject gives the reason), `superseded` (survivorship only: never assigned before build step 5), then what this dataset shows when no rule explains the record: `delivered` (this version or a newer one, and the `owned_columns` value sets agree as in HOP\_VALUE\_AGREEMENT), `divergent` (a value set differs), `stale` (an older version), `missing` (absent). A record a later explaining rule also matched is counted once, in the first, and the pair is an `overlap` diagnostic.
- **Classes:** unknown (unassessable), authorised (deleted, excluded, a reject reason the `reject_policy` authorises), breach (deleted\_not\_applied, excluded\_but\_present, orphans), defect (a reject reason the policy does not authorise; unlisted reasons are defects), ok (delivered), deficit (divergent, stale, missing).
- **Identity.** Every due record in exactly one bucket, and the buckets add up to the due population. When they do not, the result is DID\_NOT\_RUN / PLATFORM / ledger\_unbalanced: an engine defect, never PASSED or FAILED.
- **Orphans:** keys here loaded in the window that no upstream row has, and rows here with no key loaded in the window. A breach, reported beside the ledger, not part of it. Counted only when every row read here belongs to the hop: the dataset has a `load_time`, has one upstream, and a `union_partition` target has its `feed_filter` (its attribution, decision D5). Otherwise the scope says "orphans not checked" and why.
- **Outcome.** FAILED when any record is unassessable, in a breach or a defect reason, or an orphan; when `missing`, `stale` or `divergent` exceeds its `tolerance`; or when an exclusion or an authorised reject reason covers more than its `max_share` of the due records. PASSED otherwise. DID\_NOT\_RUN: empty\_population when nothing is due; unsupported\_mapping (CONFIGURATION) for a hop of kind `aggregate` (declared or inferred: aggregation is not reconciled yet, and never from an inferred kind), `join_contribution`, `survivorship` or `fan_out`; insufficient\_evidence (DEPENDENCY) when the declared `rejects` table is gone; invalid\_config or table\_missing as for the other hop checks. `population` is the due records; `violations` counts unassessable, breaches, defect rejects, orphans and deficits (tolerated deficits included).
- **observed** is the ledger line: `due 7 (pending 1): excluded drop_test_members@2 1, rejected PARSE_ERROR (defect) 1, deleted 1, delivered 2, stale 1, missing 1; orphans 0; fails: ...`.
- **Scope** (ADR-01), as JSON in `detail` and on every `dq_reconciliation` row: the edge, the kind, whether the contract is declared or inferred and a hash of it, the claim (`contract-proven` only under a declared contract without a `hop_exclusions` entry; `inferred contract` otherwise; decision D6), the population rule, the window and cutoff, whether it was the full sweep, the grain, the evidence level of each disposition and the claim ceiling (the weakest level among the dispositions a PASS rests on: observed, reconstructed for deletions and exclusions, reported for authorised rejects), the blind spots (as `dre validate` lists them, plus orphans not checked), `unverified_columns` and whether orphans were checked. A DID\_NOT\_RUN / unsupported\_mapping result carries its scope too, with a note naming the kind.
- **Evidence.** The ledger's lines go to `dq_reconciliation` (section 5): the due, pending and untimed populations, every disposition with its label (zeros too, so the identity can be read off the store), orphans, overlaps and breached volume guards. They are evidence of the result, committed with it. Counts, codes and labels only; the keys stay in a session view that the run drops with the evaluation.

Hop details:

- **A hop** is this dataset and one upstream in its `key_map`; each hop check writes one result per upstream (group `upstream`). Keys on both sides are built from the mapped columns with each side's `key_normalise`, as one string (parts joined with `|`, null written as `<null>`). Each side's `feed_filter` applies. Both datasets need `record_time` and the upstream needs `load_time`; otherwise DID\_NOT\_RUN / invalid\_config.
- **Derived upstreams** (release 2). When the upstream is a derived dataset, the hop reads its relation (the query built from its `derived` contract, section 3) wherever it would read a table, and nothing else changes: HOP\_KEY\_CURRENCY judges groups (a group MISSING here, STALE when newer rows reached the group upstream), HOP\_VALUE\_AGREEMENT compares the measures, `hop_exclusions` apply to the relation's columns, and `dre trace` reports the derived layer at its grain. A group's load time is its latest row's, so a group still receiving rows is pending until it has been quiet for `sla_hours`. HOP\_FILE\_COMPLETENESS does not apply (a derived dataset has no files). The relation aggregates the whole `from` table each run, within the compute budget.
- **Keys judged.** An upstream key is judged once, in the window where its latest load time plus `sla_hours` falls (so a key younger than the SLA is never judged); a key with a row in this dataset loaded in the window is judged again once its upstream deadline has passed, so a key that regresses here without any upstream change (for example overwritten with an older version) is caught; and every key still open at this hop in `v_open_keys`. On `full_sweep_day` (the dataset's first window ending that day, in the `defaults.yaml` time zone, so once a day), every upstream key past its deadline. HOP\_FILE\_COMPLETENESS judges files the same way, by each file's first upstream load time.
- **Ties.** Every upstream row at a key's latest record time is kept. HOP\_VALUE\_AGREEMENT requires each of them to match this dataset's latest row, so tied upstream rows that disagree with each other always fail.
- **Population.** HOP\_KEY\_CURRENCY: keys judged. HOP\_VALUE\_AGREEMENT: judged keys that are CURRENT. HOP\_FILE\_COMPLETENESS: files judged; `detail` lists the short files (up to 100).
- **Key events.** HOP\_KEY\_CURRENCY appends to `dq_key_event`, per hop (`hop` = `<upstream>-><dataset>`, so two upstreams of one dataset never clear each other's keys), and computes them inside the check, within its compute budget: FLAGGED (newly MISSING, STALE or UNASSESSABLE), STILL\_FLAGGED (open and still not CURRENT), CLEARED (open and now CURRENT; on the full sweep also open keys no longer upstream, with `state_detail` "no longer upstream"). `state_detail` holds MISSING, STALE, UNASSESSABLE or CURRENT. `key_hash` is computed in SQL by a function registered with the secret in its closure, so the secret never appears in SQL text or query plans. A row whose every key column is null has no key and is never judged at a hop (T1\_KEY\_NULLS counts it); a partly null key still is. The hop reads its open keys (the latest event FLAGGED or STILL\_FLAGGED, as `v_open_keys` defines them) from `dq_key_event`'s `run_date` partitions within `history_lookback_days`: an open key is re-emitted on every evaluation, so its latest event is never older than the hop's last evaluation. `observed` counts the judged keys that were open from earlier runs, so a growing open set is visible in every digest. Without `hmac_secret_file` no events are written and a FAILED result says so in `detail`; a configured secret file that cannot be read, or is empty, stops `dre run` before it starts (exit 3).

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

Each rule is one check: `check_id` is the rule id and results carry the rule's `severity`. It runs over the whole table after the dataset's `feed_filter`, one result per the rule's `group_by` group, when its `frequency` makes it due, whether or not its feed is due: `every_run` on every run; `daily` on the first run at or after `rule_run_at` each day; `weekly` on the first run at or after `rule_run_at` on `rule_weekly_day` (default null: the dataset's `full_sweep_day`). A rule whose last result was held by a PLATFORM or BUDGET reason is tried again on the next run. `retired` rules do not run. Population: keys (max\_rows\_per\_key, max\_open\_rows\_per\_key), rows with both values (column\_order; every row when `nulls_fail`), rows matching `open_when` (superseded\_still\_open), child rows with at least one matching parent (child\_within\_parent), non-null values (value\_format), rows (null\_rate\_max). Rules with `status: proposed` run and record results but are listed as "report only" in the email. Only `approved` rules count toward alerts. The first result of a rule is its baseline; after that the email shows change from the previous run.

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
| `file_value_compare` | `path`, `extract_regex`, `format`, `compare_to: partition \| window` | The value read from the file (first regex group), parsed with `format` (a Spark datetime pattern), is later than the failing file's partition (the same regex applied to the file's folder name, parsed the same way; for a slot, the slot's date in the feed's time zone), or outside the event window. Optional `confirm_when: later \| different` (default `later`): `different` confirms for any partition other than the cursor's |
| `log_contains` | `path_glob`, `pattern` | A log file matching the glob has a line matching the pattern that names the failure (file or key) |
| `table_contains` | `table`, `condition` (optional `id`, `code_ref`) | The table has rows matching the condition for the failure's file or key |
| `size_changed` | none | The file's size changed after it was first seen (`dq_file`) |
| `builtin` | none | Engine logic over DRE's own evidence (tables, `dq_file`, dataset config) |

Each pattern's YAML lists, per failure type, the checks it covers and the cause entries in order: `{code, probe, params}`, where `params` names a key in the feed's `probes:` that supplies the parameters, and the last entry is the fallback. A feed configures its probes by key; a key may hold one parameter set or a list (any one confirming confirms the cause; if none confirms and any set is not configured, the cause is NOT\_READY, never RULED\_OUT). `dre validate` checks every key against the feed's pattern and the probe type's parameters. A failure type lists at least one cause check before its fallback: a fallback alone explains nothing, and a FAILED result with no cause rows would be retried as an unfinished investigation. Adding a cause never touches the engine; a new kind of evidence is a new generic probe type.

```yaml
# feed config
probes:
  load_hold_marker: { path: /data/ctl/example_feed/hold.flag }
  partition_cursor: { path: /data/ctl/example_feed/cursor.prm, extract_regex: "(\\d{8})", format: yyyyMMdd, compare_to: partition }
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
| 3 | JOIN\_UNMATCHED | MISSING only, upstream a derived dataset with joins: a joined dataset had no match for the key's group (a left join; with an inner join the row is never expected) | `builtin` (NOT\_READY unless the upstream is derived with joins) |
| 4 | TIE\_RESOLVED\_BY\_RULE | Several upstream rows share the latest record time, and applying `winner_rule` picks a row other than the one expected (for example the open row over a termination) | `builtin` (NOT\_READY without `winner_rule`) |
| 5 | OLDER\_VERSION\_WRITTEN\_LATER | STALE only: this row's load time is later than the newer upstream record's load time, compared at `load_time.granularity` | `builtin` |
| 6 | FILTERED | Matches a configured filter rule | `table_contains` (`filter_rules`) |
| 7 | REJECTED | In the rejects table | `table_contains` (`rejects`) |
| fallback | MERGE\_NOT\_APPLIED | No cause confirmed |  |

**HOP\_LEDGER** has no failure type in the packaged patterns (build step 3): its dispositions already say what accounts for each record, and its missing, stale and divergent records are the keys HOP\_KEY\_CURRENCY and HOP\_VALUE\_AGREEMENT flag and explain. Its FAILED results are not investigated.

**Load wrote nothing** (T1\_ZERO\_ROWS)

| Order | Cause code | Confirmed when | Probe (`params` key) |
| --- | --- | --- | --- |
| 1 | WRONG\_PARTITION | Rows were written to a partition other than the one named in the cursor file | `file_value_compare`, `compare_to: partition` (`partition_cursor`) |
| 2 | NO\_UPSTREAM\_DATA | The upstream data is genuinely absent: every upstream dataset loaded zero rows in the failing load's business window | `builtin` (needs `upstream`; NOT\_READY without an upstream `load_time` or an earlier slot) |
| fallback | EMPTY\_LOAD | No cause confirmed |  |

A filter rule is one `table_contains` parameter set under `filter_rules`: `table` and `condition` (SQL), with optional `id` and `code_ref` (file and line, or commit). The table is matched on the hop's upstream columns: the base name of its `file_name_column` for a file, its mapped key columns (with its `key_normalise`) for a key.

**What a cause explains.** Each failing thing of a FAILED check is explained on its own, and its rows share a `failure_ref` (a check may list its own failing things through `Check.failures`; the packaged checks' lists are in `causes/failures.py`): a not-loaded file's path (T1\_FILES\_NOT\_LOADED), a missed or short slot in UTC (T1\_ON\_TIME, T1\_ZERO\_ROWS, which list their failing slots in `detail`), a short upstream file's base name (HOP\_FILE\_COMPLETENESS), or a failing key's `key_hash` (HOP\_KEY\_CURRENCY: MISSING or STALE keys; HOP\_VALUE\_AGREEMENT: CURRENT keys that disagree). At most 100 per evaluation; a `table_contains` probe looks for those refs only, so what it collects is bounded by the failures, not by the table. Without `hmac_secret_file` keys cannot be referenced: one set of rows with no `failure_ref`, all NOT\_READY. For a missed slot on a file pattern, the file probes look at the files first seen since the slot that have no raw rows; the landing causes read `dq_file` (and the raw dataset's rows per file) for files first seen since the evaluation's earliest failure only, and a `log_contains` probe keeps only the lines that name one of the evaluation's failing files, at most 500. PIPELINE\_STALLED counts the files first seen with or after the failing one (or since the slot), not the file itself; with no such file it is RULED\_OUT. NO\_UPSTREAM\_DATA counts upstream rows loaded in the failing load's business window: from the previous cadence slot (the load before it) to this slot's deadline (slot + `sla_hours`), the span in which the load could have taken upstream data. It does not use the run's execution window, which with hourly runs falls hours after the load. It comes after WRONG\_PARTITION, so direct evidence of a downstream failure is the cause when both are confirmed; both rows are kept.

**Rows.** One `dq_cause_result` row per cause check per failure, the fallback excluded: `cause_check_id` is the probe and its params key (`file_exists:load_hold_marker`) or `builtin`; `order_no` its place in the pattern's order; `hop` the hop explained (`<upstream>-><dataset>`, `landing-><raw dataset>`); `evidence` JSON with times, counts, paths and table names only, never key values, log lines or file contents. The cause of a failure is read from its rows: the first CONFIRMED by `order_no`, otherwise the pattern's fallback, not proven. A probe that raises is ERROR (its evidence is the exception type); if the failing things cannot be listed, every cause check is written as ERROR.

## 8. Command line, email and watchdog

| Command | What it does | Exit code |
| --- | --- | --- |
| `dre validate [--conf DIR]` | Static validation of all config | 0 valid, 1 errors |
| `dre dry-run --feed F` | Runs preconditions and checks for one feed, prints results, writes nothing | 0 always, unless the command itself fails |
| `dre install --print\|--apply\|--check [--conf DIR]` | `--print` renders the store DDL for the configured dq database; `--apply` creates missing tables (IF NOT EXISTS) and creates or replaces the views (they hold no data, so an upgrade's new definitions take effect), safe to rerun; `--check` compares the existing tables and views with the DDL | 0 done or matching, 1 differences found or apply failed, 3 the database does not exist |
| `dre run [--feed F] [--execution-type NORMAL\|RERUN\|REPLAY] [--window-start T --window-end T] [--from T --to T]` | NORMAL (the default) is scheduled every hour; RERUN takes `--window-start` and `--window-end`, REPLAY takes `--from` and `--to` (ISO 8601, UTC; section 4). A NORMAL run evaluates only feeds with something due: a cadence slot's deadline (slot + `sla_hours`) passed since the feed's last evaluated window, or new or changed landed files. `--feed F` evaluates F even when nothing is due. Then preconditions, checks, causes, append results, send email | 0 run completed (even with FAILED checks), 2 partial, 3 could not start (including: the store is not installed, the message names `dre install`; another NORMAL run is still STARTED and younger than `watchdog_grace_minutes`, so the two would evaluate the same windows and store everything twice; an older STARTED run is dead and does not block) |
| `dre trace --dataset D (--key-file FILE \| --key "k1,k2,...")` | Prints one line per layer along the `upstream` chain: present or not, record time, load time, file; ends with the first layer where the key is absent or older, then that hop's cause line. The key is read from the first line of `--key-file` (`-` for stdin), which keeps it off the command line, the process list and the shell history (`--key` stays for interactive use); it travels as data in a one-row temp view the layer queries join, never as a literal in SQL text, so it reaches no query plan or history-server page | 0, or 3 for an unknown dataset, a key with the wrong number of parts, or no key |
| `dre watchdog` | Checks the last expected run exists and wrote every expected check; emails if not | 0 healthy, 1 alert sent, 3 the watchdog itself could not run (configuration unreadable, or an error outside the store read; a store that cannot be read is an alert) |
| `dre retention [--apply] [--conf DIR]` | Scheduled weekly. Lists each dq table's `run_date` partitions older than its `retention_months`, and the expired ones it keeps (an investigation hold, or rows a commit promised to keep longer); `--apply` drops the others through `store/retention.py` | 0 done, 1 a table could not be read or dropped, 3 no `defaults.yaml` |
| `dre guard PATH` | Scans a source tree (an extension package) or one file under the engine's read-only and append-only rules (section 9), without the engine's own path allowances, and prints each violation | 0 clean, 1 violations found, 3 the path does not exist |

**Email digest**, one per run, plain text, in this order:

1. Subject: `DRE <env>: <n> new failures, <m> checks did not run` (or `all clear`)
2. Checks that changed state since the last run, then DID\_NOT\_RUN checks, each with dataset, check, observed versus expected, and the cause line: `cause: FILE_SKIPPED (confirmed)` or `cause not proven: ruled out NOT_RUN, INVALID_KEY; not ready FILTERED`; a reconciliation result (HOP\_LEDGER) also prints its scope in one line: `scope: <edge> <kind> (<declared or inferred>); claim <claim>; evidence <level>; cutoff <time>; <n> blind spots`
3. Still-failing checks, one line each with the date first failed
4. Proposed rules, under "report only"
5. "Evidence not verified (not counted as healthy)": the owner's current results, from any run, whose evidence is INVALID or UNKNOWN (section 5, "Commit protocol", item 7)
6. Passing checks, as a single count (PASSED with VALID evidence only)
7. When the store's integrity was last verified: the last completed integrity sweep, whether it is stale (none in 26 hours), whether the latest sweep failed, and the integrity incidents of the last day

Recipients come from each owner (a feed's, a table-wide dataset's or a rule's): `recipients` in `defaults.yaml` maps each owner to its addresses. Each owner with results in a run gets one digest for that run, holding only its own results.

Digest details:

- "Changed since the last run" compares each result with the previous result of the same dataset, check and group. A first result that FAILED counts as changed; recovered checks (now PASSED after FAILED or DID\_NOT\_RUN) are listed too. Subject: `<n>` is the checks newly FAILED, `<m>` the checks DID\_NOT\_RUN; `all clear` only when nothing is failing, did not run, or has evidence that is not verified.
- The cause line is the spec's form, naming the fallback when not proven: `cause not proven (DROPPED, insufficient evidence): ruled out NOT_RUN, INVALID_KEY; not ready FILTERED`. A check with several failing things shows one line per distinct cause with a count.
- When T1\_ON\_TIME and T1\_ZERO\_ROWS fail for the same loads of a dataset, one alert is shown (under T1\_ON\_TIME, with a note) and counted once.
- Groups the check's previous run reported and this run did not are listed, for configured group columns only (the dataset's or the rule's `group_by`, and a hop's `upstream`); per-load groups such as T1\_VOLUME's slot change every run.
- "Date first failed" is the first FAILED result since the last result that was not FAILED. Approved rules show their violations against the previous run (`3 violations (was 1)`; the first result is the `baseline`).
- With `email_sample_keys: true`, a failing hop check lists up to 5 `key_hash` values flagged in the run; never the key itself.
- `email.all_clear_digest` (default `every_run`): an owner's digest with nothing failing or not run is sent every run, `daily` (only from the run starting in the hour from `rule_run_at`) or `never`; digests with problems are always sent.
- `email` in `defaults.yaml` names the mail relay (`smtp_host`, `smtp_port`, `sender`). Unset, digests are built but not sent and the run says so. A failed send is reported and never stops the run.
- **Sinks** (release 3). `notify:` in `defaults.yaml` lists where digests and watchdog alerts go, in order: `{kind: email, smtp_host, smtp_port, sender}` or `{kind: stdout, full_body: false}` (printed to the run's stdout, which is the scheduler's log: a digest prints as its subject and counts unless `full_body: true`, because the body carries group values and, with `email_sample_keys`, key hashes). `email.all_clear_digest` decides which digests are sent at all, for every sink. With no `notify:`, the `email` block is one email sink. A sink is a registered capability (`notify/sinks.py`), so a webhook sink is a module that registers itself; every sink gets the same text, with counts, check ids and table names only, and a sink that raises is reported with the exception type and never stops the run. A run's stdout starts with `dre run: started <run_id> (<type> run, <time> UTC)` and ends with `dre run: COMPLETED|PARTIAL, N of M checks written (...)`, or `dre run: FAILED, N of M checks written (...): <exception type>` when the run itself fails (its final row is written if the store allows; a run whose final row cannot be written stays STARTED for the watchdog). The engine's log lines go to stderr with a time and a level. A failure while retrying earlier runs' pending cause investigations is printed and does not fail the run. The run's own stdout lines name a grouped result by its group column and its place in the evaluation (`[src_sys_nm #2]`), never by the group's value, which is data; a hop's `upstream` is a dataset id and is printed.

**Trace.** `dre trace` normalises the given key (in D's key order) with each layer's `key_normalise` and follows D's `key_map` upstream. One line per layer, most upstream first: `present, <n> rows; record <UTC>; loaded <UTC>; file <base name>` for the latest version, or `absent`. Then the first hop where the key is absent downstream (MISSING) or held at an older record time (STALE), or `no gap`; then that hop's latest HOP\_KEY\_CURRENCY cause line for the key, found by its `key_hash` (so it needs `hmac_secret_file`). The key itself is never printed. Exit 3 for an unknown dataset or a key with the wrong number of parts.

**Scheduling.** `dre run` is scheduled hourly; there is no per-feed schedule. Each run decides which feeds are due (see the command table) and skips the rest, writing nothing for them; a skipped feed's window simply carries on to the next run that evaluates it. A feed's `check_delay_minutes` (default 0) moves its window end back, so a slot is judged only once its deadline plus the delay has passed. Table-wide datasets run when a due feed's dataset shares their table, and every run when no feed dataset covers their table.

**Watchdog.** A separate entry point with no dependency on the main run's code beyond the store schema. The expected run times are every hour. It alerts when no NORMAL `dq_run` row exists for an expected hour plus grace, when a run's latest row is still STARTED after the grace, when `checks_written < checks_expected`, or when `investigations_done < investigations_expected` (cause investigations are tracked apart from checks, so a failed investigation never relabels a check; each NORMAL run retries today's FAILED results that are still their check's latest result and have no cause rows (only checks with a failure type in their pattern are investigated, so only theirs are retried), and once a day, in the hour from `rule_run_at`, those of the last `investigation_retry_days` (default 7), so a failure that repeats is not retried hourly for a week; a retry judges the hop's open keys like the run that failed did; and the digest shows `cause: investigation incomplete (the next run retries it)` until then). Grace is `watchdog_grace_minutes` in `defaults.yaml` (default 30). Each watchdog run judges one hour: now minus the grace, floored to the hour; a run ending FAILED is an alert too. The alert goes to `watchdog_owner`'s recipients, or every address in `recipients` when it is not set, by the same mail relay as the digest (printed, and not sent, while the relay is unset); if `dq_run` cannot be read, that is the alert. It also alerts when the store's integrity is not verified (no integrity sweep completed in the last 26 hours, once the store is older than that) and when integrity incidents (EVIDENCE\_MISMATCH, COMMIT\_CONFLICT) were recorded in the judged hour. It writes nothing. Schedule it separately from the main run (a different scheduler folder or host), so one scheduling failure doesn't stop both.

**Compute budget.** Each dataset has `compute_budget_minutes`. Before running, estimate the scan size from partition statistics; if a check exceeds its budget, cancel it and record DID\_NOT\_RUN / budget\_exceeded. Each check gets its dataset's full budget and runs in its own Spark job group; when the budget runs out the group is cancelled (its running Spark jobs are interrupted) and the result's `detail` gives the budget and the table's estimated scan size from Spark's table statistics (or says there are none). BUDGET holds the window, so the span is evaluated again, up to `max_window_hours` (section 4). `compute_budget_scope` (default `check`) set to `dataset` makes a dataset's checks share its budget in a run (a check with none left is not run). `scan_mb_per_minute` (default null), when set, refuses before running a check whose table's estimated scan would take longer than its budget. Hop checks judge only the keys the window owns (section 6), plus a full sweep on the day set by `full_sweep_day` (default Sunday). Both sides' tables are read in full, since a key's latest version needs its whole history, but on a normal day only the candidate keys go through the window functions and the key hashing: keys loaded upstream since the judged range began (judged, or pending behind their deadline), keys loaded here in the window, and open keys; the judged conditions still decide. The full sweep takes every key. T1\_PARTITION\_DRIFT reads metadata only (one directory listing per partition) and counts only the partitions whose files changed, inside the dataset's budget; its first evaluation of a table counts every partition once, and a table too large for that is DID\_NOT\_RUN / budget\_exceeded and tries again next run.

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

**Guard tests.** Tests in `tests/guard/` scan every file under `src/` (scanner: `hcsc.datalake.dre.guard`, re-exported by `tests/guard/scan.py`; `dre guard <path>` runs the same scan over any source tree, for extension packages) and fail on any breach of these rules. The scanner module itself is the one file the scan skips, because its text is the patterns; a test proves it holds no Spark or file operations of its own. Each rule has a snippet test that is caught and one that is allowed. The reasons are in `docs/decisions.md`.

1. **Naming the dq store.** SQL names the store as the literal `dq`, or as `{{ dq_database }}` (Jinja) or `{dq_database}` (Python f-string), because the real name comes from config. A write target the scanner cannot read (for example one built by string concatenation) fails. The configured dq database name must be a plain identifier (`store/names.py`, `validate_dq_database`).
2. **Read-only outside dq.** Flagged unless the target is the dq store: INSERT INTO; `insertInto` and `writeTo`; DataFrameWriter path writes (`.save`, `.orc`, `.parquet`, `.csv`, `.json`, `.text` on a `.write` chain); Hadoop FileSystem `delete`, `rename`, `mkdirs`; `os.remove`, `unlink`, `rmdir`, `removedirs`, `rename`, `replace`; `shutil.rmtree`, `shutil.move`. The dq store is addressed by table name, so a path target never qualifies. `hdfs dfs` / `hadoop fs` `-rm`, `-rmr`, `-rmdir`, `-mv` and `-mkdir` are always flagged.
3. **Append-only.** The only DML allowed on dq is INSERT INTO (or `insertInto` in append mode). Banned everywhere, dq included: UPDATE, DELETE, MERGE, TRUNCATE, INSERT OVERWRITE, `mode("overwrite")`, `overwrite=True`, `saveAsTable`, `writeTo(...)` followed by `overwrite`, `overwritePartitions`, `create`, `replace` or `createOrReplace`, DROP TABLE/VIEW/DATABASE, and every ALTER except rule 5.
4. **One write path.** The DataFrame write APIs (`.write`, `.writeTo`, `.writeStream`, `insertInto`) are allowed only in `store/writer.py`. Any `.write` elsewhere is flagged, including `w = df.write` split across statements and plain file `.write(...)` calls. `append` selects columns in the table's order before `insertInto`; a test fails if a column lands out of order.
5. **Retention.** ALTER TABLE `<dq>.<table>` DROP [IF EXISTS] PARTITION is allowed only in `store/retention.py`. That module refuses to drop any `run_date` partition on or after today minus the configured retention in months, and refuses a retention below one month; a test proves it. It also keeps every partition under an investigation hold and every partition holding rows a commit promised to keep past today (`evidence_kept_until`).
6. **Database creation.** CREATE DATABASE is allowed only for the dq database and only in `store/local_setup.py`. No other module under `src/` may reference `local_setup`, so `dre run` never creates the database.
7. **Email.** Email text contains no `key_value`. Statically, nothing in `notify/` references `key_value`; and a test renders a digest from a store whose key events carry a synthetic `key_value`, with `email_sample_keys: true`, and checks that no part of it appears in the text (`tests/notify/test_digest.py`).
8. **Store DDL only through `dre install`.** CREATE TABLE and CREATE [OR REPLACE] VIEW are allowed only on the dq store and only in `store/ddl.sql`. Only `store/install.py` (behind `dre install --apply`) and `store/local_setup.py` may reference `apply_ddl`, so `dre run` never creates the store.
9. **Generic engine.** Engine code, the config schema, pattern YAML and SQL templates (everything under `src/`) must not contain source-specific words. The denylist lives in `tests/guard/test_generic_engine.py` (stopper, handoff, file\_date, sub\_id, rms) and grows whenever a source-specific name is found; such words may appear only in `conf/` and `tests/`.
10. **Configuration SQL is a query.** SQL that comes from configuration (`feed_filter`, `hop_exclusions`, a contract's conditions, rule params, derived expressions, probe conditions, and the rule templates under `conf/templates/`) runs only inside a query the engine builds (a template as `SELECT * FROM (...) dre_rule`), so a command is a parse error. That wrap does not stop two things a query can still do: the script transform (`TRANSFORM (...) USING 'cmd'`, `MAP ... USING`, `REDUCE ... USING`), which runs an external program, and `java_method` / `reflect` (and `reflect2`), which call Java methods. `config/sqlcheck.py` therefore refuses the script transform by its `USING` (a `USING` not followed by a column list in parentheses, which only the script form and statements refused anyway have; the word `transform` alone is Spark's higher-order function and is allowed), those Java calls, and the write and DDL verbs as defence in depth, in every SQL fragment and template from configuration, as SQL words only (string literals and comments are blanked first), and once more on a template's rendered SQL before it runs, so a parameter value is held to the same rule as the template text. Tests prove it: a fragment or template carrying a script transform or DROP is a config error naming the file and line, and a template registered by other means leaves the table untouched (`tests/config/test_sqlcheck.py`, `tests/checks/test_conf_templates.py`). `dre guard <path>` scans any other tree without the engine's own path allowances, so an extension's file cannot inherit them by its name.

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
| Mail relay for the digest | `email.smtp_host`, `smtp_port`, `sender` in `defaults.yaml`; unset until confirmed (digests are built, not sent) | Platform team |
| Use of AI coding tools on HCSC code | Only through HCSC-approved access | HCSC policy |

**Decide before RMS goes live** (do not block the engine build)

| Item | Status |
| --- | --- |
| Curated load time column | Confirmed `cdc_upd_ts` (developer, 2026-10-15): a string in `yyyy-MM-dd HH:mm:ss:SSSSSS`, read at `minute` granularity like gold's, so one RERUN window matches both datasets' NORMAL windows |
| Curated winner rule across loads | Within one load confirmed (`sourcelastupdatets DESC, enddate DESC`); across loads: developer question 2 |
| Gold merge clause and dedupe | Developer question 3 |
| CDC table and key | Not confirmed; developer question 4 |
| Run logs location | Developer question 5; the log line shape is fixed in `docs/evidence-contract.md` |
| Filter rules and rejects table | Developer question 6; their shape is fixed in `docs/evidence-contract.md` |
| Plan join for the gold key | Query 4 |
| Rule baselines | Queries 5 to 7 |
| How deletions are expressed | Confirmed (developer, 2026-10-16): a soft void. The new version's end date equals its effective date and the row stays at every layer; the sample's gold contract declares it (`deletion`, applied as `soft`). Other void shapes: developer question 19 |
| Multi-source gold tables | Confirmed (developer, 2026-10-16): all sources have equal priority and their rows sit side by side, so `union_partition`, never survivorship. Whether any gold row joins columns from two sources: developer question 19 |
| Curated history class (`grain.history`) | Not confirmed: whether curated keeps every version as loaded (append-only) decides as-of replay; left null |
| DRE's YARN queue, protected queues, kill switch | `yarn_queue` will be provided by the user; the production queues go in `protected_queues` in the deployed configuration only; the kill switch's path and owner are deployment values (build step 8) |

**First configuration (RMS)**

The real RMS values (landing path, stopper and handoff file names, database and table names) are kept with the deployed instance's configuration, not in this repository. The shape the engine must support:

- One feed, pattern FILE\_CYCLIC: sequence files; six loads a day on an every-day calendar. Its source-specific cause inputs are probe configs (section 7), with the real paths kept in the deployed config:
  - the stopper file that holds the raw load: `probes.load_hold_marker: { path: <stopper file> }` (`file_exists`, for RAW\_LOAD\_HELD);
  - the handoff file naming the partition passed between phases: `probes.partition_cursor: { path: <handoff file>, extract_regex: <partition pattern>, format: <its date format>, compare_to: partition }` (`file_value_compare`, for SKIPPED\_BEHIND\_CURSOR and WRONG\_PARTITION);
  - the job log: `probes.load_log: { path_glob: <log glob>, pattern: <error pattern> }` (`log_contains`, for LOAD\_ERROR);
  - filter rules and the rejects table: `probes.filter_rules` and `probes.rejects` (`table_contains`, for FILTERED and REJECTED), once developer question 6 is answered.
- Three datasets: raw (dates MM/DD/YYYY, partitioned by file date, real-time rows told apart by file name); curated (several versions per key, its own load time `cdc_upd_ts`, a string with microseconds, and record time); gold (four-part key with a zero-padded subscriber id, minute-granular load time, `feed_filter` on source system).
- Three rules: `one_row_per_coverage` (max\_rows\_per\_key), `end_not_before_start` (column\_order), `older_coverage_still_open` (superseded\_still\_open, grouped by source system).
