# Design: making the engine extendable

Status: agreed by the user on 2026-10-12, all eight steps in order; all eight are built (docs/decisions.md, "Extensibility, step 1" to "step 8"; the how-to is docs/extending.md). Two departures from the plan, both recorded there: no `file` sink (step 6), and no table builders in the testing kit (step 7).

## What "extendable" means here

The hard constraints decide the shape of this. CLAUDE.md says: no code per source, everything source-specific in YAML, and when a source needs logic the engine lacks, add a *generic capability*. So the engine grows in two directions, and they need different things:

1. **Onboarding by configuration.** An operator describes a new feed, table, rule or pattern in YAML, with no code release. Today a new feed is YAML only, but a new *kind* of feed (a pattern), a new gold rule *template*, a new key normaliser or a new calendar still needs a code change.
2. **Adding generic capabilities.** A developer adds a check, a cause probe, a rule template or a notification sink. Today each of these works, but each needs edits in several central places (a `Literal`, a dict of imports, an `if` chain), and nothing tells a developer what the contract is or tests it for them.

The plan makes direction 1 wider and direction 2 cheaper and safer, without turning the engine into a framework of class hierarchies, and without ever loading code from configuration.

## Where the engine stands

What is already an extension point, and what is still closed:

| Capability | How it is added today | Closed parts |
| --- | --- | --- |
| Check | A `Check` subclass (`applies_to`, `required_columns`, `evaluate`) and a SQL template | Registered by hand in `checks/registry.py` (`CHECKS`, an import list); a check cannot take parameters from config |
| Pattern (kind of feed) | `patterns/<pattern>.yaml`: checks in order, cause order per failure type | Packaged inside the wheel; `Pattern` is a `Literal` in `config/models.py`; `PATTERNS` in the registry; "file patterns" are a hard-coded tuple in `files_not_loaded.py` and `config/validate.py` |
| Cause probe | A function and a parameter model | Two dicts (`causes/probes.py` `PROBES`, `config/probes.py` `PROBE_PARAMS`); builtin causes are a third dict keyed by cause code (`causes/engine.py` `BUILTINS`) |
| Gold rule template | SQL template plus a params model | `Template` is a `Literal`; `TEMPLATE_PARAMS`; an `if` chain in `RuleCheck.evaluate` and another in `required_columns` (four places) |
| Key normaliser | `checks/keys.py` | Two hard-coded forms (`strip_leading_zeros`, `parse_date`) |
| Cadence calendar | `checks/cadence.py` | `EVERYDAY` and `WEEKDAYS`; named calendars rejected (spec section 3) |
| Notification | `notify/email.py` | SMTP only; the digest builder is separate and reusable |
| Landed-file source | `sources/hdfs.py` through the Hadoop FileSystem API | Works for HDFS, local and anything Hadoop mounts (S3A); nothing else, and nothing else is in scope |
| Store | `store/writer.py` (`append` only), DDL in `store/ddl.sql` | Hive/ORC only; fine for now |
| Config | Pydantic models, `extra="forbid"` | An unknown field is an error, so nothing outside the models can carry settings |

Direction 1 is blocked mostly by three things: patterns, rule templates and normalisers being code. Direction 2 is slowed by the central dicts and `Literal`s, and by having no stated contract and no test kit.

## Principles

- **Configuration is never code.** YAML may carry SQL fragments and SQL templates, which run through `spark.sql` and are parsed first; it never names a Python module or class to import. Extension *code* lives in a package that the guard scanner has checked.
- **Registries, not hierarchies.** Each kind of capability has one registry. A capability registers itself where it is defined (a decorator or a module-level call); nothing central lists them. Pattern YAML and `dre validate` are checked against the registries.
- **A contract per extension point,** written down, with a conformance test the engine runs for every registered capability: a check can FAIL, can PASS and has a DID_NOT_RUN path; a probe is NOT_READY without its parameters and never raises; a template's SQL parses as a query and respects the denominator rule.
- **The hard constraints hold for extensions.** A plugin package is scanned by the same guard rules as `src/`, or it is not loaded. Config-defined SQL is parsed and must be a query, never a command.
- **Build the seam, not the adapters.** As the architecture-principles review decided, no adapter framework for Kafka, DB2 or cloud stores before a source needs one. Where a seam is cheap and clarifies the code (notification sinks, landed-file listing), name it with one implementation.
- **Ids stay stable.** `evaluation_id` hashes `check_id`; registries must reject a duplicate id, and the built-in ids never change.

## The plan, in steps

Each step is one PR-sized change with tests, in the order that pays off soonest. Every step keeps the 515 tests passing; the replay scenarios are the regression net.

### Step 1. Self-registering capabilities (developer direction)

Replace the central dicts with registries that a capability joins where it is defined:

- `checks/registry.py`: `register(check)` and `checks()`; each check module calls `register(OnTime())` at import. The package's check modules are imported by `checks/__init__.py` (one place, but a list of modules, not of classes and ids). A duplicate `check_id` raises at import.
- Probes: one `Probe` record holding the type name, the params model and the function, registered from `causes/probes.py`. `config/probes.py` reads the same registry, so a probe is defined once.
- Builtin causes: `@builtin("NOT_RUN")` on the function in `causes/merge.py` and `causes/landing.py`.
- Rule templates: a `TemplateSpec` (name, params model, SQL template, required columns function, observed and expected text) registered per template; `RuleCheck.evaluate` becomes one generic path. This is the biggest cleanup: the two `if` chains go.
- Key normalisers: a registry of `name -> SQL expression builder`; add the generic ones that are obviously missing (`trim`, `upper`, `lower`, `regex_replace: {pattern, replacement}`) so fewer sources need code.
- `Literal` types for pattern and template become validators against the registries, with the same file-line-fix error messages.

Done when: a test adds a check, a probe, a builtin cause and a rule template from a test module with no edit outside that module (and, for the check, a pattern YAML), and the engine runs them; and no module under `src/` lists capabilities by id. Guard rules unchanged.

### Step 2. Check parameters from configuration

Checks today read only the dataset's fields, so a generic check such as "column within a range" or "freshness of a timestamp column" has nowhere to put its parameters. Add:

- `check_params: {CHECK_ID: {...}}` on a dataset (and defaults in a feed), validated by the check's own params model, the way probes are validated by theirs. Unknown check ids and unknown fields are config errors with file and line.
- The pattern YAML may give defaults per check: `checks: [T1_ON_TIME, {check: T1_FRESHNESS, params: {max_age_hours: 6}}]`.
- `CheckContext.params` carries the validated model.

Then add two generic parametrised checks that real tables will need and that need no production names: `T1_FRESHNESS` (latest record time no older than `max_age_hours`) and `T1_COLUMN_RANGE` (values of a column within bounds, or in a set). Each with FAIL, PASS and DID_NOT_RUN tests and a cause failure type (fallback only, until there is evidence to look at).

Done when: a dataset in the sample config carries `check_params`, `dre validate` reports a bad parameter with file and line, and the two checks pass their contract tests.

### Step 3. Patterns from configuration (operator direction)

A pattern is a list of existing checks and cause orders: data, not code. Let a deployed instance define its own:

- `conf/patterns/*.yaml` is read after the packaged `patterns/`; a file with the same `pattern` name overrides the packaged one, a new name adds a pattern. Same schema, same validation (every check and probe named must be registered; exactly one fallback per failure type).
- The pattern declares what is now inferred from its name: `kind: file | table` (file patterns need `landing` on the feed and get T1_FILES_NOT_LOADED and the landing probes). `FILE_PATTERNS` and the check in `config/validate.py` read the pattern, not its name.
- `dre validate` prints which patterns are packaged and which come from `conf/`.

Done when: a test onboards a feed with a new pattern (say a daily full-table reload, composed of T1_ON_TIME, T1_ZERO_ROWS, T1_VOLUME and T1_KEY_DUPLICATES, with its own cause order) by configuration only, and `dre run` evaluates it end to end; R01 to R13 unchanged.

### Step 4. Rule templates from configuration

Gold rules are where business logic multiplies, so templates are the extension point with the most value for operators. After step 1 a template is a record; let configuration supply one:

```yaml
# conf/templates/no_future_dates.yaml
template: no_future_dates
params:                       # name -> type (string | int | float | column | sql | format), required unless default given
  column: column
  format: { type: format, default: null }
population: rows with a value
violation: rows with a value in the future
sql: |                        # must select population and violations (and g_1.. for group_by), like the packaged templates
  SELECT {% for g in group_by %}{{ g }} AS g_{{ loop.index }},{% endfor %}
         COUNT({{ params.column }}) AS population,
         SUM(CASE WHEN {{ time(params.column, params.format) }} > current_timestamp() THEN 1 ELSE 0 END) AS violations
  FROM {{ table }} {% if feed_filter %}WHERE {{ feed_filter }}{% endif %}
  {% if group_by %}GROUP BY {{ group_by | join(', ') }}{% endif %}
```

Safeguards, because this is SQL from configuration:

- A second Jinja loader for `conf/templates/`, sandboxed (`jinja2.sandbox.SandboxedEnvironment`), with a fixed set of helpers (`time`, `key_expr`, `column`) and `StrictUndefined`.
- The rendered statement is parsed by `dre dry-run` and before every run, and must be a query: a plan whose root is not a command. A new guard rule (spec section 9, rule 10) states it, and a test proves an `INSERT` or `DROP` in a template is refused with a config error naming the file.
- Templates from config follow the denominator rule through the same `from_counts`; a template that returns no `population` column is a config error.

Done when: R10 passes unchanged on the packaged template, and a test adds a template in `conf/templates/` and a rule using it, by configuration only.

### Step 5. Named calendars

Small, and spec section 3 already names it: `conf/calendars/<name>.yaml` with `holidays: [dates]` and `weekdays_only: true|false`; `cadence.calendar` may name one. `UnsupportedCalendar` goes away for named calendars.

Done when: a cadence with a named calendar skips a listed holiday in a test, and `dre validate` rejects an unknown calendar name.

### Step 6. Notification sinks

`notify/digest.py` already builds a `Digest` apart from sending. Name the seam: a `Sink` protocol (`send(digests) -> list[str]`) with `email` (today's SMTP), `file` (writes the digest text under a configured directory, for environments without a relay) and `stdout`. Config: `notify: [{kind: email, ...}, {kind: file, directory: ...}]`; `email:` stays as a shorthand for one email sink. A webhook sink (Teams, ServiceNow) is a later sink, added when someone asks and with the same guard: the digest text, counts and `key_hash` only.

The guard rule that nothing in `notify/` references `key_value` covers every sink, because they live there.

Done when: the golden-file digest tests pass unchanged, and a run with a `file` sink writes the digests and reports it.

### Step 7. Extension packages

Only once steps 1 to 4 exist, because most needs are met by configuration by then:

- A separately installed package registers capabilities through entry points (`dre.checks`, `dre.probes`, `dre.templates`, `dre.sinks`, `dre.normalisers`).
- Nothing loads unless `defaults.yaml` lists it: `extensions: [package_name]`. `dre validate` reports what each extension registered, and refuses an id that collides with a built-in.
- `dre guard <path>` runs the guard scanner (`tests/guard/scan.py`, moved into the package as `hcsc.datalake.dre.guard`) over an extension's source, so an extension's CI proves read-only, append-only and no `key_value` the same way this repository does. The README says an extension is accepted only with that check green.
- A testing kit, `hcsc.datalake.dre.testing`: the synthetic table builders and the contract tests (FAIL, PASS, DID_NOT_RUN for a check; NOT_READY and no-raise for a probe; parse-and-denominator for a template), so an extension author gets the same tests the engine runs.

Done when: a test extension package in `tests/extensions/` registers one of each kind, is loaded only when listed, and passes the contract tests through the kit.

### Step 8. Documentation

`docs/extending.md`: one section per extension point, each with the contract, a worked example, the tests required and the guard rules that apply. Spec amendments in sections 2 (layout), 3 (check_params, patterns, templates, calendars, notify, extensions), 6 (parametrised checks), 9 (guard rule 10) and a `docs/decisions.md` entry for each step, as for release 2.

## Spec changes this needs

The spec is the source of truth, so each step starts by amending it and the decisions file, as the derived datasets did. Section 1 gets a "Release 3: extensibility" paragraph. The one rule that must stay word for word: hard constraint 3, no code per source. Patterns, templates and parameters in configuration describe *kinds* of behaviour; a feed still cannot get an `if feed == ...`.

## What this plan does not do, and why

- **No adapter framework** for Kafka, DB2 or object stores. Decided in the architecture-principles review; every read already goes through Spark SQL or the Hadoop FileSystem API, and the Hadoop API already reaches S3A and similar mounts.
- **No Python in configuration.** A config directory must stay something an operator can edit without a code review. SQL in configuration is parsed and must be a query.
- **No new result states or store tables.** A new check or template still writes PASSED, FAILED or DID_NOT_RUN through the one writer.
- **No change to evaluation ids** for built-in checks: the registries change how checks are found, not what they are called.
- **No versioned store or snapshot replays.** Still waiting on the platform (`docs/architecture.md`, "Open").

## Order and size

| Step | Size | Why this order |
| --- | --- | --- |
| 1. Self-registering capabilities | M | Everything else builds on the registries; also the largest cleanup of existing code |
| 2. Check parameters, two generic checks | M | Lets real tables be covered by configuration when their names arrive |
| 3. Patterns from configuration | S | Small once the registries exist; the first operator-only extension |
| 4. Rule templates from configuration | M | The highest value for operators; needs the sandbox and the parse guard |
| 5. Named calendars | S | Already in the spec as "not in release 1" |
| 6. Notification sinks | S | Useful before the mail relay is confirmed (the `file` sink) |
| 7. Extension packages | M | Only after configuration covers the common cases |
| 8. Documentation | S | Written as each step lands, gathered at the end |

Steps 1 to 4 are the core. If only part of this is wanted, do those, in that order.
