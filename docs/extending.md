# Extending the engine

How to add a capability, by configuration where configuration is enough and by code where it is not. The rules come from `docs/spec.md` (sections 2, 3, 7, 8 and 9) and the reasoning from `docs/design/extensibility.md`; this page is the how-to. Every path below ends with tests: a capability without its contract tests is not done.

Two rules hold for everything here:

- **Configuration is never code.** YAML may carry SQL, which runs through `spark.sql` after parsing, and may name installed extensions by their entry-point name; it never names a Python module to import.
- **The hard constraints apply to extensions** (CLAUDE.md): read-only outside the `dq` store, append-only inside it, no member data outside the store, three result states. `dre guard` enforces the first two on any source tree; the engine's CI enforces them on the engine.

## By configuration

### A new feed, table or rule

`conf/feeds`, `conf/datasets`, `conf/rules`: spec section 3. Nothing else is needed for a source the existing patterns and templates cover.

### Parameters for a parametrised check

Some checks take parameters and run only where they are configured: `T1_FRESHNESS` (`max_age_hours`, `time: record | load`) and `T1_COLUMN_RANGE` (`column`, numeric `min`/`max`, `allowed`, `nulls_fail`). Set them on the dataset, or on the feed for all its datasets, or in a pattern as defaults:

```yaml
# datasets/<name>.yaml
check_params:
  T1_FRESHNESS: { max_age_hours: 72, time: load }
  T1_COLUMN_RANGE: { column: status_cd, allowed: [A, T] }
```

`dre validate` rejects an unknown check id, parameters on a check that takes none, bad values and a field missing at every level, with file and line.

### A new kind of feed: a pattern

`conf/patterns/<name>.yaml`, same layout as the packaged `patterns/*.yaml`; a packaged name replaces that pattern. Declare `kind: file | table`, the checks in run order (a parametrised check may carry defaults) and the cause checks per failure type, each with at least one cause check before its fallback.

```yaml
pattern: TABLE_RELOAD
kind: table
checks: [T1_ON_TIME, T1_ZERO_ROWS, T1_VOLUME, T1_KEY_DUPLICATES, {check: T1_FRESHNESS, params: {max_age_hours: 30}}]
causes:
  load_wrote_nothing:
    checks: [T1_ZERO_ROWS]
    order:
      - {code: NO_UPSTREAM_DATA, probe: builtin}
      - {code: EMPTY_LOAD, fallback: true}
```

A feed's `probes:` keys must be ones the pattern's causes use. `dre validate` says which patterns are packaged and which come from `conf/patterns`. Test: `tests/config/test_conf_patterns.py`, `tests/replay/test_conf_pattern_run.py`.

### A new gold rule template

`conf/templates/<name>.yaml`: typed params, the text of the result, and a Jinja SQL template returning `population` and `violations` (and `g_1..` for `group_by`). The template sees `table`, `feed_filter`, `group_by`, `params` and the helpers `column(name)`, `time(name, format)`, `key_expr`, `literal(text)` and `number(value)`.

```yaml
template: not_after
params:
  column: column
  limit: { type: string }
  format: { type: format, required: false }
population: rows with a value
violation: rows have a value after the limit
unit: rows
sql: |
  SELECT {% for g in group_by %}{{ g }} AS g_{{ loop.index }}, {% endfor %}
         COUNT({{ params.column }}) AS population,
         SUM(CASE WHEN {{ time(params.column, params.format) }} > {{ time(literal(params.limit), params.format) }}
                  THEN 1 ELSE 0 END) AS violations
  FROM {{ table }}
  {% if feed_filter %}WHERE {{ feed_filter }}{% endif %}
  {% if group_by %}GROUP BY {{ group_by | join(', ') }}{% endif %}
```

The SQL always runs wrapped as `SELECT * FROM (...) dre_rule`, so only a query can run; `dre validate` also rejects write and DDL keywords, and `dre dry-run` renders each rule against its table. A packaged template's name cannot be reused. Test: `tests/checks/test_conf_templates.py`.

### A named calendar

`conf/calendars/<name>.yaml` (`weekdays_only`, `holidays`), used as `cadence.calendar`. Test: `tests/checks/test_calendars.py`.

### Where notifications go

`notify:` in `defaults.yaml` lists sinks in order; with none listed, the `email:` block is one email sink.

```yaml
notify:
  - { kind: stdout }
  - { kind: email, smtp_host: relay.example.org, smtp_port: 25, sender: dre@example.org }
```

Test: `tests/notify/test_sinks.py`.

## By code

Each kind of capability has one registry. A capability registers itself in the module that defines it; nothing central lists them. Ids must be unique and stable (check ids go into `evaluation_id`, cause codes into `dq_cause_result`).

| Capability | Define | Register | Contract |
| --- | --- | --- | --- |
| Check | a `Check` subclass: `check_id`, `required_columns`, `evaluate`; optional `params_model`, `failures` | `register_check(MyCheck())` (`checks/registry.py`) | FAIL, PASS and DID_NOT_RUN / `empty_population`; results built with `from_counts` or `did_not_run`; `detail` never carries values |
| Cause probe | a params model (every field `None` by default) and a function `(cx, params, failure) -> Outcome` | `@probe("name", Model)` (`config/probes.py`) | NOT_READY without parameters; never raises out of the engine (ERROR); evidence is counts, paths and names only |
| Builtin cause | a function `(cx, failure) -> Outcome` | `@builtin("CODE")` (`causes/base.py`) | as a probe |
| Rule template | a `TemplateSpec` with its params model | `register_template(spec)` (`checks/rules/template.py`) | SQL returns `population` and `violations`; `related` names the other datasets it reads |
| Key normaliser | a `Normaliser` (name, SQL builder, example, argument validator) | `register_normaliser(...)` (`checks/keys.py`) | pure SQL over one column; string arguments go through `sql_string` |
| Calendar | a `Calendar` (name, `runs_on(day)`) | `CALENDARS.register(...)` (`checks/calendars.py`) | |
| Notification sink | a config model with `kind`, and a factory returning an object with `send(messages) -> list[str]` | `@sink("kind", Model)` (`notify/sinks.py`) | counts, check ids and table names only; a failing send returns a status line, never raises |

Inside the engine, a new capability is a new module under the package the registry discovers (`checks/tier1`, `checks/hop`, `causes`, `checks/rules/templates`), plus its tests. The packaged examples are the models to copy: `checks/tier1/freshness.py` (a parametrised check), `causes/probes.py` (probes), `checks/rules/templates/null_rate_max.py` (a template with its own result).

### An extension package

A separately installed package adds capabilities without a change to the engine:

1. Register them in one module, the way the engine's modules do.
2. Declare one entry point in the group `dre.extensions`: `example = my_package.dre_extension`.
3. List it in `defaults.yaml`: `extensions: [example]`. Nothing loads otherwise. `dre validate` reports what each extension registered, and a name that collides with a built-in is an error.
4. In the package's CI: `dre guard src/` (the engine's read-only and append-only rules on the package's source), and the contract tests from `hcsc.datalake.dre.testing`: `check_contract`, `probe_contract`, `template_contract`. The example extension is `tests/extensions/dre_ext_example/`, and `tests/test_extensions.py` shows both.

The kit holds no table builders (the engine's source may not create or drop tables); copy `tests/fixtures/layers.py` for synthetic tables.

## What is not extensible on purpose

- Result states, the store's tables and the writer: every capability writes through what exists.
- Packaged rule templates and calendars cannot be replaced from configuration: existing rules and feeds depend on them.
- No adapter framework for other storage or messaging systems: every read goes through Spark SQL or the Hadoop FileSystem API (`docs/decisions.md`, "Architecture principles document").
