# Handoff: where the build stands (2026-10-12)

Read this first when resuming. Then read `CLAUDE.md` (working rules), `docs/spec.md` (the source of truth), `docs/decisions.md` (every assumption, with its reason) and `docs/architecture.md` (guarantees and limits). This file only says where things stand and what is waiting.

## Status

- **Merged to `main`.** PR #1 (https://github.com/caneesh/data-reliability-engine/pull/1) was merged as merge commit `96503b9`. CI run #29 on `main` passed all three jobs: `test`, `e2e` and `package`.
- **Branch.** The working branch, `claude/claude-md-documentation-a2lxt0` (repo `caneesh/data-reliability-engine`), was restarted from `main` after the merge. New work goes there. Open a pull request only when the user asks; then merge with a merge commit, because the docs cite commit SHAs.
- **Release 1** (spec section 10, steps 1 to 11) is done. It was then extended by two code reviews and an architecture review.
- **Release 2** has started. Derived datasets (join and aggregate hops) are built.
- **Release 3** (extensibility) is built: the eight-step plan in `docs/design/extensibility.md`, agreed by the user, built in order; each step has a spec amendment and a `docs/decisions.md` entry, and `docs/extending.md` is the how-to.
- **Tests:** about 650, all passing (`pytest --collect-only -q | tail -1` gives the exact count). A full local run takes about 20 minutes; CI about 15. Replay scenarios R01 to R13 all pass, plus a three-layer acceptance test.

## History, in order (each step green in CI)

| What | Commit | CI run |
| --- | --- | --- |
| Steps 1 to 11 (release 1) | up to `5254493` | up to #21 |
| Code review: 27 of 29 findings fixed | `b49bca8` | #23 |
| Review items 6 and 7: NO_UPSTREAM_DATA business window; NORMAL, RERUN and REPLAY | `04bae92` | #24 |
| Code-review document: value ties, UNASSESSABLE keys, recoverable evidence, investigation tracking, evidence levels, `hop_exclusions` | `446abfa` | #25 |
| Architecture-principles review: excluded and pending counts, three-layer acceptance test | `15ccb3f` | #26 |
| Release 2: derived datasets | `4afcfc7` | #27 |
| Merge to `main` (PR #1) | `96503b9` | #29 |
| Handoff for compaction | `525e23c` | #31 |
| Extensibility step 1: self-registering capabilities (`docs/design/extensibility.md`) | `23d1a8a` | #32 |
| Extensibility step 2: check parameters, T1_FRESHNESS, T1_COLUMN_RANGE | `d4f452d` (#33 red: four result counts) and `30fffda` | #34 |
| Extensibility step 3: patterns from configuration, `kind` | `c1f2811` | #35 |
| Extensibility step 4: rule templates from configuration, guard rule 10 | `31fa034` | #36 |
| Extensibility step 5: named calendars | `80317cf` | #37 |
| Extensibility steps 6 and 7: notification sinks, extension packages, `dre guard`, testing kit | `122b76a` | #38 (one run for the push of both commits) |
| Extensibility step 8: `docs/extending.md` | `a8a0107` | #38 |
| Code review of release 3 (`docs/code-review-2026-10-13.md`) and its fixes A1 to A11 | `23de7a7`, then the review-fix commits | see the review page |

Each row has a section in `docs/decisions.md` saying what was decided and why.

## What is waiting

**Next action:** the user said that real keys, tables and columns come in later iterations. Until they arrive, wait for the user. When an answer arrives:

1. Turn it into configuration, or a small change.
2. Add tests, and get CI green.
3. Summarise, starting with the CI result.

**Developer: 16 questions.** They are in the user's Claude Doc, "DRE: questions for the developer" (https://claude.ai/code/artifact/0364b496-8c3e-437a-a070-ccd20af9174c):

1. The curated load-time column. This is the most urgent: until it is set, gold's hop checks are DID_NOT_RUN on every run.
2. Record-time formats.
3. The raw table's columns and load time.
4. What raw's file-name column holds.
5. How long raw keeps rows.
6. The winner rule across loads.
7. The gold merge clause.
8. Whether there is a CDC table.
9. The hold-marker file.
10. The handoff (cursor) file's path, pattern and date format.
11. How the loader picks a partition (WRONG_PARTITION).
12. Run logs and their error pattern.
13. Filter rules.
14. The rejects table.
15. Query 4.
16. Queries 5 to 7.

**Platform team:**

- The mail relay: `email.smtp_host`, `smtp_port` and `sender`.
- Client deploy mode on an edge node, with Python 3.10.
- A cluster throughput figure, if checks should be refused before they run (`scan_mb_per_minute`).
- The `dq` database and its service account.
- Where scheduler (Control-M) history can be read, so it can serve as cause evidence.

**User decisions** (each already works on its default):

- WRONG_PARTITION: probe `confirm_when` is `later` (the default) or `different`.
- `email.all_clear_digest`: how often an all-clear digest is sent.
- `compute_budget_scope`: `check` or `dataset`.
- `watchdog_owner`: who receives watchdog alerts.
- Whether the watchdog should keep state, to catch a run that starts late in its hour and then hangs (a known gap).
- Whether hourly table-wide datasets should run when no feed covers their table.

**Still open, deferred on purpose** (`docs/architecture.md`, "Open"):

- **Real derived contracts.** The first real derived dataset waits on actual aggregate or joined gold tables. The sample config has none; the examples are in the spec and the tests.
- **Derived datasets as downstreams.** Count-against-count reconciliation. Build it only if a real case needs it.
- **One-to-many hops.** One upstream row exploded into several target rows.
- **Snapshot replays.** RERUN and REPLAY read today's data. Versioned tables would be needed for true snapshots.
- **Benchmarking store reads.** Some reads of the dq store are not bounded by date. Benchmark them against a store holding a year of hourly runs.
- **A real-cluster run.** Spec section 10's two-week unattended run, and acceptance criterion 7 (an outage or heavy load does not affect source processing). Both are outside this repository.

## Settings that carry an assumption (defaults)

In `defaults.yaml` unless noted. The README table has the full list.

| Setting | Default |
| --- | --- |
| `email.smtp_host` / `smtp_port` / `sender` | null / 25 / null (digests and alerts are printed, not sent) |
| `email.all_clear_digest` | `every_run` (or `daily`, `never`) |
| `watchdog_grace_minutes` | 30 |
| `watchdog_owner` | null (every recipient) |
| `investigation_retry_days` | 7 |
| `compute_budget_scope` | `check` |
| `scan_mb_per_minute` | null |
| `max_window_hours` | 72 |
| `rule_run_at` | `"06:00"` |
| `rule_weekly_day` | null (each dataset's `full_sweep_day`) |
| `settle_minutes` | 15 |
| `initial_lookback_hours` | 24 |
| Feed probe `file_value_compare.confirm_when` | `later` |
| Feed probe `file_value_compare.format` | null (that cause is NOT_READY) |

Production table, column and path names stay null: CLAUDE.md forbids inventing them. The checks that need them report DID_NOT_RUN, and the causes NOT_READY.

## How the engine fits together

- **Run** (`runner.py`). The hourly NORMAL run evaluates only the feeds that are due, and gold rules run on their own frequency.
  - RERUN (`--window-start`, `--window-end`) and REPLAY (`--from`, `--to`) re-evaluate historical windows. They are idempotent: an outcome already stored is not written again. They read and write no key events, register no files and send no email. They read today's data, not a snapshot.
  - A window is held (evaluated again next run) after a PLATFORM or BUDGET reason, or when the run that evaluated it did not finish COMPLETED. A hold is released after `max_window_hours`, with a WINDOW_GAP row.
  - Each FAILED result goes to the cause engine. Investigations are counted in `dq_run` (`investigations_expected`, `investigations_done`) and retried by the next run if they don't finish.
- **Checks.** All check logic is SQL, in `checks/sql/*.j2`.
  - Tier 1 checks are in `checks/tier1/`, gold rules in `checks/rules/`.
  - Hop checks are in `checks/hop/`. Keys are judged at their upstream deadline, plus keys reloaded downstream in the window, plus open keys, with everything on `full_sweep_day`.
  - Key states are MISSING, STALE, UNASSESSABLE (no usable record time) and CURRENT.
  - `observed` counts the keys excluded by `hop_exclusions` and the keys still pending (within `sla_hours`).
  - HOP_VALUE_AGREEMENT compares each side's set of tied latest values.
  - Open keys are tracked per hop (`dq_key_event.hop`).
- **Derived datasets** (release 2: `checks/derived.py`, `checks/sql/derived_relation.sql.j2`). A dataset may be `derived` (from, joins, where, group_by, columns) instead of having a `table`.
  - Wherever an upstream's table would be read, its relation is read instead. Its `key` is the comparison grain.
  - The join mapping is called `match`, not `on`: YAML reads an unquoted `on` as `true`.
  - Derived datasets are never checked themselves.
  - Cause JOIN_UNMATCHED explains a MISSING key whose left-join lookup did not match.
- **Causes** (`causes/`). One row per cause check per failing item, at most 100 items. Each cause line names its evidence level: confirmed, suspected, or insufficient evidence.
  - Generic probes: `file_exists`, `file_value_compare`, `log_contains`, `table_contains` and `size_changed`, plus builtins.
  - NO_UPSTREAM_DATA looks at the load's business window (previous slot to deadline) and comes after WRONG_PARTITION in the cause order.
- **Store** (`store/`). Append-only, written only through `writer.append`.
  - Tables: `dq_run`, `dq_check_result`, `dq_cause_result`, `dq_key_event` and `dq_file`.
  - Views: `v_latest_run`, `v_latest_result` (latest window first), `v_open_keys` and `v_file_status`.
  - DDL is applied only by `dre install --apply`. `dre retention [--apply]` runs weekly.
- **Email, trace and watchdog.**
  - `notify/` builds the digest; the golden files are in `tests/notify/golden/`.
  - `trace.py` follows one key through the layers.
  - `watchdog.py` counts NORMAL runs only and alerts on incomplete investigations.
- **Packaging.** `python scripts/package.py` builds `dist/` for `spark-submit`; the README covers deployment.
- **Extension points** (release 3). Registries (`registry.py`) for checks, probes, builtin causes, rule templates, normalisers, calendars and sinks; each capability registers itself where it is defined.
  - By configuration: `check_params` (parametrised checks T1_FRESHNESS, T1_COLUMN_RANGE), `conf/patterns/` (with `kind: file | table`), `conf/templates/` (SQL rule templates, run only as a subquery: guard rule 10), `conf/calendars/`, and `notify:` sinks (email, stdout).
  - By code: an extension package with an entry point in `dre.extensions`, loaded only when `defaults.yaml` lists it; `dre guard <path>` runs the guard scanner (now `hcsc.datalake.dre.guard`) over its source; `hcsc.datalake.dre.testing` holds the contracts.

## Working conventions (learned the hard way)

- **Summaries start with CI.** Every summary of a change starts with the CI result (run number and commit), then names the tests that prove the change.
- **Commit trailer.** End commit messages with the `Co-Authored-By:` and `Claude-Session:` lines from the session's attribution reminder. Put no model name anywhere else.
- **Never commit untested work.** Run the full suite (in the background, then read its last line) and the end-to-end check by hand before committing, even when a stop hook asks for a commit.
- **Guard tests** (`tests/guard/`) scan `src/`:
  - SQL on the store names it `{dq_database}`, never `{db}`.
  - No `.write` outside `store/writer.py`.
  - No source-specific words (stopper, handoff, file_date, sub_id, rms).
  - No `key_value` anywhere in `notify/`.
- **Changing tests.** Change a test only when the user changed the rule it encodes. Never weaken a test so it passes.
- **Spark gotchas found here:**
  - **Unused CTEs.** Spark analyses CTEs even when no query uses them, so a broken unused CTE masks the real error. Add a CTE only to the queries that use it.
  - **Null `max_by` ordering.** `max_by` returns null when its ordering column is null.
  - **Driver time zone.** PySpark returns timestamps in the driver process's time zone; `session.use_utc_clock` sets it to UTC.
- **Local environment.** The venv is `.venv` (Python 3.11, Java 21); CI uses Python 3.10 and Java 17.
- **End-to-end check by hand:**
  1. In an empty directory, run `python <repo>/tests/e2e/build_synthetic.py`.
  2. Run `dre install --conf conf --apply`, then `dre run --conf conf`.
  3. Run `python <repo>/tests/e2e/verify.py`. Expect `42 results {'DID_NOT_RUN': 19, 'PASSED': 23}`, run COMPLETED. Set `PYSPARK_PYTHON` to the venv's interpreter if the workers pick up another Python.
- **CI.** Check it with the GitHub MCP `actions_list` tool. After a push, schedule a `send_later` check-in about 15 minutes out instead of polling.
- **Uploaded review documents.** Check each point against the code before acting; don't follow them blindly. Reproduce a reported defect with a failing test before fixing it.
