# Handoff: where the build stands (2026-10-11)

Read this first when resuming. Then `CLAUDE.md` (working rules), `docs/spec.md` (source of truth) and `docs/decisions.md` (every assumption, with reasons). This file only says where things stand and what is waiting.

## Status

- **Release 1 is code-complete.** All 11 build steps of spec section 10 are done, each with green CI. All replay scenarios R01 to R12 pass with their expected checks and causes, on both synthetic feeds where they apply.
- **Branch:** `claude/claude-md-documentation-a2lxt0` (repo `caneesh/data-reliability-engine`). No pull request has been opened, and none should be without the user asking.
- **Last commit before this file:** `5254493` (step 11). CI run #21 passed all three jobs: `test`, `e2e` and `package`.
- **Tests:** 457, about 13 minutes locally and about 11 minutes in CI.

| Step | What | Commit | CI run |
| --- | --- | --- | --- |
| 1 to 6 | Skeleton, config, store, check framework, Tier 1 checks, landing | up to `ae215a8` | up to #14 |
| Genericity review | Hourly due-only runs, generic probes, second feed, denylist guard, e2e job | `c648b0a` | #15 |
| 7 | Hop checks, gold rules | `f45c142` | #16 |
| 7 review | Reloaded hop keys, rule frequency, window holds (PLATFORM and BUDGET only, `max_window_hours`, WINDOW_GAP) | `b5167f0` | #17 |
| 8 | Cause engine with generic probes | `417ffd3` | #18 |
| 9 | Email digest and `dre trace` | `04265da` | #19 |
| 10 | Watchdog and compute budgets | `86b48bc` | #20 |
| 11 | Packaging; assumptions as settings | `5254493` | #21 |

## What the user asked last

1. "Continue and I will provide the answers at the end. Make the application work based on assumptions, which are configurable." Done: every open choice is a setting whose default is the assumption (README, "Settings that carry an assumption").
2. Write the context to files before compaction: this file.

**Next action:** wait for the user's answers. Then turn each answer into configuration or a small change, with tests, CI green, and a summary that starts with the CI result. The release-1 milestone after that is the spec's two-week unattended run in a test environment, outside this repository.

## Waiting on answers

- **Developer: 16 questions.** They are in the user's Claude Doc, "DRE: questions for the developer" (https://claude.ai/code/artifact/0364b496-8c3e-437a-a070-ccd20af9174c). Summary:
  1. Curated load-time column (the most urgent: until it is set, gold's hop checks are DID_NOT_RUN every run).
  2. Record-time formats.
  3. Raw table columns and load time.
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
- **Platform team:**
  - The mail relay (`email.smtp_host`, `smtp_port`, `sender`).
  - Client deploy mode on an edge node, with Python 3.10.
  - A cluster throughput figure, if checks should be refused before running (`scan_mb_per_minute`).
  - The `dq` database and service account.
- **User decisions:**
  - WRONG_PARTITION: probe `confirm_when` `later` (the default) or `different`.
  - All-clear digest volume: `email.all_clear_digest`.
  - Budget scope: `compute_budget_scope`, `check` or `dataset`.
  - Watchdog recipients: `watchdog_owner`.
  - Whether the watchdog should keep state, to catch a run that starts late in its hour and then hangs (known gap).
  - Hourly table-wide datasets when no feed covers their table.

## Settings that carry an assumption (defaults)

In `defaults.yaml` unless noted:

| Setting | Default |
| --- | --- |
| `email.smtp_host` / `smtp_port` / `sender` | null / 25 / null (digests and alerts printed, not sent) |
| `email.all_clear_digest` | `every_run` (or `daily`, `never`) |
| `watchdog_grace_minutes` | 30 |
| `watchdog_owner` | null (every recipient) |
| `compute_budget_scope` | `check` |
| `scan_mb_per_minute` | null |
| `max_window_hours` | 72 |
| `rule_run_at` | `"06:00"` |
| `rule_weekly_day` | null (each dataset's `full_sweep_day`) |
| `settle_minutes` | 15 |
| `initial_lookback_hours` | 24 |
| Feed probe `file_value_compare.confirm_when` | `later` |
| Feed probe `file_value_compare.format` | null (that cause is NOT_READY) |

Production table, column and path names stay null. CLAUDE.md forbids inventing them, so the dependent checks report DID_NOT_RUN and the causes NOT_READY.

## How the engine fits together (one paragraph per area)

- **Run** (`runner.py`): hourly; evaluates only due feeds; gold rules run on their own frequency.
  - Windows are held only by PLATFORM and BUDGET reasons, released after `max_window_hours` with a WINDOW_GAP row.
  - Each FAILED result goes to the cause engine. Then the digests are built from the store and sent, one per owner per run.
- **Checks:**
  - Tier 1: `checks/tier1/`.
  - Hop: `checks/hop/`. Keys are judged at the upstream deadline, plus keys reloaded downstream in the window, plus open keys; everything on `full_sweep_day`.
  - Rules: `checks/rules/`.
  - All SQL is in `checks/sql/*.j2`. `run_check` applies the compute budget (`checks/budget.py`, a Spark job group cancelled at the budget).
- **Causes** (`causes/`): one row per cause check per failing item (file, slot, upstream file or key_hash; at most 100). Generic probes: `file_exists`, `file_value_compare`, `log_contains`, `table_contains`, `size_changed`, plus builtins. The fallback is not stored; `engine.resolve` derives it.
- **Store** (`store/`): append-only through `writer.append` only.
  - Tables: `dq_run`, `dq_check_result`, `dq_cause_result`, `dq_key_event`, `dq_file`.
  - Views: `v_latest_run`, `v_latest_result`, `v_open_keys`, `v_file_status`.
  - DDL only through `dre install --apply`.
- **Email** (`notify/`): `digest.py` and `digest.txt.j2` (golden files in `tests/notify/golden/`), and `email.py` (smtplib).
- **Trace** (`trace.py`) and **watchdog** (`watchdog.py`). The watchdog imports only the config loader, the store's names and the mail sender; a test enforces this.
- **Packaging:** `python scripts/package.py` builds `dist/`: the wheel, `dre-pyfiles-<v>.zip` (with pkgutil namespace inits, zip only), `dre-deps-<v>-<platform>.zip` and `dre_main.py`. Deployment is described in the README.

## Working conventions in this repo (learned the hard way)

- **Every step summary starts with the CI result** (run number and commit), then the test names that prove "done when". This is a CLAUDE.md rule.
- **Commit trailer:** `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` and the `Claude-Session:` line. Never put a model name anywhere else.
- **Guard tests** (`tests/guard/`, spec section 9) scan `src/`:
  - SQL on the store names it `{dq_database}` (not `{db}`).
  - No `.write` outside `store/writer.py`.
  - No source-specific words (stopper, handoff, file_date, sub_id, rms).
  - Nothing in `notify/` mentions `key_value`, docstrings included.
- **Changing tests:** a test may change only when the user changed the rule it encodes (for example, CONFIGURATION no longer holding windows, or rules running on their own schedule). Never weaken one to pass. Count changes in replay tests carry their arithmetic in a comment.
- **Local environment:**
  - The venv is `.venv` (Python 3.11, Java 21); CI runs Python 3.10 and Java 17.
  - `pip` was added to `.venv` with `ensurepip` for `scripts/package.py`.
  - Run the full suite in the background and poll for its last line; it takes about 13 minutes.
- **E2E by hand:**
  1. Make an empty directory and `cd` into it.
  2. `python <repo>/tests/e2e/build_synthetic.py`
  3. `dre install --conf conf --apply`
  4. `dre run --conf conf`
  5. `python <repo>/tests/e2e/verify.py` (expect `41 results {'DID_NOT_RUN': 19, 'PASSED': 22}`).
- **CI** is checked with the GitHub MCP `actions_list` tool (runs, then jobs). A `send_later` check-in about 12 to 15 minutes after a push saves polling.
