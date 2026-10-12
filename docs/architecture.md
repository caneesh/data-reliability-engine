# Architecture: guarantees and limits

This page answers the architecture concerns of the October 2026 code review. For each one it gives what the engine guarantees, how it does so, and what is still open. The build rules are in `docs/spec.md`, and the decisions and their reasons are in `docs/decisions.md`.

## Zero-touch isolation

**Guarantee.** Onboarding a feed changes no pipeline code, job configuration, schedule or runtime. A failure in DRE can never block a production pipeline.

**How:**

- **Read-only access.** The engine only reads production tables (Spark SQL), HDFS listings and log files. It writes only to its own `dq` database, through `store/writer.py`, whose only operation is `append`.
- **Guard tests.** `tests/guard/` fails the build on any write, DDL, file move or delete outside that path (spec section 9).
- **Configuration only.** A feed is onboarded with YAML alone (`conf/feeds`, `conf/datasets`, `conf/rules`). There is no source-specific code, and the denylist guard keeps it that way.
- **Separate schedule.** `dre run` runs on its own hourly schedule, and the watchdog on another. Neither is a step in a pipeline job. If DRE fails, its own run is PARTIAL or FAILED and the watchdog alerts; the pipelines are unaffected.

## Transformation-aware reconciliation

**Guarantee.** A count difference between layers is reported against a stated contract. A row a business rule keeps out is not reported as lost.

**How.** A dataset's YAML is its contract with each upstream:

| Contract part | Setting | Effect |
| --- | --- | --- |
| Business filters on a side | `feed_filter` | Both sides of every check are filtered the same way |
| Key mapping across layers | `key_map`, `key_normalise` | Keys are compared on mapped, normalised columns, for example zero-padded against unpadded |
| Columns the hop must carry | `owned_columns` | HOP\_VALUE\_AGREEMENT compares only these |
| Allowed exclusions on a hop | `hop_exclusions` (per upstream: a SQL condition on the upstream's rows) | Hop checks do not expect those rows, so they are never counted as lost |
| How versions are chosen | `winner_rule` (with `formats`) | Explains ties (TIE\_RESOLVED\_BY\_RULE) |
| Expected cardinality | `key_unique`, gold rules (`max_rows_per_key`, `max_open_rows_per_key`) | Duplicates are a check of their own |

**Comparison grain:**

- **HOP\_KEY\_CURRENCY:** one row of judgement per upstream key: its latest record time, upstream against here.
- **HOP\_VALUE\_AGREEMENT:** the key's latest version on each side, taken as the set of distinct owned-column values among the rows tied at that side's latest record time. Identical multi-row versions agree.
- **HOP\_FILE\_COMPLETENESS:** row counts per upstream file, after filters and exclusions.

**Open:**

- **Joins and aggregations.** Hops are key-preserving (one upstream key to one key here). A downstream table built by joining or aggregating several upstreams needs a contract kind the engine does not have yet. Such a dataset should not be given a `key_map`.
- **Counting exclusions.** Excluded rows are left out, not counted: the check result does not say how many rows an exclusion kept out.

## Multi-source lineage

**Guarantee.** Every missing or divergent record is tied to one observable hop.

**How:**

- **The graph.** Datasets form a directed graph: `upstream` lists and `key_map` entries; `dre validate` rejects cycles.
- **Hop identity.** A hop is (this dataset, one upstream), named `<upstream>-><dataset>`. Each hop check writes one result per hop (group `upstream`).
- **Key state per hop.** Key events (`dq_key_event.hop`) and open keys (`v_open_keys`) are tracked per hop, so two upstreams of one dataset never flag or clear each other's keys. Cause rows carry the same `hop`.
- **No one-to-one assumption.** A dataset may have any number of upstreams. An error in one hop is DID\_NOT\_RUN for that hop only. `dre trace` follows a key along every `key_map` edge and names the first hop with a gap.

## Evidence quality

**Guarantee.** No root cause is stated beyond its evidence. Insufficient evidence is reported explicitly.

**How:**

- **Cause check states.** Each cause check is CONFIRMED, RULED\_OUT, NOT\_READY (an input it needs is not configured) or ERROR.
- **Cause lines.** Every cause line names its evidence level:
  - **Confirmed:** a cause check proved it.
  - **Suspected:** every cause check was ruled out, so the pattern's fallback stands by elimination.
  - **Insufficient evidence:** some cause check could not be evaluated.
- **Where existing pipelines kept nothing to go on** (no log, no cursor file, no rejects table), the cause is NOT\_READY. It is never guessed.
- **Keys that cannot be judged.** A key whose record times are missing on either side is UNASSESSABLE. That state is a violation, never passed and never given a cause.
- **Nothing to investigate.** A FAILED result whose failing items carry nothing to investigate still gets NOT\_READY rows, so the gap is explicit.

## Persistence and recovery

**Guarantee.** Retries, reruns and partial writes never leave duplicate current evidence or silently lost evidence.

**How:**

- **Deterministic ids.** `evaluation_id` is a hash of (event, check, expectation version, engine version, group). Re-evaluating the same window gives the same ids.
- **Current state.** Each evaluation's current state is its newest row in the views. `v_latest_result` orders by window, then by evaluation time.
- **Partial writes:**
  - **Key events not stored.** A result stored without its key events gets a newer DID\_NOT\_RUN / PLATFORM / `evidence_incomplete` row under the same `evaluation_id`, and the run ends PARTIAL.
  - **Interrupted runs.** Any NORMAL event evaluated by a run that did not finish COMPLETED (PARTIAL, FAILED, or died while STARTED) is held, and the next run evaluates it again.
  - **Limit on holding.** A hold lasts at most `max_window_hours`; then a WINDOW\_GAP row records the span that was never checked.
- **Idempotent reruns.** A RERUN or REPLAY writes a result only if its outcome differs from the stored one. Neither reads nor writes key events.
- **Cause investigations.** They are tracked apart from checks: `dq_run.investigations_expected` and `investigations_done`.
  - A failed investigation never changes a check's state.
  - The watchdog reports an incomplete investigation, and the digest shows `cause: investigation incomplete`.
  - Each NORMAL run retries such investigations for up to `investigation_retry_days`, as long as the failed result is still its check's latest result.

**Open.** The store is append-only Hive/ORC, which has no transactions across tables. Recovery therefore relies on deterministic ids, newest-row-wins views, and holding windows, not on atomic commits.

## Historical replay semantics

**Guarantee.** RERUN and REPLAY say what they are: a re-evaluation against today's data, not a snapshot.

**How:**

- **Same windows.** RERUN names one historical window, and REPLAY names a range. Recorded windows are replayed with their original bounds, so the evaluation ids match.
- **Today's data.** The source tables are read as they are now. Hive keeps no versions, so rows changed, merged over or purged since the window are not seen. The run prints this.
- **Excluded checks.** Gold rules and T1\_SCHEMA\_DRIFT read the table as it is now, so they are not re-run.

**Open.** Snapshot reads need a versioned table format. The sources are plain Hive tables, so this waits on the platform.

## Operational safeguards

**Bounded work:**

- **Compute budget.** Each check runs within `compute_budget_minutes`, in its own Spark job group, cancelled when the budget runs out. With `scan_mb_per_minute` set, a check whose estimated scan would exceed its budget is refused before it starts.
- **Cause investigations:**
  - At most 100 failing items per evaluation.
  - Probes read each log glob or value file once per evaluation.
  - Hop judgements are computed once and shared with the cause engine.
- **Narrowed reads.** Hop checks read only keys and files due in the window. The full sweep runs once a week.

**Retention:**

- `dre retention --apply` is scheduled weekly; the README gives the command.
- Results, cause rows and key events are kept 7 years; the file registry is kept 13 months.

**Reads of the store:**

- **Bounded by `run_date` partition:** the digest's reads of a run's rows and the retry of investigations.
- **Not bounded:**
  - Window histories (one query per run).
  - The rule schedule's last completion.
  - T1\_VOLUME and T1\_SCHEMA\_DRIFT history.

  These need each dataset's last good window or last 14 loads, which for monthly feeds can be months back.

**Open (P2):**

- Benchmark these reads against a store grown to a year or more of hourly runs, in a representative environment.
- Then bound them by `run_date` where the benchmark shows a need, for example a summary of the last window per dataset.
- Full Spark, integration and failure-injection runs in a representative cluster are still to do. CI runs everything in local-mode Spark on synthetic data.
