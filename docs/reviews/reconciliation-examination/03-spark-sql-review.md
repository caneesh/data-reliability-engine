# 03 · Spark SQL review

**Templates read:**
- in full: `hop_ledger.sql.j2`, `hop_common.sql.j2`, `hop_key_currency.sql.j2`, `chain.sql.j2`, `hop_contribution.sql.j2`;
- the relevant parts of `hop_file_completeness.sql.j2`, `t1_files_not_loaded.sql.j2`, `t1_volume.sql.j2`, `t1_key_duplicates.sql.j2` and `derived_relation.sql.j2`;
- plus the Python that renders and runs them (`checks/base.py render_sql`, `checks/keys.py`, `checks/times.py`, `checks/hop/*.py`).

A concern is listed as a defect only with executed evidence (test id) or a deterministic proof. Otherwise it is marked as unverified.

## 1. Join cardinality and duplicate multiplication

| Location | Concern | Finding |
| --- | --- | --- |
| `hop_ledger.sql.j2` `judged` (:200-236) | One upstream key against many downstream rows | **No multiplication.** Every joined CTE is grouped by `k`, and `due` counts rows = distinct keys (a14, a22). |
| `hop_ledger.sql.j2` `dn_cand` (:116-118) | `JOIN due_keys` before ranking | Correct: it narrows to due keys, and ranking happens after. |
| `hop_common.sql.j2` `candidates` (:59-67) | DISTINCT union, then joined back | Correct. It narrows the work, not the answer (the comment holds). |
| `chain.sql.j2` forward mapping | One upstream record to several next keys | DISTINCT and `collect_set`; one-to-many is `unobservable` (existing test). No multiplication. |
| `hop_contribution.sql.j2` :43-60 | Contributor versions joined per gold row | Picks one version per (key, load_ts); `<=>` comparison. No multiplication. |
| `derived_relation.sql.j2` :22-25 | Aggregates over every version of a history table, with raw `=` joins and no `key_normalise` | Static: history inflates COUNT and SUM (false FAIL) and padded keys do not match (S-13). Not executed. |

## 2. NULL semantics

| Location | Behaviour | Verdict |
| --- | --- | --- |
| `keys.py:119-122` `key_string` | `coalesce(CAST(e AS STRING), '<null>')` joined with `'|'` | **F-02**: a NULL equals the literal `'<null>'` (a20), `'|'` inside a part collides (a19), and `parse_date` on an invalid string gives `<null>` (l05). Executed. |
| `hop_ledger.sql.j2:70,75` (`up_top`), `:120,125` (`dn_top`); `hop_common.sql.j2:83-84,89-90` | `MAX(record_ts) OVER` ignores NULL; a NULL row is kept only when the whole key is NULL | **F-05**: the newest version with a NULL or unparseable record time is dropped (l01, m04). Executed. |
| `hop_common.sql.j2:113,115-117` | Judged and pending predicates on `up_load_ts` are NULL-false | **F-12**: a key with no usable load time is never judged by the release 1 hop checks, even on the full sweep (m05). Executed. |
| `hop_ledger.sql.j2:315,320` | Orphan scan `load_ts >= start AND load_ts < end` | **F-06**: a downstream row with an unparseable load time is never an orphan (l07). Executed. |
| `hop_ledger.sql.j2:221` | `us.vals <> ds.vals`, with `coalesce(diverges, TRUE)` at :263 | Correct: a NULL set counts as divergent (fails closed). |
| Value sets | `to_json(named_struct(...))` keeps field names; a NULL and an empty string differ | Correct |
| Anti-joins | `LEFT ANTI JOIN` / `LEFT SEMI JOIN` throughout; no `NOT IN` over nullable columns | Correct. `k IN (SELECT k FROM recheck)` (:96) is over non-null `k`. |
| `checks/base.py:134` `from_counts` | `violations or 0` | **F-15**: violations NULL with population > 0 gives PASSED (o01). Executed. |

## 3. Windows, ranking, tie-breaking, determinism

| Location | Concern | Finding |
| --- | --- | --- |
| `up_top` / `dn_top` | Ties at the top record time are all kept; values compared as sets | Fails closed when tied values differ (a15). It is a false FAIL when the pipeline picks a winner by `winner_rule` (S-09); the ledger ignores `winner_rule`. |
| `up_keys` :77-84 | `MAX(CASE WHEN tomb/x_i ...)` over the tied rows | **F-07** (l03, l04): one tied row explains the whole key. |
| `rejected` :184-189 | `max_by(named_struct(reason, rule), rejected_ts)` | Nondeterministic on equal `rejected_ts` (S-08). Not executed: the nondeterminism is plan-dependent and could not be forced deterministically. |
| `chain.sql.j2` recheck | `ROW_NUMBER() OVER (PARTITION BY key_hash ORDER BY observed_at DESC, run_id DESC)` | Deterministic, given run ids. |

## 4. Timestamps, implicit casts, precision

| Location | Concern | Finding |
| --- | --- | --- |
| `times.py` `utc_expr` | Parses with the configured format, then `to_utc_timestamp(..., tz)` | Static: a value that already carries an offset, or a true TIMESTAMP instant labelled with a local zone, is shifted twice (S-14, configuration-dependent). |
| Record-time comparisons | `granularity` is applied only to window bounds | Static: microseconds upstream against seconds downstream is stale forever, a false FAIL (S-14). |
| Upstream `load_time` granularity coarser than the SLA (e.g. day) | A record stamped 00:00 but visible at 10:00 falls into a range already covered | Static: hidden until the full sweep (S-02). `dre validate` has no granularity-versus-SLA rule. |
| Comparisons | Times are never compared as strings; keys are compared as normalised strings | Correct (CLAUDE.md rule met) |

## 5. Filters, empty frames, schema

- `up_cut` (:47-49) applies the cutoff upstream; downstream rows are read as they are now (a25, by design).
- An empty upstream gives `empty_population` (a24). A missing table, or a column referenced by a fragment that does not exist, gives DID_NOT_RUN (a23, a24).
- Schema drift is caught before SQL by `required_columns` and `T1_SCHEMA_DRIFT`.

## 6. SQL injection through configuration

Identifiers, table names, reason codes and rule references are validated or quoted (`sql_string`, `validate_identifier`). These raw fragments are interpolated as written:
- `feed_filter`;
- exclusion `condition`;
- `tombstone`;
- `soft_when`;
- rejects `condition`.

`config/sqlcheck.py` blocks writes, DDL, TRANSFORM and reflect, but allows comments. A `--` inside an inline fragment (for example the winner WHERE, :155) can comment out the rest of a line. In every case traced this gives a syntax error, so DID_NOT_RUN (fails closed). It is Low (S-21), but fragments should not admit comments. Permanent Hive UDFs with side effects are not blocked (speculative, S-20).

## 7. Optimizer-dependent behaviour and cost

Local plan of the ledger's `assessed` SQL (`adversarial/bench_ledger.py`):
- **18 table scans** of two tables;
- **16 exchanges**;
- **8 window sorts**;
- 12 broadcast joins (the tables are small). At scale these become sort-merge joins.

Each CTE reference re-reads the base tables:
- `up_rows` and `up_cut` are read by `up_cand`, keyless and orphans;
- `dn_rows` is read by `dn_cand` and orphans.

Results do not depend on the optimizer. Cost does; see 06 §4.
