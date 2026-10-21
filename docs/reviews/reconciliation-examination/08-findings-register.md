# 08 · Findings register

**Severity:**
- **Critical:** a silent false PASS, silent data loss, or evidence corruption under ordinary data and configuration.
- **High:** materially wrong results, or a false PASS that needs particular configuration or data.
- **Medium:** risk under specific conditions.
- **Low:** clarity or robustness.

**Classification:**
- **Confirmed:** executed in `adversarial/` (test id given).
- **Strongly supported:** deterministic from the code, not executed.
- **Unverified risk.**
- **Design limitation:** a boundary of the approved design.
- **Unsupported requirement.**
- **Documentation discrepancy.**

Line numbers refer to HEAD `c5e6237`. All paths are under `src/hcsc/datalake/dre/` unless shown otherwise.

## Summary (most severe first)

| ID | Severity | Class | Component | One line | False PASS / FAIL |
| --- | --- | --- | --- | --- | --- |
| F-05 | Critical | Confirmed (l01, m04) | ledger, hop checks | The newest version with a NULL or unparseable record time is dropped; the older version is judged, and the key may not even be due | False PASS |
| F-07 | Critical | Confirmed (l03, l04) | ledger | One tied row matching an exclusion or tombstone explains the whole key | False PASS |
| F-02 | Critical | Confirmed (a19, a20, l05, c05) | keys | The canonical key string collides on `'|'`, on `'<null>'`, and on unparseable-to-NULL parts; two records read as one | False PASS |
| F-12 | High | Confirmed (m05) | release 1 hop checks | A key with no usable upstream load time is never judged, even on the full sweep | False PASS (the ledger's sweep catches it) |
| F-06 | High | Confirmed (l02, l07) | ledger | Orphans are lost in a window where nothing is due, or when their load time does not parse; permanently | Hidden breach |
| F-18 | High | Confirmed (o04) | runner, digest | Withheld FAILED results vanish from the digest, which does not say the run was PARTIAL | Under-reporting |
| F-01 | High | Confirmed (a05, a06) | ledger, value agreement | Values are verified only through `owned_columns`; wrong values elsewhere are delivered, PASSED, **healthy** | False PASS (values) |
| F-09 | High | Confirmed (m01) | ledger (survivorship) | Without owned columns the wrong source winning is accepted on both edges | False PASS |
| F-10 | High | Confirmed (m02) | HOP_CONTRIBUTION | A contributor change that never reaches gold is never judged | False PASS |
| S-01 | High | Strongly supported | file checks | Files matched by base name across all time: a reused name hides a lost delivery | False PASS |
| F-08 | Medium | Confirmed (l06) | ledger | Downstream reloads (re-inserted excluded or tombstoned keys) are not judged until the full sweep | Delayed detection |
| F-13 | Medium | Confirmed (m06) | ledger, hop checks | A history-keeping target is judged by its newest record time, not its current row | False PASS (configuration) |
| F-14 | Medium | Confirmed (m07) | chain | The chain stops following a record at an authorised disposition (soft void not applied downstream) | False chain PASS (the hop ledger catches it) |
| F-03 | Medium | Confirmed (a18) | ledger | Tolerated deficits give an unqualified, healthy PASS; no upper bound | Silent acceptance |
| F-04 | Medium | Confirmed (a13) | ledger | `rejected` outranks a present-and-divergent record (qualified, never healthy) | Hidden divergence |
| F-11 | Medium | Confirmed (m03) | HOP_CONTRIBUTION | No optional contributor: a legitimate NULL reference FAILs | False FAIL |
| F-15 | Medium | Confirmed (o01) | base | `from_counts`: NULL violations with population > 0 gives PASSED; negative populations are accepted | False PASS (operator SQL) |
| F-16 | Medium | Confirmed (o02) / deduced | events, publisher | Evaluation ids collide (NULL vs `''`; `,` `=` in values): the evaluation is withheld on every run | Failures never published |
| F-17 | Medium | Confirmed (o03) | T1_VOLUME | `volume_tolerance_pct` ≥ 100 lets an empty slot PASS with population 0 | False PASS (configuration) |
| S-02 | Medium | Strongly supported | coverage | An upstream load time coarser than the SLA (a day stamp) hides records until the full sweep | Delayed detection |
| S-03 | Medium | Strongly supported | digest | Vanished groups do not stop "all clear" | Under-reporting |
| S-04 | Medium | Strongly supported | causes/landing | Any read exception is UNREADABLE (CONFIRMED) | Overstated cause |
| S-05 | Medium | Strongly supported | causes/probes, merge | Missing logs or lineage become RULED_OUT, so the fallback reads "suspected"; substring name match | Overstated cause |
| S-08 | Medium | Strongly supported | ledger | Reject chosen by `max_by` on a tie: nondeterministic | PASS/FAIL flip |
| S-09 | Medium | Confirmed via a15 / static | ledger | `winner_rule` ignored: tied rows resolved by the pipeline read as divergent | False FAIL |
| S-10 | Medium | Strongly supported | HOP_CONTRIBUTION | Gold reference not normalisable; version chosen by exact load time; exclusions, tolerance and deletion ignored | False FAIL |
| S-11 | Medium | Strongly supported | attribution | Contributors switch off primary orphan checks; one-dataset two-union "verified" | False PASS (attribution) |
| S-12 | Medium | Strongly supported | validate | A `key_map` onto a subset of the upstream key collapses records silently | False PASS |
| S-13 | Medium | Strongly supported | derived | Aggregates: no `key_normalise`, all history versions counted | False FAIL |
| S-14 | Medium | Strongly supported | times | Double UTC conversion of offset values; precision mismatch | False PASS / FAIL (configuration) |
| S-15 | Medium | Strongly supported | value agreement | Without the downstream load time, a key later fixed with wrong values is not re-judged until the sweep | Delayed detection |
| S-16 | Medium | Strongly supported | runs | Overlapping NORMAL runs (grace 30 < budget 40; not atomic) | Doubled load; duplicate events |
| S-17 | Medium | Strongly supported | watchdog | A run started late in the hour, and hours without a watchdog run, are never judged | Missed alert |
| S-06 | Low | Strongly supported (spec-mandated) | causes/engine | First CONFIRMED wins even when an earlier stage was not judged | Overstated cause |
| S-07 | Low | Strongly supported | runner | Retried causes read current data and never reach a digest | Lost diagnosis |
| S-19 | Low | Strongly supported | runner, store | RERUN "unchanged" ignores qualifications; `healthy` ignores `evaluation_mode` | Stale health |
| S-20 | Low | Unverified risk | install, retention | `dq_database` may name a monitored database; permanent UDFs not blocked | Read-only boundary |
| S-21 | Low | Strongly supported | sqlcheck | SQL comments allowed in raw fragments (fail closed in every case traced) | — |
| S-22 | Low | Strongly supported | ledger | The balance check cannot see keys lost before `due_keys` | Weak guard |
| S-23 | Low | Strongly supported | ledger | Soft `deleted` ignores the target's version | — |
| D-01 to D-07 | Low | Documentation discrepancy | spec, model | See 01 §3 | — |

## Detailed findings: Critical and High

### F-05: A newer version with a NULL or unparseable record time is dropped

- **Location:** `checks/sql/hop_ledger.sql.j2:70, 75` (upstream), `:120, :125` (downstream); `checks/sql/hop_common.sql.j2:83-84, 89-90`.
- **Description:** `MAX(record_ts) OVER (PARTITION BY k)` ignores NULLs. A NULL row is kept only when every row of the key is NULL (`OR top_ts IS NULL`).
- **Trigger:** the key's latest row has a NULL or unparseable record time (`to_timestamp` returns NULL with ANSI off) while older rows parse.
- **Expected:** unassessable ("no record time"), FAILED.
- **Actual:** the older version is judged. Its load time is old, so the key is neither due, pending nor untimed. The ledger PASSes without it, and HOP_KEY_CURRENCY judges the old version, even on the full sweep.
- **Reproduction:** `adversarial/test_ledger_static_candidates.py::test_l01_*`, `test_mapping_and_time_candidates.py::test_m04_*`.
- **Business impact:** a corrupted timestamp on the latest change, a common data defect, makes that change invisible to every completeness check.
- **Potential:** false PASS.
- **Remediation:** treat any NULL record time among a key's rows loaded in range (or among its top candidates) as unassessable "partial record time". Equivalently, rank with NULLs as the greatest and flag them. Apply the same rule in `hop_common.sql.j2`.
- **Regression test:** l01 and m04, inverted to expect FAILED / unassessable.
- **Confidence:** high (executed).

### F-07: One tied row explains the whole key

- **Location:** `hop_ledger.sql.j2:80` (`MAX(CASE WHEN tomb ...)`) and `:82` (`MAX(CASE WHEN x_i ...)`) over `up_top`.
- **Trigger:** two or more upstream rows tied at a key's latest record time, of which only one matches an exclusion condition or tombstone, and the record absent downstream.
- **Expected:** missing, FAILED (or unassessable for an ambiguous version).
- **Actual:** excluded or deleted (authorised), PASSED.
- **Reproduction:** `test_l03_*`, `test_l04_*`.
- **Business impact:** coarse CDC timestamps (second or minute) routinely produce ties. A live record lost alongside a test or void row is accepted.
- **Potential:** false PASS.
- **Remediation:** require every tied row to match (`MIN`), or resolve ties with `winner_rule` (which also fixes S-09), or make a disagreeing tie unassessable.
- **Regression test:** l03 and l04, inverted.
- **Confidence:** high.

### F-02: Canonical key collisions

- **Location:** `checks/keys.py:119-122` (`concat_ws('|', coalesce(CAST(e AS STRING), '<null>'))`). The collision guard at `hop_ledger.sql.j2:71, 202` uses the same string.
- **Trigger:** key parts containing `'|'`; a part whose value is the string `'<null>'` next to a NULL; a normaliser that maps an invalid value to NULL (`parse_date`), matched against a NULL on the other side; lossy normalisers (`strip_leading_zeros`: `'0'`, `'00'` and `''` all become the same value).
- **Expected:** distinct records judged separately, or unassessable.
- **Actual:** one key; one record judged; the other never counted. PASSED.
- **Reproduction:** `test_a19_*`, `test_a20_*`, `test_l05_*`, `test_chain_adversarial.py::test_c05_*`.
- **Business impact:** silent loss of the second record. `T1_KEY_DUPLICATES` groups by the separate columns, so it does not flag this.
- **Potential:** false PASS.
- **Remediation:** build the key as a struct or array (or with length-prefixed or escaped parts) instead of a delimited string. Treat a part that is NULL after normalisation but non-NULL raw as unassessable "unparseable key", as the model requires. Use a distinct NULL sentinel.
- **Regression test:** a19, a20, l05 and c05, inverted.
- **Confidence:** high. Realism: medium for unparseable dates, low for `'|'`.

### F-12: Keys with no usable upstream load time are never judged by HOP_KEY_CURRENCY, HOP_VALUE_AGREEMENT or HOP_FILE_COMPLETENESS

- **Location:** `hop_common.sql.j2:113, 115-117` (judged predicates on `up_load_ts`, NULL-false, including on the full sweep); `hop_file_completeness.sql.j2` (`MIN(load_ts)`).
- **Trigger:** an upstream load-time value that does not parse.
- **Expected:** reported (unassessable or untimed).
- **Actual:** silently absent from the population. The ledger reports it only on its full sweep.
- **Reproduction:** `test_m05_*`.
- **Potential:** false PASS for these checks.
- **Remediation:** an "untimed" count and a FAIL on the full sweep, as the ledger does.
- **Regression test:** m05, inverted.
- **Confidence:** high.

### F-06: Orphans lost in empty windows, or with unparseable load times

- **Location:** `checks/hop/ledger.py:602` (empty_population returned before orphans are considered); `hop_ledger.sql.j2:315, 320` (orphan scan by the downstream load window; a NULL load is never inside).
- **Trigger:** a downstream row with no upstream record, loaded in a window where nothing upstream is due; or a downstream load time that does not parse.
- **Expected:** FAILED (orphan).
- **Actual:** DID_NOT_RUN / empty_population, which is covering (the range moves on), or not counted. Orphans are scanned by load window, even on the full sweep, so the row is never counted later.
- **Reproduction:** `test_l02_*`, `test_l07_*`.
- **Business impact:** substituted or phantom target rows (another writer) go unreported.
- **Remediation:** evaluate orphans before returning empty_population (FAIL on orphans); count unparseable downstream load times as unassessable orphans; include an all-time orphan scan on the full sweep.
- **Confidence:** high.

### F-18: The digest omits withheld failures and the PARTIAL status

- **Location:** `runner.py:1282-1284` (the digest is emailed whatever the run's status); `notify/digest.py:114-115` (`all_clear` over committed rows only), `read_run`.
- **Trigger:** the publisher withholds a unit: a stored-row count mismatch, a failed count, or an unconfirmed commit. Also F-16.
- **Expected:** the digest says that N checks were withheld and that the run was PARTIAL.
- **Actual:** fewer failures, no mention. `v_latest_result` keeps the previous state (possibly a healthy PASS) until the held window is re-evaluated. Only the watchdog alerts, and it goes to its own owner.
- **Reproduction:** `test_operational_candidates.py::test_o04_*`, a control comparison: the failures are counted without withholding and missing with it.
- **Potential:** an under-reported failure, which can read "all clear" when the withheld results were the only failures.
- **Remediation:** pass the run's withheld and unconfirmed counts to the digest; never "all clear" on a PARTIAL run; a line per withheld check.
- **Confidence:** high.

### F-01: Values are verified only through `owned_columns`

- **Location:** `hop_ledger.sql.j2:221` (`FALSE` without owned columns), `:263-264`; the release 1 checks likewise.
- **Trigger:** a wrong value in any column not listed in `owned_columns`, or no owned columns at all.
- **Expected:** divergent, or a qualification that keeps the PASS from being healthy.
- **Actual:** delivered, PASSED, **no qualification**. The scope lists `unverified_columns`, and `values.coverage` is "none".
- **Reproduction:** `test_a05_*`, `test_a06_*`.
- **Business impact:** "delivered" means present at a current version; it does not mean correct. A gold load that writes wrong attributes PASSes.
- **Classification:** design limitation (disclosed in the scope only), with an impact on health.
- **Remediation:** make `values.coverage` of "none" (and optionally "partial") a qualification. Require `owned_columns` on declared contracts for any claim beyond presence.
- **Confidence:** high.

### F-09: Survivorship without owned columns accepts the wrong winner

- **Location:** `hop_ledger.sql.j2:224` (`superseded_by` compares values only when owned columns exist) and the residual.
- **Trigger:** precedence [A, B]; gold holds B's newer row; no owned columns.
- **Actual:** A's edge delivered, B's edge superseded, both PASSED.
- **Reproduction:** `test_m01_*` (with owned columns A is divergent, which is the control).
- **Remediation:** validation must require `owned_columns` for survivorship, or the result is qualified.
- **Confidence:** high.

### F-10: Contributor staleness is never detected

- **Location:** `hop_contribution.sql.j2:26-30` (population = target rows loaded in the window).
- **Trigger:** a contributor (producer, subsidy) changes; the gold row is not reloaded.
- **Actual:** HOP_CONTRIBUTION is DID_NOT_RUN / empty_population (or judges other rows); HOP_LEDGER on the primary edge PASSes.
- **Reproduction:** `test_m02_*`.
- **Business impact:** the multi-source gold entity can carry outdated producer or subsidy values indefinitely, and no check reports it.
- **Remediation:** add a contributor-side population: contributor versions loaded in range whose referencing gold rows were not reloaded with the new values. This is a second assessed mode of the same template.
- **Classification:** design limitation, not disclosed in the blind spots.
- **Confidence:** high.

### S-01: File checks match files by base name across all time

- **Location:** `hop_file_completeness.sql.j2:9, 21` (`regexp_extract(.., '([^/]+)$')`, `MIN(load_ts)`); `t1_files_not_loaded.sql.j2:11`.
- **Trigger:** the same file name delivered again (dated folders, daily `members.csv`).
- **Actual:** in hourly runs the second delivery is never judged (its first load is the first delivery's). On the full sweep the counts are summed, so a loss in one delivery is masked by another. A missing load of a reused name PASSes T1_FILES_NOT_LOADED.
- **Remediation:** identify files by full path, or by name plus landing date, as the registry already does.
- **Confidence:** high (deterministic). Not executed (07 §5).

## Medium and Low findings: compact entries

| ID | Location | Trigger → actual | Remediation | Regression test | Confidence |
| --- | --- | --- | --- | --- | --- |
| F-08 | `hop_ledger.sql.j2:86-100` (due is driven by upstream loads) | Gold re-inserts an excluded key → not due and not an orphan: PASSED until the sweep (l06) | Add keys loaded downstream in the window to the candidates (as `hop_common.sql.j2:116`) | l06 inverted | High |
| F-13 | `:120-131` | History-keeping gold, older version re-loaded as current → delivered (m06) | Declare the current-row predicate (a `feed_filter`) or `grain.history`; rank by load time for append-only targets | m06 | High |
| F-14 | `chain.sql.j2:48` | Soft void applied at curated but not at gold → chain accounted, PASSED (m07) | Carry authorised-but-present records (soft deletes) to the next edge | m07 inverted | High |
| F-03 | `ledger.py:432-434`; `models.py` Tolerance `ge=0` | Tolerated missing → PASSED, healthy (a18) | A `tolerated_deficit` qualification; an upper bound or share-based tolerance | a18 | High |
| F-04 | `hop_ledger.sql.j2:259` | Authorised reject while the record is present and divergent → rejected (a13) | `rejected` only when absent or downstream older (the L14 table) | a13 inverted | High |
| F-11 | `hop_contribution.sql.j2:65` | NULL reference → unresolved, FAILED (m03) | `optional: true` on a contribution; a NULL reference is then accounted | m03 | High |
| F-15 | `checks/base.py:134` | `from_counts(5, None)` → PASSED (o01) | NULL violations or a negative population → DID_NOT_RUN | o01 inverted | High |
| F-16 | `checks/events.py:70-72` | NULL vs `''` groups share an evaluation id → withheld every run (o02) | Encode groups as JSON with an explicit null | o02 inverted | High (collision); medium (withholding, deduced) |
| F-17 | `checks/tier1/volume.py:86`; `models.py:288` | Tolerance ≥ 100% → an empty slot PASSes with population 0 (o03) | Validate the tolerance below 100; population 0 → FAIL or DID_NOT_RUN | o03 inverted | High |
| S-02 | `coverage.py`; `validate.py` | Upstream load stamped at day granularity, visible hours later → never due; hidden until the sweep | Validate granularity against `sla_hours`; disclose | new | Medium |
| S-03 | `digest.py:379` (`gone` ignored by `all_clear`) | A failing group stops arriving → "all clear" | Count vanished groups against all clear | new | Medium |
| S-04 | `causes/landing.py:135-138` | Permission or platform error → UNREADABLE (CONFIRMED) | Treat non-format exceptions as ERROR | new | High |
| S-05 | `causes/probes.py` (`log_contains`), `causes/merge.py` | No logs → RULED_OUT → "suspected" | NOT_READY for absent evidence; exact name match | new | High |
| S-08 | `hop_ledger.sql.j2:186` | Equal `rejected_ts` → arbitrary reason | A deterministic order (defect before authorised) | new | Medium |
| S-09 | `hop_ledger.sql.j2:135-142` | Tied rows resolved by `winner_rule` → divergent (a15 shows the mechanism) | Apply `winner_rule` | a15 variant | High |
| S-10 | `contribution.py:86`; `validate.py:290-294, 414`; `hop_contribution.sql.j2:49, 59` | Padded gold reference / snapshot timing / ignored contract fields → false FAIL | Allow reference normalisers; use the version as of the contributor snapshot; reject unsupported fields in validate | new | Medium |
| S-11 | `ledger.py:105-117, 147-160`; `attribution.py:49-52`; `chain.py:387` | Contributors disable orphan checks; a single dataset with two union contracts proves "verified"; chains through unions always unverified | Count primary-key upstreams only; forbid two union contracts on one dataset; pass the proof to the chain | new | Medium |
| S-12 | `validate.py:259-264` | `key_map` onto a subset of the upstream key → records merge | Warn or require a declared grain change (aggregate) | new | Medium |
| S-13 | `derived_relation.sql.j2:22-25` | History inflation; no key normalisation | Latest-per-key; apply `key_normalise` | new | Medium |
| S-14 | `times.py:150-156`; `events.py:89-92` | Offset values double-shifted; precision mismatch → stale forever | Detect offset formats in validate; truncate both sides to the declared precision | new | Medium |
| S-15 | `value_agreement.py`; `hop_common.sql.j2:95, 116` | No downstream load time → a fixed-but-wrong key not re-judged until the sweep | Require the downstream load time, or judge keys cleared by HOP_KEY_CURRENCY | new | Medium |
| S-16 | `store/runs.py:87-99`; `models.py:299, 332` | Hourly run longer than 60 min → the next run starts alongside it | An atomic run lease; grace > budget plus investigations | new | High |
| S-17 | `watchdog.py:73-77` | A run started under `grace` before the check is skipped and never re-judged | Re-judge the previous hour's late runs | new | Medium |
| S-06 | `causes/engine.py:155` | A later CONFIRMED wins over an earlier ERROR | Say "earlier stage not evaluated" | new | High |
| S-07 | `runner.py:774-803`; `digest.py:201` | Retried causes never reported | Report retried causes in the next digest | new | Medium |
| S-19 | `runner.py:86-87`; `ddl.sql:283` | RERUN outcome ignores qualifications; `healthy` ignores mode | Include qualifications; exclude CURRENT_RECHECK from current health | new | Medium |
| S-20 | `store/install.py`, `retention.py` | `dq_database` = a monitored database | Refuse a dq database that holds non-dq tables | new | Medium |
| S-21 | `config/sqlcheck.py` | `--` in fragments | Reject comments | new | High |
| S-22 | `ledger.py:408-410` | Balance computed from the same rows | An independent count of candidate keys | new | High |
| S-23 | `hop_ledger.sql.j2:246` | Soft void at an older version → deleted | Require the target at or after the version | new | Medium |

## Documentation discrepancies

These are covered in 01 §3:
- **D-01:** the `superseded` precedence (three documents disagree).
- **D-02:** the L14 table vs `rejected` (F-04).
- **D-03:** `winner_rule` in the model, absent from the ledger.
- **D-04:** "unparseable key is unassessable", not implemented.
- **D-05:** "latest load time + sla" wording.
- **D-06:** the cutoff applied only in the ledger.
- **D-07:** the chain's claim reads as event completeness.
