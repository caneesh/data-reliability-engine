# 04 · CDC, versions and time

## 1. Which guarantee is implemented?

| Kind of reconciliation | Implemented? | Evidence |
| --- | --- | --- |
| **Current-state reconciliation**: the latest version of each key, by record time, is present downstream at that version or later, with owned values equal | **Yes.** This is what HOP_LEDGER, HOP_KEY_CURRENCY, HOP_VALUE_AGREEMENT and CHAIN_COMPLETENESS verify | a01-a08, a16, c01-c04 |
| **Event-completeness reconciliation**: every change event was processed | **No.** Intermediate versions are never judged. ACTIVE → TERMINATED → ACTIVE with gold holding only the final ACTIVE is `delivered`; nothing proves the TERMINATED was processed | a17 |
| **Historical point-in-time reconciliation**: the target as it stood at time T | **Only in AS_OF_REPLAY, for HOP_LEDGER**, and only where every dataset read declares `grain.history: append_only` with drift evidence; otherwise DID_NOT_RUN / no_as_of_evidence. NORMAL runs and RERUN read the target **as it is now** | a25; `checks/modes.py`; KI-06 |

**Correct for current state.** DRE does not demand that every historical version exist in current-state gold (a17 PASS). It also makes **no** claim that intermediate events were processed. The chain's wording ("every expected business record reached the terminal") should say "latest version" so nobody reads it as event completeness (01 §3).

## 2. Version selection

| Case | Behaviour | Evidence | Verdict |
| --- | --- | --- | --- |
| Insert | The key's only version is judged at its deadline | a01 | Correct |
| Update (several in one window) | The latest record time is judged | a17 | Correct for current state |
| Out-of-order CDC (an older record time loaded later) | The latest **record time** is still the version judged; gold holding it passes, older is stale | a16 | Correct |
| Duplicate identical events | One record | a14 | Correct |
| Duplicate events with conflicting values at one record time | Divergent: the sets differ (fails closed); a false FAIL when the pipeline resolves the tie by `winner_rule` | a15; S-09 | Fails closed |
| Delete (hard or soft tombstone) | deleted / deleted_not_applied | a12 | Correct, except tied rows (F-07, l04) and the soft version (S-23) |
| The newest version with a NULL or unparseable record time | **Dropped**: the older version is judged, and the key may not even be due | l01, m04 | **F-05 (false PASS)** |
| Late-arriving data (loaded after the window end) | Upstream: belongs to a later window (cutoff). Downstream: counted as delivered now (no cutoff) | a25 | By design (current state) |
| Historical correction or backfill (an older record time loaded now) | Not the top version: the key is not judged again. A gold that lacks the backfilled history is not a deficit | a17 (by the same mechanism) | Current-state semantics. History completeness is not claimed |
| Replay of events with a new load time and the same record time | The key's `up_load_ts` = MAX over the tied rows, so the key becomes due again and is counted in a second window | static | Low: double reporting, not a false PASS |
| A target keeping history (SCD2, append-only), current row chosen by load order or a flag | The newest **record time** is taken as current: a regression that re-loads an older version is invisible | m06 | **F-13** (configuration-dependent; a `feed_filter` that selects current rows avoids it) |

## 3. Time handling

| Aspect | Behaviour | Verdict |
| --- | --- | --- |
| Record time vs load time | Never compared with each other (verified in the SQL) | Correct (CLAUDE.md rule) |
| Parsing | Every time column parsed with its configured format, then converted to UTC | Correct. Double conversion is possible when a value already carries an offset (S-14) |
| DST fall-back hour | Reads as the first offset; spec section 4 and `dre validate` disclose it | Disclosed limitation |
| Precision | `granularity` applies to window bounds only, not to record-time comparisons | False FAIL risk (S-14) |
| SLA deadlines | `load + sla_hours`, judged once in the deadline range that continues from committed coverage | Correct, provided the load time reflects when the record became **visible** downstream-readable. Otherwise S-02 |
| Window boundaries | `[start, end)`; a deadline exactly at the range end is pending | Correct (existing `test_the_range_bounds_are_exact`) |
| Retention boundaries | CA-01 (`coverage_gap`), CA-02 (chain carry), KI-19 (open keys) | Verified by the existing tests (not re-executed here beyond the CA and KI-19 runs of 2026-10-21) |

## 4. Record identity (section 4 of the brief)

| Aspect | Behaviour | Evidence |
| --- | --- | --- |
| Composite keys | Normalised parts joined with `'|'` | **F-02**: collisions on `'|'` and `'<null>'` (a19, a20, c05) |
| Null key parts | A partly null key is a key (`<null>`); a fully null key is unassessable 'no key' | a20; existing |
| Duplicate keys upstream (same version) | One record; tied values compared as sets | a14, a15 |
| Zero padding | `strip_leading_zeros` | Existing tests. Lossy: `'0'` and `'00'` become `''` (seen in the benchmark fixture) |
| Case and whitespace | Only through the `upper`/`lower`/`trim` normalisers. Without them a false FAIL, never a false PASS | a21 |
| Date normalisation | `parse_date`; an invalid date becomes `<null>` and joins with a real NULL | l05 |
| Hash collisions | HMAC-SHA-256 of the canonical key, used for key events only; joins use the canonical string | Negligible. Changing `key_normalise` or the secret orphans the open keys until the full sweep |
| Key reuse across entities | Indistinguishable when the raw values are identical | Inherent; no check can detect it from keys alone |
| Grain differences (RMS at coverage grain, System80 at member grain) | `key_map` may map onto **part** of the upstream key, and validate allows a strict subset. The upstream records then collapse into one ledger key, and only the latest is judged | Static, S-12. An additional coverage row is neither a duplicate nor missing; it is merged into its member key, and its own completeness is **not** verified |
