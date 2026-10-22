# 05 · Release N+1 (Gate A) acceptance criteria

Release N+1 implements key encoding 2, the temporal-eligibility rule, the tie rule, downstream key integrity and the transition. It starts only after release N is merged, deployed and writing `key_encoding = 1` (04).

## 0. Preconditions

| ID | Criterion |
| --- | --- |
| P-P1 | Amendments A-1, A-2, A-4 to A-8, A-10 (transition), A-12 (after Q-1), A-13 and A-14 approved and applied with the decision-log entry (A-15) |
| P-P2 | Release N deployed; its evidence note recorded |
| P-P3 | Owner inputs recorded: O-2 (no refused key types, or the affected datasets named and accepted as DID_NOT_RUN until a normaliser is declared); O-3 acknowledged |
| P-P4 | Explicit instruction to begin release N+1 |

## 1. Functional criteria (new tests)

### F-02: canonical key (contract §1)

| ID | Proves |
| --- | --- |
| T-F02-1 | a19 (a `|` inside a part): two records give two keys; each judged on its own |
| T-F02-2 | a20 (NULL against the text `<null>`): two keys |
| T-F02-3 | l05: an upstream NULL part never matches a target unparseable part; the record is `missing`; the target row is counted as `unparseable target key` |
| T-F02-4 | c05 (chain): no record is carried under another record's key |
| T-F02-5 | The engine's key equals the test mirror on generated tuples (the l1 domain); split and NULL pairs never collide (l2); invalid UTF-8 stays distinct (l3); the key read back from ORC matches (l5) |
| T-F02-6 | Each refused type gives DID_NOT_RUN / invalid_config, naming the dataset, column and type, with the fix; each supported type is judged |
| T-F02-7 | `key_hash` of keys differing only in invalid bytes differ; for valid UTF-8 the hash equals `HMAC(secret, key text)`; `test_sql_key_hash_matches_python_hmac` passes unchanged |
| T-F02-8 | Guard (A-14): `key_string_v1` is called only by the transition and trace |
| T-F02-9 | TIMESTAMP parts render as `yyyy-MM-dd HH:mm:ss.SSSSSS` UTC; DATE as `yyyy-MM-dd`; DECIMAL as cast to STRING (negative, very small in scientific notation, large: the values of `check_char_and_decimal.py`) |

### F-05: temporal eligibility (contract §2)

| ID | Proves |
| --- | --- |
| T-F05-1 | l01: the newest row untimed gives unassessable "no record time", due in its window (never an older version judged) |
| T-F05-2 | m04 shape: an unparseable newest row gives "unparseable record time" |
| T-F05-3 | No D: an old untimed row keeps the key unassessable |
| T-F05-4 | D declared: a row loaded more than D before the version's load is set aside and counted; the key is judged; the boundary (exactly D) is relevant |
| T-F05-5 | An untimed row with no load time is always relevant |
| T-F05-6 | A late untimed row after the cutoff belongs to a later window; a backfill does not re-judge the version |
| T-F05-7 | Refutation: one key's out-of-order timed pair beyond D makes the whole evaluation strict; the scope and the digest say so (rev-2 t2) |
| T-F05-8 | The target side: labels ending "… here" |
| T-F05-9 | Every hop check that set a row aside is qualified `version_by_disorder_bound` and never healthy |
| T-F05-10 | Point-in-time (AS_OF_REPLAY): only rows loaded before the cutoff count, untimed ones included |

### F-07: ties and dispositions (contract §4)

| ID | Proves |
| --- | --- |
| T-F07-1 | l03: live + excluded tied gives `tied rows disagree` (no longer `excluded`) |
| T-F07-2 | l04: live + tombstone tied gives `tied rows disagree` (no longer `deleted`) |
| T-F07-3 | Tombstone + exclusion (hard): `deleted`, with `excluded_by` recorded; target present: `deleted_not_applied` |
| T-F07-4 | Two different approved exclusions: `excluded` under both; each volume guard counts it |
| T-F07-5 | Exclusion effectiveness at each row's own load time (two tied rows with load times either side of an effective date) |
| T-F07-6 | Soft-deleted + absent: `tied rows disagree` |
| T-F07-7 | Rejects: defect-first total order at equal time (repeated reads, shuffle partitions 1 and 8); a reject among differing tied rows gives `reject ambiguous among tied rows` |

### Downstream, health, configuration and parity

| ID | Proves |
| --- | --- |
| T-DN-1 | Unparseable target rows never match |
| T-DN-2 | Counted on `unparseable target key` with the orphans when orphans are checked |
| T-DN-3 | Counted in the scope and triage when orphans are not checked (the result keeps `orphans_unchecked`) |
| T-DN-4 | Duplicates are counted per row; the full sweep counts every such row |
| T-DN-5 | Release 1 checks, chain and HOP_CONTRIBUTION behave per contract M-4 |
| T-H-1 | `dq_check_result.qualifications` holds `version_by_disorder_bound`, and `v_latest_result.healthy` is false |
| T-H-2 | The digest lists the result under "Passed with qualifications", counts it in the subject, excludes it from the healthy count, and suppresses "all clear" |
| T-H-3 | A chain over an edge that set a row aside carries the qualification |
| T-H-4 | The scope's claim reads `… qualified: version_by_disorder_bound` |
| T-CFG-1 … 4 | A `disorder_bound` with a missing field, a non-integer, or `hours` < 1 fails validation, naming the file, line and field; `null` validates and means strict |
| T-R1-1 … 3 | HOP_KEY_CURRENCY and HOP_VALUE_AGREEMENT agree with HOP_LEDGER on the same fixtures for the unassessable reasons (unparseable key, record times, ties) |

### Transition (contract §6)

| ID | Proves |
| --- | --- |
| T-MIG-5 | A provable 1:1 continuation: REKEYED (encoding 1) plus STILL_FLAGGED (encoding 2) with `predecessor_hash`; age kept in `v_open_keys` |
| T-MIG-7 | 1:n, a non-provable 1:1 (a `<null>` token in a string part), and 1:0: the legacy identity stays open with its original age ("awaiting successors") until no successor is open, then is CLEARED; 1:0 gives CLEARED "no longer upstream"; the successors carry their predecessor |
| T-MIG-9 | An interrupted transition is invisible (no commit row); the rerun is idempotent; N+1 writes encoding-1 events **only** for legacy identities |

## 2. Restated existing tests (their rule changes; assertions otherwise unchanged)

| Test | Why it is restated | What changes |
| --- | --- | --- |
| `tests/checks/test_hop.py` `KEY` (36), lines 370 and 562 | A-1: the stored `key_value` is encoding 2 | The expected key string, computed by one test helper |
| `tests/replay/test_r04_to_r07_hops.py` `KEY` (22) | A-1 | The same |
| `tests/causes/test_causes.py` `KEY` (41) | A-1 | The same |

No other existing test may change. If one fails, the engine is fixed, or the conflict is raised as a spec question (CLAUDE.md).

## 3. Non-regression criteria

| ID | Criterion |
| --- | --- |
| P-R1 | The approved-semantics tests pass **unchanged**: `test_value_agreement_compares_the_tied_latest_rows_as_sets`, `test_r06_tied_open_and_terminated_rows_is_value_agreement_failed`, `test_no_record_time_and_no_key_are_unassessable`, `test_an_identity_hop_requires_one_record_on_each_side`, `test_a_key_whose_upstream_record_times_are_all_null_is_unassessable_not_hidden` |
| P-R2 | Every replay scenario (R01 to R12 and later) passes unchanged |
| P-R3 | The full suite, e2e, guard tests, `dre validate`, packaging and dry-run pass; CI is green on the pushed commit |
| P-R4 | The examination's eight finding scenarios (a19, a20, l05, c05, l01, m04, l03, l04) are ported into `tests/` with the fixed outcome. The files under `docs/reviews/reconciliation-examination/adversarial/` stay unchanged as historical evidence. Against N+1 they are expected to fail, and the evidence note says so |

## 4. Rollback and operational criteria

| ID | Criterion |
| --- | --- |
| P-O1 | **End-to-end demonstration** (R-9) on a scratch store with synthetic data, using the real N and N+1 builds: N → N+1 → N → N+1. At each switch, every open discrepancy is open afterwards (with its age where continuity is provable), no event is lost, and no stored row changes. The record goes to `docs/design/evidence/gate-a-release-n-plus-1.md` |
| P-O2 | Indicative cost on synthetic volume: key width (expected about 10 bytes more per key), the one-off transition cost per hop, and the per-evaluation cost of legacy identities "awaiting successors" (bounded by their number). Recorded, not claimed as cluster performance. The representative-cluster load test (ADR-08 proof) remains a separate, unmet production gate |
| P-O3 | The release notes state `KEY_ENCODING = 2`, that every `key_hash` changes once (R-8, O-3), and how to roll back to N |
| P-O4 | No migration or run against a shared environment without separate authorisation. No production-readiness claim |
