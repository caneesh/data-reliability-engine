# 02 · Specification amendments (proposed text; not applied)

**Nothing in this document has been applied.** `docs/spec.md`, `docs/decisions.md` and `docs/design/*` are unchanged.

**What each amendment gives:**
- the **existing section** (with its line at `1d88659`);
- the **current text**, quoted;
- the **proposed replacement**;
- the **reason**;
- the **affected tests**: existing tests that must stay unchanged, existing tests that must be restated because their rule changes (named, never weakened), and new tests (the IDs of 04 and 05).

**Ordering.** Amendments A-3 and A-9 to A-11 belong to release N; the rest to release N+1. All are approved together before release N starts (U-6). Release N must not implement N+1 behaviour.

---

## A-1 · Canonical key string (release N+1)

**Section:** §6 "Hop details", first bullet (line 641).

**Current:**
> Keys on both sides are built from the mapped columns with each side's `key_normalise`, as one string (parts joined with `|`, null written as `<null>`).

**Proposed:**
> Keys on both sides are built from the mapped columns with each side's `key_normalise`, in this dataset's key order, as one canonical key (key encoding 2):
>
> - Each part is rendered as one of three byte sequences:
>   - `~;` for a NULL part;
>   - `<n>:<bytes>;` for a value, where `<bytes>` is the part's canonical text and `<n>` is its length in bytes, in ASCII decimal without leading zeros (the empty string is `0:;`);
>   - `!<n>:<raw bytes>;` for an **unparseable** part, whose raw value is not NULL but whose normaliser returned NULL. `<raw bytes>` is the raw value's canonical text.
> - The key is the concatenation of its parts.
> - The canonical text of a part depends on its type after `key_normalise`:
>   - STRING, VARCHAR and CHAR: the bytes as read, with no Unicode normalisation, trimming or case change beyond the declared normalisers;
>   - TINYINT to BIGINT: ASCII decimal with `-` for negatives;
>   - DECIMAL(p, s): the value cast to STRING (exactly s fractional digits; scientific notation below 10^-6, as Spark renders it);
>   - DATE: `yyyy-MM-dd`;
>   - TIMESTAMP: `yyyy-MM-dd HH:mm:ss.SSSSSS` in UTC;
>   - BOOLEAN: `true` or `false`.
> - A key part of any other type (FLOAT, DOUBLE, BINARY, ARRAY, MAP, STRUCT, INTERVAL, TIMESTAMP_NTZ, VOID) makes every check that builds the hop's keys DID_NOT_RUN / invalid_config. The detail names the dataset, the column and its type, and says to declare a normaliser that yields a supported type.
> - A key with an unparseable part is `unassessable` ("unparseable key") on the side where it occurs.
> - A row whose every key column is NULL **as read** has no key (unchanged).
> - Equal canonical keys are the same record; distinct tuples always give distinct keys.
> - The engine records the encoding of every stored key in `dq_key_event.key_encoding` (section 5). Encoding 1 (parts joined with `|`, null written as `<null>`) is read only to carry open keys across the change (section 6, "Key events").

**Reason:** F-02 (examination a19, a20, l05, c05); revision 2 R2-01 to R2-05; ADR-02 ("collisions … detected at run time").

**Affected tests:**
- *Restated (the rule changed, so the expected key string changes; assertions otherwise identical):*
  - `tests/checks/test_hop.py` `KEY` (line 36) and its uses at lines 370 and 562 (`"<null>|01|2026-01-01|AGR-A"`);
  - `tests/replay/test_r04_to_r07_hops.py` `KEY` (line 22);
  - `tests/causes/test_causes.py` `KEY` (line 41).

  Each will compute the expected key with a test helper that mirrors encoding 2, so the restatement is visible in one place.
- *Unchanged:* `tests/test_trace.py` (the CLI key input format is unchanged).
- *New:* T-F02-1 to T-F02-9 (05).

## A-2 · The key hash (release N+1)

**Section:** §5, after the views (line 574).

**Current:**
> `key_hash` is HMAC-SHA256 of the normalised key, with the secret read from a protected file named in config, never from the repository.

**Proposed:**
> `key_hash` is HMAC-SHA256, as lower-case hex, of the bytes of the canonical key (section 6, "Hop details"), with the secret read from a protected file named in config, never from the repository. The hash is computed in SQL over the key's bytes (the key cast to BINARY), so no byte is altered on the way to the HMAC. For a key that is valid UTF-8 this equals the HMAC of the key's text. The algorithm, the secret and its handling are unchanged by key encoding 2; only the hashed input changes, so every key's hash changes once.

**Reason:** R2-03 (a Python UDF receiving text loses invalid bytes); U-3 (same algorithm, same key management).

**Affected tests:**
- *Unchanged:* `tests/checks/test_hop.py::test_sql_key_hash_matches_python_hmac` (the STRING argument must still work).
- *New:* T-F02-7.

## A-3 · `dq_key_event` columns and events (release N)

**Section:** §5 DDL, `dq.dq_key_event` (lines 461-466).

**Current:**
> `event STRING, -- FLAGGED | STILL_FLAGGED | CLEARED; CHAIN_COMPLETENESS: LOST | UNOBSERVABLE | PENDING | RESOLVED (never open keys)`
> `state_detail STRING, observed_at TIMESTAMP`

**Proposed:**
> `event STRING, -- FLAGGED | STILL_FLAGGED | CLEARED | REKEYED; CHAIN_COMPLETENESS: LOST | UNOBSERVABLE | PENDING | RESOLVED | REKEYED (never open keys)`
> `state_detail STRING, observed_at TIMESTAMP,`
> `key_encoding INT, -- migration 2: the encoding of key_value and key_hash (2: section 6); NULL: 1, rows stored before the column existed`
> `predecessor_hash STRING -- migration 2: the hash, in the other encoding, whose open spell this event continues; NULL otherwise`
>
> The DDL is followed by:
> `REKEYED` closes nothing. It records that an identity's open spell continues under another key encoding (section 6, "Key events").

**Reason:** R2-D12; the age of discrepancies preserved (U-4).

**Affected tests:**
- *Unchanged:* `tests/test_evaluation_modes.py` migration tests. `checkpoint3_store` strips only `evaluation_mode`, so `pending == [1]` still holds. Line 107 checks contiguous versions, which 1 and 2 satisfy.
- *New:* T-MIG-1, T-MIG-3.

## A-4 · Untimed rows and the version (release N+1)

**Sections:** §6 HOP_KEY_CURRENCY "Logic" (line 605); HOP_LEDGER "Dispositions" (line 613).

**Current (605):**
> UNASSESSABLE (all of the key's upstream rows, or its rows here, have no usable record time: kept and reported, never passed)

**Proposed (605):**
> UNASSESSABLE (the key's version cannot be determined: a relevant row of the key, upstream or here, has no usable record time; section 6, "Untimed rows"; or the key is unparseable or collides: kept and reported, never passed)

**Current (613), within `unassessable`:**
> no record time on either side

**Proposed (613):**
> a relevant row with no usable record time on either side ("unparseable record time" when its raw value is not NULL, "no record time" otherwise; "… here" for this dataset)

**New paragraph, §6 "Hop details", after "Ties":**
> **Untimed rows.** A row whose record time is NULL or does not parse is untimed. An untimed row of a key, among the rows loaded before the cutoff, is **relevant** unless the dataset declares a disorder bound D and the row was loaded more than D before the load time of the key's version (the latest load time among the rows at the latest record time).
> - With no D, every untimed row is relevant. So is a row with no load time, and so is any untimed row of a key whose version has no load time or that has no timed row.
> - A relevant untimed row makes the key unassessable.
> - A row that is not relevant is set aside and counted in the scope. Every result that set aside at least one row is qualified `version_by_disorder_bound` (section 5).
> - The comparison is between two load times, never with a record time. D never chooses a version.
> - A key's deadline is its version's load time, or the latest load time of a relevant untimed row if later, plus `sla_hours`. So a new untimed row makes the key due again, and a set-aside row never does.
> - **Refutation.** When any key of the evaluation has a timed row older than its version but loaded more than D after the version's load time, the evaluation applies no bound (every untimed row is relevant). The scope and the digest say so.

**Reason:** F-05 (l01, m04); R2-06; U-5.

**Affected tests:**
- *Unchanged:*
  - `test_hop.py::test_a_key_whose_upstream_record_times_are_all_null_is_unassessable_not_hidden` (line 392; its observed text at 403 is kept);
  - `test_hop_ledger.py::test_no_record_time_and_no_key_are_unassessable` (line 170);
  - `test_hop_ledger.py::test_a_key_with_no_load_time_is_untimed_then_unassessable_on_the_full_sweep` (line 180).
- *New:* T-F05-1 to T-F05-10.

## A-5 · The disorder bound in configuration (release N+1)

**Section:** §3, the dataset's `record_time` block (around line 187), and §3 "Validation" (line 362).

**Proposed addition to the `record_time` example:**
```yaml
record_time: { column: sourcelastupdatets, format: "...", timezone: America/Chicago,
               disorder_bound: null }   # optional; { hours: <int >= 1>, approved_by: <name>, approved_on: <date>,
                                        #   evidence: <reference> } -- see section 6, "Untimed rows"
```

**Proposed addition to "Validation":**
> A `disorder_bound` needs `hours` (an integer ≥ 1), `approved_by`, `approved_on` and `evidence`; any missing field is an error naming the file, line and field. `null` (the default) means no bound.

**Reason:** U-5 (NULL by default; a non-NULL value needs dataset evidence and owner approval).

**Affected tests:**
- *Unchanged:* `dre validate --conf conf/` (the sample stays `null`).
- *New:* T-CFG-1 to T-CFG-4.

## A-6 · Tied rows in the ledger (release N+1)

**Section:** §6 HOP_LEDGER "Dispositions" (line 613).

**Proposed insertion after the list of `unassessable` reasons:**
> … "tied rows disagree" (the rows at the version expect different target states: present, absent (a tombstone under `applied_as: hard`, or an approved exclusion matching the row), or soft-deleted (a tombstone under `applied_as: soft`)); "reject ambiguous among tied rows" (a reject names this version, and the rows at the version are not identical).

**Proposed replacement for the `excluded` clause:**
> `excluded[rule@version]` (every row at the version is matched by an approved exclusion effective at **that row's** load time, and the record is absent here; rows covered by different exclusions are excluded under each, and each rule's volume guard counts the record)

**Proposed replacement for the `deleted` clause:**
> `deleted` (every row at the version expects the record deleted: tombstones, or under `applied_as: hard` tombstones and approved exclusions together, and this dataset shows the delete …)

**Proposed replacement for "the latest such reject gives the reason and the rule":**
> the latest such reject gives the reason and the rule; rejects at the same time are ordered defect before authorised, then by reason, then by rule id, so the choice never depends on the read order.

**Reason:** F-07 (l03, l04); R2-07; C-3 (the per-row load time is the executable behaviour, `common.py:136-160`).

**Affected tests:**
- *Unchanged:* all of `tests/checks/test_hop_ledger.py`; `tests/replay/test_r19_cdc_versions.py` (single-row versions); R06.
- *New:* T-F07-1 to T-F07-7.

## A-7 · The qualification list (release N+1)

**Sections:** §5 item 7 (line 555); §6 HOP_LEDGER "Qualifications and the claim" (line 620).

**Proposed:** add to both lists:
> `version_by_disorder_bound` (untimed rows were set aside on the dataset's declared disorder bound; section 6, "Untimed rows")

and to line 620:
> It applies to every hop check and is carried by CHAIN_COMPLETENESS.

**Reason:** U-1.

**Affected tests:**
- *Unchanged:* `test_hop_ledger.py` lines 486-532 (their fixtures declare no D).
- *New:* T-F05-9, T-H-1 to T-H-4.

## A-8 · Unparseable target keys (release N+1)

**Section:** §6 HOP_LEDGER "Orphans" (line 616).

**Proposed addition:**
> **Unparseable target keys:** rows here, loaded in the window (every row on the full sweep), whose key has an unparseable part. They never match an upstream key and are counted on their own breach line, `unparseable target key`. When orphans are counted, they are reported with the orphans; otherwise they are counted in the scope and the triage line. The release 1 hop checks exclude them from this dataset's keys and count them in `observed`.

**Reason:** R2-08, C-2.

**Affected tests:** new T-DN-1 to T-DN-5.

## A-9 · `v_open_keys` (release N)

**Section:** §5 views (line 570).

**Current:**
> `dq.v_open_keys`: keys whose latest committed `dq_key_event` at a hop is FLAGGED or STILL_FLAGGED, with first flagged date (per dataset, check, hop and key)

**Proposed:**
> `dq.v_open_keys`: keys whose latest committed `dq_key_event` at a hop is FLAGGED or STILL_FLAGGED, with `key_encoding`, `predecessor_hash` and the first flagged date of the key's open spell. A spell ends only at CLEARED. When the spell began with an event naming a `predecessor_hash` whose own latest event is REKEYED with a provable continuation (section 6, "Key events"), the first flagged date is the predecessor's. A key whose latest event is REKEYED is not open.

**Reason:** R2-09 (the age reset); U-4.

**Affected tests:**
- *Unchanged:*
  - `tests/store/test_views.py::test_open_keys` (its assertions hold: same rows, same dates, `key_value` absent);
  - `tests/test_execution_types.py:99`;
  - `tests/store/test_install.py:74-79`.
- *New:* T-MIG-2.

## A-10 · Key events across the encoding change (release N: reading; release N+1: transition)

**Section:** §6 "Key events" (lines 651-653), and §7 / §8 "Trace" (lines 779, 821).

**Proposed new sub-bullet under "Key events":**
> **Key encodings.** Each event records its `key_encoding`. A hop reads open keys of its engine's encoding only, and takes REKEYED as no event for that purpose.
>
> On the first NORMAL evaluation of a hop by an engine of encoding 2 while encoding-1 keys are open there, the evaluation reads every upstream row before the cutoff once and finds each open key's successors: the keys whose encoding-1 string equals the stored `key_value`.
> - **A provable continuation**, written REKEYED (encoding 1) and STILL_FLAGGED or CLEARED under encoding 2 with `predecessor_hash`: the stored string decodes to exactly one tuple given the types of the parts, and that tuple's key exists. Its age carries over.
> - **No successor**, written CLEARED "no longer upstream": the full-sweep conclusion.
> - **Otherwise** (several successors, or a string that does not decode to one tuple): the encoding-1 key stays open, re-emitted STILL_FLAGGED with `state_detail` "awaiting successors" and its original first flagged date, while any successor is open. Each successor still wrong is FLAGGED under encoding 2 with `predecessor_hash` and its own age. The encoding-1 key is CLEARED "successors current" when none is open.
>
> All of this is written in the evaluation's own commit. An engine of encoding 1 that finds encoding-2 keys open at a hop judges the hop as on the full sweep and writes REKEYED for them, continuing the encoding-1 spells, which REKEYED never ended. CHAIN_COMPLETENESS's PENDING records follow the same rules.

**Proposed addition to §7, "What a cause explains":**
> A `failure_ref` that is a `key_hash` is in the key encoding of the engine of its run (`dq_run.engine_version`). A retried investigation writes its own engine's encoding.

**Proposed addition to "Trace":**
> … found by its `key_hash` in either key encoding, with each line labelled by its encoding.

**Reason:** R2-09, R2-D13, C-1, C-16, U-4.

**Affected tests:**
- *Unchanged:* `tests/checks/test_open_keys_idle.py`; `tests/checks/test_chain.py` (single-encoding stores).
- *New:* T-MIG-4 to T-MIG-7, T-MIG-9.

## A-11 · Session time zone precondition (release N)

**Section:** §8 command table, `dre run` exit 3 list (line 790).

**Proposed addition to the exit-3 list:**
> the Spark session's time zone is not UTC after the engine set it (`spark.sql.session.timeZone`)

**Reason:** R2-05 / k4 (a DST zone makes two instants render alike); C-7.

**Affected tests:** new T-N-6.

## A-12 · Ties in HOP_VALUE_AGREEMENT: wording only (needs confirmation, Q-1)

**Section:** §6 "Ties" (line 649).

**Current:**
> Every upstream row at a key's latest record time is kept. HOP\_VALUE\_AGREEMENT requires each of them to match this dataset's latest row, so tied upstream rows that disagree with each other always fail.

**Proposed:**
> Every upstream row at a key's latest record time is kept. HOP\_VALUE\_AGREEMENT compares the set of their owned-column values with the set at this dataset's latest record time (as stated above), so tied upstream rows that disagree with each other fail unless this dataset holds the same set of values. `winner_rule` does not resolve the tie; TIE\_RESOLVED\_BY\_RULE explains such a failure (section 7).

**Reason:** SC-1. This is the executable behaviour (`test_value_agreement_compares_the_tied_latest_rows_as_sets`, `test_r06_…`). No behaviour change.

**Affected tests:** none change.

## A-13 · Reconciliation model wording (release N+1)

**Section:** `docs/design/reconciliation-model.md` §4 (line 90) and the `dedup` row (line 97).

**Current (90):**
> its version is the dataset's `record_time`, with ties ordered by a hop's `winner_rule`.

**Proposed (90):**
> its version is the dataset's `record_time`; every row at the latest record time is kept, and `winner_rule` orders nothing in a check (it is read only by the TIE_RESOLVED_BY_RULE cause).

**Current (97, status column):** `Built (winner_rule)`

**Proposed (97):** `Built (key_map; version by record_time)`

**Proposed addition to model §8, after the time table:**
> A dataset's disorder bound compares load time with load time (spec section 6, "Untimed rows"); it never chooses a version.

**Reason:** D-03; R2-D15. The model is frozen (target-architecture status line), so this change needs a decision-log entry (A-15).

**Affected tests:** none.

## A-14 · Guard rule (release N+1)

**Section:** §9 "Guard tests", new rule 12.

**Proposed:**
> 12. **Key encoding.** `key_string_v1` (encoding 1) is called only from the transition (`checks/hop/rekey.py`) and from trace; every other key-building call site uses the canonical key.

**Reason:** a partial switch would compare encodings and match nothing (rev-2 02 §6).

**Affected tests:** new `tests/guard/test_key_encoding.py` (T-F02-8).

## A-15 · Decision log entry (applied only after approval)

**Proposed text for `docs/decisions.md`, under a new heading "Gate A: key identity, untimed rows, ties (approved YYYY-MM-DD)":**

> Approved by the user on YYYY-MM-DD: spec amendments A-1 to A-14 of `docs/reviews/gate-a-step0/02-specification-amendments.md`, superseding the original Gate A design's G-1, G-3 and G-7 and refining G-2, G-5 and G-6. The previously approved behaviours changed: the key string (spec §6, line 641), HOP_KEY_CURRENCY's UNASSESSABLE (line 605) and the ledger's "no record time" (line 613). Unchanged: `winner_rule` semantics, R06, the value-set tie rule, ADR-01 to ADR-08 (ADR-02's collision rule is implemented, not changed). Open decisions taken as defaults: (list from 06).
