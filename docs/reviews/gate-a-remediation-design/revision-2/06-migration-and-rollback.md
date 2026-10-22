# 06 · Migration and rollback (revision 2)

This replaces the original G-7 transition (a forced full sweep with closing events) and enlarges release N (R2-09). R2-10 turns out to be covered by an existing guard (§3.6).
- **Status.** Proposed.
- **Evidence.**
  - Failure injection on the **real** writer (`test_migration.py::test_m1`).
  - A prototype of the tolerant writer (`test_m2`).
  - A prototype of the key-event state machine (`test_m3`–`test_m7`).
- **Rollback** is **designed and prototyped at state-machine level, not demonstrated end to end** with real release binaries. That demonstration is a gate before N+1 (10).

## 1. Facts the design relies on (read)

| Fact | Where |
| --- | --- |
| The writer requires a frame to carry exactly the table's columns: a missing or extra column raises | `store/writer.py:19-39`; failure injected in `test_m1` |
| Migrations only `ADD COLUMNS`; stored rows read the new column as NULL; pending = a column absent; no bookkeeping table | `store/migrations.py:1-60` |
| `dre run` refuses to start while a migration is pending | `tests/test_evaluation_modes.py:75` (`missing_objects` reports `migration pending`) |
| Views are replaced by `dre install --apply` (`CREATE OR REPLACE VIEW`), not by `dre run` | `store/ddl.sql`, `store/schema.py:13-15` |
| Every current-state view reads committed rows only. An evaluation interrupted before its commit row is invisible and is evaluated again | ADR-07, `ddl.sql:60-75` |
| `v_open_keys.first_flagged_at` is per `key_hash`: the MIN of the open events after the last CLEARED | `ddl.sql:299-322` |
| The engine's own open-key logic is in `hop_common.sql.j2:36-50` (reads `v_committed_key_event`, re-implementing `v_open_keys`). Chain carry is in `chain.py:338-345` (latest event `PENDING` with a `key_value`) | read |
| `evaluation_id` includes `engine_version`, so results of a new engine never overwrite old ones | `events.py:75-78` |
| `dq_run.engine_version` is recorded per run | `ddl.sql:11` |

## 2. Why the original transition is replaced

- **Age reset.** Closing every legacy open hash and opening a new one resets `first_flagged_at` for every open discrepancy on the upgrade day. Age-based escalation and the digest's "open since" would silently lose history (R2-09).
- **Rollback claim.** The original "release N's writer tolerates the new column" covered only the writer. Release N's **readers** would still:
  - see encoding-2 events they cannot interpret;
  - see legacy hashes closed by events written for a successor they cannot compute.

  And a pre-N engine cannot write at all once migration 2 is applied (`test_m1`).

## 3. The design

### 3.1 Migration 2 (ships in release N, not N+1)

```
Migration(2, "dq_key_event", (("key_encoding", "INT"), ("predecessor_hash", "STRING")),
          "Gate A: key encoding version; the hash this key's open spell continues from")
```

- **Shipping it in N** means rolling back from N+1 to N needs **no schema change**: N already knows both columns.
- **NULL `key_encoding`** means encoding 1 (every row stored before N).
- **N writes `key_encoding = 1`** and `predecessor_hash = NULL` explicitly.

### 3.2 New event: `REKEYED`

- `REKEYED` is written for an identity whose open spell **continues under another encoding**.
- It does **not** end the spell: only `CLEARED` does, as today. This keeps the age of a discrepancy unbroken across an upgrade, a rollback and a re-upgrade.
- Its `state_detail` is one of:
  - `key encoding 2`;
  - `ambiguous successor`;
  - `key encoding 1 (rollback)`.

### 3.3 `v_open_keys` (replaced in release N; one definition for both releases)

- Adds `key_encoding` and `predecessor_hash` to the projection.
- `REKEYED` as the latest event means **not open**: the discrepancy is open under its successor.
- **Lineage:** for an open hash whose current spell began with an event carrying `predecessor_hash`, and whose predecessor's latest event is `REKEYED 'key encoding 2'` or `REKEYED 'key encoding 1 (rollback)'` (a 1:1 continuation), `first_flagged_at` is the predecessor's `first_flagged_at`.
- **One level of lineage is enough.** Encodings alternate between the same two hashes, and REKEYED never ends a spell, so the predecessor's own spell already carries the original age.
- A 1:n successor does **not** inherit an age (§3.4): which successor was the open one is unknown. Its predecessor is recorded for audit.

### 3.4 Release N+1's transition: remap, not sweep

The transition is per hop, and per chain edge for PENDING records. It runs in the first N+1 evaluation of each hop whose legacy state is open: the latest event of an encoding-1 hash, excluding `REKEYED`, is `FLAGGED` or `STILL_FLAGGED` (or `PENDING` for the chain).

1. **Read the legacy open set:** `(key_hash, key_value)` of encoding-1 open identities (`key_value` is the stored encoding-1 string).
2. **Compute each successor once.** Compute `key_string_v1` and the LPB key over **every** upstream row before the cutoff (`up_all`, the full-sweep read), and join on `key_value`.
3. **Emit, by multiplicity:**

| Successors | Legacy identity | Successor identity | Evidence |
| --- | --- | --- | --- |
| 1 (1:1), still wrong | `REKEYED 'key encoding 2'` (enc 1) | `STILL_FLAGGED` (enc 2, `predecessor_hash` = legacy hash): age inherited | `test_m3` (K1) |
| 1 (1:1), now correct | `REKEYED 'key encoding 2'` (enc 1) | none; the key is judged normally from here | `test_m3` model (K2's branch) |
| n > 1 (a legacy collision) | `REKEYED 'ambiguous successor'` (enc 1) | each still-wrong successor `FLAGGED` (enc 2, `predecessor_hash` recorded): age **not** inherited | `test_m3` (K2/K3) |
| 0 | `CLEARED 'no longer upstream'` (enc 1). The remap read every upstream row, so this is the full-sweep conclusion (same as the existing `GONE` branch, `hop_key_currency.sql.j2:28-32`) | none | `test_m3` (K4) |

- **One committed evaluation.** The transition events are written in the same evaluation as the hop's normal events and committed by the same commit row (ADR-07).
  - An interrupted transition is invisible, and the rerun is idempotent (`test_m4`).
  - Once the commit lands, nothing legacy is open, so the remap never runs again (`test_m3`: `legacy_needing_remap` is empty).
- **Chain PENDING records.**
  - The same remap applies. A 1:1 record is continued as `PENDING` under encoding 2; a 1:n record is carried for each successor; 1:0 uses the chain's existing outcome for a record no longer upstream.
  - *Proposed; the prototype models key-currency events only.* This is implementation test T-MIG-6 (09).
- **Cost.** One read of `up_all` per hop with legacy state, once, the same order as one full sweep. Encoding 1 is computed only in this step (not in every evaluation, the cost the original design rejected the "bridge" for). It is bounded by the open set, not the population.

### 3.5 Release N's readers: encoding-aware and rollback-safe

**Release N is the rollback target**, so it must handle a store that N+1 has written to.

| Reader | Release N behaviour | Evidence |
| --- | --- | --- |
| Open keys (`hop_common.sql.j2:36-50`) | Reads `coalesce(key_encoding, 1) = 1`, and takes the latest event **excluding `REKEYED`**. So a legacy identity N+1 continued is open again for N: its spell was never ended, and its age is intact. | `test_m5` (`open_for_release_n`) |
| Foreign open keys | Detects encoding-2 identities whose latest event is open. When any exist for a hop, N **forces a full sweep** of that hop (the existing `force_full_sweep` path, `runner.py`), which re-judges every key under encoding 1. It writes `REKEYED 'key encoding 1 (rollback)'` (enc 2) for each foreign open hash. | `test_m5` (`foreign_for_release_n` is empty afterwards; every discrepancy open before the rollback is open after it) |
| Chain carry (`chain.py:338-345`) | The same filter on `key_encoding`. Foreign PENDING records force the chain's full recheck. | *Proposed* (T-MIG-6) |
| Trace (`trace.py:115-123`) | Shows encoding-1 events, and labels encoding-2 events as "written by a later engine". | *Proposed* |
| Writer (`writer.py:19-39`) | Tolerates a column absent from the frame **only** when a known migration added it (`MIGRATIONS`), writing NULL. Any other missing column, and any extra column, still raises. This also covers migration 1 (Q7). | `test_m2` |

- **Re-upgrade after a rollback:** N's continuation events re-open legacy identities, so N+1's remap runs again on its next evaluation (`test_m6`).
- **Auditability:** every new event names its encoding, every encoding-2 continuation names its predecessor, and encoding-1 events only ever close encoding-1 identities (`test_m7`).

### 3.6 Concurrent engine versions (R2-10): already fenced

- **Only NORMAL runs write key events** (`checks/base.py:156-158`; RERUN and REPLAY neither read nor change open-key state).
- **`dre run` refuses a NORMAL run** while another NORMAL run is STARTED within `watchdog_grace_minutes` (`runs.in_progress`, `store/runs.py:86-98`; `runner.py:1075-1082`), whatever either run's engine version.
- So N and N+1 cannot interleave key events, and **no new fence is needed**.
- **Residual (existing, version-independent):** a live run older than the grace counts as dead.
- One test pins the guard for a STARTED run of a different `engine_version` (T-MIG-8). The original revision-2 idea of a separate version fence is withdrawn.

## 4. Scenarios

| # | Scenario | Outcome | Evidence |
| --- | --- | --- | --- |
| M-01 | Fresh store, N+1 directly | DDL creates both columns; all events are encoding 2; no transition | Read (`pending` ignores absent tables) |
| M-02 | Pre-N engine after migration 2 is applied | Fails closed: the writer raises, so the result is `evidence_incomplete` and the window is evaluated again. No evidence is lost or forged. Rollback **below N** is unsupported once migration 2 is applied | `test_m1` (the real writer) |
| M-03 | N on a store with migration 2 pending | `dre run` refuses to start until `dre install --apply` | Existing test (`test_evaluation_modes.py:75`) |
| M-04 | N runs normally | Writes encoding 1; behaviour otherwise unchanged (the full regression suite is the gate) | Gate (10) |
| M-05 | N → N+1, open 1:1 still wrong | Continued, age kept | `test_m3` |
| M-06 | N → N+1, legacy collision | `ambiguous successor`; each wrong successor flagged | `test_m3` |
| M-07 | N → N+1, legacy key gone | `CLEARED 'no longer upstream'` | `test_m3` |
| M-08 | N+1 interrupted mid-transition | Invisible; rerun idempotent | `test_m4` |
| M-09 | N+1 → N rollback | Every open discrepancy stays visible; foreign identities closed by REKEYED | `test_m5` |
| M-10 | N → N+1 again after a rollback | Remap runs again | `test_m6` |
| M-11 | Replay (AS_OF_REPLAY) of a window judged by N, run by N+1 | New evaluation ids (they include `engine_version`); old results kept as stored; results differ only where a defect was hidden | Read (`events.py:75-78`) |
| M-12 | N and N+1 started concurrently on one store | The second NORMAL run refuses to start (existing overlapping-run guard) | Read (`runner.py:1075-1082`); pinned by T-MIG-8 |

## 5. Rollback assessment

| Rollback | Supported? | How | Status |
| --- | --- | --- | --- |
| N+1 → N | **Yes** | No schema change; N's readers ignore REKEYED and sweep foreign identities | Prototyped (m5, m6); end-to-end demonstration is a gate |
| N → pre-N, migration 2 not applied | Yes | Nothing changed in the store | Read |
| N → pre-N, migration 2 applied | **No** (fails closed) | The writer raises; no silent loss | Verified (m1) |
| Schema rollback (drop columns) | **Never** | Migrations are additive only; rows are never rewritten | Policy (`migrations.py`) |
