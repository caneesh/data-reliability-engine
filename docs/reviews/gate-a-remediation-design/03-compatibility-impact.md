# 03 · Compatibility assessment

| Area | Change | Required? | Backward compatibility |
| --- | --- | --- | --- |
| **Configuration** | None required. `winner_rule` keeps its current meaning (the cause engine). Optional fields only if Q1 or Q2 (07) are adopted later: `winner_rule.resolves_ties`, a load-order declaration | No | Every existing configuration loads and validates unchanged |
| **Reconciliation contracts** | None. Mappings, exclusions, deletion, rejects, survivorship, union and contribution keep their fields and meaning | No | Unchanged |
| **Result vocabulary** | New `unassessable` labels: `unparseable key`, `unparseable record time`, `no record time` (now also partial), `tied rows disagree: …`, and the downstream `… here` forms. No new state, class or disposition | — | Labels are free text in `dq_reconciliation.label` and `observed`; readers already accept any label |
| **Store schema** | **Migration 2:** `dq_key_event ADD COLUMNS (key_encoding INT)`, forward-only, as migration 1 (`store/migrations.py`); `store/ddl.sql` declares it last among the data columns | Yes | Existing rows read NULL = encoding 1; no row is rewritten; `dre install --check` reports PENDING until applied, and `dre run` refuses to start until it is (the existing rule) |
| **Views** | `v_open_keys`: none needed (it partitions by `key_hash`; closing events close legacy hashes). Optionally project `key_encoding` | No | — |
| **Persisted identity** | New keys are written in encoding 2 (JSON array); legacy open or pending keys are closed by events (02 §4) | Yes | Legacy events stay readable and attributable (`key_encoding` NULL) |
| **Evaluation ids, commits, results** | None. Evaluation ids hold no keys and include `engine_version`, so post-upgrade re-evaluations get new ids | No | Committed evidence is untouched (Invariant E) |
| **Historical replay** (RERUN, REPLAY, AS_OF_REPLAY) | A replay after the upgrade judges with the corrected semantics. Its results may differ from the stored ones exactly where F-02, F-05 or F-07 hid a defect | — | The stored results remain; the new ones are written as new evaluations, attributable by `dq_run.engine_version`. `v_latest_result` changes only when the replayed window is the latest (its existing rule) |
| **Trace** (`dre trace`) | Looks up both encodings' hashes and labels each event's encoding | Yes | Old traces keep working |
| **Digest** | Key-hash samples change once (new hashes). New labels appear in observed and triage. On the first post-upgrade run: forced full sweeps and the closing events | — | No format change |
| **Monitoring and thresholds** | Expect a **one-time step** in failures: keys hidden by F-02, F-05 or F-07 now show as unassessable (correct). Also a one-off cost from the forced sweeps (05) | Operational note | Thresholds on FAILED counts should not be re-tuned to hide the step |
| **Packaging and deployment** | Pure Python and Jinja SQL, Spark 3.5.1 built-ins (`to_json`, `array`, window aggregates). No new dependency; the same spark-submit artifacts | No | Unchanged |
| **Read-only guarantee** | Unchanged. Writes stay `writer.append` plus migrations (ADD COLUMNS) on the dq store only | — | Guard tests unchanged |

## Migration and rollback

**Forward (per environment, after release N is in place).**
1. Deploy the code.
2. `dre install --conf <conf> --apply` applies migration 2. This is idempotent: it is pending while the column is absent.
3. The first NORMAL run: forced full sweeps where legacy open or pending keys exist, and closing events.
4. The runs after it are normal.

**Rollback: not a plain code rollback.** This was found while designing and verified against the code.
- `store/writer.py:19-39` requires a frame to carry **exactly** the table's columns.
- After migration 2, an engine that predates it fails every `dq_key_event` append with "missing ['key_encoding']".
- That fails closed: the evaluation is marked incomplete, its window held and the run PARTIAL. So there is no false PASS, but a rolled-back engine makes no progress on any check that writes key events.
- Migration 1 (`dq_check_result.evaluation_mode`) has the same property already; it was never exercised by a rollback.

**Hence a two-step deployment:**

| Step | Release | Content | Rollback target |
| --- | --- | --- | --- |
| 1 | N (preparatory) | `writer.append` tolerates the **absence**, from the frame, of a column added by a known migration, and writes NULL for it. The strict check stays for every other column, and for unexpected extra columns. Nothing else changes | The previous release (no migration applied yet) |
| 2 | N+1 (Gate A) | Encoding 2, F-05, F-07, migration 2, the transition (02 §4) | Release N: it writes `key_encoding` NULL, which means encoding 1, and its key strings *are* encoding 1, so the store stays consistent. Its next full sweep re-establishes open keys under encoding 1 (the `GONE` branch CLEARs encoding-2 hashes as "no longer upstream": a misleading state detail, recorded in the runbook) |

Alternatively, release N+1 can be forward-only, with roll-forward fixes and no rollback after migration 2. That is decision G-6 in 07.

No evidence is lost either way: the column is never dropped (migrations never drop).

**Not proposed:** rewriting `key_value`, re-hashing stored rows, or deleting legacy events.
