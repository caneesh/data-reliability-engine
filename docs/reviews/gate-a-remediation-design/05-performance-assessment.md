# 05 · Performance assessment

**Basis.**
- The current physical plan of the ledger (examination `adversarial/bench_ledger.py`): 18 scans, 16 exchanges, 8 window sorts and 12 broadcast joins for one evaluation of a small hop.
- The prototypes in `verification/test_design_prototypes.py`.

All numbers are local (2 cores) and indicative only. **No representative-cluster benchmark has been run (KI-01).**

## 1. Cost per fix

| Fix | Shuffles | Joins | Key width | Sorts | Memory | Scans | Checkpoint size | Driver collection | Spark jobs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **F-02 encoding 2** | None added | None added | **Wider:** brackets and quotes per part. Measured average length 31.9 against 21.9 chars (+46%) on 4 short parts; the relative growth falls as parts lengthen | None added | Shuffle and window buffers grow with key width (+~10 bytes per key per row here) | None added | The ledger's `assessed` local checkpoint grows with `k` width | None | None |
| **F-02 `key_parsed` flag** | None (a row expression) | None | — | None | Negligible | None | — | None | None |
| **F-02 raw-variant guard in `hop_common`** | None added: `collect_set` over the existing `PARTITION BY k` window | None | — | None added | A small set per key | None | — | None | None |
| **F-05 untimed and unparsed counts, latest load** | None added: `COUNT`/`SUM`/`MAX` over the **same** window spec as the existing `MAX(record_ts)`, so the same Window operator | None | — | None added | A few longs per row | None | +3 columns | None | None |
| **F-05 `raw_record` column** | None | None | — | None | One string column carried to `up_ranked` | None | +1 column | None | None |
| **F-07 MIN and MAX per rule** | None added (same `GROUP BY k` aggregate) | None | — | None | 2 integers per rule per key | None | — | None | None |
| **F-07 reject order** | None (same aggregate; a wider ordering struct) | None | — | None | Negligible | None | — | None | None |
| **Transition: forced full sweeps** | One-off: a full sweep per hop and chain with legacy open or pending keys, on the first post-upgrade run | One-off | — | One-off | One-off | **One-off full scans** | One-off | None | One-off |
| **Trace: dual hash** | Negligible (CLI, one key) | — | — | — | — | — | — | One row | — |

**Measured** (V7: 1,000,000 four-part keys, `COUNT(DISTINCT key)`, local): encoding 1 took 1.97 s and encoding 2 1.79 s. So CPU is comparable, and width is the main cost.

**Avoidable overhead to watch while implementing:**
- Compute `to_json(array(...))` **once** per row in `up_rows`/`dn_rows` (already the pattern: `k` is a column), never inside join conditions.
- Keep every new window aggregate in the existing `WINDOW w AS (PARTITION BY k)`. A second window spec (for example one ordered by load) would add a sort.
- Do not compute encoding 1 during normal evaluations. Only `dre trace` needs it.

## 2. The transition's one-off cost

The first NORMAL run after the upgrade runs a **full sweep** for each hop or chain with legacy open or pending keys, so a run can exceed the 40-minute budget.

The existing controls:
- **Backpressure** defers full sweeps when the previous run was heavy (SWEEP_DEFERRED, recorded).
- **The run budget** writes the remaining checks as DID_NOT_RUN / BUDGET, which holds their windows.

The transition is therefore **resumable**: each hop's closing events are written only by its forced sweep, and an unswept hop stays in transition (its legacy keys are still open and still detected). A deferred sweep delays the transition and never loses a key.

**Recommendation:** run the upgrade at the weekly full-sweep time, when sweeps are expected anyway.

## 3. Benchmark plan (representative HDFS/Hive workload; KI-01)

| # | Measure | Workload | Pass criterion |
| --- | --- | --- | --- |
| B1 | Ledger elapsed time, scan bytes and shuffle bytes: encoding 1 vs 2 | The largest member hop (production-like volume and key width), a normal hourly window | Encoding 2 within +15% elapsed and +key-width-proportional shuffle bytes |
| B2 | The same for HOP_KEY_CURRENCY and HOP_VALUE_AGREEMENT | The same hop | As B1 |
| B3 | The chain (3 edges) | The member chain | As B1 |
| B4 | The transition run: forced full sweeps over every hop | A copy of the store with realistic open-key and pending volumes | Completes within the budget, or is deferred and resumed with no key lost |
| B5 | Skew | A key distribution with hot keys (subscribers with many coverages) | No executor OOM; record the max task time |
| B6 | Driver | Every check | No driver collection beyond summary rows (unchanged) |

**Instrumentation:**
- `scripts/measure_run.py` (elapsed against the budget, per-check durations);
- the Spark event log (shuffle read and write, spill, scan bytes);
- `EXPLAIN FORMATTED` on the rendered SQL, to confirm no new Exchange or Window operators.
