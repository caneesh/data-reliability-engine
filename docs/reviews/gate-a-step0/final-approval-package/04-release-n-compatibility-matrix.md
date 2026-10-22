# 04 · Release N compatibility matrix

**Release N is limited to schema and compatibility preparation.**
- It must **not** activate key encoding 2.
- It must **not** change F-02, F-05 or F-07 semantics, the qualification list, the configuration schema or any check result.

Amendments in release N:
- **A-3:** the key-event columns.
- **A-9r:** the views.
- **A-10r, reading half only:** encoding filter, REKEYED as no event, foreign identities, the one-event rule, cause encoding, trace labels.

A-11 (UTC) moved to N+1.

## 1. What release N may and may not change

| May change | Must not change |
| --- | --- |
| `store/migrations.py`: migration 2 (`dq_key_event` + `key_encoding INT`, `predecessor_hash STRING`) | `checks/keys.py` `key_string` (encoding 1 stays the only encoding computed) |
| `store/ddl.sql`: the two columns; `v_open_keys` (A-9r); new `v_key_lineage`; `store/schema.py` `VIEWS` | `hop_ledger.sql.j2` dispositions, record-time rules, tie rules, orphan rules |
| `store/writer.py` `append`: tolerate a missing column only if `MIGRATIONS` added it (typed NULL) | The hash input or `register_key_hash` |
| `store/key_events.py` `append_key_events`: write `key_encoding = 1`, `predecessor_hash` NULL | `ledger.py` qualifications, scope fields other than those listed here |
| `hop_common.sql.j2` `open_keys` (:36-50): read encoding 1 only; REKEYED as no event; list foreign (encoding 2) open identities | `models.py` / `validate.py` (no `disorder_bound` yet) |
| `checks/hop/common.py` + `runner.py` context: on foreign identities, judge the hop as on the full sweep (existing `force_full_sweep`); write REKEYED "key encoding 1 (rollback)" for them in the same commit | `runner.py` start preconditions (the UTC check is N+1) |
| `checks/hop/chain.py` (:338-345): the same for PENDING carry | Cause probes, digest counts, CLI output (beyond trace labels) |
| `trace.py` (:115-123): label events by encoding; report later-encoding events as present but not searchable | Any configuration in `conf/` |
| Tests: new T-MIG-1 to T-MIG-4, T-MIG-6, T-MIG-8, T-N-5 | Any existing test (none needs restating in N) |

## 2. Compatibility matrix

| Concern | Requirement | How it is met | Test |
| --- | --- | --- | --- |
| **Schema evolution** | Additive, nullable, at the end of the data columns; no rewrite | Existing migration mechanism (`migrations.py`; guard rule 11); NULL means encoding 1 | T-MIG-1 |
| **Migration idempotency** | Re-applying does nothing; a partially applied store completes | "Pending while any column is absent" (existing; `test_evaluation_modes.py:95-101` pattern) | T-MIG-1 |
| **Pending migration** | `dre run` refuses to start until applied | Existing (`missing_objects`, `test_evaluation_modes.py:75`) | Existing test, unchanged |
| **Writer: N on a migrated store** | Writes both columns | `append_key_events` selects them | T-MIG-3 |
| **Writer: N on a future-migrated store** | A column added by a later migration is written as NULL; anything else fails | Tolerance keyed on `MIGRATIONS` (rev-2 m2 prototype) | T-MIG-3 |
| **Writer: pre-N on a migrated store** | Fails closed (no evidence written, window held) | The current writer raises (rev-2 m1, executed on the real writer) | Documented; rollback below N unsupported |
| **Reader: N on encoding-1 store** | Identical behaviour to the previous release | Encoding filter matches every existing row (NULL means 1) | N-R2 equivalence |
| **Reader: N on a store N+1 wrote** | Every open discrepancy stays visible; foreign identities closed by REKEYED; legacy spells continue with their age | REKEYED as no event; forced sweep (rev-2 m5 state machine) | T-MIG-4, T-MIG-6 |
| **Views** | One definition serves N and N+1 | `v_open_keys` (A-9r) and `v_key_lineage` ship in N | T-MIG-2 |
| **Determinism** | At most one event per identity per evaluation | Rule in A-10r; N writes one event per key as today, plus at most one REKEYED for a foreign identity (a different hash) | T-MIG-4 |
| **Mixed versions** | N and N+1 never write concurrently | Existing NORMAL-run overlap guard (`runner.py:1075-1082`); only NORMAL runs write key events (`checks/base.py:156-158`) | T-MIG-8 |
| **Rechecks** | RERUN, REPLAY and AS_OF_REPLAY write no key events; new evaluation ids per engine version | Existing (`events.py:75-78`) | Existing tests |
| **Historical evidence** | No stored row changed; legacy events stay readable | Append-only; guard rules 2, 4, 11 | Guard tests |
| **Causes** | `failure_ref` hashes interpreted by their run's engine | Rule in A-10r; N writes encoding-1 hashes as today | T-N-5 (trace) |
| **Rollback N → previous** | Supported only while migration 2 is not applied | Release notes | N-O1 |
| **Rollback N+1 → N** | No schema change; open discrepancies survive | R-1, R-2 (step 0 contract) | T-MIG-4 now; end-to-end gate at N+1 (05) |

## 3. Acceptance and independent verification before release N merges

1. **Approvals:** A-3, A-9r, A-10r and A-15r (release N part) approved; explicit implementation authorisation for release N (06).
2. **New tests pass:** T-MIG-1, T-MIG-2, T-MIG-3, T-MIG-4, T-MIG-6, T-MIG-8, T-N-5 (definitions: `../04-release-n-acceptance-criteria.md`, with T-N-6 moved to N+1).
3. **Equivalence (N-R2):** an encoding-1 store gives identical results, apart from the new columns, under the previous release and N.
4. **No existing test restated.** Full suite, replays, guard tests, e2e, packaging and CI green on the pushed commit.
5. **Independent verification:** a reviewer other than the implementer re-runs T-MIG-4 with a store written by fixtures that stand in for N+1, confirms no N+1 behaviour is reachable (no encoding-2 key computed anywhere: a grep and the A-14-style guard), and signs the evidence note.
6. **Deployment** (separate approval): migration 2 is applied only with platform approval (D-1). Never against a shared database during development.
