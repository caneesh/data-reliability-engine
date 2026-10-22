# 06 · Specification approval checklist

To approve, mark each item. Approving the specification **does not** authorise implementation or deployment (05).

## Release N amendments (schema and compatibility only)

- [ ] **A-3:** `dq_key_event` gains `key_encoding INT` and `predecessor_hash STRING` (migration 2); event `REKEYED` never ends a spell.
- [ ] **A-9r:** `v_open_keys` gains `key_encoding`, `predecessor_hash`, `category` (`active` / `legacy_unresolved`) and lineage-aware first flagged date; new view `v_key_lineage`.
- [ ] **A-10r (reading half):**
  - encoding filter;
  - REKEYED as no event;
  - foreign identities trigger a full sweep and are closed by REKEYED;
  - **at most one event per identity per evaluation**;
  - `failure_ref` encoding by run;
  - trace labels.

## Release N+1 amendments

- [ ] **A-1r:** length-prefixed canonical key (encoding 2):
  - CHAR without pad;
  - DECIMAL by numeric value in plain notation;
  - unparseable parts kept as raw bytes;
  - refused types: FLOAT, DOUBLE, BINARY, ARRAY, MAP, STRUCT, INTERVAL, TIMESTAMP_NTZ, VOID.
- [ ] **A-2:** the same HMAC-SHA256 over the canonical key's bytes; every hash changes once.
- [ ] **A-4r:** relevant untimed rows make a key unassessable; a bound in force sets older ones aside; a refuted bound stays refuted until a new version; unverifiable means refuted.
- [ ] **A-5r:** `record_time.disorder_bound` (`id`, `version`, `hours`, `approved_by`, `approved_on`, `evidence`), default `null`.
- [ ] **A-6:** tied rows must agree on expected presence; per-row exclusion time; ambiguous rejects; total reject order.
- [ ] **A-7:** qualification `version_by_disorder_bound` (never healthy) on every hop check and the chain.
- [ ] **A-8:** `unparseable target key` breach line.
- [ ] **A-10r (transition half):**
  - provable continuation only by syntax;
  - ambiguous legacy identities stay open with their age;
  - digest counts by category.
- [ ] **A-11:** `dre run` refuses to start unless the session time zone is UTC.
- [ ] **A-12:** line 649 reworded to the set rule (Q-1; no behaviour change).
- [ ] **A-13:** reconciliation model wording (`winner_rule`; the bound compares load with load).
- [ ] **A-14:** guard: encoding 1 built only by the transition and trace.

## Records

- [ ] **A-15r:** the decision-log entry, listing the approved-behaviour changes (key string, CHAR, DECIMAL, UNASSESSABLE, "no record time", per-row exclusion time).
- [ ] **A-16:** KI-20 (NF-1) recorded as a known issue, deferred outside Gate A.

## Decisions

- [ ] Q-1 to Q-7 as in 02.
- [ ] R2-D1 to R2-D15 as in 02's final status table.

## Conditions accepted with the approval (verified before the dependent release merges)

- [ ] T-F02-9 (TIMESTAMP rendering).
- [ ] T-Q4-1 to T-Q4-5 (refutation persistence).
- [ ] T-F07-6 (soft + absent tie).
- [ ] T-DN-4 (full-sweep count of unparseable target rows).
- [ ] End-to-end rollback demonstration (GP-4).

## Not part of this approval

- Implementation authorisation for release N or N+1.
- Deployment, migration on shared databases, any production value (O-1 to O-3).
- The NF-1 fix.
