# 09 · Production readiness: evidence-based assessment

**Verdict: not ready to be trusted as the authority on completeness in production.** This is not an approval or a deployment decision.

The engine detects the common failure modes correctly and fails closed on internal errors. But this examination found reproducible **false-PASS conditions under ordinary data** (F-05, F-07, F-02), a limited notion of value correctness (F-01), gaps in the multi-source guarantees (F-09, F-10, F-11), and a digest that can under-report (F-18). The representative load test has not been performed (KI-01).

## Answers to the decision questions

| # | Question | Answer | Implementation evidence | Test evidence | Confidence |
| --- | --- | --- | --- | --- | --- |
| 1 | Can the current DRE produce a false PASS? | **Yes.** In each case below, the evidence is wrong but the code reads it as fine. | `hop_ledger.sql.j2:70-82`, `keys.py:119-122`, `:221/:263-264` | l01, l03, l04, a19, a20, l05, c05, a05, a06, m01, m02, m05, m06 | High |
| 2 | Can it miss records that disappear between stages? | **For ordinary records, no:** missing, equal counts with different keys, losses at several stages, reappearance and wrong partition are all caught at key level. **Yes for edge cases:** collided keys (F-02), a newest version with a bad record time (F-05), tied exclusion or tombstone rows (F-07), unparseable load times (F-12) and reused file names (S-01). | ledger, chain | a02, a03, a26, c01-c03 (caught); l01, l03, a19, m05 (missed) | High |
| 3 | Can it misclassify business transformations as missing data? | **Sometimes (false FAIL):** absent optional contributors (F-11), ties the pipeline resolves by `winner_rule` (S-09), unnormalised key formats (a21; configuration), padded join references (S-10), aggregates over history (S-13). Approved exclusions, deletes and rejects are accounted for correctly. | `hop_contribution.sql.j2:65`; `:135-142` | m03, a15, a21; a09, a12 | High |
| 4 | Can it accept records present with wrong values? | **Yes**, for every column outside `owned_columns`, and for all columns when none are owned. Under survivorship it can also accept the wrong source. | `:221` | a05, a06, m01 | High |
| 5 | Can it reconcile CDC updates, deletes and history? | **Current state: yes** (out-of-order and duplicate events, deletes; a16, a14, a12), except F-05 and F-07. **Event completeness: no** (a17). **Point in time:** only AS_OF_REPLAY on append-only data. History-keeping targets are judged by their newest record time (F-13). | 04 | a12, a14, a16, a17, m06 | High |
| 6 | Can it handle multi-source-to-one-target accurately? | **Partly.** It verifies presence on the primary edge, union attribution (proved), survivorship (only with owned columns), and contributions as of the target row's load. It does **not** verify contributor staleness (F-10), optional contributors (F-11), effective-dated contributions, precedence between join contributors, or orphans once contributors exist (S-11). | 05 §2 | m01, m02, m03 | High |
| 7 | Can it identify the first failing boundary? | **Yes, for the first observed loss.** CHAIN_COMPLETENESS reads each edge and names the first edge where a record was lost, saying it is "not necessarily the origin". It stops following a record at an authorised disposition, so a soft void not applied downstream is not seen by the chain (F-14); gold's hop ledger does see it. | `chain.py`, `chain.sql.j2:48` | c01-c04, m07 | High |
| 8 | Can it distinguish proven root cause from probable cause? | **Mostly.** There are three levels: confirmed, suspected and insufficient evidence. But missing logs or lineage count as "ruled out" (S-05), platform read errors count as a confirmed UNREADABLE (S-04), and the first CONFIRMED wins over an earlier stage that could not be judged (S-06). HOP_LEDGER gives triage lines, not causes. | `causes/engine.py:133-155`, `landing.py:135-138`, `probes.py` | static | Medium-high |
| 9 | Can it recover safely from partial or concurrent execution? | **Evidence: yes.** The commit protocol makes withheld and uncommitted rows invisible; the run goes PARTIAL and its windows are held (o04; existing suite). **Reporting: no.** The digest omits withheld failures (F-18), and a group-value collision withholds an evaluation on every run (F-16). **Concurrency:** overlapping NORMAL runs are possible (S-16). | `runner.Publisher`, `runs.py:87-99` | o04, o02 | High (evidence); medium (S-16) |
| 10 | What prevents it from being trusted in production today? | **Blockers:** F-05, F-07, F-02, F-12, F-06, F-18 (silent false PASS or under-reporting); F-01 and F-09 (a PASS that does not verify values is still healthy); F-10 (multi-source staleness); S-01 (file names). **Not yet evidenced:** performance at volume (no partition pruning on the ledger's reads, 18 scans per evaluation locally; KI-01). **Deployment:** queue values, HMAC secret, retention schedule, a guard on the dq database name, ratification of the open decisions (KI-16). | 08 | 07 | High |

## Read-only and independence

**Confirmed (statically, and by the existing guard tests):** DRE writes only to its own dq store and does not modify or hook into the pipelines. It is a standalone observer. The residual risks are a misconfigured `dq_database` (S-20) and raw SQL fragments (S-21), neither of which was found exploitable.

## What was not established

- Representative-cluster performance (resource consumption, scan volume, the 40-minute budget): **not performed** (KI-01).
- R18 (aggregation through `dre run`): NOT_STARTED. R22 (file-level record counts): BLOCKED.
- Cause accuracy with several simultaneous failures: static only.

## Conditions before production trust

1. The mandatory fixes in `10-remediation-plan.md` (items 1 to 6), each with the inverted adversarial test as its regression test.
2. `owned_columns` declared for every hop whose consumers rely on attribute values. Or, after remediation item 7, PASSes without value coverage are qualified.
3. A representative load test against the run budget.
4. Deployment prerequisites (KI-05, KI-16) and an independent acceptance of the fixes.
