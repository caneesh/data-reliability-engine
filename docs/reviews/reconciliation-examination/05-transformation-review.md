# 05 · Transformation-aware reconciliation: mapping kinds

The kinds come from `config/models.py` (`EdgeContract.mapping`) and `config/contracts.py` (`LEDGER_KINDS`, `blind_spots`).

**Status key:**
- **Implemented:** there is a ledger.
- **Partial:** some guarantees only.
- **Unsupported:** DID_NOT_RUN / unsupported_mapping.

## 1. Support table

| Kind | Status | Contract information required | Legitimate transformation, false FAIL? | Incorrect transformation, false PASS? | Inconclusive (DID_NOT_RUN) when |
| --- | --- | --- | --- | --- | --- |
| identity | Implemented (HOP_LEDGER) | key_map; record_time both sides | Owned values compared as strings (a formatting change looks divergent) | Wrong values outside owned columns (F-01); key collisions (F-02) | Collisions or ties are unassessable; a missing table |
| dedup | Implemented | key_map | Tied rows resolved by `winner_rule` look divergent (S-09) | F-01, F-02, F-05, F-07; duplicates in the target pass (a22) | As above |
| merge | Implemented (as dedup, plus deletion) | key_map, deletion | As dedup | Tied tombstones (F-07); a non-union multi-upstream target where one source's loss is covered by another source's row (qualified only by `orphans_unchecked`) | As above |
| union_partition | Implemented (with attribution proof, T1_SOURCE_ATTRIBUTION) | key_map; a `feed_filter` on each sibling dataset | — | One dataset with two union contracts and one `feed_filter`: the proof is trivially "verified" (S-11; static) | A proof failure is a qualification, not DID_NOT_RUN |
| survivorship | Partial | key_map; identical `precedence` | Winner legitimately filtered or tombstoned: the winner's edge reports a breach (static, S-11) | Without owned columns the wrong winner is accepted (**F-09**, m01) | An unreadable winner gives invalid_config |
| join_contribution | Partial (HOP_CONTRIBUTION; no completeness ledger) | `reference` (= the contributor's full key), `contributes` | An absent optional contributor fails (**F-11**, m03); the gold-side reference cannot be normalised (S-10); version chosen by exact load time (S-10); exclusions, tolerance and deletion are silently ignored (S-10) | Contributor updates that never reach gold (**F-10**, m02) | Nothing loaded in the window; missing configuration |
| aggregate | Group level only (derived datasets, `checks/derived.py`) | A derived upstream definition | No `key_normalise` in the joins; history versions inflate COUNT and SUM (S-13; static) | A pipeline that duplicates rows the same way reproduces its own error (speculative) | HOP_LEDGER: unsupported_mapping. Chain: partial_chain. R18 NOT_STARTED |
| fan_out | Unsupported | — | — | — | Always unsupported_mapping (R23) |

## 2. Multi-source gold entity

Example: membership details, coverage, producer assignment and subsidy all feed one gold row.

| Question | What DRE verifies | Evidence |
| --- | --- | --- |
| Presence of the target entity | On the **primary** edge only (HOP_LEDGER). Adding any contributor makes the target multi-upstream, which turns off orphan detection on the primary edge (`orphans_unchecked`; static S-11) | a01-a03; `ledger.py:105-117` |
| Correct contribution from each source | For target rows **loaded in the window**: the reference resolves, and the contributed columns equal the contributor version current at that row's load time | existing `test_hop_contribution.py` |
| Correct source precedence | Survivorship only, and only with owned columns (**F-09**). None between join contributors | m01 |
| Correct attribute values | The primary edge's `owned_columns` (one map shared by every upstream of the dataset) plus the contributors' `contributes` | a04, a05 |
| Correct effective-date selection | **Not supported.** The reference must equal the contributor's full key exactly, with no range or period join | `hop_contribution.sql.j2:43-60` |
| Absent optional contributors | FAIL: there is no optional concept (**F-11**) | m03 |
| Contributor changes after the gold row was written | **Not detected** (**F-10**) | m02 |
| That the right contributor record was joined | Only that the reference exists and the values agree as of load | — |

**Conclusion.** DRE can detect a missing gold entity, a wrong primary attribute in `owned_columns`, an unresolved reference, and a contributed value that disagrees at load time. It cannot verify:
- effective-dated contributions;
- optional contributors;
- contributor staleness;
- precedence between join contributors.

For the member-coverage-producer-subsidy entity, that makes the multi-source guarantee **partial**.

## 3. Chain completeness

- **How it is proven.** Independently: it reads every edge's layers with the ledger's assessed SQL over the carried keys (`chain.py`). It does not infer from hop results.
- **How it was verified.** The existing `test_chain.py` (25 tests) and adversarial c01-c05, m07.

| Scenario | Behaviour | Evidence |
| --- | --- | --- |
| A record disappears at one stage | Lost at that edge | existing; c03 |
| Disappears, then reappears downstream | Lost at the first gap (no longer carried) | c02 |
| Different records lost at different stages | Each counted at its own edge; the first observed gap is reported, "not necessarily the origin" | c03 |
| Counts match but keys differ | FAILED: the join is per key | c01 |
| Two intermediate stages, equal counts, different populations | FAILED at the first edge that lost a record | c01 |
| A transformation changes keys | Followed through the intermediate table's rows (`forward` mode) | existing |
| An intermediate stage unavailable | DID_NOT_RUN / table_missing, naming the edge | existing |
| A source legitimately filtered | `accounted` (approved exclusion) | existing |
| **An authorised soft delete applied at one stage and not the next** | `accounted` at the first edge; the chain **stops** following the record: PASSED. Gold's own HOP_LEDGER finds `deleted_not_applied` | **F-14** (m07) |
| Multiple paths converging on one gold | DID_NOT_RUN / invalid_config (unique path required) | existing |
| Key collision (F-02) | Carried through: PASSED | c05 |
