# 05 · Implementation prerequisites and gates

Three separate kinds of approval. **None is given by this package.**

| Kind | Meaning | Who | Unlocks |
| --- | --- | --- | --- |
| **Architecture (specification) approval** | The amendments and decisions are accepted as the contract | The user (spec owner) | Applying the spec text and decision-log entry |
| **Implementation authorisation** | Permission to write code for one release | The user, explicitly, per release | Release N work; later, release N+1 work |
| **Production deployment approval** | Permission to install a release and run its migration on a shared environment | The user with the platform team; dataset owners for their inputs | Deployment |

## 1. Before release N implementation

| ID | Prerequisite | Status |
| --- | --- | --- |
| PN-1 | Architecture approval of A-3, A-9r, A-10r (reading half) and A-15r (release N part) | Pending |
| PN-2 | The approved text applied to `docs/spec.md` and `docs/decisions.md` in the first commit of release N | Pending (after PN-1) |
| PN-3 | Explicit implementation authorisation for release N | Pending |
| PN-4 | Test definitions T-MIG-1 to T-MIG-4, T-MIG-6, T-MIG-8, T-N-5 accepted | Defined (`../04-…`, this package 04) |

## 2. Before release N merges and before it deploys

| ID | Gate | Kind |
| --- | --- | --- |
| GN-1 | New tests pass; no existing test restated; full suite, replays, guard tests, e2e, packaging; CI green on the pushed commit | Merge |
| GN-2 | Equivalence on encoding-1 stores (N-R2) | Merge |
| GN-3 | Independent verification (04 §3, item 5) | Merge |
| GN-4 | Deployment approval (D-1), with the release notes stating that rollback below N is impossible once migration 2 is applied | Deployment |

## 3. Before release N+1 implementation

| ID | Prerequisite | Status |
| --- | --- | --- |
| PP-1 | Architecture approval of A-1r, A-2, A-4r, A-5r, A-6 to A-8, A-10r (transition half), A-11 to A-14, A-16 and A-15r (N+1 part) | Pending |
| PP-2 | Release N merged and deployed, writing `key_encoding = 1` | Pending |
| PP-3 | Explicit implementation authorisation for release N+1 | Pending |

## 4. Before release N+1 merges and before it deploys

| ID | Gate | Kind |
| --- | --- | --- |
| GP-1 | All N+1 tests pass: T-F02, T-F05, T-F07, T-DN, T-H, T-CFG, T-R1, T-Q2 to T-Q5, the T-MIG transition tests, T-N-6 | Merge |
| GP-2 | The conditions attached to approvals pass: T-F02-9 (TIMESTAMP rendering), T-Q4-1 to T-Q4-5 (refutation persistence), T-F07-6 (soft + absent), T-DN-4 (full-sweep count) | Merge |
| GP-3 | Restated tests limited to the three named (`test_hop.py`, `test_r04_to_r07_hops.py`, `test_causes.py` KEY constants); the approved-semantics tests and every replay unchanged | Merge |
| GP-4 | **End-to-end rollback demonstration** with the real N and N+1 builds on a scratch store: N → N+1 → N → N+1 | Merge |
| GP-5 | **O-2:** production key-column types inventoried and checked (no FLOAT, DOUBLE, BINARY, ARRAY, MAP, STRUCT, INTERVAL, TIMESTAMP_NTZ, VOID after normalisation), or the affected datasets named and accepted as DID_NOT_RUN until a normaliser is declared | **Deployment** |
| GP-6 | **O-3:** stakeholders acknowledge that every `key_hash` changes once at N+1 | **Deployment** |
| GP-7 | Deployment approval (D-1) | **Deployment** |
| GP-8 | The representative-cluster load test (ADR-08 proof) remains a separate production gate, not met by any local evidence | Production |

## 5. Dataset-owner decisions (deployment prerequisites, not architecture)

| ID | Decision | Default until decided |
| --- | --- | --- |
| O-1 | Any non-NULL disorder bound D: per dataset, with `id`, `version`, `hours`, evidence and approver | **NULL** (strict). No value is assumed or invented |
| O-2 | The key-type inventory (GP-5) | No deployment of N+1 |
| O-3 | The hash-change acknowledgement (GP-6) | No deployment of N+1 |
