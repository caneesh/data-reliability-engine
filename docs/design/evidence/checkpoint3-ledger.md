# Evidence package: checkpoint 3 (HOP_LEDGER and dq_reconciliation)

For the reviewer of checkpoint 3. It shows the step 3 requirements of the authorisation (the reconciliation contract, the ledger, the evidence scope, under the commit protocol of checkpoint 2) against the code, the tests and a real run. Everything here is synthetic: the store is the end-to-end test's, built by `tests/e2e/build_synthetic.py`.

## (a) Source changes

The step 3 commit on branch `claude/claude-md-documentation-a2lxt0` (its SHA and CI run are in `docs/design/checkpoints.md`, "Checkpoint 3"). The change set is its diff against `3d3f3c2`.

| Area | Files |
| --- | --- |
| The check | `src/hcsc/datalake/dre/checks/hop/ledger.py` (new), `checks/sql/hop_ledger.sql.j2` (new), `checks/hop/common.py` (`exclusion_terms`), `checks/base.py` (reason codes, `CheckResult.reconciliation`), `patterns/file_cyclic.yaml`, `file_periodic.yaml`, `table_merge.yaml` |
| Configuration | `config/models.py` (`RejectsSource`, `EdgeContract.rejects`), `config/validate.py`, `config/contracts.py` (`LEDGER_KINDS`, blind spots), `conf/defaults.yaml` (`retention_months.dq_reconciliation`) |
| Store | `store/ddl.sql` (`dq_reconciliation`, `dq_commit.reconciliation_rows`, `v_committed_reconciliation`, `v_commit_state`, `v_commit_integrity`), `store/reconciliation.py` (new), `store/commits.py`, `store/retention.py`, `store/names.py`, `store/schema.py` |
| Run | `runner.py` (ledger lines written with the evidence and declared in the commit; the session view dropped) |
| Reporting | `notify/digest.py` (`scope_line`), `notify/digest.txt.j2` |
| Spec and design | `docs/spec.md` sections 1, 3, 4, 5, 6, 7, 8; `docs/decisions.md` ("Phase 1 build step 3", deviations L1 to L15); `docs/design/checkpoints.md`; `docs/design/implementation.md`; `docs/design/traceability-matrix.md`; `docs/architecture.md` |

## (b) Tests

`docs/design/checkpoints.md`, "Checkpoint 3", maps every requirement to its tests. New: `tests/checks/test_hop_ledger.py` (31), `tests/store/test_reconciliation.py` (4); extended: `tests/config/test_contracts.py`, `tests/notify/test_digest.py`, `tests/test_registries.py`, `tests/e2e/verify.py`.

## (c) Committed-view and integrity-view output

From the end-to-end store after `dre run` (both synthetic feeds; the run was a full sweep, the sweep day being a Sunday). Each HOP\_LEDGER result, its scope (shortened), its committed `dq_reconciliation` lines (`v_committed_reconciliation`) and its commit (`result_rows`, `reconciliation_rows`, `evidence_kept_until`):

```text
RESULT gold_member_coverage PASSED population=20 violations=0
  observed: due 20 (pending 4): delivered 20; orphans 0
  scope: {"edge": "example_curated_enrollment->gold_member_coverage", "mapping": "union_partition", "contract": "declared", "contract_hash": "7765c0e10011a571", "claim": "contract-proven", "cutoff": "2026-10-18T18:55:00+00:00", "full_sweep": true, "evidence_level": "observed", "orphans": "checked"}
  blind_spots: ['attribution (each row here owned by exactly one source) is not proven until build step 5', 'no rejects evidence: a record the pipeline rejected counts as missing', 'values compared: owned_columns only (identity transforms); other columns are not compared', 'as-of replay unavailable: example_curated_enrollment does not declare grain.history', 'as-of replay unavailable: gold_member_coverage is mutable']
    deleted                None           authorised       0 reconstructed
    deleted_not_applied    None           breach           0 reconstructed
    delivered              None           ok              20 observed
    divergent              None           deficit          0 observed
    due                    None           population      20 observed
    excluded               None           authorised       0 reconstructed
    excluded_but_present   None           breach           0 reconstructed
    missing                None           deficit          0 observed
    orphan                 None           breach           0 observed
    pending                None           population       4 observed
    rejected               None           defect           0 reported
    stale                  None           deficit          0 observed
    superseded             None           authorised       0 observed
    unassessable           None           unknown          0 observed
    untimed                None           population       0 observed
  commit: [(1, 15, datetime.date(2033, 10, 18))]
RESULT provider_directory PASSED population=38 violations=0
  observed: due 38 (pending 1): delivered 38; orphans 0
  scope: {"edge": "provider_roster_raw->provider_directory", "mapping": "dedup", "contract": "inferred from key_map", "contract_hash": "30298c0cfc9382a2", "claim": "inferred contract", "cutoff": "2026-10-18T18:55:00+00:00", "full_sweep": true, "evidence_level": "observed", "orphans": "checked"}
  blind_spots: ['kind dedup is inferred from key_map, not declared: its results can never claim contract-proven completeness', 'deletions not modelled: a deleted record is judged like any other version', 'no rejects evidence: a record the pipeline rejected counts as missing', 'values compared: owned_columns only (identity transforms); other columns are not compared', 'as-of replay unavailable: provider_directory is mutable']
    deleted                None           authorised       0 reconstructed
    deleted_not_applied    None           breach           0 reconstructed
    delivered              None           ok              38 observed
    divergent              None           deficit          0 observed
    due                    None           population      38 observed
    excluded               None           authorised       0 reconstructed
    excluded_but_present   None           breach           0 reconstructed
    missing                None           deficit          0 observed
    orphan                 None           breach           0 observed
    pending                None           population       1 observed
    rejected               None           defect           0 reported
    stale                  None           deficit          0 observed
    superseded             None           authorised       0 observed
    unassessable           None           unknown          0 observed
    untimed                None           population       0 observed
  commit: [(1, 15, datetime.date(2033, 10, 18))]
integrity view rows: 0
```

What it shows:

- **The identity, readable off the store:** the dispositions add up to the due population (20 and 38), every disposition has a line, zeros included; pending records are beside the ledger.
- **The claim:** the declared gold contract is `contract-proven`; the provider hop, inferred from `key_map`, is `inferred contract` and says so in its blind spots (D6 conditions 1 and 2).
- **The commit:** each ledger's 15 lines are declared in its commit and counted before it was written; the integrity view is empty. Evidence is kept 84 months.
- **Orphans** were checked on both hops: gold is attributed to its source by `feed_filter` (decision D5), and the provider directory has one upstream.

## (d) Commands

Local, from the repository root (Python 3.11 and Java 21 here; CI uses Python 3.10 and Java 17):

```bash
pip install -e ".[dev]"
export PYSPARK_PYTHON=$PWD/.venv/bin/python
pytest tests/checks/test_hop_ledger.py tests/store/test_reconciliation.py   # the step 3 tests
pytest                                                                       # everything
mkdir e2e && cd e2e && python ../tests/e2e/build_synthetic.py && dre install --conf conf --apply \
  && dre run --conf conf && python ../tests/e2e/verify.py                    # the end-to-end check
```

The end-to-end check fails unless both feeds' HOP\_LEDGER results PASSED with their lines committed and balanced.
