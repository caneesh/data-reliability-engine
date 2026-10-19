# Evidence package: checkpoint 3 corrections (C3-01 to C3-04, L1 to L15)

For the reviewer of the revised checkpoint 3. It answers the review's handoff request: changed files, test results, CI, remaining risks. Everything here is synthetic: the store is the end-to-end test's, built by `tests/e2e/build_synthetic.py`.

## (a) Source changes

The corrections commit on branch `claude/claude-md-documentation-a2lxt0` (its SHA and CI run are in `docs/design/checkpoints.md`, "Checkpoint 3, revised"). The change set is its diff against `a5e8190`.

| Area | Files |
| --- | --- |
| Coverage continuity (L1) | `checks/hop/coverage.py` (new), `checks/hop/common.py`, `checks/hop/key_currency.py`, `checks/hop/value_agreement.py`, `checks/hop/file_completeness.py`, `causes/merge.py`, `checks/sql/hop_common.sql.j2`, `checks/sql/hop_file_completeness.sql.j2`, `runner.py` (`covers`, `result_rows`, `incomplete_rows`) |
| The ledger (C3-01 to C3-04, L4, L13) | `checks/hop/ledger.py`, `checks/sql/hop_ledger.sql.j2`, `checks/base.py` (`CheckResult.coverage`, `qualifications`) |
| Configuration (C3-01) | `config/models.py` (`RejectLineage`, `BatchLineage`, rule columns, `RejectReason.rules`), `config/validate.py` |
| Store | `store/ddl.sql` (`dq_check_result.qualifications`, `coverage_from`, `coverage_to`; `v_latest_result.healthy`; the dq_integrity comment), `store/integrity.py` (LEDGER_UNBALANCED, incidents) |
| Windows (L15) | `checks/events.py` (`BLOCKING_CODES`, `blocking_sql`), `checks/rules/schedule.py` |
| Reporting | `notify/digest.py`, `notify/digest.txt.j2`, `watchdog.py` |
| Evidence contract (C3-01) | `docs/evidence-contract.md` (rejects: `record_version`, `rule_id`, `rule_version`) |
| Spec and design | `docs/spec.md` sections 1, 3, 4, 5, 6, 8; `docs/decisions.md`; `docs/design/reconciliation-model.md` section 12; `docs/design/checkpoints.md`; `docs/design/implementation.md`; `docs/design/traceability-matrix.md` section 6a; `docs/architecture.md` |

## (b) Tests

`docs/design/checkpoints.md`, "Checkpoint 3, revised", maps every decision and acceptance criterion to its tests, by name. New: `tests/checks/test_hop_coverage.py` (10); extended: `tests/checks/test_hop_ledger.py`, `tests/store/test_reconciliation.py`, `tests/checks/test_windows.py`, `tests/config/test_contracts.py`, `tests/notify/test_digest.py`, `tests/e2e/verify.py`.

## (c) Committed-view and integrity-view output

From the end-to-end store after `dre run` (both synthetic feeds; a normal run, not the full sweep). The hop checks' current results, with their qualifications, `healthy` and the coverage each recorded, then each ledger's claim and scope:

```text
v_latest_result, the hop checks (state, qualifications, healthy, coverage):
  gold_member_coverage   HOP_KEY_CURRENCY       PASSED  healthy=True  q=None coverage=[2026-10-17 23:47:00, 2026-10-18 23:47:00)
  gold_member_coverage   HOP_LEDGER             PASSED  healthy=False q=attribution_unverified coverage=[2026-10-17 23:47:00, 2026-10-18 23:47:00)
  gold_member_coverage   HOP_VALUE_AGREEMENT    PASSED  healthy=True  q=None coverage=[2026-10-17 23:47:00, 2026-10-18 23:47:00)
  provider_directory     HOP_KEY_CURRENCY       PASSED  healthy=True  q=None coverage=[2026-09-13 07:47:00, 2026-10-18 07:47:00)
  provider_directory     HOP_LEDGER             PASSED  healthy=False q=inferred_contract coverage=[2026-09-13 07:47:00, 2026-10-18 07:47:00)
  provider_directory     HOP_VALUE_AGREEMENT    PASSED  healthy=True  q=None coverage=[2026-09-13 07:47:00, 2026-10-18 07:47:00)

HOP_LEDGER gold_member_coverage: due 12 (pending 4): delivered 12; orphans 0
  claim: "declared contract, qualified: attribution_unverified"
  qualifications: ["attribution_unverified"]
  attribution: "feed_filter_unverified"
  deadline_range: ["2026-10-17T23:47:00+00:00", "2026-10-18T23:47:00+00:00"]
  evidence_level: "observed"
  values: {"coverage": "partial", "unverified": ["src_sys_nm"], "verified": [{"here": "mbr_mbrshp_covrg_end_dt", "upstream": "enddate"}]}
  orphans: "checked"

HOP_LEDGER provider_directory: due 35 (pending 1): delivered 35; orphans 0
  claim: "inferred contract"
  qualifications: ["inferred_contract"]
  attribution: "single_source"
  deadline_range: ["2026-09-13T07:47:00+00:00", "2026-10-18T07:47:00+00:00"]
  evidence_level: "observed"
  values: {"coverage": "partial", "unverified": ["provider_name"], "verified": [{"here": "specialty", "upstream": "specialty"}]}
  orphans: "checked"

healthy counts: [(False, 22), (True, 28)]
integrity view rows: 0
dq_integrity records: [('SWEEP', 'COMPLETED', 1)]
```

What it shows:

- **Claim-aware health.** Both ledgers PASSED and are not healthy: the provider hop's contract is inferred, gold's union attribution is unverified (step 5). The release 1 hop checks make no contract claim and stay healthy.
- **Coverage.** Every hop check of a dataset recorded the same deadline range: one shared calculation. The provider hop's range reaches back to its first evaluated window (its feed is monthly).
- **Values.** Each scope names the verified owned column and the unverified ones; coverage is partial and disclosed, not a qualification.
- **Integrity.** No commit mismatches; the sweep completed; no LEDGER_UNBALANCED.

## (d) Commands

Local, from the repository root (Python 3.11 and Java 21 here; CI uses Python 3.10 and Java 17):

```bash
pip install -e ".[dev]"
export PYSPARK_PYTHON=$PWD/.venv/bin/python
pytest tests/checks/test_hop_coverage.py tests/checks/test_hop_ledger.py tests/store/test_reconciliation.py   # the corrections
pytest                                                                                                        # everything
mkdir e2e && cd e2e && python ../tests/e2e/build_synthetic.py && dre install --conf conf --apply \
  && dre run --conf conf && python ../tests/e2e/verify.py                                                     # end to end
```

The end-to-end check fails unless both ledgers PASSED balanced and committed, every covering hop result has its coverage, the ledgers carry their expected qualifications, and no qualified PASS is healthy.
