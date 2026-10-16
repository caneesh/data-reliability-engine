# Architectural checkpoints

Each build step of the frozen Phase 1 design (`docs/design/target-architecture.md`, section 7) ends at a checkpoint. The next step starts only after the user has reviewed it (the user's instruction, 2026-10-16: "a controlled implementation phase").

A checkpoint report states:

- what was planned and what was delivered;
- how the work conforms to each approved ADR;
- every deviation from the written design, for the user to ratify;
- the invariants, re-verified;
- the evidence: CI run, commit, tests;
- what changed for existing configuration;
- the blind spots that remain;
- the risks;
- the gate to the next step.

## Checkpoint 1: the contract schema and its validation (2026-10-16)

**Status:** built; awaiting the user's review. **CI:** run #76 on `f9d86d4`, green (`test`, `e2e`, `package`). **Proving tests:** `tests/config/test_contracts.py` (the schema, its rules, the aliases, the report) and `tests/checks/test_hop_contracts.py::test_a_contract_exclusion_counts_only_rows_loaded_while_it_is_effective` (a contract changes what the hop checks expect, and only as written).

### Planned against delivered

| Planned (step 1 plan, 2026-10-16) | Delivered |
| --- | --- |
| Spec first: section 3 blocks, section 11 values | Spec sections 1, 3, 6, 9 and 11 amended |
| `grain` with key, version rule and history class | `grain.history`; key and version are the existing `key` and `record_time`, with `winner_rule` for ties (deviation D1) |
| One mapping kind per hop | `contracts.<upstream>.mapping`, the eight kinds; rules per kind in `dre validate` |
| Versioned exclusions with effective dates, approval and `max_share` | Built and validated. The hop checks honour them now; `max_share` is applied by the ledger (step 3) |
| `reject_policy`, default defect | Built and validated: `{disposition, max_share}` per reason code (deviation D4) |
| `deletion`: tombstone rule, soft or hard | Built and validated: `tombstone`, `applied_as`, `soft_when` (deviations D2, D3). Conditions parsed by `dre dry-run` |
| Aliases, so today's configuration keeps working | `config/contracts.py`: a hop with no contract is `dedup` (or `aggregate` for a derived upstream). `hop_exclusions` is used exactly as written, with a warning |
| `dre validate`: unsupported mappings, blind spots, tolerance limits, exclusion ids, protected queue | All built. The per-hop report is printed after a valid configuration |
| Sample configuration: gold's soft-void deletion and a `grain` block | Gold: `union_partition`, soft void, `mutable`. Curated: history left null (a platform fact). Provider feed: `append_only` and `mutable` |
| Tests; the aliases giving identical results on today's sample | 26 configuration tests, 5 check tests, 1 dry-run test. Alias equivalence: `test_the_alias_and_an_equivalent_contract_judge_alike`, plus the unchanged full suite and the e2e run |
| Decisions entry; traceability matrix moved to coded and unit-tested | `docs/decisions.md`, "Phase 1 build step 1"; matrix sections 4 to 6 |

### Conformance by ADR

| ADR | What step 1 makes true | Left for later steps |
| --- | --- | --- |
| 01 Populations, dispositions, scope | A tolerance can name only `missing`, `stale` and `divergent`, as non-negative counts. Anything else is a configuration error | The ledger, the identity, scope columns (step 3) |
| 02 Grain, identity, mapping kinds | Every hop has exactly one kind, declared or resolved. A kind that is compared through `key_map` must have an entry, and one that cannot be compared that way must not. `aggregate` is the kind for a derived upstream, and only for one | Key collisions, transforms, DID_NOT_RUN / `unsupported_mapping` (step 3) |
| 03 Multi-source shapes | `union_partition` requires an attribution (`feed_filter`). `join_contribution` requires a reference covering the contributor's key. `survivorship` requires one shared precedence | T1_SOURCE_ATTRIBUTION, HOP_CONTRIBUTION (step 5) |
| 04 Exclusions, rejects, deletions | Exclusions carry an id, version, approval and volume guard, and the hop checks apply them only while effective. Rejects default to defect. Deletions are declared | Volume guards, the reject and deletion dispositions (step 3) |
| 05 Time and modes | Each dataset can declare its history class; an undeclared or mutable one is reported as no as-of replay | Evaluation modes, AS_OF_REPLAY (step 4) |
| 06 Evidence levels | Blind spots per hop are reported by `dre validate` | Levels on dispositions and results (steps 3 and 6) |
| 07 Commit rows | Untouched | Step 2 |
| 08 Isolation | `yarn_queue` can never be a protected queue (static check) | Run budget, kill switch, run-start check (step 8) |

### Deviations to ratify

Each is recorded in `docs/decisions.md`, and the reconciliation model document now matches what was built.

| # | The design said | Built | Why |
| --- | --- | --- | --- |
| D1 | `grain: { key, version, history }` | `grain: { history }`. The key is `key`, and the version is `record_time` plus the hop's `winner_rule` | One source of truth. Repeating the key and version under `grain` would let the two copies disagree |
| D2 | `deletion.formats` lists the date patterns | No `formats`. Conditions parse their dates with `to_date(column, 'pattern')` | Every configuration condition is plain SQL. A pattern map would mean rewriting user SQL |
| D3 | `applied_as: { soft: <SQL> }` | `applied_as: soft` plus `soft_when: <SQL>` | A plain enum, with its condition beside it, validates more clearly |
| D4 | `reject_policy: { CODE: authorised }` | `{ CODE: { disposition, max_share } }` | ADR-04 gives authorised reasons a volume guard, and the short form had nowhere to put it |
| D5 | Union attribution as a contract field | The dataset's `feed_filter` | It already partitions a shared gold table among per-source datasets. A second field could contradict it |
| D6 | A hop without a contract defaults to `identity` (implied) | `dedup` (or `aggregate` for a derived upstream) | `dedup` is what HOP_KEY_CURRENCY judges today: the latest upstream version per key |

### Invariants re-verified

- **Hard constraints 1 to 7.** The guard tests pass. No store table, writer or view changed, and nothing writes anywhere new. The sample's new conditions use spec-example columns only, and the queue names stay out of the repository.
- **Three result states.** No result state, reason code or check result shape changed.
- **Existing configuration is judged identically.** A legacy `hop_exclusions` condition reaches the SQL exactly as before (`test_the_legacy_condition_is_used_exactly_as_written`). The full suite passes unchanged, apart from the new tests, and the e2e run still gives 48 results (28 PASSED, 20 DID_NOT_RUN) with the sample's contract declared.
- **The one behaviour change** is opt-in: a hop that declares contract exclusions has them honoured, and its `observed` text names them by id and version.

### Blind spots now visible on the sample

From `dre validate --conf conf`:

```
example_curated_enrollment->gold_member_coverage: union_partition (contract)
  attribution (each row here owned by exactly one source) is not proven until build step 5
  deletions declared (soft); counted as deleted from build step 3
  as-of replay unavailable: example_curated_enrollment does not declare grain.history
  as-of replay unavailable: gold_member_coverage is mutable
provider_roster_raw->provider_directory: dedup (from key_map)
  deletions not modelled: a deleted record is judged like any other version
  as-of replay unavailable: provider_directory is mutable
```

### Risks and notes

1. **Over-broad exclusions still pass silently until step 3.** That was already true of `hop_exclusions`. `max_share` is now required on every new exclusion, so the guard needs no configuration change when it arrives.
2. **A `fan_out` or `join_contribution` hop is not judged.** The warning and the blind-spot line are its only trace until step 3 adds the DID_NOT_RUN result, and step 5 adds the contribution check. It can never be judged one-to-one, because keying it is an error.
3. **Effective dates are local dates.** They are read at midnight in `defaults.yaml` `timezone` and compared with the upstream row's load time. A rule meant to apply by record time would need a new field; none was asked for.
4. **Blind spots appear only in `dre validate`.** The digest does not show them yet. They belong on each result's scope (step 3).

### Gate to step 2 (commit rows, ADR-07)

1. The user ratifies deviations D1 to D6, or asks for changes.
2. ~~CI is green on the step 1 commit.~~ Run #76, green.
3. Nothing else blocks step 2. The open inputs (DRE's queue name, the kill switch's path and owner, developer questions 17 and 19, curated's history class) are needed by later steps, not by step 2.
