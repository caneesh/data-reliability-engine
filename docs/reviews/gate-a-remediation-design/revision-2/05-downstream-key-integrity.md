# 05 · Downstream (target) key integrity (revision 2)

**Question.** A row in the target layer has a key part its normaliser cannot parse (for example, gold writes an effective date `2026-13-01`). Can it ever make an upstream record look delivered?

**Answer:** no, under revision 2, and today's qualification already keeps the hop's PASS from being healthy when orphans go uncounted (R2-08).
- **Status.** Proposed. Model-verified in `test_rules_model.py` d01–d04, g03. Code facts were read at the lines cited.

## 1. The defect today

- `parse_date` on an invalid string yields NULL, and encoding 1 writes NULL as `'<null>'` (`keys.py:119-122`).
- So a target row with an unparseable part **matches an upstream record whose part is genuinely NULL**: a false PASS (examination l05).

## 2. The rule

| # | Rule | Evidence |
| --- | --- | --- |
| D-1 | A target row whose normaliser turns a non-NULL raw part into NULL gets the `!` form of that part (02 §2). Its key can equal no parsed upstream key, and no key with a genuine NULL part. | `test_d02` (l05 fixed), `test_g01b` |
| D-2 | Such rows are taken out of `dn_cand` before matching, so they cannot satisfy any due key, even an upstream key with the same unparseable bytes (that upstream key is `unassessable 'unparseable key'` anyway, order step 2). | `test_d01`, `test_g03` |
| D-3 | **Orphans checked** (`orphans_unchecked(...)` returns None, `ledger.py:105-118`): each such row loaded in the window is counted on the existing orphan line, labelled `unparseable key`. Duplicates are counted per row. | `test_d01`, `test_d03` |
| D-4 | **Orphans not checked:** the result already carries the qualification `orphans_unchecked` (`ledger.py:156-161`; never healthy, `QUALIFICATIONS` at `:120-131`). The rows are counted in the scope (`unparseable target rows: n`), and the digest's triage line repeats the count. No new qualification is added (Q5, refined: the existing one already prevents a healthy PASS). | Read; `test_d03` (the scope count) |
| D-5 | **Full sweep:** counts every unparsed target row, not only those loaded in the window (the full sweep is the reconciliation of record). | *Proposed; not prototyped* |
| D-6 | The upstream record whose target row is malformed is therefore **missing** (or stale or divergent if another, parsed target row exists): FAIL direction. | `test_d01`, `test_g03` |
| D-7 | An untimed target row beside a timed one, for one key: `unassessable 'no record time here'` (03 §3 on the target side). | `test_d04` |

## 3. Other consumers of target keys

| Consumer | Behaviour under revision 2 | Status |
| --- | --- | --- |
| HOP_KEY_CURRENCY (`hop_common.sql.j2`) | Upstream key absent from the target, so MISSING. Unparsed target rows are excluded from the target key set and counted in the scope. | Proposed (the same macro as the ledger) |
| HOP_VALUE_AGREEMENT | No pair is formed for an unparsed target row, so it is never compared, and it is counted in the scope. | Proposed |
| Chain (`chain.py:196-197`, `chain.sql.j2`) | A record whose next-layer key is unparsed is not found at the next layer: carried as pending, then lost, under the existing chain rules (FAIL direction). A `!` key at the source layer of an edge is reported as unassessable `unparseable key`, not carried. | Proposed |
| Contribution (`contribution.py:86, 89`) | A reference with a `!` part never matches, so it is counted as unmatched (FAIL direction). | Proposed |
| Target raw-variant guard (`dn_raw`, `ledger.py:294`) | Unchanged role. Two raw target keys merged by a lossy normaliser are `key collision`. | Existing, now correct under LPB |

## 4. Decision

- Invalid downstream keys **never match, are always counted, and can only push a hop toward FAIL**. They never push it toward PASS.
- Reporting:
  - when orphans are checked, they are orphans with a reason;
  - otherwise they are a scope count, beside the existing `orphans_unchecked` qualification.
- **Recommendation for Q5:** the scope count plus a triage line. A dedicated qualification would add a second "not healthy" flag for a state that is already not healthy.
