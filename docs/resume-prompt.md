# Prompt to paste after compacting

Copy everything below the line into Claude after `/compact`, or at the start of a new session on this repository.

---

We are building the Data Reliability Engine (DRE) in `caneesh/data-reliability-engine`. Work on branch `claude/claude-md-documentation-a2lxt0` (ahead of `main`; PR #1 was merged as `96503b9`, and everything since is on the branch with CI green per commit).

Before doing anything else, read these files in order:

1. `docs/handoff.md`: where the build stands, what is waiting, and the working conventions.
2. `CLAUDE.md`: the hard constraints. They are never optional.
3. `docs/spec.md`: the source of truth, including release 2 (derived datasets).
4. `docs/decisions.md`: every assumption and decision, with its reason.
5. `docs/architecture.md`: the guarantees, the limits, and what is deliberately open.
6. `docs/design/outside-in-evidence.md` (release 4, in progress) and `docs/design/reconciliation-contracts.md` (release 5, proposed): the current plans.

Then tell me in a few lines where things stand and what you think is next, and wait for my instruction.

The rules from earlier work that still apply:

- Every summary starts with the CI result (run number and commit), then names the tests that prove the change.
- Run the full test suite and the end-to-end check by hand before every commit. Never commit untested work, even if a hook asks for a commit.
- Never invent production table, column or path names. Real keys and tables come in later iterations, from me.
- When I upload a review or principles document, check each point against the code and tell me where you disagree. Don't follow it blindly.
- No pull request or merge unless I ask. When I do ask, use a merge commit.

- Gate every commit on the test suite's exit status (`pytest ...; test $? -eq 0 && git commit`), never on a command that cannot fail: a commit chained after `tail` went out red twice (CI runs #33 and #44).
