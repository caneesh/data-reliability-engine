# Prompt to paste after compacting

Copy everything below the line into Claude after `/compact`, or at the start of a new session on this repository.

---

We are building the Data Reliability Engine (DRE) in `caneesh/data-reliability-engine`. Work on branch `claude/claude-md-documentation-a2lxt0` (ahead of `main`; PR #1 was merged as `96503b9`, and everything since is on the branch, CI green per commit; the latest code commit is `0c4fabd`, CI #98 green).

Before doing anything else, read these files in order:

1. `docs/handoff.md`: where the build stands, what is waiting, the history with CI runs, and the working conventions (including those learned on 2026-10-20 about worktrees, long runs and the concurrency gate).
2. `CLAUDE.md`: the hard constraints. They are never optional.
3. `docs/known-issues.md`: what is open, blocked or not started, and what the final review corrections closed.
4. `docs/spec.md`: the source of truth.
5. `docs/decisions.md`: every assumption and decision, with its reason; the newest sections first ("Final consolidated review", then build steps 8, 6, 5, 4).
6. The evidence packages in `docs/design/evidence/`: `gate-a.md` (checkpoints 3 to 5), `gate-b.md` (6 to 8), `final-corrections.md` (P1-01, P1-02, P1-03, O6).
7. As needed: `docs/design/target-architecture.md` and `docs/design/reconciliation-model.md` (the frozen Phase 1 design), `docs/design/checkpoints.md`, `docs/design/traceability-matrix.md`, `docs/design/workstreams-c4-c8.md`, `docs/architecture.md`.

Then tell me in a few lines where things stand and what you think is next, and wait for my instruction.

Where it stands: the Phase 1 build order (steps 1 to 8) is built and integrated, and the final review's four corrections are built, green in CI. Nothing from step 3 onwards is ratified: the revised checkpoint 3, Gate A, Gate B and the final corrections each await my review, with the defaults listed in `docs/decisions.md`. R18 is NOT_STARTED, R22 and the load test are BLOCKED; none counts as passed. This is controlled development, not production certification: never claim production readiness.

The rules from earlier work that still apply:

- Every summary starts with the CI result (run number and commit), then names the tests that prove the change.
- Run the full test suite and the end-to-end check by hand before every code commit, in the background with a long timeout, and gate the commit on the suite's exit status. Never commit untested work, even if a stop hook asks for a commit: say the suite is running and wait.
- Reproduce a reported defect with a failing test on the old code before fixing it, and keep both runs' output for the evidence.
- Never invent production table, column, path or queue names. Real values come from me, and production values (such as the high-priority queue) never go into the repository.
- When I upload a review or principles document, check each point against the code and tell me where you disagree. Don't follow it blindly.
- Never weaken a test to make it pass; restate one only when the rule it encodes changed, and say so.
- No pull request or merge unless I ask. When I do ask, use a merge commit. Push only the branch above.
- Commit messages end with the `Co-Authored-By:` and `Claude-Session:` lines from the session's attribution reminder; no model name anywhere else.
- After a push, check CI with the GitHub MCP `actions_list` tool and schedule a check-in instead of polling; a red run on my branch is fixed (diagnosed, reproduced, fixed, pushed), never left.
