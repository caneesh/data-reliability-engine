# Decisions

Answers to the open decisions in spec section 11. Fill in **Answer** before build step 1, or write "spec default". Claude Code adds a line here whenever it takes a default.

| Decision | Spec default | Answer | Decided by, date |
| --- | --- | --- | --- |
| Language | PySpark 3.5.1 | | |
| Python version and packages on the cluster | Python 3.10+, dependencies shipped in a zip | | |
| Where it runs and is scheduled | Control-M for the main run; a separate folder or cron for the watchdog | | |
| Database name and service account | `dq`; a read-only account with write access only to `dq` | | |
| Use of AI coding tools on HCSC code | Only through HCSC-approved access | | |

## Defaults taken during the build

- 2026-10-09, step 1: Language: no answer recorded; took the spec default, PySpark 3.5.1.
- 2026-10-09, step 1: Python version and packages on the cluster: no answer recorded; took the spec default, Python 3.10+ with dependencies shipped in a zip. CI runs on Python 3.10.
- 2026-10-09, step 1: Where it runs and is scheduled: no answer recorded; took the spec default (Control-M for the main run, a separate folder or cron for the watchdog). Nothing built in step 1 depends on it.
- 2026-10-09, step 1: Database name and service account: no answer recorded; took the spec default, `dq`. The guard test accepts `dq` or the placeholder `dq_database` (see tests/guard/scan.py), since the real name comes from config.
- 2026-10-09, step 1: Use of AI coding tools on HCSC code: no answer recorded; took the spec default (only through HCSC-approved access). This is a policy question that only HCSC can answer; please confirm.
