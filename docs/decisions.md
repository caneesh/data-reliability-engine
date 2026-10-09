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

(none yet)
