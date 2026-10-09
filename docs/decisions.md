# Decisions

Answers to the open decisions in spec section 11. Fill in **Answer** before build step 1, or write "spec default". Claude Code adds a line here whenever it takes a default.

| Decision | Spec default | Answer | Decided by, date |
| --- | --- | --- | --- |
| Language | PySpark 3.5.1 | | |
| Python version and packages on the cluster | Python 3.10+, dependencies shipped in a zip | | |
| Where it runs and is scheduled | Control-M for the main run; a separate folder or cron for the watchdog | | |
| Database name and service account | `dq`; a read-only account with write access only to `dq` | | |
| Use of AI coding tools on HCSC code | Only through HCSC-approved access | | |
| Code repository location | (not in spec) | Approved by HCSC: the repo is hosted on Aneesh Chan's GitHub (`caneesh/data-reliability-engine`) | HCSC, 2026-10-09 |

## Defaults taken during the build

- 2026-10-09, step 1: Language: no answer recorded; took the spec default, PySpark 3.5.1.
- 2026-10-09, step 1: Python version and packages on the cluster: no answer recorded; took the spec default, Python 3.10+ with dependencies shipped in a zip. CI runs on Python 3.10.
- 2026-10-09, step 1: Where it runs and is scheduled: no answer recorded; took the spec default (Control-M for the main run, a separate folder or cron for the watchdog). Nothing built in step 1 depends on it.
- 2026-10-09, step 1: Database name and service account: no answer recorded; took the spec default, `dq`. The guard test accepts `dq` or the placeholder `dq_database` (see tests/guard/scan.py), since the real name comes from config.
- 2026-10-09, step 1: Use of AI coding tools on HCSC code: handled with HCSC separately, outside this build. Not a build blocker; no default recorded here.

## Guard rules: reasons (decided after step 1, 2026-10-09)

The rules themselves are in spec section 9 (Guard tests). Reasons, by rule:

1. **Naming the dq store.** The real database name comes from config, so code cannot hard-code `dq`; a fixed placeholder keeps the target visible to a static scan. Validating the name as a plain identifier stops it carrying dots, quotes or SQL into statements. Step 2 must call `validate_dq_database` when loading config.
2. **Read-only outside dq.** Hard rule 1. Path writes, FileSystem and os/shutil calls are other ways to change production data than SQL, so they are flagged the same way.
3. **Append-only.** Hard rule 5. Overwrite modes and `saveAsTable` are the DataFrame forms of INSERT OVERWRITE and of creating tables outside `store/ddl.sql`.
4. **One write path** (added before step 2). A static scan cannot follow a write chain split across statements; allowing the write APIs in one module closes that gap. Column order: `insertInto` matches by position, so a frame in a different order would silently write values into the wrong columns.
5. **Retention.** Retention needs old partitions dropped, which append-only otherwise bans; keeping it in one module, with a refusal for anything inside retention, limits the exception.
6. **Database creation** (added before step 2). On the cluster the platform team owns the database and the service account; the engine must not create it. Tests and local runs still need it.
7. **Email.** Hard rule 6. The digest does not exist yet, so the guard is static until step 9, which must add a test on the rendered email text.

CI runs on Python 3.10 with Java 17 (`.github/workflows/ci.yml`). Nothing may depend on Java 21.
