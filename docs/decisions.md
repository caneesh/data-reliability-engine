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
- 2026-10-09, step 1: Use of AI coding tools on HCSC code: handled with HCSC separately, outside this build. Not a build blocker; no default recorded here.

## Guard rules (decided after step 1, 2026-10-09)

Enforced by `tests/guard/` (scanner in `tests/guard/scan.py`). Each rule has a snippet test that is caught and one that is allowed.

1. **Naming the dq store.** SQL targets the store as the literal `dq` or through the placeholder `{{ dq_database }}` (Jinja) / `{dq_database}` (Python f-string), because the real name comes from config. A target the scanner cannot read fails. The configured dq database name must be a plain identifier (letters, digits, underscores, not starting with a digit): `store/names.py`, `validate_dq_database`. Step 2 must call it when loading config.
2. **Read-only outside dq.** Flagged unless the target is the dq store: INSERT INTO, CREATE, DataFrameWriter `insertInto`, DataFrameWriter path writes (`.save/.orc/.parquet/.csv/.json/.text` on a `.write` chain), Hadoop FileSystem `delete/rename/mkdirs`, and `os.remove/unlink/rmdir/removedirs/rename/replace`, `shutil.rmtree/move`. The dq store is addressed by table name, so a path target never qualifies. `hdfs dfs` / `hadoop fs` `-rm`, `-rmr`, `-rmdir`, `-mv`, `-mkdir` are always flagged.
3. **Append-only.** The only DML allowed on dq is INSERT INTO (or `insertInto` in append mode). Banned everywhere, dq included: UPDATE, DELETE, MERGE, TRUNCATE, INSERT OVERWRITE, `mode("overwrite")`, `overwrite=True`, `saveAsTable` (dq tables come from `store/ddl.sql`), DROP TABLE/VIEW/DATABASE, and every ALTER except the one below. DDL allowed on dq only: CREATE TABLE, CREATE [OR REPLACE] VIEW, and CREATE DATABASE for the dq database itself.
4. **Retention.** ALTER TABLE `<dq>.<table>` DROP [IF EXISTS] PARTITION is allowed only in `src/hcsc/datalake/dre/store/retention.py` (allowlisted by path). That module refuses to drop any `run_date` partition on or after today minus the configured retention in months, and refuses a retention below one month (`tests/test_retention.py`). Step 2 supplies the per-table retention from `defaults.yaml` (results and events 84 months, file registry 13 months).
5. **Email.** Until the digest exists the email guard is static (nothing in `notify/` references `key_value`). **Step 9 must add a test on the rendered email text** asserting it contains no `key_value`.
6. **CI.** Python 3.10 and Java 17 (`.github/workflows/ci.yml`). Nothing may depend on Java 21.
