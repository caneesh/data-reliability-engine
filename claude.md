Data Reliability Engine (DRE)

A read-only engine that watches the membership data lake, finds data issues before consumers report them, and works out why each one happened. It checks feeds and tables against configuration, writes every result to its own append-only store, and emails a digest.

Source of truth: docs/spec.md. Read the relevant sections before every task. If anything here or in docs/background/ disagrees with the spec, the spec wins. If the spec is unclear or contradicts itself, stop and ask; do not guess.

Hard constraints (never break these)

	1.	Read-only. The engine reads production tables, HDFS listings and scheduler history. It writes only to its own database (dq in the spec; the real name comes from config). No INSERT, UPDATE, DELETE, MERGE, DROP, ALTER or TRUNCATE on anything else. No file moves or deletes in HDFS.
	2.	No pipeline changes. Nothing in this repo modifies or hooks into existing jobs, scripts or schedules.
	3.	No code per source. Everything source-specific lives in YAML. If a source needs logic the engine lacks, add a generic capability. Never write if feed == ... or if dataset == ....
	4.	Three result states. Every check, every run, writes exactly one of PASSED, FAILED, DID_NOT_RUN. A check that evaluated zero rows is DID_NOT_RUN / empty_population, never PASSED. An exception inside a check becomes DID_NOT_RUN with a reason; it never stops the run or disappears.
	5.	Append-only evidence. Result and event tables are only appended to. The writer exposes append and nothing else. Current state comes from views.
	6.	No member data outside the dq store. Logs, exceptions and emails carry counts, check ids and table names only. key_value never appears in logs or emails. The HMAC secret is read from a file path given in config and never committed.
	7.	Synthetic data only. No production extracts, real keys, real file names from production or real member values anywhere in the repo, including tests, fixtures, docs and commit messages.

How to work

	•	Build in the order in spec section 10, one step at a time. Start each step by stating the plan: files to add or change, and tests to write. A step is done only when its "done when" condition passes. Then stop and summarise what was built, anything in the spec that was unclear, and any assumption made.
	•	Do not build ahead. Anything listed under "Not in release 1" (spec section 1) is out of scope, as is everything in docs/background/ that is not in the spec.
	•	Tests first where practical. Every check and cause check needs a test that shows it can FAIL and a test that shows it can PASS, plus the DID_NOT_RUN path.
	•	Never weaken a test to make it pass. If a replay scenario (R01 to R12) fails, fix the engine or raise the spec question.
	•	Unknown production values stay unknown. Section 11 lists values still waiting on the developer or platform team. Leave them as null in config, so that dre validate reports them or the dependent cause checks return NOT_READY. Never invent table names, paths or column names.
	•	Open decisions. If a "Decide before step 1" item in section 11 has no recorded answer in docs/decisions.md, use the spec's default and add a line to that file saying so.

Technical conventions

	•	Python 3.10+, PySpark 3.5.1, Pydantic v2, PyYAML, Jinja2, pytest. No other runtime services: no database server, web server or queue.
	•	Package hcsc.datalake.dre under src/. src/hcsc/ and src/hcsc/datalake/ are namespace packages and must have no __init__.py. Distribution hcsc-datalake-dre. Console command dre.
	•	All check logic is SQL in Jinja2 templates under checks/sql/, run through spark.sql. Python classes only render the SQL, run it and build the result.
	•	SQL must run on Spark SQL against Hive tables. Never use positional GROUP BY 1, 2. Quote nothing that config did not provide. Build identifiers from validated config only.
	•	Times: record time, load time and first-seen time are never compared with each other. Parse every time column with its configured format before comparing. Never compare times as strings.
	•	Keys: apply key_normalise before any join or comparison (for example, zero-padded sub_id in gold against unpadded upstream).
	•	Config errors name the file, line and field, and say how to fix them.
	•	Type hints throughout. Keep modules small. Prefer plain functions and dataclasses or Pydantic models over class hierarchies, except the Check interface in spec section 6.

Commands

pip install -e ".[dev]"      # needs Java 17 for local-mode Spark
pytest                       # all tests, local-mode Spark
pytest tests/replay -k R05   # one replay scenario
dre validate --conf conf/    # static config validation

Guard tests (keep them passing)

	•	A test scans src/ for write statements (INSERT, MERGE, UPDATE, DELETE, DROP, ALTER, TRUNCATE, hdfs dfs -rm, -mv) and fails unless the target is the dq database.
	•	A test asserts that email text contains no key_value.

Repo map

	•	docs/spec.md: build spec (source of truth)
	•	docs/decisions.md: answers to open decisions and any defaults taken
	•	docs/background/: feature list and use cases, context only
	•	src/hcsc/datalake/dre/: engine code (layout in spec section 2)
	•	conf/: sample synthetic configuration only. Real feed configuration lives with the deployed instance, not in this repo.
	•	tests/fixtures/: synthetic data builders. tests/replay/: one test per replay scenario.