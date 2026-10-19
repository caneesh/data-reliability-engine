-- dq store tables and views (spec section 5), applied by `dre install --apply`.
-- Rendered by store/schema.py with a validated dq_database; statements are
-- separated by semicolons. Tables are CREATE ... IF NOT EXISTS and views CREATE OR REPLACE,
-- so applying it again changes nothing, and applying it after an upgrade replaces the views
-- (they hold no data). Tables are append-only; views derive current state.

CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_run (
  run_id STRING, started_at TIMESTAMP, ended_at TIMESTAMP,
  engine_version STRING, config_commit STRING, status STRING,  -- STARTED | COMPLETED | PARTIAL | FAILED
  checks_expected INT, checks_written INT,
  investigations_expected INT, investigations_done INT,  -- cause investigations of FAILED results
  execution_type STRING                 -- NORMAL | RERUN | REPLAY
) PARTITIONED BY (run_date DATE) STORED AS ORC;

CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_check_result (
  evaluation_id STRING, run_id STRING, event_id STRING,
  window_start TIMESTAMP, window_end TIMESTAMP,
  execution_type STRING,
  feed STRING, dataset STRING, check_id STRING, expectation_version INT,
  state STRING,
  reason_category STRING, reason_code STRING,
  population BIGINT, violations BIGINT, observed STRING, expected STRING,
  group_values MAP<STRING,STRING>,
  severity STRING, evaluated_at TIMESTAMP, duration_ms BIGINT, detail STRING,
  qualifications STRING, coverage_from TIMESTAMP, coverage_to TIMESTAMP
) PARTITIONED BY (run_date DATE) STORED AS ORC;

CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_cause_result (
  run_id STRING, evaluation_id STRING, failure_ref STRING,
  hop STRING, cause_check_id STRING, order_no INT,
  state STRING,
  cause_code STRING, evidence STRING, evaluated_at TIMESTAMP
) PARTITIONED BY (run_date DATE) STORED AS ORC;

CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_key_event (
  run_id STRING, evaluation_id STRING, dataset STRING, check_id STRING,
  hop STRING,
  key_hash STRING, key_value STRING,
  event STRING,
  state_detail STRING, observed_at TIMESTAMP
) PARTITIONED BY (run_date DATE) STORED AS ORC;

CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_file (
  path STRING, feed STRING, first_seen_at TIMESTAMP, size_bytes BIGINT,
  modified_at TIMESTAMP, observed_at TIMESTAMP, run_id STRING
) PARTITIONED BY (run_date DATE) STORED AS ORC;

-- Release 4: what each partition of a table looked like at a run (T1_PARTITION_DRIFT).
-- row_count is the latest known count, carried forward when the files did not change.
CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_partition_state (
  dataset STRING, table_name STRING, partition_value STRING,
  file_count INT, total_bytes BIGINT, latest_modified_at TIMESTAMP,
  row_count BIGINT, counted_at TIMESTAMP,
  observed_at TIMESTAMP, run_id STRING
) PARTITIONED BY (run_date DATE) STORED AS ORC;

-- Phase 1 design, ADR-07: one row per committed evaluation id (kind result) and one per committed
-- cause investigation (kind cause). An evaluation's evidence (key events, partition states) and
-- its result rows are written first, an investigation's cause rows first; the commit row last,
-- once the rows it vouches for were counted in the store. Every view of current state shows only
-- committed rows, so an evaluation interrupted anywhere before its commit is invisible, and its
-- window is evaluated again. Evidence rows of a commit with a count of 0 stay invisible (an
-- evidence_incomplete marker commits its result rows only). result_run_id and result_run_date
-- name the run whose committed result the row publishes (a result commit: its own run) or
-- explains (a cause commit). evidence_kept_until is the last day the rows it vouches for are kept:
-- run_date plus the shortest retention among the tables they are in; retention refuses to drop
-- them sooner.
CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_commit (
  run_id STRING, evaluation_id STRING, dataset STRING, check_id STRING,
  kind STRING, result_run_id STRING, result_run_date DATE,
  result_rows INT, key_event_rows BIGINT, partition_state_rows INT, cause_rows INT,
  reconciliation_rows INT, evidence_kept_until DATE, committed_at TIMESTAMP
) PARTITIONED BY (run_date DATE) STORED AS ORC;

-- Phase 1 build step 3: HOP_LEDGER's accounting, one row per line of an evaluation's ledger: the
-- due, pending and untimed populations, each disposition (with its label: the exclusion rule@version
-- or the reject reason), orphans, overlap diagnostics and volume guards. class: population, unknown,
-- authorised, breach, defect, ok, deficit, diagnostic, guard. evidence_level: observed,
-- reconstructed, reported, inferred, unavailable (ADR-06). scope: the result's scope (ADR-01), the
-- same JSON on every row of the evaluation. Evidence of its result: written before it, committed with it.
CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_reconciliation (
  run_id STRING, evaluation_id STRING, dataset STRING, check_id STRING, upstream STRING,
  line STRING, label STRING, class STRING, records BIGINT, evidence_level STRING, scope STRING,
  recorded_at TIMESTAMP
) PARTITIONED BY (run_date DATE) STORED AS ORC;

-- Checkpoint 2 corrections (R1, R2): what DRE learned about its own store after publishing.
-- record: SWEEP (an integrity sweep: status COMPLETED or FAILED, scope DAILY or FULL over
-- scope_from to scope_to), EVIDENCE_MISMATCH (a commit's stored rows no longer match it before its
-- evidence_kept_until), COMMIT_CONFLICT (one commit identity stored with different contents), or
-- REBASELINED (a later evaluation re-established the baseline an INVALID unit fed), or
-- LEDGER_UNBALANCED (a HOP_LEDGER evaluation whose dispositions did not add up: an engine defect,
-- an incident that makes no unit INVALID; checkpoint 3, L5). The
-- subject columns name the commit. Observations, not evaluations: not commit-gated.
CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_integrity (
  run_id STRING, record STRING, status STRING,
  subject_run_id STRING, subject_run_date DATE, evaluation_id STRING, commit_kind STRING,
  dataset STRING, check_id STRING,
  scope STRING, scope_from DATE, scope_to DATE,
  detail STRING, recorded_at TIMESTAMP
) PARTITIONED BY (run_date DATE) STORED AS ORC;

-- A run's state is its latest dq_run row: the final row (COMPLETED, PARTIAL or
-- FAILED) once written, otherwise STARTED.
CREATE OR REPLACE VIEW {{ dq_database }}.v_latest_run AS
SELECT run_id, started_at, ended_at, engine_version, config_commit, status,
       checks_expected, checks_written, investigations_expected, investigations_done, execution_type, run_date
FROM (
  SELECT r.*,
         ROW_NUMBER() OVER (
           PARTITION BY run_id
           ORDER BY CASE WHEN status = 'STARTED' THEN 0 ELSE 1 END DESC, ended_at DESC
         ) AS latest_rank
  FROM {{ dq_database }}.dq_run r
) ranked
WHERE latest_rank = 1;

-- One row per commit identity (run, evaluation id, kind, run_date), with its evidence state:
-- INVALID: the identity was stored with different contents, or a sweep recorded its rows lost or
--   in conflict (dq_integrity) - compromised; never used as a baseline, never healthy;
-- EXPIRED: past evidence_kept_until - its evidence rows may be gone by retention, as documented;
-- UNKNOWN: the latest integrity sweep covering its run_date failed and no later completed sweep
--   covers it - not verified since; never healthy;
-- VALID: verified when published, and not found lost since.
CREATE OR REPLACE VIEW {{ dq_database }}.v_commit_state AS
WITH c AS (
  SELECT run_id, evaluation_id, kind, run_date,
         MAX(dataset) AS dataset, MAX(check_id) AS check_id,
         MAX(result_run_id) AS result_run_id, MAX(result_run_date) AS result_run_date,
         MAX(result_rows) AS result_rows, MAX(key_event_rows) AS key_event_rows,
         MAX(partition_state_rows) AS partition_state_rows, MAX(cause_rows) AS cause_rows,
         MAX(reconciliation_rows) AS reconciliation_rows, MIN(evidence_kept_until) AS evidence_kept_until,
         COUNT(DISTINCT concat_ws('|', CAST(result_rows AS STRING), CAST(key_event_rows AS STRING),
                                  CAST(partition_state_rows AS STRING), CAST(cause_rows AS STRING),
                                  CAST(reconciliation_rows AS STRING), result_run_id,
                                  CAST(result_run_date AS STRING), CAST(evidence_kept_until AS STRING))) AS versions
  FROM {{ dq_database }}.dq_commit
  GROUP BY run_id, evaluation_id, kind, run_date
),
lost AS (
  SELECT DISTINCT subject_run_id, subject_run_date, evaluation_id, commit_kind
  FROM {{ dq_database }}.dq_integrity
  WHERE record IN ('EVIDENCE_MISMATCH', 'COMMIT_CONFLICT')
),
latest_sweep AS (
  SELECT scope, status, scope_from, scope_to, recorded_at FROM (
    SELECT scope, status, scope_from, scope_to, recorded_at,
           ROW_NUMBER() OVER (PARTITION BY scope ORDER BY recorded_at DESC, run_id DESC) AS rn
    FROM {{ dq_database }}.dq_integrity WHERE record = 'SWEEP'
  ) s WHERE rn = 1
),
sweeps AS (
  SELECT MAX(CASE WHEN scope = 'DAILY' THEN status END) AS d_status,
         MAX(CASE WHEN scope = 'DAILY' THEN scope_from END) AS d_from,
         MAX(CASE WHEN scope = 'DAILY' THEN scope_to END) AS d_to,
         MAX(CASE WHEN scope = 'DAILY' THEN recorded_at END) AS d_at,
         MAX(CASE WHEN scope = 'FULL' THEN status END) AS f_status,
         MAX(CASE WHEN scope = 'FULL' THEN scope_from END) AS f_from,
         MAX(CASE WHEN scope = 'FULL' THEN scope_to END) AS f_to,
         MAX(CASE WHEN scope = 'FULL' THEN recorded_at END) AS f_at
  FROM latest_sweep
)
SELECT c.run_id, c.evaluation_id, c.kind, c.run_date, c.dataset, c.check_id, c.result_run_id,
       c.result_run_date, c.result_rows, c.key_event_rows, c.partition_state_rows, c.cause_rows,
       c.reconciliation_rows, c.evidence_kept_until, c.versions,
       CASE WHEN c.versions > 1 OR l.evaluation_id IS NOT NULL THEN 'INVALID'
            WHEN c.evidence_kept_until < current_date() THEN 'EXPIRED'
            WHEN (s.d_status = 'FAILED' AND c.run_date BETWEEN s.d_from AND s.d_to
                  AND NOT coalesce(s.f_status = 'COMPLETED' AND s.f_at > s.d_at
                                   AND c.run_date BETWEEN s.f_from AND s.f_to, FALSE))
              OR (s.f_status = 'FAILED' AND c.run_date BETWEEN s.f_from AND s.f_to
                  AND NOT coalesce(s.d_status = 'COMPLETED' AND s.d_at > s.f_at
                                   AND c.run_date BETWEEN s.d_from AND s.d_to, FALSE)) THEN 'UNKNOWN'
            ELSE 'VALID' END AS evidence_state
FROM c
LEFT JOIN lost l
  ON l.subject_run_id = c.run_id AND l.subject_run_date = c.run_date AND l.evaluation_id = c.evaluation_id
  AND l.commit_kind = c.kind
CROSS JOIN sweeps s;

-- Committed rows only (ADR-07). Every reader of results, key events, partition states and causes
-- reads these, never the tables, except the window history, which holds every event of a run
-- that did not finish COMPLETED (a stricter rule that also covers uncommitted rows). A committed
-- result is kept whatever happens to its evidence later, with its evidence state; evidence rows of
-- an INVALID unit are never shown, so no baseline is built on compromised evidence.
CREATE OR REPLACE VIEW {{ dq_database }}.v_committed_result AS
SELECT r.*, c.evidence_state
FROM {{ dq_database }}.dq_check_result r
JOIN {{ dq_database }}.v_commit_state c
  ON c.kind = 'result' AND c.run_id = r.run_id AND c.evaluation_id = r.evaluation_id AND c.run_date = r.run_date;

CREATE OR REPLACE VIEW {{ dq_database }}.v_committed_key_event AS
SELECT e.*
FROM {{ dq_database }}.dq_key_event e
LEFT SEMI JOIN {{ dq_database }}.v_commit_state c
  ON c.kind = 'result' AND c.run_id = e.run_id AND c.evaluation_id = e.evaluation_id AND c.run_date = e.run_date
  AND c.key_event_rows > 0 AND c.evidence_state <> 'INVALID';

CREATE OR REPLACE VIEW {{ dq_database }}.v_committed_partition_state AS
SELECT s.*
FROM {{ dq_database }}.dq_partition_state s
LEFT SEMI JOIN {{ dq_database }}.v_commit_state c
  ON c.kind = 'result' AND c.run_id = s.run_id AND c.dataset = s.dataset AND c.run_date = s.run_date
  AND c.check_id = 'T1_PARTITION_DRIFT' AND c.partition_state_rows > 0 AND c.evidence_state <> 'INVALID';

-- HOP_LEDGER's accounting rows of committed evaluations whose commit vouches for them and is not INVALID.
CREATE OR REPLACE VIEW {{ dq_database }}.v_committed_reconciliation AS
SELECT x.*
FROM {{ dq_database }}.dq_reconciliation x
LEFT SEMI JOIN {{ dq_database }}.v_commit_state c
  ON c.kind = 'result' AND c.run_id = x.run_id AND c.evaluation_id = x.evaluation_id AND c.run_date = x.run_date
  AND c.reconciliation_rows > 0 AND c.evidence_state <> 'INVALID';

-- A cause row counts only with a cause commit of its own run, evaluation id and run_date that is
-- not INVALID, and only while the result that commit names (result_run_id, result_run_date,
-- evaluation_id) has a result commit: explicit identity, no timestamps. A cause left by a run that
-- never committed it, or naming a result that was never committed, stays out. An INVALID cause set
-- is not a valid cause: the next run investigates the result again.
CREATE OR REPLACE VIEW {{ dq_database }}.v_committed_cause AS
SELECT x.*, k.result_run_id, k.result_run_date, k.evidence_state
FROM {{ dq_database }}.dq_cause_result x
JOIN (
  SELECT s.run_id, s.evaluation_id, s.run_date, s.result_run_id, s.result_run_date, s.evidence_state
  FROM {{ dq_database }}.v_commit_state s
  LEFT SEMI JOIN {{ dq_database }}.v_commit_state r
    ON r.kind = 'result' AND r.run_id = s.result_run_id AND r.run_date = s.result_run_date
    AND r.evaluation_id = s.evaluation_id
  WHERE s.kind = 'cause' AND s.cause_rows > 0 AND s.evidence_state <> 'INVALID'
) k ON k.run_id = x.run_id AND k.evaluation_id = x.evaluation_id AND k.run_date = x.run_date;

-- Audit: commits whose stored rows now differ from what they declare, with their evidence
-- state and how long their evidence is kept. A run checks the counts before it writes a commit,
-- so a row here means rows were lost (or added) afterwards; an EXPIRED row is evidence past its
-- documented retention. The integrity sweep reads it. Query it with a run_date bound.
CREATE OR REPLACE VIEW {{ dq_database }}.v_commit_integrity AS
SELECT c.run_id, c.evaluation_id, c.dataset, c.check_id, c.kind, c.run_date,
       c.result_rows, coalesce(r.n, 0) AS stored_result_rows,
       c.key_event_rows, coalesce(e.n, 0) AS stored_key_event_rows,
       c.partition_state_rows, coalesce(s.n, 0) AS stored_partition_state_rows,
       c.cause_rows, coalesce(x.n, 0) AS stored_cause_rows,
       c.reconciliation_rows, coalesce(g.n, 0) AS stored_reconciliation_rows,
       c.evidence_kept_until, c.evidence_state
FROM {{ dq_database }}.v_commit_state c
LEFT JOIN (SELECT run_id, evaluation_id, run_date, COUNT(*) AS n FROM {{ dq_database }}.dq_check_result
           GROUP BY run_id, evaluation_id, run_date) r
  ON c.kind = 'result' AND r.run_id = c.run_id AND r.evaluation_id = c.evaluation_id AND r.run_date = c.run_date
LEFT JOIN (SELECT run_id, evaluation_id, run_date, COUNT(*) AS n FROM {{ dq_database }}.dq_key_event
           GROUP BY run_id, evaluation_id, run_date) e
  ON c.kind = 'result' AND e.run_id = c.run_id AND e.evaluation_id = c.evaluation_id AND e.run_date = c.run_date
LEFT JOIN (SELECT run_id, dataset, run_date, COUNT(*) AS n FROM {{ dq_database }}.dq_partition_state
           GROUP BY run_id, dataset, run_date) s
  ON c.kind = 'result' AND c.check_id = 'T1_PARTITION_DRIFT' AND s.run_id = c.run_id AND s.dataset = c.dataset
  AND s.run_date = c.run_date
LEFT JOIN (SELECT run_id, evaluation_id, run_date, COUNT(*) AS n FROM {{ dq_database }}.dq_cause_result
           GROUP BY run_id, evaluation_id, run_date) x
  ON c.kind = 'cause' AND x.run_id = c.run_id AND x.evaluation_id = c.evaluation_id AND x.run_date = c.run_date
LEFT JOIN (SELECT run_id, evaluation_id, run_date, COUNT(*) AS n FROM {{ dq_database }}.dq_reconciliation
           GROUP BY run_id, evaluation_id, run_date) g
  ON c.kind = 'result' AND g.run_id = c.run_id AND g.evaluation_id = c.evaluation_id AND g.run_date = c.run_date
WHERE (c.kind = 'result' AND (coalesce(r.n, 0) <> c.result_rows
                              OR (c.key_event_rows > 0 AND coalesce(e.n, 0) <> c.key_event_rows)
                              OR (c.partition_state_rows > 0 AND coalesce(s.n, 0) <> c.partition_state_rows)
                              OR (c.reconciliation_rows > 0 AND coalesce(g.n, 0) <> c.reconciliation_rows)))
   OR (c.kind = 'cause' AND coalesce(x.n, 0) <> c.cause_rows);

-- Every row of the latest evaluation per (dataset, check_id): one row for an
-- ungrouped check, one row per group for a grouped one. healthy is the one definition of a
-- healthy result: PASSED with VALID evidence (never INVALID, UNKNOWN or EXPIRED) and no
-- qualifications (a claim that rests on an unproven premise: an inferred contract, an unverified
-- attribution, orphans not checked, reported evidence; checkpoint 3). Latest = the latest window, then
-- the latest evaluation of it, so a RERUN or REPLAY of an older window never becomes the
-- current state, while a RERUN of the current window replaces it.
CREATE OR REPLACE VIEW {{ dq_database }}.v_latest_result AS
SELECT evaluation_id, run_id, event_id, window_start, window_end, execution_type, feed, dataset, check_id,
       expectation_version, state, reason_category, reason_code, population,
       violations, observed, expected, group_values, severity, evaluated_at,
       duration_ms, detail, qualifications, coverage_from, coverage_to, run_date, evidence_state,
       state = 'PASSED' AND evidence_state = 'VALID' AND qualifications IS NULL AS healthy
FROM (
  SELECT r.*,
         DENSE_RANK() OVER (
           PARTITION BY dataset, check_id
           ORDER BY window_end DESC, evaluated_at DESC, run_id DESC
         ) AS latest_rank
  FROM {{ dq_database }}.v_committed_result r
) ranked
WHERE latest_rank = 1;

-- Keys whose latest event at a hop is FLAGGED or STILL_FLAGGED, with the time the key
-- was first flagged in its current open spell (after its last CLEARED).
-- key_value is restricted and not exposed here.
CREATE OR REPLACE VIEW {{ dq_database }}.v_open_keys AS
SELECT dataset, check_id, hop, key_hash, latest_event, state_detail,
       first_flagged_at, last_observed_at, run_id, evaluation_id
FROM (
  SELECT dataset, check_id, hop, key_hash,
         event AS latest_event, state_detail, run_id, evaluation_id,
         observed_at AS last_observed_at,
         MIN(CASE WHEN event IN ('FLAGGED', 'STILL_FLAGGED') AND
                       (last_cleared_at IS NULL OR observed_at > last_cleared_at)
                  THEN observed_at END)
           OVER (PARTITION BY dataset, check_id, hop, key_hash) AS first_flagged_at,
         ROW_NUMBER() OVER (
           PARTITION BY dataset, check_id, hop, key_hash
           ORDER BY observed_at DESC, run_id DESC
         ) AS latest_rank
  FROM (
    SELECT e.*,
           MAX(CASE WHEN event = 'CLEARED' THEN observed_at END)
             OVER (PARTITION BY dataset, check_id, hop, key_hash) AS last_cleared_at
    FROM {{ dq_database }}.v_committed_key_event e
  ) with_cleared
) ranked
WHERE latest_rank = 1 AND latest_event IN ('FLAGGED', 'STILL_FLAGGED');

-- One row per file path: first-seen time, and size and times from the latest observation.
CREATE OR REPLACE VIEW {{ dq_database }}.v_file_status AS
SELECT path, feed, first_seen_at, size_bytes AS latest_size_bytes,
       modified_at AS latest_modified_at, observed_at AS last_observed_at
FROM (
  SELECT path, feed, size_bytes, modified_at, observed_at,
         MIN(first_seen_at) OVER (PARTITION BY path) AS first_seen_at,
         ROW_NUMBER() OVER (PARTITION BY path ORDER BY observed_at DESC, run_id DESC) AS latest_rank
  FROM {{ dq_database }}.dq_file
) ranked
WHERE latest_rank = 1;

-- The latest recorded state per dataset and partition (T1_PARTITION_DRIFT compares against it).
CREATE OR REPLACE VIEW {{ dq_database }}.v_latest_partition_state AS
SELECT dataset, table_name, partition_value, file_count, total_bytes, latest_modified_at,
       row_count, counted_at, observed_at, run_id, run_date
FROM (
  SELECT s.*,
         ROW_NUMBER() OVER (PARTITION BY dataset, partition_value ORDER BY observed_at DESC, run_id DESC) AS latest_rank
  FROM {{ dq_database }}.v_committed_partition_state s
) ranked
WHERE latest_rank = 1
