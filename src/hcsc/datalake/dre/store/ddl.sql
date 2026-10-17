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
  severity STRING, evaluated_at TIMESTAMP, duration_ms BIGINT, detail STRING
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
-- evidence_incomplete marker commits its result rows only). result_run_id names the run whose
-- committed result the row publishes (a result commit: its own run) or explains (a cause commit).
CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_commit (
  run_id STRING, evaluation_id STRING, dataset STRING, check_id STRING,
  kind STRING, result_run_id STRING,
  result_rows INT, key_event_rows BIGINT, partition_state_rows INT, cause_rows INT,
  committed_at TIMESTAMP
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

-- Committed rows only (ADR-07). Every reader of results, key events, partition states and causes
-- reads these, never the tables, except the window history, which holds every event of a run
-- that did not finish COMPLETED (a stricter rule that also covers uncommitted rows).
CREATE OR REPLACE VIEW {{ dq_database }}.v_committed_result AS
SELECT r.*
FROM {{ dq_database }}.dq_check_result r
LEFT SEMI JOIN {{ dq_database }}.dq_commit c
  ON c.kind = 'result' AND c.run_id = r.run_id AND c.evaluation_id = r.evaluation_id AND c.run_date = r.run_date;

CREATE OR REPLACE VIEW {{ dq_database }}.v_committed_key_event AS
SELECT e.*
FROM {{ dq_database }}.dq_key_event e
LEFT SEMI JOIN {{ dq_database }}.dq_commit c
  ON c.kind = 'result' AND c.run_id = e.run_id AND c.evaluation_id = e.evaluation_id AND c.run_date = e.run_date
  AND c.key_event_rows > 0;

CREATE OR REPLACE VIEW {{ dq_database }}.v_committed_partition_state AS
SELECT s.*
FROM {{ dq_database }}.dq_partition_state s
LEFT SEMI JOIN {{ dq_database }}.dq_commit c
  ON c.kind = 'result' AND c.run_id = s.run_id AND c.dataset = s.dataset AND c.run_date = s.run_date
  AND c.check_id = 'T1_PARTITION_DRIFT' AND c.partition_state_rows > 0;

-- A cause row counts only with a cause commit of its own run and evaluation id, and only while the
-- result that commit names (result_run_id, evaluation_id) has a result commit: explicit identity,
-- no timestamps. A cause left by a run that never committed it stays out, whichever run later
-- commits the same evaluation id. result_run_id says which committed result the cause explains.
CREATE OR REPLACE VIEW {{ dq_database }}.v_committed_cause AS
SELECT x.*, c.result_run_id
FROM {{ dq_database }}.dq_cause_result x
JOIN (
  SELECT DISTINCT k.run_id, k.evaluation_id, k.run_date, k.result_run_id
  FROM {{ dq_database }}.dq_commit k
  LEFT SEMI JOIN {{ dq_database }}.dq_commit r
    ON r.kind = 'result' AND r.run_id = k.result_run_id AND r.evaluation_id = k.evaluation_id
  WHERE k.kind = 'cause' AND k.cause_rows > 0
) c ON c.run_id = x.run_id AND c.evaluation_id = x.evaluation_id AND c.run_date = x.run_date;

-- Audit (no run reads it): commits whose stored rows now differ from what they declare. A run
-- checks the counts before it writes a commit, so a row here means rows were lost afterwards,
-- including evidence past its table's retention (partition states are kept for less time than
-- results). Query it with a run_date bound.
CREATE OR REPLACE VIEW {{ dq_database }}.v_commit_integrity AS
SELECT c.run_id, c.evaluation_id, c.dataset, c.check_id, c.kind, c.run_date,
       c.result_rows, coalesce(r.n, 0) AS stored_result_rows,
       c.key_event_rows, coalesce(e.n, 0) AS stored_key_event_rows,
       c.partition_state_rows, coalesce(s.n, 0) AS stored_partition_state_rows,
       c.cause_rows, coalesce(x.n, 0) AS stored_cause_rows
FROM (
  SELECT run_id, evaluation_id, dataset, check_id, kind, run_date,
         MAX(result_rows) AS result_rows, MAX(key_event_rows) AS key_event_rows,
         MAX(partition_state_rows) AS partition_state_rows, MAX(cause_rows) AS cause_rows
  FROM {{ dq_database }}.dq_commit
  GROUP BY run_id, evaluation_id, dataset, check_id, kind, run_date
) c
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
WHERE (c.kind = 'result' AND (coalesce(r.n, 0) <> c.result_rows
                              OR (c.key_event_rows > 0 AND coalesce(e.n, 0) <> c.key_event_rows)
                              OR (c.partition_state_rows > 0 AND coalesce(s.n, 0) <> c.partition_state_rows)))
   OR (c.kind = 'cause' AND coalesce(x.n, 0) <> c.cause_rows);

-- Every row of the latest evaluation per (dataset, check_id): one row for an
-- ungrouped check, one row per group for a grouped one. Latest = the latest window, then
-- the latest evaluation of it, so a RERUN or REPLAY of an older window never becomes the
-- current state, while a RERUN of the current window replaces it.
CREATE OR REPLACE VIEW {{ dq_database }}.v_latest_result AS
SELECT evaluation_id, run_id, event_id, window_start, window_end, execution_type, feed, dataset, check_id,
       expectation_version, state, reason_category, reason_code, population,
       violations, observed, expected, group_values, severity, evaluated_at,
       duration_ms, detail, run_date
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
