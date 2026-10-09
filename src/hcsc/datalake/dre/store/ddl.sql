-- dq store tables and views (spec section 5).
-- Rendered by store/schema.py with a validated dq_database; statements are
-- separated by semicolons. Tables are append-only; views derive current state.

CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_run (
  run_id STRING, started_at TIMESTAMP, ended_at TIMESTAMP,
  engine_version STRING, config_commit STRING, status STRING,
  checks_expected INT, checks_written INT
) PARTITIONED BY (run_date DATE) STORED AS ORC;

CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_check_result (
  evaluation_id STRING, run_id STRING, event_id STRING, execution_type STRING,
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
  key_hash STRING, key_value STRING,
  event STRING,
  state_detail STRING, observed_at TIMESTAMP
) PARTITIONED BY (run_date DATE) STORED AS ORC;

CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_file (
  path STRING, feed STRING, first_seen_at TIMESTAMP, size_bytes BIGINT,
  modified_at TIMESTAMP, observed_at TIMESTAMP, run_id STRING
) PARTITIONED BY (run_date DATE) STORED AS ORC;

-- Every row of the latest evaluation per (dataset, check_id): one row for an
-- ungrouped check, one row per group for a grouped one.
CREATE OR REPLACE VIEW {{ dq_database }}.v_latest_result AS
SELECT evaluation_id, run_id, event_id, execution_type, feed, dataset, check_id,
       expectation_version, state, reason_category, reason_code, population,
       violations, observed, expected, group_values, severity, evaluated_at,
       duration_ms, detail, run_date
FROM (
  SELECT r.*,
         DENSE_RANK() OVER (
           PARTITION BY dataset, check_id
           ORDER BY evaluated_at DESC, run_id DESC
         ) AS latest_rank
  FROM {{ dq_database }}.dq_check_result r
) ranked
WHERE latest_rank = 1;

-- Keys whose latest event is FLAGGED or STILL_FLAGGED, with the time the key
-- was first flagged in its current open spell (after its last CLEARED).
-- key_value is restricted and not exposed here.
CREATE OR REPLACE VIEW {{ dq_database }}.v_open_keys AS
SELECT dataset, check_id, key_hash, latest_event, state_detail,
       first_flagged_at, last_observed_at, run_id, evaluation_id
FROM (
  SELECT dataset, check_id, key_hash,
         event AS latest_event, state_detail, run_id, evaluation_id,
         observed_at AS last_observed_at,
         MIN(CASE WHEN event IN ('FLAGGED', 'STILL_FLAGGED') AND
                       (last_cleared_at IS NULL OR observed_at > last_cleared_at)
                  THEN observed_at END)
           OVER (PARTITION BY dataset, check_id, key_hash) AS first_flagged_at,
         ROW_NUMBER() OVER (
           PARTITION BY dataset, check_id, key_hash
           ORDER BY observed_at DESC, run_id DESC
         ) AS latest_rank
  FROM (
    SELECT e.*,
           MAX(CASE WHEN event = 'CLEARED' THEN observed_at END)
             OVER (PARTITION BY dataset, check_id, key_hash) AS last_cleared_at
    FROM {{ dq_database }}.dq_key_event e
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
WHERE latest_rank = 1
