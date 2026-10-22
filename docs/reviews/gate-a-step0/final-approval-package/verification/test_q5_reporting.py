"""Q-5: reporting categories over key events after the encoding transition (a model over a temp view;
verification code only). Proves that active business discrepancies, unresolved legacy identities,
migration lineage and ambiguous transitions can be counted apart, that no age is reset, and that
lineage is never counted as a business defect."""

from __future__ import annotations

from datetime import datetime, timedelta

T0, T1 = datetime(2026, 1, 1), datetime(2026, 3, 1)
SCHEMA = "hash STRING, event STRING, detail STRING, enc INT, predecessor STRING, at TIMESTAMP"

# Legacy (encoding 1): L1 provable 1:1 (still wrong), L2 ambiguous (two successors, one still wrong),
# L3 provable 1:1 now correct, L4 gone. Then N+1's transition at T1.
EVENTS = [
    ("L1", "FLAGGED", "MISSING", None, None, T0), ("L2", "FLAGGED", "MISSING", None, None, T0),
    ("L3", "FLAGGED", "STALE", None, None, T0), ("L4", "FLAGGED", "MISSING", None, None, T0),
    ("L1", "REKEYED", "key encoding 2", 1, None, T1), ("S1", "STILL_FLAGGED", "MISSING", 2, "L1", T1 + timedelta(seconds=1)),
    ("L2", "STILL_FLAGGED", "awaiting successors", 1, None, T1),   # one event per identity per evaluation
    ("S2a", "FLAGGED", "MISSING", 2, "L2", T1 + timedelta(seconds=1)),
    ("L3", "REKEYED", "key encoding 2", 1, None, T1), ("S3", "CLEARED", "CURRENT", 2, "L3", T1 + timedelta(seconds=1)),
    ("L4", "CLEARED", "no longer upstream", 1, None, T1),
]

CATEGORIES = """
WITH ev AS (SELECT * FROM q5_events),
latest AS (SELECT * FROM (SELECT ev.*, ROW_NUMBER() OVER (PARTITION BY hash ORDER BY at DESC) rn FROM ev) WHERE rn = 1),
spell AS (SELECT hash, MIN(CASE WHEN event IN ('FLAGGED', 'STILL_FLAGGED') THEN at END) AS first_flagged FROM ev GROUP BY hash),
open_ AS (SELECT l.*, s.first_flagged FROM latest l JOIN spell s USING (hash) WHERE l.event IN ('FLAGGED', 'STILL_FLAGGED')),
continued AS (SELECT DISTINCT hash FROM ev WHERE event = 'REKEYED' AND detail = 'key encoding 2')
SELECT o.hash,
       CASE WHEN o.detail = 'awaiting successors' THEN 'legacy_unresolved' ELSE 'active' END AS category,  -- A-9r
       CASE WHEN o.predecessor IN (SELECT hash FROM continued) THEN p.first_flagged ELSE o.first_flagged END AS age_from
FROM open_ o LEFT JOIN spell p ON p.hash = o.predecessor
"""


def setup(spark):
    spark.createDataFrame(EVENTS, SCHEMA).createOrReplaceTempView("q5_events")


def test_active_discrepancies_and_unresolved_legacy_identities_are_counted_apart(spark) -> None:
    setup(spark)
    rows = {r.hash: (r.category, r.age_from) for r in spark.sql(CATEGORIES).collect()}
    assert rows == {"S1": ("active", T0),               # provable continuation: age kept from T0
                    "S2a": ("active", T1 + timedelta(seconds=1)),  # ambiguous successor: its own age
                    "L2": ("legacy_unresolved", T0)}    # the original discrepancy, age kept, not closed


LINEAGE = """
SELECT e.predecessor AS legacy, e.hash AS successor,
       CASE WHEN e.predecessor IN (SELECT hash FROM q5_events WHERE event = 'REKEYED') THEN 'continuation'
            ELSE 'ambiguous' END AS kind
FROM q5_events e WHERE e.predecessor IS NOT NULL
"""


def test_lineage_and_ambiguous_transitions_are_records_not_defects(spark) -> None:
    setup(spark)
    links = sorted((r.legacy, r.successor, r.kind) for r in spark.sql(LINEAGE).collect())
    assert links == [("L1", "S1", "continuation"), ("L2", "S2a", "ambiguous"), ("L3", "S3", "continuation")]
    active = {r.hash for r in spark.sql(CATEGORIES).collect() if r.category == "active"}
    rekeyed = {r.hash for r in spark.sql("SELECT hash FROM q5_events WHERE event = 'REKEYED'").collect()}
    assert not active & rekeyed                         # no lineage record is an active defect


def test_at_most_one_event_per_identity_per_evaluation(spark) -> None:
    """Two events of one hash in one evaluation share observed_at and run_id, the keys v_open_keys orders by:
    the latest would be arbitrary. The transition writes one event per identity."""
    setup(spark)
    dup = spark.sql("SELECT hash, at, COUNT(*) n FROM q5_events GROUP BY hash, at HAVING COUNT(*) > 1").collect()
    assert dup == []


def test_no_discrepancy_open_before_the_transition_is_lost(spark) -> None:
    """L1 -> S1 (open), L2 (open itself, with S2a), L3 -> S3 cleared because current, L4 cleared: gone upstream."""
    setup(spark)
    open_now = {r.hash for r in spark.sql(CATEGORIES).collect()}
    assert open_now == {"S1", "S2a", "L2"}
