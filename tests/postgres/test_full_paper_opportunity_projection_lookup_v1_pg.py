from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SOURCE = (
    ROOT
    / "db/migrations/20260828_full_paper_opportunity_projection_lookup_v1.sql"
).read_text()
MIGRATION = "\n".join(
    line for line in SOURCE.splitlines() if not line.lstrip().startswith("\\")
)


SCHEMA = """
CREATE TABLE entry_opportunity_evidence_v1(
  snapshot_id bigint PRIMARY KEY,
  decision_key text NOT NULL,
  captured_at timestamptz NOT NULL,
  evidence_payload_hash text NOT NULL
);
CREATE TABLE entry_trace_events(
  id bigint PRIMARY KEY,
  symbol text NOT NULL,
  interval text NOT NULL,
  strategy text NOT NULL,
  candle_open_time timestamptz NOT NULL,
  created_at timestamptz NOT NULL
);
"""


QUERY_A = """
SELECT e.* FROM entry_opportunity_evidence_v1 e
WHERE e.decision_key=%s
ORDER BY e.captured_at DESC LIMIT 1
"""


QUERY_B = """
SELECT t.id FROM entry_trace_events t
WHERE t.strategy=%s AND t.symbol=%s AND t.interval=%s
  AND t.candle_open_time=%s AND t.created_at<=%s
ORDER BY t.created_at DESC,t.id DESC LIMIT 1
"""


def apply_migration(cur):
    for statement in MIGRATION.split(";"):
        if statement.strip():
            cur.execute(statement)


def test_indexes_preserve_exact_rows_ordering_and_use_matching_plans(
    disposable_postgres_v16,
):
    name = "waltrade_baseline_test_full_opportunity_projection_lookup_v1"
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    at = datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
        cur.executemany(
            "INSERT INTO entry_opportunity_evidence_v1 VALUES(%s,%s,%s,%s)",
            [
                (1, "decision-a", at, "old"),
                (2, "decision-a", at + timedelta(seconds=1), "new"),
                (3, "decision-b", at + timedelta(seconds=2), "other"),
            ],
        )
        cur.executemany(
            "INSERT INTO entry_trace_events VALUES(%s,%s,%s,%s,%s,%s)",
            [
                (10, "BTCUSDC", "1m", "RSI", at, at + timedelta(seconds=1)),
                (11, "BTCUSDC", "1m", "RSI", at, at + timedelta(seconds=2)),
                (12, "BTCUSDC", "1m", "RSI", at, at + timedelta(seconds=3)),
                (13, "BTCUSDC", "1m", "RSI", at + timedelta(minutes=1), at),
            ],
        )
        cur.execute(QUERY_A, ("decision-a",))
        baseline_a = cur.fetchone()
        cur.execute(QUERY_B, ("RSI", "BTCUSDC", "1m", at, at + timedelta(seconds=2)))
        baseline_b = cur.fetchone()
    conn.commit()

    conn.autocommit = True
    with conn.cursor() as cur:
        apply_migration(cur)
        apply_migration(cur)
        cur.execute("ANALYZE entry_opportunity_evidence_v1")
        cur.execute("ANALYZE entry_trace_events")
        cur.execute(QUERY_A, ("decision-a",))
        assert cur.fetchone() == baseline_a
        cur.execute(QUERY_B, ("RSI", "BTCUSDC", "1m", at, at + timedelta(seconds=2)))
        assert cur.fetchone() == baseline_b == (11,)

        cur.execute("SET enable_seqscan=off")
        cur.execute("EXPLAIN " + QUERY_A, ("decision-a",))
        plan_a = "\n".join(row[0] for row in cur.fetchall())
        cur.execute(
            "EXPLAIN " + QUERY_B,
            ("RSI", "BTCUSDC", "1m", at, at + timedelta(seconds=2)),
        )
        plan_b = "\n".join(row[0] for row in cur.fetchall())
        assert "ix_entry_opportunity_evidence_decision_captured_v1" in plan_a
        assert "ix_entry_trace_events_opportunity_projection_v1" in plan_b

        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname='public' "
            "AND indexname LIKE 'ix_entry_%_v1' ORDER BY indexname"
        )
        assert {row[0] for row in cur.fetchall()} == {
            "ix_entry_opportunity_evidence_decision_captured_v1",
            "ix_entry_trace_events_opportunity_projection_v1",
        }
    conn.close()
