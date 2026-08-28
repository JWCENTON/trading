import hashlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "db/migrations/20260828_full_paper_opportunity_projection_lookup_v1.sql"
)
MIGRATION = MIGRATION_PATH.read_text()
MIGRATION_SHA = MIGRATION_PATH.with_suffix(".sha256").read_text().strip()


def normalized(value: str) -> str:
    return "".join(value.lower().split()).replace('"', "")


def test_migration_matches_both_exact_projection_access_paths():
    sql = normalized(MIGRATION)
    assert (
        "onpublic.entry_opportunity_evidence_v1("
        "decision_key,captured_atdesc)"
    ) in sql
    assert (
        "onpublic.entry_trace_events("
        "symbol,interval,strategy,candle_open_time,created_atdesc,iddesc)"
    ) in sql
    assert sql.count("createindexconcurrentlyifnotexists") == 2


def test_migration_is_index_only_and_preserves_projection_contracts():
    upper = MIGRATION.upper()
    for forbidden in (
        "INSERT INTO",
        "UPDATE ",
        "DELETE FROM",
        "ALTER TABLE",
        "DROP ",
        "PAPER_OPPORTUNITY_OBSERVATION_V1",
        "PAPER_OPPORTUNITY_OUTCOME_V1",
        "CANONICAL_FINANCIAL_TRUTH_V1",
    ):
        assert forbidden not in upper
    assert hashlib.sha256(MIGRATION_PATH.read_bytes()).hexdigest() == MIGRATION_SHA
