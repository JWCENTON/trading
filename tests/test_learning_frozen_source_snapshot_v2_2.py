from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "db/migrations/20260804_learning_frozen_source_snapshot_v2_2_generation_scoped_projection.sql"
).read_text()
PG_TEST = (
    ROOT
    / "tests/postgres/test_learning_frozen_source_snapshot_v2_2_generation_scope_pg.py"
).read_text()


def test_generation_scopes_all_mutable_projection_reads():
    for contract in (
        "LEARNING_FROZEN_SOURCE_V2_2_RECLASSIFY_ANCHOR_CONFLICT",
        "LEARNING_FROZEN_SOURCE_V2_2_DELETE_ANCHOR_CONFLICT",
        "LEARNING_FROZEN_SOURCE_V2_2_V1_CANDIDATE_ANCHOR_CONFLICT",
        "LEARNING_FROZEN_SOURCE_V2_2_V1_1_CANDIDATE_ANCHOR_CONFLICT",
        "LEARNING_FROZEN_SOURCE_V2_2_OBSERVATION_ANCHOR_CONFLICT",
        "s.source_snapshot_token = o.source_snapshot_token",
    ):
        assert contract in MIGRATION


def test_fail_closed_and_history_contract_are_not_weakened():
    assert "LEARNING_FROZEN_SOURCE_PAYLOAD_CONFLICT" in MIGRATION
    for forbidden in (
        "DISABLE TRIGGER",
        "DROP TRIGGER",
        "UPDATE learning_canonical_source_snapshots_v2",
        "DELETE FROM learning_slot_statistics_v1",
        "ON CONFLICT DO UPDATE",
        "bot_control",
        "TREATMENT",
    ):
        assert forbidden not in MIGRATION


def test_regression_uses_production_v1_1_and_all_universe_shapes():
    for source in (
        "20260710_learning_feedback_engine_v1.sql",
        "20260710_learning_feedback_engine_v1_1.sql",
        "20260710_learning_feedback_engine_v1_3_validation.sql",
    ):
        assert source in PG_TEST
    for contract in (
        "generation_scoped",
        "shrink_same_grow_and_reappearing",
        "wrong_explicit_token_remains_fail_closed",
        "migration_ledger_is_exactly_once",
    ):
        assert contract in PG_TEST
