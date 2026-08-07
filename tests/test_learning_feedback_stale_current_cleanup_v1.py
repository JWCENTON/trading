from pathlib import Path


SQL = Path(
    "db/migrations/20260807_learning_feedback_stale_current_cleanup_v1.sql"
).read_text()


def test_cleanup_uses_canonical_learning_universe():
    assert "learning_canonical_evidence_universe_v1" in SQL
    assert "eligibility_reason = 'ELIGIBLE'" in SQL


def test_cleanup_is_environment_and_window_scoped():
    assert "p_environment" in SQL
    assert "p_window_days" in SQL


def test_cleanup_removes_stale_current_statistics():
    assert "DELETE FROM public.learning_slot_statistics_v1" in SQL


def test_cleanup_preserves_frozen_snapshot_isolation():
    assert "waltrade.learning_source_snapshot_token" in SQL
    assert "source_snapshot_token IS NOT DISTINCT FROM v_snapshot_token" in SQL


def test_cleanup_does_not_delete_applied_or_rejected_history():
    delete_section = SQL.split(
        "DELETE FROM public.learning_calibration_proposals_v1", 1
    )[1].split("GET DIAGNOSTICS", 1)[0]

    assert "'PENDING'" in delete_section
    assert "'VALIDATING'" in delete_section
    assert "'EXPIRED'" in delete_section
    assert "'APPLIED'" not in delete_section
    assert "'REJECTED'" not in delete_section


def test_cleanup_resets_live_stale_validation_state():
    assert "IF v_snapshot_token IS NULL THEN" in SQL
    assert "UPDATE public.learning_proposal_validation_state_v1" in SQL
    assert "validation_status = 'RESET'" in SQL


def test_cleanup_is_automatically_hooked_into_base_refresh():
    assert "refresh_learning_feedback_engine_v1(integer,integer,integer)" in SQL
    assert "GET DIAGNOSTICS v_stats_upserted = ROW_COUNT;" in SQL
    assert (
        "PERFORM public.cleanup_learning_feedback_stale_current_state_v1("
        in SQL
    )


def test_cleanup_is_injected_immediately_after_stats_upsert():
    assert (
        "v_anchor || E'\\n\\n'"
        in SQL
    )
    assert (
        "'    PERFORM public.cleanup_learning_feedback_stale_current_state_v1('"
        in SQL
    )
    assert (
        "v_anchor TEXT :=\n"
        "        'GET DIAGNOSTICS v_stats_upserted = ROW_COUNT;'"
        in SQL
    )


def test_cleanup_does_not_mutate_trading_or_financial_truth():
    forbidden = [
        "UPDATE positions",
        "DELETE FROM positions",
        "UPDATE canonical_financial_truth_v1",
        "DELETE FROM canonical_financial_truth_v1",
        "UPDATE bot_control",
    ]

    for marker in forbidden:
        assert marker not in SQL
