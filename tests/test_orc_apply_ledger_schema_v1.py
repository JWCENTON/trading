from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (ROOT / "db/migrations/20260720_orc_immutable_apply_ledger_v1.sql").read_text()
UPGRADE = (ROOT / "db/migrations/20260720_orc_immutable_apply_ledger_v1_1_counter_semantics.sql").read_text()
V12 = (ROOT / "db/migrations/20260720_orc_immutable_apply_ledger_v1_2_observe_only.sql").read_text()
BOT_CONTROL_PARITY = (ROOT / "db/migrations/20260720_bot_control_transition_timestamps_v1.sql").read_text()
BOT_CONTROL_PARITY_PG = (ROOT / "tests/postgres/bot_control_transition_timestamps_v1.sql").read_text()
AUTOMATION = (ROOT / "automation_runner/main.py").read_text()
WRITER = (ROOT / "common/orc_apply_ledger.py").read_text()
AUTOMATION_DOCKERFILE = (ROOT / "automation_runner/Dockerfile").read_text()
COMPOSE = (ROOT / "docker-compose.yaml").read_text()


def test_schema_is_append_only_and_duplicate_safe():
    assert "PRIMARY KEY (deployment_id, environment, run_id)" in MIGRATION
    assert "PRIMARY KEY (deployment_id, environment, run_id, slot_key)" in MIGRATION
    assert "BEFORE UPDATE OR DELETE ON orc_apply_runs_v1" in MIGRATION
    assert "BEFORE UPDATE OR DELETE ON orc_apply_slot_decisions_v1" in MIGRATION
    assert "DEFERRABLE INITIALLY DEFERRED" in MIGRATION
    assert "source_candidate_count INTEGER NOT NULL" in MIGRATION
    assert "slot_decision_count INTEGER NOT NULL" in MIGRATION
    assert "source_excluded_count INTEGER NOT NULL" in MIGRATION


def test_additive_upgrade_preserves_legacy_rows_without_update_or_delete():
    assert "ADD COLUMN IF NOT EXISTS source_candidate_count" in UPGRADE
    assert "ADD COLUMN IF NOT EXISTS slot_decision_count" in UPGRADE
    assert "ADD COLUMN IF NOT EXISTS source_excluded_count" in UPGRADE
    assert "ALTER COLUMN schema_version SET DEFAULT 'ORC_APPLY_LEDGER_V1_1'" in UPGRADE
    statements = [line.strip().upper() for line in UPGRADE.splitlines()]
    assert not any(line.startswith("UPDATE ") for line in statements)
    assert not any(line.startswith("DELETE ") for line in statements)


def test_writer_metadata_is_build_immutable_and_new_runs_receive_it():
    assert 'WRITER_VERSION = "ORC_APPLY_WRITER_V1_3"' in WRITER
    assert "GIT_SHA_PATTERN" in WRITER
    assert 'os.getenv("COMMIT_SHA")' not in WRITER
    assert 'os.getenv("ORC_WRITER_VERSION")' not in WRITER
    assert 'ARG GIT_SHA' in AUTOMATION_DOCKERFILE
    assert 'org.opencontainers.image.revision="${GIT_SHA}"' in AUTOMATION_DOCKERFILE
    assert 'ENV GIT_SHA="${GIT_SHA}"' in AUTOMATION_DOCKERFILE
    automation_build = COMPOSE[
        COMPOSE.index("  automation-runner:"):
        COMPOSE.index("  bot-rsi-btc:")
    ]
    assert "GIT_SHA: ${GIT_SHA}" in automation_build
    committed_insert = AUTOMATION[
        AUTOMATION.index("def run_orc_cycle"):
        AUTOMATION.index("def run_orc_candidate_context_refresh")
    ]
    assert "identity.version,identity.git_sha" in committed_insert
    diagnostic_insert = AUTOMATION[
        AUTOMATION.index("def _with_orc_apply_failure_ledger"):
        AUTOMATION.index("def _learning_feedback_runner_stats")
    ]
    assert "identity.version" in diagnostic_insert
    assert "identity.git_sha" in diagnostic_insert


def test_legacy_null_metadata_rows_remain_valid_without_backfill():
    assert "writer_version TEXT," in MIGRATION
    assert "git_sha TEXT," in MIGRATION
    for upgrade in (UPGRADE, V12):
        assert "UPDATE orc_apply_runs_v1" not in upgrade
        assert "writer_version SET NOT NULL" not in upgrade
        assert "git_sha SET NOT NULL" not in upgrade


def test_control_mutation_and_both_ledgers_share_existing_transaction():
    section = AUTOMATION[
        AUTOMATION.index("def run_orc_cycle"):
        AUTOMATION.index("def run_orc_candidate_context_refresh")
    ]
    slot_insert = section.index("persist_orc_slot_ledger")
    first_validation = section.index("validate_slot_counts")
    second_validation = section.index("validate_slot_counts", first_validation + 1)
    control_update = section.index("apply_orc_control_transitions")
    run_insert = section.index("INSERT INTO orc_apply_runs_v1")
    commit = section.index("conn.commit()")
    assert first_validation < slot_insert < second_validation < control_update < run_insert < commit
    assert section.count("conn.commit()") == 1


def test_failure_paths_are_fail_closed_by_transaction_structure():
    section = AUTOMATION[
        AUTOMATION.index("def run_orc_cycle"):
        AUTOMATION.index("def run_orc_candidate_context_refresh")
    ]
    # No swallowed ledger exception between evidence preparation and commit.
    atomic_section = section[
        section.index("persist_orc_slot_ledger"):section.index("conn.commit()")
    ]
    assert "except" not in atomic_section
    assert section.index("persist_orc_slot_ledger") < section.index("apply_orc_control_transitions")
    assert section.index("INSERT INTO orc_apply_runs_v1") < section.index("conn.commit()")
    wrapper = AUTOMATION[
        AUTOMATION.index("def _with_orc_apply_failure_ledger"):
        AUTOMATION.index("def _learning_feedback_runner_stats")
    ]
    assert wrapper.index("conn.rollback()") < wrapper.index("'ROLLED_BACK'")
    assert "ROLLBACK_EVIDENCE_FAILED" in wrapper
    assert '"error_classification"' in wrapper


def test_mismatch_rolls_back_bot_control_and_writes_diagnostic_header():
    wrapper = AUTOMATION[
        AUTOMATION.index("def _with_orc_apply_failure_ledger"):
        AUTOMATION.index("def _learning_feedback_runner_stats")
    ]
    assert wrapper.index("conn.rollback()") < wrapper.index("INSERT INTO orc_apply_runs_v1")
    assert "LEDGER_SLOT_COUNT_MISMATCH" in (ROOT / "common/orc_apply_ledger.py").read_text()
    assert "'ROLLED_BACK'" in wrapper


def test_duplicate_retry_is_a_no_op_before_source_or_control_mutation():
    section = AUTOMATION[
        AUTOMATION.index("def run_orc_cycle"):
        AUTOMATION.index("def run_orc_candidate_context_refresh")
    ]
    duplicate_lookup = section.index("FROM orc_apply_runs_v1")
    duplicate_return = section.index("idempotent replay skipped")
    source_read = section.index('cur.execute(f"SELECT * FROM {active_picks_view}")')
    control_mutation_call = section.index("apply_orc_control_transitions")
    assert duplicate_lookup < duplicate_return < source_read < control_mutation_call


def test_observability_logs_run_identity_without_slot_payloads():
    assert '"run_id": str(run_id)' in AUTOMATION
    assert "outcome=COMMITTED execution_mode=%s duration_ms=%s" in AUTOMATION
    assert "snapshot_json" not in AUTOMATION[AUTOMATION.index("logging.info(", AUTOMATION.index("def run_orc_cycle")):AUTOMATION.index("def run_orc_candidate_context_refresh")]


def test_learning_auto_apply_was_not_enabled_by_change():
    assert "CAUSAL_LEARNING_AUTO_APPLY" not in MIGRATION
    assert "learning_feedback" not in MIGRATION.lower()


def test_v12_is_additive_idempotent_and_preserves_history():
    assert "ADD COLUMN IF NOT EXISTS execution_mode" in V12
    assert "ADD COLUMN IF NOT EXISTS decision_effect" in V12
    assert "LEGACY_APPLY" in V12 and "LEGACY" in V12
    statements = [line.strip().upper() for line in V12.splitlines()]
    assert not any(line.startswith("UPDATE ") for line in statements)
    assert not any(line.startswith("DELETE ") for line in statements)


def test_v12_schema_enforces_observe_only_effects():
    assert "execution_mode IN ('APPLY','OBSERVE_ONLY')" in V12
    assert "resulting_live_on_count=previous_live_on_count" in V12
    assert "touched_on_count=0 AND touched_off_count=0" in V12
    assert "WOULD_ENABLE" in V12 and "WOULD_DISABLE" in V12
    assert "source_candidate_count>=candidate_universe_count" in V12
    assert "candidate_universe_count=slot_decision_count" in V12


def test_observe_only_has_no_control_order_cancel_or_fill_path():
    helper = AUTOMATION[
        AUTOMATION.index("def apply_orc_control_transitions"):
        AUTOMATION.index("@_with_orc_apply_failure_ledger", AUTOMATION.index("def apply_orc_control_transitions"))
    ]
    observe_branch = helper[:helper.index("return attempted_writes") + len("return attempted_writes")]
    assert "UPDATE bot_control" not in observe_branch
    assert "attempted_writes == 0" in observe_branch
    assert all(word not in observe_branch.lower() for word in ("order", "cancel", "fill"))


def test_paper_scheduler_flag_is_independent_of_disable_only():
    assert 'ORC_LEDGER_OBSERVE_ONLY_ENABLED = _env_bool' in AUTOMATION
    assert 'cfg.trading_mode == "PAPER" and ORC_LEDGER_OBSERVE_ONLY_ENABLED' in AUTOMATION


def test_bot_control_transition_timestamp_migration_is_additive_only():
    assert "ADD COLUMN IF NOT EXISTS live_since TIMESTAMPTZ" in BOT_CONTROL_PARITY
    assert "ADD COLUMN IF NOT EXISTS last_disabled_at TIMESTAMPTZ" in BOT_CONTROL_PARITY
    upper = BOT_CONTROL_PARITY.upper()
    assert " UPDATE " not in f" {upper} "
    assert " DELETE " not in f" {upper} "
    assert " DEFAULT " not in f" {upper} "
    assert "NOT NULL" not in upper
    assert "IDX_BOT_CONTROL_SLOT" not in upper


def test_bot_control_transition_timestamp_postgres_harness_covers_contract():
    assert "generate_series(1, 32)" in BOT_CONTROL_PARITY_PG
    assert BOT_CONTROL_PARITY_PG.count(
        "20260720_bot_control_transition_timestamps_v1.sql"
    ) == 6
    assert "PAPER-like rows were not preserved" in BOT_CONTROL_PARITY_PG
    assert "PAPER-like rows were backfilled" in BOT_CONTROL_PARITY_PG
    assert "LIVE-like timestamp values were not preserved" in BOT_CONTROL_PARITY_PG
    assert "fresh schema transition columns missing" in BOT_CONTROL_PARITY_PG
    assert "bot_control_audit_trg" in BOT_CONTROL_PARITY_PG
    assert "idx_bot_control_lookup" in BOT_CONTROL_PARITY_PG
    assert "idx_bot_control_slot" in BOT_CONTROL_PARITY_PG


def test_north_star_scheduler_policy_keeps_paper_default_off_and_live_apply():
    assert '"ORC_LEDGER_OBSERVE_ONLY_ENABLED", "0"' in AUTOMATION
    loop = AUTOMATION[
        AUTOMATION.index("# PAPER only: publish promotions to LIVE"):
        AUTOMATION.index('logging.info("tick ok")')
    ]
    assert 'cfg.trading_mode == "LIVE" and mode != "DISABLE_ONLY"' in loop
    assert 'cfg.trading_mode == "PAPER" and ORC_LEDGER_OBSERVE_ONLY_ENABLED' in loop
    assert "run_orc_cycle(conn)" in loop
    assert "publish_promotions(conn)" in loop


def test_paper_optional_observation_requires_execution_flags_off():
    cycle = AUTOMATION[
        AUTOMATION.index("def run_orc_cycle"):
        AUTOMATION.index("if execution_mode is None:", AUTOMATION.index("def run_orc_cycle"))
    ]
    assert 'parse_required_execution_guard(\n                "LIVE_ORDERS_ENABLED"' in cycle
    assert 'parse_required_execution_guard(\n                "OKX_EXECUTION_ENABLED"' in cycle
    helper = (ROOT / "common/orc_apply_ledger.py").read_text()
    assert "if not observe_only_enabled:\n        return None" in helper
    assert "PAPER OBSERVE_ONLY requires LIVE_ORDERS_ENABLED=0" in helper
