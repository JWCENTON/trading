import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "automation_runner/main.py"
MIGRATION = ROOT / "db/migrations/20260710_learning_feedback_engine_v1_4_shadow_confidence.sql"

RUNNER_SOURCE = RUNNER.read_text(encoding="utf-8")
SQL = MIGRATION.read_text(encoding="utf-8")


def learning_runner_section():
    start = RUNNER_SOURCE.index("def run_learning_feedback_engine_refresh")
    end = RUNNER_SOURCE.index("def run_market_regime_confidence_refresh")
    return RUNNER_SOURCE[start:end]


class LearningEngineObservabilityContractTests(unittest.TestCase):
    def test_scheduler_and_engine_versions_are_reported_separately(self):
        self.assertIn(
            'LEARNING_FEEDBACK_SCHEDULER_VERSION = "LEARNING_FEEDBACK_SCHEDULER_V1_2"',
            RUNNER_SOURCE,
        )
        self.assertIn(
            'LEARNING_FEEDBACK_SOURCE_ENGINE_VERSION = "LEARNING_FEEDBACK_ENGINE_V1_2"',
            RUNNER_SOURCE,
        )
        self.assertIn(
            'LEARNING_SHADOW_CONFIDENCE_ENGINE_VERSION = "LEARNING_ENGINE_V1_4"',
            RUNNER_SOURCE,
        )
        self.assertIn("learning_feedback_engine_runner_scheduler_version", RUNNER_SOURCE)
        self.assertIn("learning_feedback_engine_runner_engine_version", RUNNER_SOURCE)
        self.assertIn("learning_feedback_engine_runner_source_refresh_engine_version", RUNNER_SOURCE)
        self.assertIn("learning_feedback_engine_runner_last_success_at", RUNNER_SOURCE)
        self.assertIn("learning_feedback_engine_runner_next_due_at", RUNNER_SOURCE)

    def test_engine_mode_is_shadow_and_apply_remains_false(self):
        self.assertIn('LEARNING_SHADOW_CONFIDENCE_ENGINE_MODE = "SHADOW"', RUNNER_SOURCE)
        self.assertIn("LEARNING_SHADOW_CONFIDENCE_APPLY_ENABLED = False", RUNNER_SOURCE)
        self.assertIn("learning_feedback_engine_runner_engine_mode", RUNNER_SOURCE)
        self.assertIn("learning_feedback_engine_runner_apply_enabled", RUNNER_SOURCE)
        self.assertIn("'mode', 'SHADOW_ONLY'", SQL)
        self.assertIn("'apply_enabled', false", SQL)

    def test_due_gate_still_prevents_refresh_when_not_due(self):
        section = learning_runner_section()
        not_due_idx = section.index("if not is_due:")
        return_idx = section.index("return", not_due_idx)
        v12_call_idx = section.index("SELECT refresh_learning_feedback_engine_v1_2_if_due")
        v14_call_idx = section.index("run_learning_shadow_confidence_v14")
        self.assertLess(return_idx, v12_call_idx)
        self.assertLess(return_idx, v14_call_idx)
        self.assertIn('"status=not_due last_success_at=%s next_due_at=%s"', section)

    def test_not_due_stats_do_not_create_proposals(self):
        section = learning_runner_section()
        not_due_block = section[
            section.index("if not is_due:"):section.index('cur.execute(\n            "SET LOCAL statement_timeout')
        ]
        self.assertIn('"not_due"', not_due_block)
        self.assertNotIn("refresh_learning_shadow_confidence_proposals_v1_4", not_due_block)
        self.assertNotIn("learning_shadow_confidence_proposals_v1", not_due_block)

    def test_refresh_idempotency_is_preserved(self):
        self.assertIn("SOURCE_RUN_ALREADY_PROCESSED", SQL)
        self.assertIn("ux_learning_shadow_confidence_run_source", SQL)
        self.assertIn("ON CONFLICT (proposal_key) DO UPDATE", SQL)

    def test_existing_proposals_evidence_and_audit_are_not_deleted(self):
        self.assertNotRegex(SQL, r"\bDELETE\s+FROM\s+learning_shadow_confidence_proposals_v1\b")
        self.assertIn("status = 'SUPERSEDED'", SQL)
        self.assertIn("evidence = EXCLUDED.evidence", SQL)
        self.assertIn("v_learning_shadow_confidence_safety_audit_v1", SQL)

    def test_learning_section_has_no_runtime_trading_writes(self):
        section = learning_runner_section()
        forbidden = (
            "bot_control", "strategy_params", "runtime_params",
            "allocation_policy", "positions", "binance_orders",
            "binance_order_fills", "simulated_orders",
        )
        for table in forbidden:
            pattern = rf"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO)\s+{table}\b"
            self.assertIsNone(re.search(pattern, section, re.I), table)
            self.assertIsNone(re.search(pattern, SQL, re.I), table)

    def test_orc_and_bot_control_behavior_are_not_changed_by_learning_patch(self):
        section = learning_runner_section()
        self.assertNotIn("ORC_PICKS_VIEW", section)
        self.assertNotIn("apply_bot_control", section)
        self.assertNotRegex(section, r"\bUPDATE\s+bot_control\b", re.I)


if __name__ == "__main__":
    unittest.main()
