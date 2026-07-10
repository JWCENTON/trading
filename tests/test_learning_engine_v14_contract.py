import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "db/migrations/20260710_learning_feedback_engine_v1_4_shadow_confidence.sql"
SQL = MIGRATION.read_text(encoding="utf-8")


def expected_delta(action: str, decisions: int, confidence: float):
    if action not in {"INCREASE_CONFIDENCE", "REDUCE_CONFIDENCE"}:
        return None
    sample = 0.03 if decisions >= 100 else 0.02 if decisions >= 50 else 0.01 if decisions >= 30 else 0.0
    cap = 0.03 if confidence >= 0.85 else 0.02 if confidence >= 0.70 else 0.01
    value = min(sample, cap, 0.05)
    return value if action == "INCREASE_CONFIDENCE" else -value


class LearningEngineV14ContractTests(unittest.TestCase):
    def test_zero_stable_is_success_without_placeholder(self):
        self.assertIn("'status', 'ok'", SQL)
        self.assertIn("'stable_inputs', v_stable_inputs", SQL)
        self.assertNotIn("PLACEHOLDER", SQL.upper())

    def test_increase_delta(self):
        self.assertEqual(expected_delta("INCREASE_CONFIDENCE", 35, 0.65), 0.01)
        self.assertIn("WHEN 'INCREASE_CONFIDENCE' THEN v_unsigned_delta", SQL)

    def test_reduce_delta(self):
        self.assertEqual(expected_delta("REDUCE_CONFIDENCE", 75, 0.78), -0.02)

    def test_confidence_cap(self):
        self.assertEqual(expected_delta("INCREASE_CONFIDENCE", 500, 0.65), 0.01)

    def test_idempotency_contract(self):
        self.assertIn("SOURCE_RUN_ALREADY_PROCESSED", SQL)
        self.assertIn("ON CONFLICT (proposal_key) DO UPDATE", SQL)
        self.assertIn("ux_learning_shadow_confidence_run_source", SQL)

    def test_superseding_contract(self):
        self.assertIn("status = 'SUPERSEDED'", SQL)
        self.assertIn("superseded_by_key = v_proposal_key", SQL)
        self.assertIn("v_active.proposal_key = v_proposal_key", SQL)

    def test_unsupported_actions_are_skipped(self):
        for action in ("OBSERVE", "PROMOTE_CANDIDATE", "BLOCK_CANDIDATE"):
            self.assertIsNone(expected_delta(action, 500, 0.99))
        action_constraint = re.search(
            r"ck_learning_shadow_confidence_action.*?CHECK\s*\((.*?)\)",
            SQL,
            re.S,
        ).group(1)
        self.assertNotIn("OBSERVE", action_constraint)
        self.assertNotIn("PROMOTE_CANDIDATE", action_constraint)
        self.assertNotIn("BLOCK_CANDIDATE", action_constraint)

    def test_no_runtime_table_writes(self):
        forbidden = (
            "bot_control", "strategy_params", "runtime_params",
            "allocation_policy", "positions", "orders", "fills",
        )
        for table in forbidden:
            pattern = rf"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+{table}\b"
            self.assertIsNone(re.search(pattern, SQL, re.I), table)

    def test_delta_range_is_constrained(self):
        self.assertIn(
            "CHECK (proposed_delta >= -0.05 AND proposed_delta <= 0.05)",
            SQL,
        )
        for action in ("INCREASE_CONFIDENCE", "REDUCE_CONFIDENCE"):
            for decisions in (30, 49, 50, 99, 100, 500):
                for confidence in (0.0, 0.69, 0.70, 0.849999, 0.85, 1.0):
                    self.assertLessEqual(abs(expected_delta(action, decisions, confidence)), 0.05)

    def test_one_active_per_full_slot(self):
        self.assertRegex(
            SQL,
            r"ux_learning_shadow_confidence_one_active_slot[\s\S]*?"
            r"environment, symbol, interval, strategy, window_days[\s\S]*?"
            r"WHERE status = 'ACTIVE'",
        )


if __name__ == "__main__":
    unittest.main()
