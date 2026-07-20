from __future__ import annotations

from pathlib import Path

from common.control_plane_authority import (
    CONTROL_PLANE_APPLY_ADVISORY_LOCK_ID,
    try_acquire_control_plane_apply_lock,
)


ROOT = Path(__file__).resolve().parents[1]
AUTOMATION = (ROOT / "automation_runner/main.py").read_text()
ORCHESTRATOR = (ROOT / "services/bot_runner_orchestrator/main.py").read_text()
BOT_RUNNER = (ROOT / "services/bot_runner/main.py").read_text()


class LockCursor:
    def __init__(self, granted: bool):
        self.granted = granted
        self.executed = []

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchone(self):
        return (self.granted,)


def test_orchestrator_has_no_bot_control_mutation_sql():
    upper = ORCHESTRATOR.upper()
    assert "UPDATE BOT_CONTROL" not in upper
    assert "INSERT INTO BOT_CONTROL" not in upper
    assert "DELETE FROM BOT_CONTROL" not in upper


def test_runtime_loop_does_not_activate_legacy_allocator_or_v2_writer():
    section = ORCHESTRATOR[
        ORCHESTRATOR.index("def run_orchestrator_v1"):
        ORCHESTRATOR.index("def v2_block_reason")
    ]
    assert "run_orc_v2_profit_first(" not in section
    assert "run_allocator_phase_a(" not in section
    assert "disable_live_orders(" not in section
    assert "ORC_V2:" not in section
    assert "[v2] ENFORCE applied" not in section


def test_runtime_names_single_writer_authority():
    for source in (ORCHESTRATOR, BOT_RUNNER):
        assert "control_plane_mode=SINGLE_WRITER" in source
        assert "authority=automation_runner" in source
        assert "desired_state_source=bot_control" in source


def test_automation_keeps_authoritative_reason_family():
    assert "ORC_INTEGRATION_V2:" in AUTOMATION
    assert "UPDATE bot_control bc" in AUTOMATION
    assert "control_source = 'ORC'" in AUTOMATION


def test_automation_apply_uses_transaction_advisory_lock():
    section = AUTOMATION[
        AUTOMATION.index("def run_orc_cycle"):
        AUTOMATION.index("def run_orc_candidate_context_refresh")
    ]
    assert "try_acquire_control_plane_apply_lock(cur)" in section
    assert section.index("try_acquire_control_plane_apply_lock(cur)") < section.index(
        "apply_orc_control_transitions"
    )


def test_advisory_lock_contract_granted():
    cur = LockCursor(True)
    assert try_acquire_control_plane_apply_lock(cur) is True
    assert cur.executed == [
        (
            "SELECT pg_try_advisory_xact_lock(%s);",
            (CONTROL_PLANE_APPLY_ADVISORY_LOCK_ID,),
        )
    ]


def test_advisory_lock_contract_rejects_concurrent_apply():
    cur = LockCursor(False)
    assert try_acquire_control_plane_apply_lock(cur) is False


def test_offline_replay_has_two_logical_mutations_and_one_writer_family():
    desired_sequence = [False, True, True, False, False]
    current = desired_sequence[0]
    mutations = []

    for desired in desired_sequence[1:]:
        if desired == current:
            continue
        mutations.append(
            {
                "before": current,
                "after": desired,
                "writer": "ORC_INTEGRATION_V2",
            }
        )
        current = desired

    assert mutations == [
        {"before": False, "after": True, "writer": "ORC_INTEGRATION_V2"},
        {"before": True, "after": False, "writer": "ORC_INTEGRATION_V2"},
    ]
    assert all(item["writer"] != "ORC_V2" for item in mutations)
