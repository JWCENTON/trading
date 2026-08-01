from pathlib import Path

import pytest

from common.entry_position_projection import (
    EntryPositionProjectionMode,
    entry_ack_requires_projection,
    run_entry_position_projection,
)


ROOT = Path(__file__).resolve().parents[1]


class NoDatabaseAccess:
    def cursor(self):  # pragma: no cover - failure documents the OFF contract
        raise AssertionError("OFF/UNSET must not open a database cursor")


def test_projection_mode_defaults_off_and_performs_zero_database_writes():
    assert EntryPositionProjectionMode.from_env({}) is EntryPositionProjectionMode.OFF
    stats = run_entry_position_projection(NoDatabaseAccess(), environment={})
    assert stats.mode is EntryPositionProjectionMode.OFF
    assert stats.scanned == 0
    assert not entry_ack_requires_projection({})


def test_projection_mode_is_strict_and_enforce_requires_lei1b_and_lei1c():
    with pytest.raises(
        ValueError, match="LIVE_ENTRY_POSITION_PROJECTION_MODE_INVALID"
    ):
        EntryPositionProjectionMode.from_env(
            {"LIVE_ENTRY_POSITION_PROJECTION_MODE": "AUTO"}
        )
    assert entry_ack_requires_projection(
        {"LIVE_ENTRY_POSITION_PROJECTION_MODE": "ENFORCE"}
    )
    with pytest.raises(
        RuntimeError, match="LEI1D_PREREQUISITE_FEATURES_NOT_ENFORCED"
    ):
        run_entry_position_projection(
            NoDatabaseAccess(),
            environment={"LIVE_ENTRY_POSITION_PROJECTION_MODE": "ENFORCE"},
        )


def test_integration_has_single_writer_selection_and_no_strategy_or_api_edits():
    ingest = (ROOT / "common/exchange_ingest_trades.py").read_text()
    execution = (ROOT / "common/execution.py").read_text()
    assert "if lei1d_mode is EntryPositionProjectionMode.ENFORCE" in ingest
    assert "run_entry_position_projection" in ingest
    assert "else:\n            # The due gate" in ingest
    assert "ENTRY_FILL_AWAITING_LEI1D_PROJECTION" in execution
    assert "api/main.py" not in "\n".join(
        ("common/entry_position_projection.py", "common/execution.py")
    )
