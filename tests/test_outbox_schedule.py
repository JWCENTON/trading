from pathlib import Path
from unittest.mock import Mock

import pytest

from common.outbox_schedule import OutboxSchedule, local_paper_schedule


@pytest.mark.parametrize('deployment,db,mode,expected', [
    ('local-paper','trading_paper','PAPER',True),
    ('local-live','trading_live','LIVE',False),
    ('vps-paper','trading_paper','PAPER',False),
    ('vps-live','trading_live','LIVE',False),
    ('local-paper','trading_live','PAPER',False),
])
def test_local_only(deployment,db,mode,expected):
    assert local_paper_schedule({'DEPLOYMENT_ID':deployment,'DB_NAME':db},mode) is expected


@pytest.mark.parametrize('fails', [False, True])
def test_one_batch_then_full_cooldown_even_on_failure(fails):
    poll=Mock(side_effect=RuntimeError('safe test') if fails else None,return_value=100)
    s=OutboxSchedule(poll)
    s.stop=Mock()
    s.stop.is_set.return_value=False
    s.stop.wait.return_value=True
    s.run()
    poll.assert_called_once_with(max_duration_seconds=15)
    s.stop.wait.assert_called_once_with(60)


def test_start_once_and_no_parallel_legacy_schedule(monkeypatch):
    thread=Mock()
    monkeypatch.setattr('common.outbox_schedule.threading.Thread',thread)
    s=OutboxSchedule(Mock())
    s.start();s.start()
    assert thread.call_count==1
    source=(Path(__file__).resolve().parents[1]/'automation_runner/main.py').read_text()
    assert 'if not independent_outbox:\n                    causal_processed' in source


def test_capacity_at_full_budget_has_drain_margin():
    # This is a service envelope, not proof of real projection cost.
    service=100*60/(60+15)
    assert service==80 and service>23.8*3
