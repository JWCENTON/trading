import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from common.trend_candle_snapshot import load_snapshot, snapshot_atr

SOURCE = Path('bot_trend/main.py').read_text()


class DB:
    def __init__(self, rows):
        self.rows = rows
        self.closed = False
    def cursor(self): return self
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, sql, params):
        assert 'close_time < %s' in sql
        self.selected = sorted((r for r in self.rows if r[8] < params[2]),
                               key=lambda r:r[2], reverse=True)[:params[3]]
    def fetchall(self): return self.selected
    def close(self): self.closed = True


@pytest.mark.parametrize('minutes',[1,5])
@pytest.mark.parametrize('clock',['00:05:57.202189','02:54:19.156965','03:00:52.075563',
                                  '03:02:54.390415','03:08:27.301142'])
def test_actual_incident_times_exclude_open_candle_and_freeze_evaluator_input(minutes, clock):
    at = datetime.fromisoformat('2026-09-10T'+clock+'+00:00')
    current = at.replace(minute=at.minute//minutes*minutes,second=0,microsecond=0)
    rows=[]
    for i in range(102):
        op=current-timedelta(minutes=i*minutes)
        price=999999 if i==0 else 100+i/100
        rows.append(('BTCUSDC',f'{minutes}m',op,price,100,50,price+1,price-1,
                     op+timedelta(minutes=minutes,microseconds=-1),i))
    db=DB(rows)
    frozen=load_snapshot(lambda:db,'BTCUSDC',f'{minutes}m',at,100)
    assert db.closed and frozen.source_id==1
    assert all(r[8]<at for r in frozen.rows)
    assert snapshot_atr(frozen)==2
    # Execute the real evaluator through its indicator calculations. No DB
    # reread may occur; stop before control/position/execution side effects.
    fn=next(n for n in ast.parse(SOURCE).body if isinstance(n,ast.FunctionDef) and n.name=='_run_trend_strategy')
    class Done(Exception): pass
    observed={}
    def snapshot(**kw):
        observed.update(kw)
        raise Done
    def forbidden(**kw): raise AssertionError('unexpected source reread')
    env=dict(datetime=datetime,timezone=timezone,pd=pd,EMA_SLOW=30,EMA_FAST=10,
             get_latest_candles=forbidden,get_runtime_snapshot=snapshot,
             logging=SimpleNamespace(info=lambda *a:None),TREND_SOURCE_VERSION='test',
             TREND_SOURCE_FINGERPRINT='test')
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'real_trend','exec'),env)
    with pytest.raises(Done): env['_run_trend_strategy'](frozen)
    assert observed=={'price':100.01,'open_time':frozen.open_time}


def test_loop_and_evaluator_share_snapshot_and_live_branch_is_preserved():
    assert '_final_decision = run_trend_strategy(final_snapshot)' in SOURCE
    assert '_run_trend_strategy(candle_snapshot)' in SOURCE
    assert 'rows = candle_snapshot.rows if candle_snapshot is not None else get_latest_candles' in SOURCE
    assert 'snapshot_atr(candle_snapshot) if candle_snapshot is not None' in SOURCE
    fn=next(n for n in ast.parse(SOURCE).body if isinstance(n,ast.FunctionDef) and n.name=='_local_paper_final_source_enabled')
    import os
    from unittest.mock import patch
    for mode,deployment,expected in [('PAPER','local-paper',True),('LIVE','local-paper',False),('PAPER','vps-paper',False)]:
        env={'cfg':SimpleNamespace(trading_mode=mode),'os':os}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'scope','exec'),env)
        with patch.dict(os.environ,{'DEPLOYMENT_ID':deployment}):
            assert env['_local_paper_final_source_enabled']() is expected
