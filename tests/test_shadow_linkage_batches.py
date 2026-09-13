import json
import subprocess
from unittest.mock import patch

import pytest
from scripts import l3_market_context_shadow_v1 as s


def test_all_pages_revisited_and_disjoint_counts(tmp_path):
    db = s.open_store(tmp_path)
    def scan():
        pages = iter([[dict(id=i, created_at='2026-09-09T12:10:00Z',
                            decision='ENTRY_CHECK', why='POLICY_ALLOW') for i in (1,2)],
                      [dict(id=i, created_at='2026-09-09T12:11:00Z',
                            decision='ENTRY_CHECK', why='POLICY_ALLOW') for i in (3,4)], []])
        def read(sql, **meta):
            assert 'NOT EXISTS' not in sql
            if meta['query_id']=='linkage_gates_page_v1':
                assert 'LIMIT 2' in sql and 'created_at <=' in sql
                return next(pages)
            if meta['query_id']=='linkage_decisions_batch_v1':
                return ([dict(event_id='2',gate_id=2)] if 'IN (1,2)' in sql else
                        [dict(event_id=str(i),gate_id=i) for i in (3,4)])
            if "'2'::uuid" in sql:
                return []
            return [dict(causal_event_id=str(i), observation_key=str(i),
                         deployment_id='local-paper',environment='trading_paper',
                         raw_signal_state='PRESENT',base_decision='BUY',outcome_eligible=True,
                         observation_type='GATE_BLOCKED',evaluation_started_at='2026-09-09T12:11:00Z')
                    for i in (3,4)]
        return s.linkage_counts(db,'2026-09-09T12:09:00Z','2026-09-09T12:12:00Z',read=read,batch_size=2)
    s.record_error(db,'4','snapshot',ValueError('historical'),{})
    result=scan()
    assert result['gates_total']==4
    assert result['missing_decision']==result['missing_projection']==result['missing_snapshot']==1
    assert result['snapshot_explicit_error']==1
    assert result['pending_source_gate_records']==2
    db.execute('INSERT INTO snapshots VALUES(?,?,?,?)',('3','t','{}','fp'))
    assert scan()['missing_snapshot']==0  # no durable watermark skipping old pages


def test_failed_page_never_returns_partial_total(tmp_path):
    db=s.open_store(tmp_path)
    with pytest.raises(s.SourceQueryError):
        s.linkage_counts(db,'2026-09-09T12:09:00Z','2026-09-09T12:12:00Z',
                        read=lambda *a,**k: (_ for _ in ()).throw(s.SourceQueryError('failed')))


def test_sql_error_cause_is_safe_and_identified():
    secret='postgres://user:SECRET@host/db'
    exc=subprocess.CalledProcessError(3,['SECRET'],stderr='ERROR: canceling statement due to statement timeout\nDETAIL: '+secret)
    with patch.object(s.subprocess,'run',side_effect=exc):
        with pytest.raises(s.SourceQueryError) as caught:
            s.query('SELECT 1',stage='linkage',query_id='pending_v1')
    payload=json.loads(str(caught.value))
    assert payload['sql_cause']=='canceling statement due to statement timeout'
    assert payload['query_id']=='pending_v1' and payload['stage']=='linkage'
    assert 'SECRET' not in str(caught.value)
    assert s.safe_sql_cause('DETAIL: '+secret)=='UNCLASSIFIED_SQL_ERROR_REDACTED'


def test_transport_timeout_is_redacted():
    with patch.object(s.subprocess,'run',side_effect=subprocess.TimeoutExpired(['SECRET'],25)):
        with pytest.raises(s.SourceQueryError,match='TRANSPORT_TIMEOUT') as caught:
            s.query('SELECT 1')
    assert 'SECRET' not in str(caught.value)
