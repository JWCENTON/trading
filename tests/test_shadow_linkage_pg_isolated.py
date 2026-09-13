"""Opt-in disposable PostgreSQL only; never uses application DB credentials."""
import json
import os
import subprocess

import pytest
from scripts import l3_market_context_shadow_v1 as s


def test_complete_batched_scan_and_index_plans(tmp_path):
    container = os.environ.get('SHADOW_ISOLATED_PG_CONTAINER')
    if not container:
        pytest.skip('requires a disposable PostgreSQL container')
    assert container.startswith('shadow-linkage-isolated-')
    def sql(text):
        return subprocess.run(['docker','exec','-i',container,'psql','-X','-qAt',
                               '-v','ON_ERROR_STOP=1','-U','postgres'], input=text,
                              capture_output=True,text=True,check=True).stdout
    sql('''
    CREATE TABLE regime_gate_events(id bigint PRIMARY KEY,created_at timestamptz,decision text,why text);
    INSERT INTO regime_gate_events SELECT n,'2026-09-09T12:10Z'::timestamptz+n*interval '1 millisecond',
      CASE WHEN n<=1000 THEN 'ENTRY_CHECK' ELSE 'TICK' END,'POLICY_ALLOW' FROM generate_series(1,100000)n;
    CREATE TABLE causal_decision_observation_v1(event_id uuid PRIMARY KEY,regime_gate_event_id bigint);
    INSERT INTO causal_decision_observation_v1 SELECT md5(n::text)::uuid,n FROM generate_series(1,100000)n WHERE n<=800 OR n>1000;
    CREATE TABLE paper_opportunity_observation_v1 AS SELECT event_id causal_event_id,event_id::text observation_key,
      'local-paper'::text deployment_id,'trading_paper'::text environment,'PRESENT'::text raw_signal_state,
      'BUY'::text base_decision,true outcome_eligible,'GATE_BLOCKED'::text observation_type,
      '2026-09-09T12:10Z'::timestamptz evaluation_started_at FROM causal_decision_observation_v1 WHERE regime_gate_event_id<=700 OR regime_gate_event_id>1000;
    CREATE UNIQUE INDEX ON paper_opportunity_observation_v1(causal_event_id);
    CREATE INDEX shadow_gate_lookup ON regime_gate_events(created_at,id)
      WHERE decision='ENTRY_CHECK' AND why IN ('POLICY_ALLOW','POLICY_WOULD_BLOCK');
    CREATE INDEX shadow_decision_lookup ON causal_decision_observation_v1(regime_gate_event_id);
    ANALYZE;
    ''')
    plans=[]
    def read(statement, **meta):
        plan=sql('EXPLAIN (FORMAT JSON) '+statement+';')
        assert 'Seq Scan' not in plan, (meta,plan)
        plans.append(meta['query_id'])
        return [json.loads(line) for line in sql('BEGIN READ ONLY;\n'+statement+';\nROLLBACK;').splitlines()]
    db=s.open_store(tmp_path)
    result=s.linkage_counts(db,'2026-09-09T12:09:00Z',
                           '2026-09-09T12:12:00Z',read=read,batch_size=128)
    assert result['gates_total']==1000
    assert result['missing_decision']==200
    assert result['missing_projection']==100
    assert result['missing_snapshot']==700
    assert result['pending_source_gate_records']==300
    assert result['linkage_scan_complete']
    assert len(plans)>3  # all pages, not just the first batch
