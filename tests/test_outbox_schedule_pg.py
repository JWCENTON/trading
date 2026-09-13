"""Opt-in disposable PostgreSQL: real consumer queue/retry transactions.

Projection sink is synthetic; this measures transport, not full production
projection cost. No application DSN or credentials are read.
"""
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg2
import pytest

from common.decision_observation import stable_hash
from common.decision_observation_transport import DecisionObservationOutboxConsumer, TransportFlags


@pytest.fixture
def queue():
    port=os.environ.get('OUTBOX_ISOLATED_PG_PORT')
    if not port:
        pytest.skip('requires disposable PostgreSQL bound to loopback')
    connect=lambda:psycopg2.connect(host='127.0.0.1',port=int(port),dbname='postgres',user='postgres',connect_timeout=2)
    conn=connect();conn.autocommit=True
    schema='isolated_'+uuid.uuid4().hex
    with conn.cursor() as cur:
        cur.execute('CREATE SCHEMA '+schema)
        cur.execute('SET search_path TO '+schema)
        sql=(Path(__file__).resolve().parents[1]/'db/migrations/20260717_causal_decision_observation_transport_v1.sql').read_text()
        cur.execute(sql.split('CREATE OR REPLACE FUNCTION')[0].replace('BEGIN;',''))
        cur.execute('CREATE TABLE sink(id integer PRIMARY KEY, materialized_at timestamptz DEFAULT now())')
    def factory():
        c=connect()
        with c.cursor() as cur:cur.execute('SET search_path TO '+schema)
        c.commit()
        return c
    def seed(n,status='PENDING',delay=None):
        payload={'id':n}
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO causal_decision_observation_outbox_v1
            (event_id,deployment_id,decision_key,event_schema_version,event_payload,event_payload_hash,
             semantic_digest,source_service,decision_created_at,processing_status,next_attempt_at)
            VALUES(%s,'local-paper',%s,'test',%s,%s,'test','test',
              '2020-01-01'::timestamptz+%s*interval '1 second',%s,%s)""",
              (str(uuid.uuid4()),str(n),json.dumps(payload),stable_hash(payload),n,status,delay))
    class Consumer(DecisionObservationOutboxConsumer):
        def _persist(self,cur,payload,digest):
            cur.execute('SHOW statement_timeout');assert cur.fetchone()[0]=='5s'
            cur.execute('SHOW lock_timeout');assert cur.fetchone()[0]=='500ms'
            cur.execute('INSERT INTO sink(id) VALUES(%s)',(payload['id'],))
    flags=TransportFlags(decision_observation_enabled=True,kill_switch=False,deployment_id='local-paper')
    yield conn,seed,lambda:Consumer(factory,flags,consumer_id='isolated')
    with conn.cursor() as cur:cur.execute('DROP SCHEMA '+schema+' CASCADE')
    conn.close()


def test_concurrent_resume_old_retry_and_rate(queue):
    conn,seed,consumer=queue
    for n in range(300):seed(n,'RETRY' if n==0 else 'PENDING')
    seed(999,'RETRY','2099-01-01')
    with conn.cursor() as cur:
        cur.execute('SELECT event_id,decision_created_at,inserted_at,event_payload_hash FROM causal_decision_observation_outbox_v1 ORDER BY event_id')
        before=cur.fetchall()
    started=time.monotonic()
    with ThreadPoolExecutor(max_workers=2) as pool:
        counts=list(pool.map(lambda _:consumer().poll(max_duration_seconds=15),range(2)))
    assert counts==[100,100]
    assert consumer().poll(max_duration_seconds=15)==100
    assert consumer().poll(max_duration_seconds=15)==0
    elapsed=time.monotonic()-started
    with conn.cursor() as cur:
        cur.execute('SELECT count(*),count(DISTINCT id) FROM sink');assert cur.fetchone()==(300,300)
        cur.execute("SELECT count(*),min(attempt_count),max(attempt_count) FROM causal_decision_observation_outbox_v1 WHERE processing_status='PROCESSED'")
        assert cur.fetchone()==(300,1,1)
        cur.execute('SELECT event_id,decision_created_at,inserted_at,event_payload_hash FROM causal_decision_observation_outbox_v1 ORDER BY event_id')
        assert cur.fetchall()==before
    print({'transport_seconds_300':elapsed,'conservative_service_per_minute':100*60/(60+elapsed)})
    assert elapsed<15  # three batches together below one scheduling budget


def test_budget_order_rollback_and_retry(queue,monkeypatch):
    conn,seed,consumer=queue
    for n in range(3):seed(n)
    c=consumer()
    # No row claimed when budget is already exhausted.
    ticks=iter([0,16,16])
    monkeypatch.setattr('common.decision_observation_transport.time.monotonic',lambda:next(ticks))
    assert c.poll(max_duration_seconds=15)==0
    monkeypatch.undo()
    original=c._persist
    def fail_once(cur,payload,digest):
        original(cur,payload,digest)
        if payload['id']==0:raise ValueError('isolated transient')
    c._persist=fail_once
    assert c.poll(max_duration_seconds=15)==2
    with conn.cursor() as cur:
        cur.execute('SELECT id FROM sink ORDER BY id');assert cur.fetchall()==[(1,),(2,)]
        cur.execute("SELECT processing_status,attempt_count FROM causal_decision_observation_outbox_v1 WHERE decision_key='0'")
        assert cur.fetchone()==('RETRY',1)
        # Test clock setup ONLY inside disposable fixture, never production.
        cur.execute("UPDATE causal_decision_observation_outbox_v1 SET next_attempt_at=now()-interval '1s' WHERE decision_key='0'")
    assert consumer().poll(max_duration_seconds=15)==1
    assert consumer().poll(max_duration_seconds=15)==0


def test_stale_claim_recovered_in_order_without_timestamp_rewrite(queue):
    conn,seed,consumer=queue
    seed(0,'PROCESSING');seed(1)
    with conn.cursor() as cur:
        cur.execute("UPDATE causal_decision_observation_outbox_v1 SET claimed_at=now()-interval '1 hour',claimed_by='dead-process' WHERE decision_key='0'")
    c=consumer();order=[];original=c._persist
    def record(cur,payload,digest):
        order.append(payload['id']);original(cur,payload,digest)
    c._persist=record
    assert c.poll(max_duration_seconds=15)==2
    assert order==[0,1]
    assert consumer().poll(max_duration_seconds=15)==0
