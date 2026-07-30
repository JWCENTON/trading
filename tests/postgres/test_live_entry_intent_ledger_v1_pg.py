"""Disposable PostgreSQL contract tests for LEI1A entry intent evidence."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import pytest

from common.entry_intent import (
    EntryIntentInsertOutcome,
    LiveEntryIntent,
    classify_insert_outcome,
)


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT / "db/migrations/20260730_live_entry_intent_ledger_v1.sql"
).read_text()

ADOPTION_SCHEMA = """
CREATE TABLE runtime_contract_adoption_v2 (
  adoption_id BIGINT PRIMARY KEY,
  contract_name TEXT NOT NULL,
  environment TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  generation BIGINT NOT NULL
);
INSERT INTO runtime_contract_adoption_v2(
  adoption_id,contract_name,environment,deployment_id,generation
) VALUES
  (1,'FEE_AWARE_INVENTORY_C2_2','live','local-live',1),
  (2,'FEE_AWARE_INVENTORY_C2_2','paper','local-paper',1),
  (3,'OTHER_CONTRACT','live','local-live',1);
"""

INSERT_SQL = """
INSERT INTO live_entry_intents_v1(
  intent_id,environment,deployment_id,git_revision,adoption_id,generation,
  decision_id,symbol,strategy,"interval",slot_identity,exchange_source,
  client_order_id,order_purpose,side,requested_qty,content_fingerprint,
  prepared_at,producer_identity,contract_version
) VALUES (
  %(intent_id)s,%(environment)s,%(deployment_id)s,%(git_revision)s,
  %(adoption_id)s,%(generation)s,%(decision_id)s,%(symbol)s,%(strategy)s,
  %(interval)s,%(slot_identity)s,%(exchange_source)s,%(client_order_id)s,
  %(order_purpose)s,%(side)s,%(requested_qty)s,%(content_fingerprint)s,
  %(prepared_at)s,%(producer_identity)s,%(contract_version)s
)
"""


def _intent(**changes):
    values = {
        "environment": "live",
        "deployment_id": "local-live",
        "git_revision": "7" * 40,
        "adoption_id": 1,
        "generation": 1,
        "decision_id": uuid.UUID("91cc3845-137f-5d2d-9e4e-c18e8e973653"),
        "symbol": "BNBUSDC",
        "strategy": "TREND",
        "interval": "1m",
        "exchange_source": "okx",
        "client_order_id": "ORC-L-BNBUSDC-TREN-1m-E-pg-regression",
        "requested_qty": "0.033895",
        "prepared_at": datetime(2026, 7, 30, 11, 7, 5, tzinfo=timezone.utc),
        "producer_identity": "postgres-contract-test",
    }
    values.update(changes)
    return LiveEntryIntent.build(**values)


def _params(row: LiveEntryIntent) -> dict:
    return {
        "intent_id": str(row.intent_id),
        "environment": row.environment.value,
        "deployment_id": row.deployment_id.value,
        "git_revision": row.git_revision,
        "adoption_id": row.adoption_id,
        "generation": row.generation,
        "decision_id": str(row.decision_id),
        "symbol": row.symbol,
        "strategy": row.strategy,
        "interval": row.interval,
        "slot_identity": row.slot_identity,
        "exchange_source": row.exchange_source,
        "client_order_id": row.client_order_id,
        "order_purpose": row.order_purpose.value,
        "side": row.side.value,
        "requested_qty": str(row.requested_qty),
        "content_fingerprint": row.content_fingerprint,
        "prepared_at": row.prepared_at,
        "producer_identity": row.producer_identity,
        "contract_version": row.contract_version.value,
    }


def _expect_rejected(conn, params):
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT invalid_entry_intent")
        with pytest.raises(psycopg2.Error):
            cur.execute(INSERT_SQL, params)
        cur.execute("ROLLBACK TO SAVEPOINT invalid_entry_intent")
        cur.execute("RELEASE SAVEPOINT invalid_entry_intent")


def test_lei1a_migration_and_append_only_contract(disposable_postgres_v16):
    database = f"waltrade_baseline_test_lei1a_{uuid.uuid4().hex[:8]}"
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        with conn.cursor() as cur:
            cur.execute(ADOPTION_SCHEMA)
            cur.execute(MIGRATION)
            cur.execute(MIGRATION)
            cur.execute("SELECT count(*) FROM live_entry_intents_v1")
            assert cur.fetchone()[0] == 0
        conn.commit()

        row = _intent()
        with conn.cursor() as cur:
            cur.execute(INSERT_SQL, _params(row))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT content_fingerprint
                FROM live_entry_intents_v1
                WHERE environment=%s AND deployment_id=%s
                  AND exchange_source=%s AND client_order_id=%s
                """,
                row.natural_key,
            )
            existing_fingerprint = cur.fetchone()[0]
        assert classify_insert_outcome(
            existing_fingerprint, row
        ) is EntryIntentInsertOutcome.IDEMPOTENT_EXISTING
        assert classify_insert_outcome(
            existing_fingerprint, _intent(generation=2)
        ) is EntryIntentInsertOutcome.CONFLICT

        with conn.cursor() as cur:
            with pytest.raises(psycopg2.errors.UniqueViolation):
                cur.execute(INSERT_SQL, _params(row))
        conn.rollback()

        invalid_cases = []
        base = _params(row)
        for field, value in (
            ("environment", "trading_paper"),
            ("deployment_id", "LOCAL"),
            ("requested_qty", "0"),
            ("requested_qty", "-1"),
            ("order_purpose", "EXIT"),
            ("side", "SELL"),
            ("generation", 2),
            ("adoption_id", 3),
        ):
            changed = dict(base)
            changed["intent_id"] = str(uuid.uuid4())
            changed["client_order_id"] = f"{base['client_order_id']}-{len(invalid_cases)}"
            changed[field] = value
            invalid_cases.append(changed)
        for case in invalid_cases:
            _expect_rejected(conn, case)

        with conn.cursor() as cur:
            with pytest.raises(psycopg2.Error, match="immutable"):
                cur.execute(
                    "UPDATE live_entry_intents_v1 SET requested_qty=9 "
                    "WHERE intent_id=%s",
                    (str(row.intent_id),),
                )
        conn.rollback()
        with conn.cursor() as cur:
            with pytest.raises(psycopg2.Error, match="immutable"):
                cur.execute(
                    "DELETE FROM live_entry_intents_v1 WHERE intent_id=%s",
                    (str(row.intent_id),),
                )
        conn.rollback()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname='public'
                  AND tablename='live_entry_intents_v1'
                ORDER BY indexname
                """
            )
            indexes = {item[0] for item in cur.fetchall()}
            assert {
                "live_entry_intents_v1_pkey",
                "ux_live_entry_intent_natural_key_v1",
                "ix_live_entry_intent_slot_v1",
                "ix_live_entry_intent_client_order_v1",
                "ix_live_entry_intent_adoption_generation_v1",
            }.issubset(indexes)
            cur.execute(
                "SELECT pg_column_size(i) FROM live_entry_intents_v1 i "
                "WHERE intent_id=%s",
                (str(row.intent_id),),
            )
            assert 200 <= cur.fetchone()[0] < 2048
            cur.execute("SELECT count(*) FROM live_entry_intents_v1")
            assert cur.fetchone()[0] == 1
    finally:
        conn.close()
