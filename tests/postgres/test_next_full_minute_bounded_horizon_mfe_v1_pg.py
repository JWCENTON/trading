from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import uuid

import pytest


ROOT = Path(__file__).resolve().parents[2]
MIGRATION_SOURCE = (
    ROOT
    / "db/migrations/20260815_next_full_minute_bounded_horizon_mfe_v1.sql"
).read_text()
MIGRATION = "\n".join(
    line for line in MIGRATION_SOURCE.splitlines()
    if not line.lstrip().startswith("\\")
)
CORRECTION_SOURCE = (
    ROOT
    / "db/migrations/20260815_next_full_minute_bounded_horizon_mfe_v1_1_reference_fix.sql"
).read_text()
CORRECTION = "\n".join(
    line for line in CORRECTION_SOURCE.splitlines()
    if not line.lstrip().startswith("\\")
)

BASE_SCHEMA = r"""
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE schema_migration_ledger_v1(
  ledger_id bigserial PRIMARY KEY,migration_id text NOT NULL,
  checksum_sha256 text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now(),
  environment text NOT NULL,deployment_id text NOT NULL,database_name text NOT NULL,
  applied_by text NOT NULL,status text NOT NULL,success boolean NOT NULL,
  execution_duration_ms bigint NOT NULL,git_sha text NOT NULL,error_summary text,
  schema_baseline_version text NOT NULL
);
CREATE TABLE entry_opportunity_evidence_v1(
  snapshot_id uuid PRIMARY KEY,decision_id uuid NOT NULL UNIQUE,
  decision_created_at timestamptz NOT NULL,environment text NOT NULL,
  deployment_id text NOT NULL,source_revision text,strategy text NOT NULL,
  symbol text NOT NULL,interval text NOT NULL,signal_action text,
  strategy_features jsonb NOT NULL DEFAULT '{}',captured_at timestamptz NOT NULL
);
CREATE TABLE candles(
  id bigserial PRIMARY KEY,symbol text NOT NULL,interval text NOT NULL,
  open_time timestamptz NOT NULL,open numeric,high numeric,low numeric,close numeric,
  volume numeric,close_time timestamptz NOT NULL,trades integer
);
CREATE TABLE positions(id bigint PRIMARY KEY,marker text);
CREATE TABLE simulated_orders(id bigint PRIMARY KEY,marker text);
CREATE TABLE simulated_execution_fills_v1(id bigint PRIMARY KEY,marker text);
CREATE TABLE canonical_financial_truth_v1(position_id bigint PRIMARY KEY,marker text);
"""


def _database(pg, suffix: str):
    name = f"waltrade_baseline_test_bounded_horizon_v1_{suffix}".lower()
    pg.create_database(name)
    connection = pg.connect(name)
    with connection.cursor() as cur:
        cur.execute(BASE_SCHEMA)
        cur.execute(MIGRATION)
        cur.execute(CORRECTION)
    connection.commit()
    return connection


def _database_before_reference_fix(pg, suffix: str):
    name = f"waltrade_baseline_test_bounded_horizon_v1_{suffix}".lower()
    pg.create_database(name)
    connection = pg.connect(name)
    with connection.cursor() as cur:
        cur.execute(BASE_SCHEMA)
        cur.execute(MIGRATION)
    connection.commit()
    return connection


def _snapshot(
    cur,
    *,
    anchor: datetime,
    price: Decimal = Decimal("100"),
    action: str = "BUY",
    deployment: str = "LOCAL",
    symbol: str = "BTCUSDC",
):
    snapshot_id = uuid.uuid4()
    decision_id = uuid.uuid4()
    cur.execute(
        """
        INSERT INTO entry_opportunity_evidence_v1(
          snapshot_id,decision_id,decision_created_at,environment,deployment_id,
          source_revision,strategy,symbol,interval,signal_action,
          strategy_features,captured_at
        ) VALUES(
          %s,%s,%s,'trading_paper',%s,'a5e154d','RSI',%s,'1m',%s,
          jsonb_build_object('price',%s::text,'signal_created_at',%s::timestamptz),
          %s
        )
        """,
        (
            str(snapshot_id),str(decision_id),anchor-timedelta(minutes=1),
            deployment,symbol,action,price,anchor-timedelta(seconds=1),anchor,
        ),
    )
    return snapshot_id, decision_id


def _candles(
    cur,
    *,
    symbol: str,
    start: datetime,
    count: int,
    highs: list[Decimal] | None = None,
    lows: list[Decimal] | None = None,
    include_watermark: bool = True,
    skip_index: int | None = None,
):
    highs = highs or [Decimal("100") for _ in range(count)]
    lows = lows or [Decimal("100") for _ in range(count)]
    for index in range(count):
        if index == skip_index:
            continue
        opened = start + timedelta(minutes=index)
        cur.execute(
            """
            INSERT INTO candles(
              symbol,interval,open_time,open,high,low,close,close_time
            ) VALUES(%s,'1m',%s,100,%s,%s,100,%s)
            """,
            (
                symbol,opened,highs[index],lows[index],
                opened+timedelta(minutes=1)-timedelta(milliseconds=1),
            ),
        )
    if include_watermark:
        watermark = start+timedelta(minutes=count)
        cur.execute(
            """
            INSERT INTO candles(
              symbol,interval,open_time,open,high,low,close,close_time
            ) VALUES(%s,'1m',%s,100,100,100,100,%s)
            """,
            (symbol,watermark,watermark+timedelta(minutes=1)-timedelta(milliseconds=1)),
        )


def _label(cur, snapshot_id, horizon=15):
    cur.execute(
        """
        SELECT evaluation_start_at,evaluation_end_at,alignment_delay_ms,
               direction,reference_price,bounded_mfe_pct,label_status,
               market_rows_expected,market_rows_used,market_data_gaps,
               first_market_timestamp,last_market_timestamp,payload_hash
        FROM entry_opportunity_bounded_horizon_labels_v1
        WHERE snapshot_id=%s AND horizon_minutes=%s
        """,
        (str(snapshot_id),horizon),
    )
    return cur.fetchone()


def test_alignment_long_short_boundaries_and_no_favorable_move(
    disposable_postgres_v16,
):
    connection = _database(disposable_postgres_v16, "formula")
    try:
        anchor = datetime(2026,8,1,10,0,37,250000,tzinfo=timezone.utc)
        start = datetime(2026,8,1,10,1,tzinfo=timezone.utc)
        with connection.cursor() as cur:
            long_id,_ = _snapshot(cur,anchor=anchor,symbol="BTCUSDC")
            short_id,_ = _snapshot(
                cur,anchor=anchor,action="SELL",symbol="ETHUSDC"
            )
            flat_id,_ = _snapshot(cur,anchor=anchor,symbol="SOLUSDC")
            long_highs = [Decimal("100.2"),Decimal("100.8"),Decimal("100.5")]
            long_highs += [Decimal("100")]*12
            short_lows = [Decimal("99.8"),Decimal("99.1"),Decimal("99.5")]
            short_lows += [Decimal("100")]*12
            _candles(cur,symbol="BTCUSDC",start=start,count=15,highs=long_highs)
            _candles(cur,symbol="ETHUSDC",start=start,count=15,lows=short_lows)
            _candles(
                cur,symbol="SOLUSDC",start=start,count=15,
                highs=[Decimal("99.9")]*15,
            )
            cur.execute(
                "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1(%s,%s,%s)",
                ('trading_paper','LOCAL',100),
            )
            assert cur.fetchone()[0] == 3
            long_row = _label(cur,long_id)
            short_row = _label(cur,short_id)
            flat_row = _label(cur,flat_id)

        assert long_row[0] == start
        assert long_row[1] == start+timedelta(minutes=15)
        assert long_row[2] == 22750
        assert long_row[3] == "LONG"
        assert long_row[5] == Decimal("0.800")
        assert short_row[3] == "SHORT"
        assert short_row[5] == Decimal("0.900")
        assert flat_row[5] == Decimal("0")
        assert all(row[6] == "COMPLETE" for row in (long_row,short_row,flat_row))
    finally:
        connection.close()


def test_exact_boundary_horizon_independence_and_terminal_exclusion(
    disposable_postgres_v16,
):
    connection = _database(disposable_postgres_v16, "horizons")
    try:
        anchor = datetime(2026,8,1,10,0,tzinfo=timezone.utc)
        with connection.cursor() as cur:
            snapshot_id,_ = _snapshot(cur,anchor=anchor)
            highs = [Decimal("100")]*60
            highs[14] = Decimal("101")
            highs[29] = Decimal("102")
            highs[59] = Decimal("103")
            _candles(cur,symbol="BTCUSDC",start=anchor,count=60,highs=highs)
            cur.execute(
                "UPDATE candles SET high=999 WHERE symbol='BTCUSDC' "
                "AND open_time=%s",
                (anchor+timedelta(minutes=60),),
            )
            cur.execute(
                "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1(%s,%s,%s)",
                ('trading_paper','LOCAL',100),
            )
            assert cur.fetchone()[0] == 3
            rows = [_label(cur,snapshot_id,h) for h in (15,30,60)]

        assert [row[2] for row in rows] == [0,0,0]
        assert [row[5] for row in rows] == [Decimal("1.00"),Decimal("2.00"),Decimal("3.00")]
        assert [row[8] for row in rows] == [15,30,60]
        assert [row[9] for row in rows] == [0,0,0]
    finally:
        connection.close()


@pytest.mark.parametrize("available,skip_index",[(14,None),(15,7)])
def test_incomplete_market_data_for_14_rows_or_interior_gap(
    disposable_postgres_v16,available,skip_index
):
    connection = _database(
        disposable_postgres_v16,f"incomplete_{available}_{skip_index}"
    )
    try:
        anchor = datetime(2026,8,1,10,0,tzinfo=timezone.utc)
        with connection.cursor() as cur:
            snapshot_id,_ = _snapshot(cur,anchor=anchor)
            _candles(
                cur,symbol="BTCUSDC",start=anchor,count=available,
                include_watermark=False,skip_index=skip_index,
            )
            watermark = anchor+timedelta(minutes=15)
            cur.execute(
                """
                INSERT INTO candles(
                  symbol,interval,open_time,open,high,low,close,close_time
                ) VALUES('BTCUSDC','1m',%s,100,100,100,100,%s)
                """,
                (watermark,watermark+timedelta(minutes=1)),
            )
            cur.execute(
                "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1(%s,%s,%s)",
                ('trading_paper','LOCAL',100),
            )
            assert cur.fetchone()[0] == 1
            row = _label(cur,snapshot_id)

        assert row[5] is None
        assert row[6] == "INCOMPLETE_MARKET_DATA"
        assert row[8] < 15
        assert row[9] > 0
    finally:
        connection.close()


def test_partial_start_candle_duplicate_detection_retry_immutability_and_fencing(
    disposable_postgres_v16,
):
    connection = _database(disposable_postgres_v16, "safety")
    try:
        anchor = datetime(2026,8,1,10,0,37,250000,tzinfo=timezone.utc)
        start = datetime(2026,8,1,10,1,tzinfo=timezone.utc)
        with connection.cursor() as cur:
            snapshot_id,_ = _snapshot(cur,anchor=anchor)
            partial_open = datetime(2026,8,1,10,0,tzinfo=timezone.utc)
            cur.execute(
                """
                INSERT INTO candles(
                  symbol,interval,open_time,open,high,low,close,close_time
                ) VALUES('BTCUSDC','1m',%s,100,999,1,100,%s)
                """,
                (partial_open,start-timedelta(milliseconds=1)),
            )
            _candles(cur,symbol="BTCUSDC",start=start,count=15)
            cur.execute(
                "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1(%s,%s,%s)",
                ('trading_paper','LOCAL',100),
            )
            assert cur.fetchone()[0] == 1
            first = _label(cur,snapshot_id)
            cur.execute(
                "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1(%s,%s,%s)",
                ('trading_paper','LOCAL',100),
            )
            assert cur.fetchone()[0] == 0
            assert _label(cur,snapshot_id) == first
            assert first[5] == Decimal("0")
            assert first[7] == 15
            with pytest.raises(Exception,match="IMMUTABLE"):
                cur.execute(
                    "UPDATE entry_opportunity_bounded_horizon_labels_v1 "
                    "SET bounded_mfe_pct=1 WHERE snapshot_id=%s",
                    (str(snapshot_id),),
                )
            connection.rollback()
            with pytest.raises(Exception,match="DEPLOYMENT_NOT_ALLOWED"):
                with connection.cursor() as fenced:
                    fenced.execute(
                        "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1(%s,%s,%s)",
                        ('trading_paper','local-paper',100),
                    )
            connection.rollback()
    finally:
        connection.close()


def test_duplicate_open_time_is_incomplete_and_cross_deployment_insert_is_rejected(
    disposable_postgres_v16,
):
    connection = _database(disposable_postgres_v16, "duplicate_fence")
    try:
        anchor = datetime(2026,8,1,10,0,tzinfo=timezone.utc)
        with connection.cursor() as cur:
            snapshot_id,decision_id = _snapshot(cur,anchor=anchor)
            _candles(cur,symbol="BTCUSDC",start=anchor,count=15)
            duplicate_time = anchor+timedelta(minutes=7)
            cur.execute(
                """
                INSERT INTO candles(
                  symbol,interval,open_time,open,high,low,close,close_time
                ) VALUES('BTCUSDC','1m',%s,100,100,100,100,%s)
                """,
                (
                    duplicate_time,
                    duplicate_time+timedelta(minutes=1)-timedelta(milliseconds=1),
                ),
            )
            cur.execute(
                "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1(%s,%s,%s)",
                ('trading_paper','LOCAL',100),
            )
            assert cur.fetchone()[0] == 1
            row = _label(cur,snapshot_id)
            assert row[5] is None
            assert row[6] == "INCOMPLETE_MARKET_DATA"
            assert row[8] == 16

            with pytest.raises(Exception,match="IDENTITY_MISMATCH"):
                cur.execute(
                    """
                    INSERT INTO entry_opportunity_bounded_horizon_labels_v1(
                      snapshot_id,decision_id,environment,deployment_id,
                      runtime_deployment_id,target_version,horizon_minutes,
                      prediction_anchor_at,evaluation_start_at,evaluation_end_at,
                      alignment_delay_ms,reference_price_source,
                      market_data_source,market_data_granularity,
                      market_data_start_at,market_data_end_at,
                      market_rows_expected,market_rows_used,
                      duplicate_market_rows,market_data_gaps,label_status,
                      producer_version,payload_hash
                    ) VALUES(
                      %s,%s,'trading_paper','VPS','vps-paper',
                      'NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_V1',30,
                      %s,%s,%s,0,'FROZEN_STRATEGY_SIGNAL_EVENT_PRICE',
                      'candles','1m',%s,%s,30,0,0,30,
                      'INCOMPLETE_MARKET_DATA',
                      'NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_PRODUCER_V1',repeat('a',64)
                    )
                    """,
                    (
                        str(snapshot_id),str(decision_id),anchor,anchor,
                        anchor+timedelta(minutes=30),anchor,
                        anchor+timedelta(minutes=30),
                    ),
                )
            connection.rollback()
    finally:
        connection.close()


def test_only_finalizable_horizon_is_inserted(disposable_postgres_v16):
    connection = _database(disposable_postgres_v16, "pending_horizons")
    try:
        anchor = datetime(2026,8,1,10,0,tzinfo=timezone.utc)
        with connection.cursor() as cur:
            snapshot_id,_ = _snapshot(cur,anchor=anchor)
            _candles(cur,symbol="BTCUSDC",start=anchor,count=15)
            cur.execute(
                "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1(%s,%s,%s)",
                ('trading_paper','LOCAL',100),
            )
            assert cur.fetchone()[0] == 1
            cur.execute(
                """
                SELECT horizon_minutes,label_status
                FROM entry_opportunity_bounded_horizon_labels_v1
                WHERE snapshot_id=%s ORDER BY horizon_minutes
                """,
                (str(snapshot_id),),
            )
            assert cur.fetchall() == [(15,"COMPLETE")]
    finally:
        connection.close()


def test_migration_idempotency_and_invalid_input_statuses(disposable_postgres_v16):
    connection = _database(disposable_postgres_v16, "idempotent_invalid")
    try:
        anchor = datetime(2026,8,1,10,0,tzinfo=timezone.utc)
        with connection.cursor() as cur:
            cur.execute(MIGRATION)
            cur.execute(CORRECTION)
            cur.execute(
                """
                SELECT count(*) FROM schema_migration_ledger_v1
                WHERE migration_id=
                  '20260815_next_full_minute_bounded_horizon_mfe_v1.sql'
                """
            )
            assert cur.fetchone()[0] == 1

            invalid_reference,_ = _snapshot(
                cur,anchor=anchor,price=Decimal("0"),symbol="BTCUSDC"
            )
            unsupported_direction,_ = _snapshot(
                cur,anchor=anchor,action="HOLD",symbol="ETHUSDC"
            )
            _candles(cur,symbol="BTCUSDC",start=anchor,count=15)
            _candles(cur,symbol="ETHUSDC",start=anchor,count=15)
            cur.execute(
                "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1(%s,%s,%s)",
                ('trading_paper','LOCAL',100),
            )
            assert cur.fetchone()[0] == 2
            assert _label(cur,invalid_reference)[6] == "INVALID_REFERENCE"
            assert _label(cur,unsupported_direction)[6] == "UNSUPPORTED_DIRECTION"
    finally:
        connection.close()


def test_reference_fix_reprojects_only_parser_generated_invalid_label(
    disposable_postgres_v16,
):
    connection = _database_before_reference_fix(
        disposable_postgres_v16,"reference_repair"
    )
    try:
        anchor = datetime(2026,8,1,10,0,tzinfo=timezone.utc)
        with connection.cursor() as cur:
            snapshot_id,_ = _snapshot(cur,anchor=anchor)
            _candles(cur,symbol="BTCUSDC",start=anchor,count=15)
            cur.execute(
                "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1(%s,%s,%s)",
                ('trading_paper','LOCAL',100),
            )
            assert cur.fetchone()[0] == 1
            assert _label(cur,snapshot_id)[6] == "INVALID_REFERENCE"

            cur.execute(CORRECTION)
            assert _label(cur,snapshot_id) is None
            cur.execute(
                "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1(%s,%s,%s)",
                ('trading_paper','LOCAL',100),
            )
            assert cur.fetchone()[0] == 1
            repaired = _label(cur,snapshot_id)
            assert repaired[4] == Decimal("100")
            assert repaired[6] == "COMPLETE"
    finally:
        connection.close()


def test_actual_exit_independence_and_no_trading_mutation(disposable_postgres_v16):
    connection = _database(disposable_postgres_v16, "isolation")
    try:
        anchor = datetime(2026,8,1,10,0,tzinfo=timezone.utc)
        with connection.cursor() as cur:
            snapshot_id,_ = _snapshot(cur,anchor=anchor)
            _candles(cur,symbol="BTCUSDC",start=anchor,count=15)
            cur.execute("INSERT INTO positions VALUES(1,'exit-a')")
            cur.execute("INSERT INTO simulated_orders VALUES(1,'order-a')")
            cur.execute("INSERT INTO simulated_execution_fills_v1 VALUES(1,'fill-a')")
            cur.execute("INSERT INTO canonical_financial_truth_v1 VALUES(1,'ft-a')")
            cur.execute(
                "SELECT table_name,marker FROM ("
                "SELECT 'positions' table_name,marker FROM positions UNION ALL "
                "SELECT 'orders',marker FROM simulated_orders UNION ALL "
                "SELECT 'fills',marker FROM simulated_execution_fills_v1 UNION ALL "
                "SELECT 'ft',marker FROM canonical_financial_truth_v1"
                ") x ORDER BY table_name"
            )
            before = cur.fetchall()
            cur.execute(
                "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1(%s,%s,%s)",
                ('trading_paper','LOCAL',100),
            )
            assert cur.fetchone()[0] == 1
            first = _label(cur,snapshot_id)
            cur.execute("UPDATE positions SET marker='different-exit' WHERE id=1")
            assert _label(cur,snapshot_id) == first
            cur.execute("UPDATE positions SET marker='exit-a' WHERE id=1")
            cur.execute(
                "SELECT table_name,marker FROM ("
                "SELECT 'positions' table_name,marker FROM positions UNION ALL "
                "SELECT 'orders',marker FROM simulated_orders UNION ALL "
                "SELECT 'fills',marker FROM simulated_execution_fills_v1 UNION ALL "
                "SELECT 'ft',marker FROM canonical_financial_truth_v1"
                ") x ORDER BY table_name"
            )
            assert cur.fetchall() == before
    finally:
        connection.close()
