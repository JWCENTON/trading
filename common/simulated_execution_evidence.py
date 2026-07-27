from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
import uuid

from common.financial_truth_identity import IDENTITY_VERSION


SIMULATION_MODEL_VERSION = "PAPER_SIMULATOR_FINANCIAL_MODEL_V1"
SIMULATION_FEE_RATE = Decimal("0.0004")
SIMULATED_IDENTITY_VERSION = "SIMULATED_ACCOUNT_IDENTITY_V1"
INSTRUMENT_METADATA_VERSION = "EXECUTION_INSTRUMENT_SNAPSHOT_V1"


def _hash(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _assets(symbol: str) -> tuple[str, str]:
    symbol = str(symbol).upper()
    for quote in ("USDC", "USDT", "USD", "EUR"):
        if symbol.endswith(quote):
            return symbol[:-len(quote)], quote
    raise ValueError("unsupported quote asset")


def _instrument_values(client, symbol: str, *, allow_remote: bool = True):
    try:
        from common.sizing import _FILTERS_CACHE
        exchange = os.getenv("EXCHANGE", "BINANCE").strip().upper()
        cached = _FILTERS_CACHE.get(f"{exchange}:{symbol}")
        if cached is not None:
            step = Decimal(str(cached.step))
            return (
                step,
                Decimal(str(cached.min_qty)),
                Decimal(str(cached.min_notional)),
                abs(step.normalize().as_tuple().exponent) if step else None,
                None,
            )
        if not allow_remote:
            return None
        info = client.get_symbol_info(symbol)
        filters = {item.get("filterType"): item for item in info.get("filters", [])}
        lot = filters.get("LOT_SIZE") or {}
        notional = filters.get("MIN_NOTIONAL") or {}
        step = Decimal(str(lot.get("stepSize")))
        min_qty = Decimal(str(lot.get("minQty") or 0))
        min_notional = Decimal(str(notional.get("minNotional") or 0))
        raw = info.get("raw") or {}
        quantity_precision = (
            abs(step.normalize().as_tuple().exponent) if step else None
        )
        price_tick = raw.get("tickSz")
        price_precision = (
            abs(Decimal(str(price_tick)).normalize().as_tuple().exponent)
            if price_tick else None
        )
        return step, min_qty, min_notional, quantity_precision, price_precision
    except Exception:
        return None


def record_simulated_fill_evidence(
    connection_factory,
    *,
    client,
    simulated_order_id: int,
    position_id: int,
    environment: str,
    deployment_id: str,
) -> bool:
    """Persist additive evidence after an existing PAPER lifecycle action."""
    if str(environment).lower() != "paper":
        return False
    conn = connection_factory()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol,side,price,quantity_btc,is_exit,created_at
                    FROM simulated_orders WHERE id=%s
                    """,
                    (int(simulated_order_id),),
                )
                order = cur.fetchone()
                if order is None:
                    return False
                symbol, side, price, qty, is_exit, execution_at = order
                cur.execute(
                    """
                    INSERT INTO financial_truth_simulated_account_v1 (
                      deployment_id,simulated_account_uid,identity_version
                    ) VALUES (%s,%s,%s)
                    ON CONFLICT (deployment_id) DO UPDATE
                      SET deployment_id=EXCLUDED.deployment_id
                    RETURNING simulated_account_uid
                    """,
                    (
                        str(deployment_id), str(uuid.uuid4()),
                        SIMULATED_IDENTITY_VERSION,
                    ),
                )
                simulated_uid = str(cur.fetchone()[0])
                identity_payload = {
                    "exchange": "SIMULATOR", "uid": simulated_uid,
                    "main_uid": simulated_uid, "scope": "SIMULATED",
                    "source": "SIMULATED_ACCOUNT_LEDGER",
                    "version": SIMULATED_IDENTITY_VERSION,
                }
                identity_fingerprint = _hash(identity_payload)
                cur.execute(
                    """
                    INSERT INTO financial_truth_account_identity_v1 (
                      source_authority,exchange,account_uid,main_account_uid,
                      account_scope,identity_source,identity_version,
                      identity_fingerprint,captured_at
                    ) VALUES (
                      'SIMULATED_EXECUTION','SIMULATOR',%s,%s,'SIMULATED',
                      'SIMULATED_ACCOUNT_LEDGER',%s,%s,clock_timestamp()
                    )
                    ON CONFLICT (identity_fingerprint) DO UPDATE
                      SET identity_fingerprint=EXCLUDED.identity_fingerprint
                    RETURNING id
                    """,
                    (
                        simulated_uid, simulated_uid,
                        SIMULATED_IDENTITY_VERSION, identity_fingerprint,
                    ),
                )
                identity_id = cur.fetchone()[0]
                metadata = _instrument_values(client, symbol, allow_remote=False)
                instrument_id = None
                metadata_fingerprint = None
                base_asset, quote_asset = _assets(symbol)
                if metadata is not None:
                    step, min_qty, min_notional, qty_precision, price_precision = metadata
                    metadata_payload = {
                        "source_authority": "SIMULATED_EXECUTION",
                        "exchange": os.getenv("EXCHANGE", "OKX").upper(),
                        "symbol": symbol, "base_asset": base_asset,
                        "quote_asset": quote_asset, "step_size": str(step),
                        "min_qty": str(min_qty), "min_notional": str(min_notional),
                        "quantity_precision": qty_precision,
                        "price_precision": price_precision,
                        "source": "EXCHANGE_PUBLIC_AT_EXECUTION",
                        "version": INSTRUMENT_METADATA_VERSION,
                    }
                    metadata_fingerprint = _hash(metadata_payload)
                    cur.execute(
                        """
                        INSERT INTO financial_truth_instrument_snapshot_v1 (
                          source_authority,exchange,symbol,base_asset,quote_asset,
                          step_size,min_qty,quantity_precision,price_precision,
                          min_notional,metadata_source,metadata_version,
                          metadata_fingerprint,captured_at
                        ) VALUES (
                          'SIMULATED_EXECUTION',%s,%s,%s,%s,%s,%s,%s,%s,%s,
                          'EXCHANGE_PUBLIC_AT_EXECUTION',%s,%s,clock_timestamp()
                        )
                        ON CONFLICT (metadata_fingerprint) DO UPDATE
                          SET metadata_fingerprint=EXCLUDED.metadata_fingerprint
                        RETURNING id
                        """,
                        (
                            os.getenv("EXCHANGE", "OKX").upper(), symbol,
                            base_asset, quote_asset, step, min_qty, qty_precision,
                            price_precision, min_notional,
                            INSTRUMENT_METADATA_VERSION, metadata_fingerprint,
                        ),
                    )
                    instrument_id = cur.fetchone()[0]
                quantity = Decimal(str(qty))
                fill_price = Decimal(str(price))
                notional = quantity * fill_price
                fee_usdc = notional * SIMULATION_FEE_RATE
                source_payload = {
                    "simulated_order_id": int(simulated_order_id),
                    "position_id": int(position_id), "purpose": (
                        "EXIT" if is_exit else "ENTRY"
                    ),
                    "quantity": str(quantity), "price": str(fill_price),
                    "fee_usdc": str(fee_usdc),
                    "identity": identity_fingerprint,
                    "instrument": metadata_fingerprint,
                    "environment": environment, "deployment_id": deployment_id,
                    "model": SIMULATION_MODEL_VERSION,
                }
                cur.execute(
                    """
                    INSERT INTO simulated_execution_fills_v1 (
                      simulated_order_id,position_id,order_purpose,side,symbol,
                      fill_qty,fill_price,fill_notional,fee_qty,fee_asset,
                      authoritative_fee_usdc,account_identity_id,
                      instrument_snapshot_id,environment,deployment_id,
                      simulation_model_version,execution_at,source_fingerprint
                    ) VALUES (
                      %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                    ON CONFLICT (simulated_order_id,fill_index) DO NOTHING
                    """,
                    (
                        int(simulated_order_id), int(position_id),
                        "EXIT" if is_exit else "ENTRY", str(side).upper(), symbol,
                        quantity, fill_price, notional, fee_usdc, quote_asset,
                        fee_usdc, identity_id, instrument_id, environment,
                        deployment_id, SIMULATION_MODEL_VERSION, execution_at,
                        _hash(source_payload),
                    ),
                )
                return cur.rowcount == 1
    finally:
        conn.close()
