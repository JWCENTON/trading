from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional, Tuple

from common.db import get_db_conn


@dataclass(frozen=True)
class RegimeGateDecision:
    allow: bool
    why: str                  # ENUM: POLICY_ALLOW / POLICY_BLOCK / POLICY_WOULD_BLOCK / REGIME_DISABLED / NO_REGIME_STATE
    regime: Optional[str]
    mode: Optional[str]
    would_block: Optional[bool]
    meta: dict


def get_current_regime(symbol: str, interval: str) -> Optional[str]:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT regime FROM regime_state WHERE symbol=%s AND interval=%s",
        (symbol, interval),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None


def get_policy(strategy: str, regime: str) -> Tuple[bool, Optional[str]]:
    """
    Returns: (allow_entry, note)
    """
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT allow_entry, note FROM regime_policy WHERE strategy=%s AND regime=%s",
        (strategy, regime),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        # Should not happen after your autofill, but keep safe
        return True, "MISSING_POLICY_ROW_DEFAULT_ALLOW"
    return bool(row[0]), (row[1] if row[1] is not None else None)


def decide_regime_gate(
    *,
    symbol: str,
    interval: str,
    strategy: str,
    decision: str,
    regime_enabled: bool,
    regime_mode: str,   # DRY_RUN or ENFORCE
) -> RegimeGateDecision:
    """
    Canonical SSOT for regime gating.
    - why is a short ENUM
    - detailed note goes to meta, NOT to why
    """
    if not regime_enabled:
        return RegimeGateDecision(
            allow=True,
            why="REGIME_DISABLED",
            regime=None,
            mode=regime_mode,
            would_block=False,
            meta={"strategy": strategy, "decision": decision},
        )

    regime = get_current_regime(symbol, interval)
    if not regime or regime == "UNKNOWN":
        return RegimeGateDecision(
            allow=True,   # default allow, but explicitly logged
            why="NO_REGIME_STATE",
            regime=regime,
            mode=regime_mode,
            would_block=False,
            meta={"strategy": strategy, "decision": decision},
        )

    allow_entry, note = get_policy(strategy, regime)

    if allow_entry:
        return RegimeGateDecision(
            allow=True,
            why="POLICY_ALLOW",
            regime=regime,
            mode=regime_mode,
            would_block=False,
            meta={
                "strategy": strategy,
                "decision": decision,
                "policy_note": note,
            },
        )

    # policy says block
    if (regime_mode or "").upper() == "ENFORCE":
        return RegimeGateDecision(
            allow=False,
            why="POLICY_BLOCK",
            regime=regime,
            mode=regime_mode,
            would_block=True,
            meta={
                "strategy": strategy,
                "decision": decision,
                "policy_note": note,
            },
        )

    # DRY_RUN: allow but mark would_block
    return RegimeGateDecision(
        allow=True,
        why="POLICY_WOULD_BLOCK",
        regime=regime,
        mode=regime_mode,
        would_block=True,
        meta={
            "strategy": strategy,
            "decision": decision,
            "policy_note": note,
        },
    )


def emit_regime_gate_event(
    *,
    symbol: str,
    interval: str,
    strategy: str,
    decision: str,
    d: RegimeGateDecision,
) -> None:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO regime_gate_events(symbol, interval, strategy, decision, allow, regime, mode, would_block, why, meta)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        """,
        (
            symbol,
            interval,
            strategy,
            decision,
            d.allow,
            d.regime,
            d.mode,
            d.would_block,
            d.why,
            json.dumps(d.meta or {}),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()