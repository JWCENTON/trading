from common.user_settings import get_user_settings_snapshot, upsert_user_settings
import os
import math
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict

import psycopg2
from psycopg2.errors import UndefinedTable
from fastapi.encoders import jsonable_encoder
from fastapi import FastAPI, HTTPException, Query, Request, Response, Depends, Cookie, Header, status
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from binance.client import Client
from contextlib import contextmanager

import logging
import time
import json
import threading
import requests
from openai import OpenAI
import secrets
import hashlib


DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "trading")
DB_USER = os.environ.get("DB_USER", "botuser")
DB_PASS = os.environ.get("DB_PASS", "botpass")
PAPER_START_USDT = float(os.environ.get("PAPER_START_USDT", "1000"))
API_BUILD = os.environ.get("API_BUILD", "dev-unknown")

FEE_RATE = float(os.environ.get("PAPER_FEE_RATE", "0.0004"))       # 0.04% (taker-ish)
SLIPPAGE_RATE = float(os.environ.get("PAPER_SLIPPAGE_RATE", "0.0002"))  # 0.02%

BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# Jeśli będziesz kiedyś odpalał API na innym porcie/host, ustaw ENV.
# W kontenerze "api" localhost jest OK, bo wątek działa w tym samym procesie.
INTERNAL_API_BASE = os.environ.get("INTERNAL_API_BASE", "http://127.0.0.1:8000")

QUOTE_ASSET = os.environ.get("QUOTE_ASSET", "USDC").upper()
ALL_INTERVALS = (os.environ.get("ALL_INTERVALS","1m,5m")).split(",")
def sym(base: str) -> str:
    return f"{base.upper()}{QUOTE_ASSET}"

ALL_SYMBOLS: list[str] = [
    sym("BTC"), sym("ETH"), sym("SOL"), sym("BNB")
]
DEFAULT_SYMBOL = os.environ.get("DEFAULT_SYMBOL", sym("BTC"))

ALL_STRATEGIES: list[str] = ["RSI", "TREND", "BBRANGE", "SUPERTREND"]

SESSION_COOKIE_NAME = os.environ.get("SESSION_COOKIE_NAME", "trading_session")
SESSION_TTL_HOURS = int(os.environ.get("SESSION_TTL_HOURS", "24"))
SESSION_COOKIE_SECURE = os.environ.get("SESSION_COOKIE_SECURE", "false").lower() == "true"
SESSION_COOKIE_SAMESITE = os.environ.get("SESSION_COOKIE_SAMESITE", "lax")

LOGIN_LOCK_WINDOW_MINUTES = int(os.environ.get("LOGIN_LOCK_WINDOW_MINUTES", "15"))
LOGIN_LOCK_MAX_FAILED = int(os.environ.get("LOGIN_LOCK_MAX_FAILED", "5"))

# Uwaga: tu mają być tylko paramy, które realnie chcesz pozwolić AI zmieniać.
ALLOWED_PARAMS: dict[str, set[str]] = {
    "RSI": {
        "RSI_OVERSOLD",
        "RSI_OVERBOUGHT",
        "STOP_LOSS_PCT",
        "TAKE_PROFIT_PCT",
        "MAX_POSITION_MINUTES",
        "DAILY_MAX_LOSS_PCT",
    },
    "TREND": {
        "STOP_LOSS_PCT",
        "TAKE_PROFIT_PCT",
        "MAX_POSITION_MINUTES",
        "DAILY_MAX_LOSS_PCT",
        "ENTRY_BUFFER_PCT",
        "TREND_FILTER_PCT",
    },
    "BBRANGE": {
        "STOP_LOSS_PCT",
        "TAKE_PROFIT_PCT",
        "MAX_POSITION_MINUTES",
        "DAILY_MAX_LOSS_PCT",
        "BB_PERIOD",
        "BB_STD",
        "MIN_BB_WIDTH_PCT",
        "RSI_LONG_MAX",
        "RSI_SHORT_MIN",
        "RSI_BLOCK_EXTREME_LOW",
        "RSI_BLOCK_EXTREME_HIGH",
    },
    "SUPERTREND": {
        "ATR_PERIOD",
        "ST_MULTIPLIER",
        "STOP_LOSS_PCT",
        "TAKE_PROFIT_PCT",
        "MAX_POSITION_MINUTES",
        "DAILY_MAX_LOSS_PCT",
        "MIN_ATR_PCT",
        "TREND_BUFFER",
        "RSI_LONG_MAX",
        "RSI_SHORT_MIN",
        "RSI_BLOCK_EXTREME_LOW",
        "RSI_BLOCK_EXTREME_HIGH",
        "ORDER_QTY_BTC",
    },
}

def _parse_allowed_origins() -> list[str]:
    raw = os.environ.get("ALLOWED_ORIGINS", "").strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]

ALLOWED_ORIGINS = _parse_allowed_origins()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

AI_TUNE_INTERVAL_MINUTES = int(os.environ.get("AI_TUNE_INTERVAL_MINUTES", "1440"))
AI_AUTO_APPLY = os.environ.get("AI_AUTO_APPLY", "false").lower() == "true"

ENVIRONMENT = os.environ.get("ENVIRONMENT", "").lower()
TRADING_MODE = os.environ.get("TRADING_MODE", "").upper()

ALLOW_AI_DB_WRITES = (ENVIRONMENT == "paper" and TRADING_MODE == "PAPER" and AI_AUTO_APPLY)

binance_client = (
    Client(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
    if BINANCE_API_KEY and BINANCE_API_SECRET
    else None
)

app = FastAPI(title="Trading Bot API", version="0.1.0")

if ALLOWED_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


class CurrentUser(BaseModel):
    id: int
    username: str
    is_active: bool
    is_admin: bool
    must_change_password: bool


class LoginRequest(BaseModel):
    username: str
    password: str


class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str


class AuthMeResponse(BaseModel):
    authenticated: bool
    user: CurrentUser | None = None


class Candle(BaseModel):
    open_time: datetime
    close: float
    ema_21: Optional[float] = None
    rsi_14: Optional[float] = None


class CandleSummary(BaseModel):
    symbol: str
    interval: str
    open_time: datetime
    close: float
    ema_21: Optional[float]
    rsi_14: Optional[float]
    atr_14: Optional[float] = None
    supertrend: Optional[float] = None
    supertrend_direction: Optional[int] = None


class SimulatedOrder(BaseModel):
    id: int
    created_at: datetime
    symbol: str
    interval: str
    side: str
    price: float
    quantity_btc: float
    reason: Optional[str] = None
    rsi_14: Optional[float] = None
    ema_21: Optional[float] = None
    candle_open_time: datetime


class EquityPoint(BaseModel):
    time: datetime
    equity_usdt: float


class PnLSummary(BaseModel):
    start_balance_usdt: float
    final_balance_usdt: float
    pnl_usdt: float
    pnl_pct: float
    trades: int
    history: List[EquityPoint]
    fees_usdt_est: float = 0.0
    slippage_usdt_est: float = 0.0
    pnl_net_usdt_est: float = 0.0
    pnl_net_pct_est: float = 0.0
    final_balance_net_usdt_est: float = 0.0


class TradeWithPnl(BaseModel):
    id: int
    created_at: datetime
    symbol: str
    interval: str
    side: str
    price: float
    quantity_btc: float
    rsi_14: Optional[float]
    ema_21: Optional[float]
    candle_open_time: datetime
    equity_after: float
    pnl_trade: float
    pnl_cumulative: float


class RoundtripTrade(BaseModel):
    id: int
    direction: str
    entry_order_id: int
    exit_order_id: int
    entry_time: datetime
    exit_time: datetime
    qty_btc: float
    entry_price: float
    exit_price: float
    pnl_usdt: float
    entry_rsi_14: Optional[float]
    entry_ema_21: Optional[float]


class RoundtripPage(BaseModel):
    total: int
    items: List[RoundtripTrade]


class SimulatedOrderPage(BaseModel):
    total: int
    items: List[SimulatedOrder]


class AssetBalance(BaseModel):
    asset: str
    free: float
    locked: float
    total: float
    total_usdt: float


class AccountSummary(BaseModel):
    total_usdt: float
    balances: List[AssetBalance]


class UIAccountSummary(BaseModel):
    total_account_value_usdc: float
    quote_asset: str
    assets: Dict[str, float]
    asset_values_usdc: Dict[str, float]
    updated_at: datetime


class UITrading24hSummary(BaseModel):
    closed_pnl_24h: float
    trades_24h: int
    wins_24h: int
    losses_24h: int
    win_rate_24h: float
    updated_at: datetime


class StrategyMetrics(BaseModel):
    symbol: str
    interval: str
    strategy: str
    trades: int
    winning_trades: int
    losing_trades: int
    winrate_pct: float
    total_pnl_usdt: float
    avg_pnl_usdt: float
    max_drawdown_pct: float
    sharpe_ratio: Optional[float]
    z_score: Optional[float]


class WatchdogEvent(BaseModel):
    id: int
    created_at: datetime
    symbol: str
    interval: Optional[str] = None
    strategy: Optional[str] = None
    severity: str
    event: str
    details: Optional[dict] = None


class WatchdogEventPage(BaseModel):
    total: int
    items: List[WatchdogEvent]


class AIAnalyzeRequest(BaseModel):
    prompt: str


class AIAnalyzeResponse(BaseModel):
    analysis: str


class StrategyLeaderboardRow(BaseModel):
    symbol: str
    interval: str
    strategy: str
    trades: int
    winrate_pct: float
    total_pnl_usdt: float
    max_drawdown_pct: float
    sharpe_ratio: Optional[float]
    z_score: Optional[float]
    score: float


class RegimePoint(BaseModel):
    symbol: str
    interval: str
    ts: datetime
    regime: Optional[str] = None
    vol_regime: Optional[str] = None
    trend_dir: Optional[int] = None
    trend_strength_pct: Optional[float] = None
    atr_pct: Optional[float] = None
    shock_z: Optional[float] = None


class UIPanicControlRequest(BaseModel):
    enabled: bool
    reason: Optional[str] = None


class UIControlResponse(BaseModel):
    ok: bool
    message: str
    applied_at: datetime
    new_state: Dict


class UISlotControlRequest(BaseModel):
    symbol: str
    interval: str
    strategy: str
    enabled: Optional[bool] = None
    live_orders_enabled: Optional[bool] = None
    reason: Optional[str] = None


class UIRegimeControlRequest(BaseModel):
    symbol: str
    interval: str
    strategy: str
    regime_enabled: bool
    regime_mode: str
    reason: Optional[str] = None


class UISlotManualControlRequest(BaseModel):
    symbol: str
    interval: str
    strategy: str
    enabled: bool
    live_orders_enabled: bool
    regime_enabled: bool = True
    regime_mode: str
    reason: Optional[str] = None


class UISlotAutoControlRequest(BaseModel):
    symbol: str
    interval: str
    strategy: str
    reason: Optional[str] = None


def _safe_float(value):
    return float(value) if value is not None else None


def _safe_int(value, default=0):
    return int(value) if value is not None else default


def _normalize_regime_mode(value: Optional[str]) -> str:
    mode = str(value or "DRY_RUN").strip().upper()
    return mode if mode in {"DRY_RUN", "ENFORCE"} else "DRY_RUN"


def ensure_ui_audit_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ui_audit_log (
          id BIGSERIAL PRIMARY KEY,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          actor TEXT NOT NULL,
          actor_role TEXT,
          action TEXT NOT NULL,
          target_type TEXT NOT NULL,
          target_key TEXT NOT NULL,
          before_json JSONB,
          after_json JSONB,
          source TEXT,
          note TEXT
        );
        """
    )


def log_ui_action(cur, *, actor: str, actor_role: Optional[str], action: str, target_type: str, target_key: str, before_json: Optional[dict], after_json: Optional[dict], source: Optional[str], note: Optional[str]):
    ensure_ui_audit_table(cur)
    before_payload = json.dumps(jsonable_encoder(before_json)) if before_json is not None else None
    after_payload = json.dumps(jsonable_encoder(after_json)) if after_json is not None else None
    cur.execute(
        """
        INSERT INTO ui_audit_log (
          actor, actor_role, action, target_type, target_key, before_json, after_json, source, note
        )
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
        """,
        (
            actor,
            actor_role,
            action,
            target_type,
            target_key,
            before_payload,
            after_payload,
            source,
            note,
        ),
    )


def get_request_actor(request: Optional[Request]) -> tuple[str, Optional[str], str]:
    if request is None:
        return ("system", "system", "api")

    current_user = getattr(request.state, "current_user", None)
    if current_user is not None:
        role = "admin" if current_user.is_admin else "user"
        return (current_user.username, role, "session")

    return ("anonymous", None, "api")


def compute_max_drawdown(equity_curve: List[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for v in equity_curve:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100.0
        if dd > max_dd:
            max_dd = dd
    return max_dd


def compute_sharpe(returns: List[float]) -> Optional[float]:
    n = len(returns)
    if n < 2:
        return None
    mean_r = sum(returns) / n
    var = sum((r - mean_r) ** 2 for r in returns) / (n - 1)
    std = math.sqrt(var)
    if std == 0:
        return None
    return mean_r / std


def compute_z_score(wins: int, losses: int) -> Optional[float]:
    n = wins + losses
    if n == 0:
        return None
    p_hat = wins / n
    p0 = 0.5
    denom = math.sqrt(p0 * (1 - p0) / n)
    if denom == 0:
        return None
    return (p_hat - p0) / denom


def load_current_params(symbol: str, strategy: str, interval: str) -> Dict[str, float]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT param_name, param_value
        FROM strategy_params
        WHERE symbol = %s AND strategy = %s AND interval = %s
        """,
        (symbol, strategy, interval),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {name: float(val) for (name, val) in rows}


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def apply_human_rules(strategy: str,
                      baseline: Dict[str, float],
                      ai_params: Dict[str, float]) -> Dict[str, float]:
    """
    - clamp do sensownych zakresów
    - limit zmiany vs baseline
    - relacje między paramami
    """
    final: Dict[str, float] = {}

    def limit_change(name: str, v: float,
                     min_val: float, max_val: float,
                     max_change_ratio: float = 0.3) -> float:
        v = clamp(v, min_val, max_val)
        base = baseline.get(name)
        if base is not None and base > 0:
            lo = base * (1.0 - max_change_ratio)
            hi = base * (1.0 + max_change_ratio)
            v = clamp(v, lo, hi)
        return v

    for name, value in ai_params.items():
        v = float(value)

        if name in ("RSI_OVERSOLD", "RSI_OVERBOUGHT"):
            v = limit_change(name, v, 5.0, 95.0, max_change_ratio=0.3)
            final[name] = v

        elif name == "STOP_LOSS_PCT":
            # 0.05–5.0 (%)
            v = limit_change(name, v, 0.05, 5.0, max_change_ratio=0.5)
            final[name] = v

        elif name == "TAKE_PROFIT_PCT":
            # 0.1–10.0 (%)
            v = limit_change(name, v, 0.1, 10.0, max_change_ratio=0.5)
            final[name] = v

        elif name == "MAX_POSITION_MINUTES":
            v = limit_change(name, v, 5.0, 1440.0, max_change_ratio=0.5)
            final[name] = v

        elif name == "DAILY_MAX_LOSS_PCT":
            # 0.0–10.0 (0 wyłącza)
            v = limit_change(name, v, 0.0, 10.0, max_change_ratio=0.5)
            final[name] = v

        elif name in ("ENTRY_BUFFER_PCT", "TREND_FILTER_PCT"):
            # ułamek: 0.0000–0.0200 (0–2.0%)
            v = limit_change(name, v, 0.0, 0.02, max_change_ratio=0.5)
            final[name] = v

        elif name == "BB_PERIOD":
            v = limit_change(name, v, 10.0, 200.0, max_change_ratio=0.5)
            final[name] = v

        elif name == "BB_STD":
            v = limit_change(name, v, 1.0, 4.0, max_change_ratio=0.5)
            final[name] = v

        elif name == "MIN_BB_WIDTH_PCT":
            # ułamek: 0.0005–0.02 (0.05%–2.0%)
            v = limit_change(name, v, 0.0005, 0.02, max_change_ratio=0.5)
            final[name] = v

        elif name == "ATR_PERIOD":
            v = limit_change(name, v, 5.0, 100.0, max_change_ratio=0.5)
            final[name] = v

        elif name == "ST_MULTIPLIER":
            v = limit_change(name, v, 1.0, 10.0, max_change_ratio=0.5)
            final[name] = v

        elif name == "MIN_ATR_PCT":
            # tutaj trzymasz "0.25" jako 0.25% (bo porównujesz do ATR%),
            # więc sensownie: 0.05–5.0
            v = limit_change(name, v, 0.05, 5.0, max_change_ratio=0.5)
            final[name] = v

        elif name == "TREND_BUFFER":
            # ułamek: 0.0001–0.02
            v = limit_change(name, v, 0.0001, 0.02, max_change_ratio=0.5)
            final[name] = v

        elif name == "RSI_LONG_MAX":
            v = limit_change(name, v, 40.0, 95.0, max_change_ratio=0.3)
            final[name] = v

        elif name == "RSI_SHORT_MIN":
            v = limit_change(name, v, 5.0, 60.0, max_change_ratio=0.3)
            final[name] = v

        elif name == "RSI_BLOCK_EXTREME_LOW":
            v = limit_change(name, v, 0.0, 30.0, max_change_ratio=0.3)
            final[name] = v

        elif name == "RSI_BLOCK_EXTREME_HIGH":
            v = limit_change(name, v, 70.0, 100.0, max_change_ratio=0.3)
            final[name] = v

        elif name == "ORDER_QTY_BTC":
            v = limit_change(name, v, 0.00001, 1.0, max_change_ratio=0.5)
            final[name] = v

        else:
            final[name] = v

    # Relacje
    if "RSI_OVERSOLD" in final and "RSI_OVERBOUGHT" in final:
        lo = final["RSI_OVERSOLD"]
        hi = final["RSI_OVERBOUGHT"]
        if lo >= hi - 5.0:
            mid = (lo + hi) / 2.0
            final["RSI_OVERSOLD"] = clamp(mid - 7.0, 5.0, 50.0)
            final["RSI_OVERBOUGHT"] = clamp(mid + 7.0, 50.0, 95.0)

    if "STOP_LOSS_PCT" in final and "TAKE_PROFIT_PCT" in final:
        sl = final["STOP_LOSS_PCT"]
        tp = final["TAKE_PROFIT_PCT"]
        if tp < sl * 1.1:
            final["TAKE_PROFIT_PCT"] = sl * 1.2

    return final


def normalize_strategy_name(s: str) -> str:
    x = (s or "").strip().upper()
    if x == "SUPER_TREND":
        return "SUPERTREND"
    return x


def upsert_strategy_params(symbol: str, strategy: str, interval: str, params: Dict[str, float], source: str = "AI"):
    if source == "AI" and not ALLOW_AI_DB_WRITES:
        logging.warning(
            "AI tuner: blocked DB write (ENVIRONMENT=%s TRADING_MODE=%s AI_AUTO_APPLY=%s) for %s %s %s params=%s",
            ENVIRONMENT, TRADING_MODE, AI_AUTO_APPLY, symbol, strategy, interval, list(params.keys())
        )
        return
    conn = get_conn()
    cur = conn.cursor()

    for name, value in params.items():
        cur.execute(
            """
            SELECT param_value FROM strategy_params
            WHERE symbol = %s AND strategy = %s AND interval = %s AND param_name = %s
            """,
            (symbol, strategy, interval, name),
        )
        row = cur.fetchone()
        old_value = float(row[0]) if row else None

        cur.execute(
            """
            INSERT INTO strategy_params (symbol, strategy, interval, param_name, param_value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (symbol, strategy, interval, param_name)
            DO UPDATE SET param_value = EXCLUDED.param_value,
                          updated_at = now()
            """,
            (symbol, strategy, interval, name, float(value)),
        )

        cur.execute(
            """
            INSERT INTO strategy_params_history (
                symbol, strategy, interval, param_name, old_value, new_value, source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (symbol, strategy, interval, name, old_value, float(value), source),
        )

    conn.commit()
    cur.close()
    conn.close()


def fetch_metrics_and_losers(symbol: str, strategy: str, interval: str):
    params = {"symbol": symbol, "interval": interval, "strategy": strategy}

    internal_token = os.environ.get("INTERNAL_API_TOKEN")
    headers = {"X-Internal-Token": internal_token} if internal_token else {}

    m_resp = requests.get(
        f"{INTERNAL_API_BASE}/simulated/metrics",
        params=params,
        headers=headers,
        timeout=10,
    )
    m_resp.raise_for_status()
    metrics = m_resp.json()

    rt_params = {**params, "losers_only": True, "limit": 20, "offset": 0}
    r_resp = requests.get(
        f"{INTERNAL_API_BASE}/simulated/roundtrips",
        params=rt_params,
        headers=headers,
        timeout=10,
    )
    r_resp.raise_for_status()
    roundtrips_page = r_resp.json()
    losing_trades = roundtrips_page.get("items", [])

    return metrics, losing_trades


def build_ai_prompt(symbol: str, strategy: str, interval: str, metrics: dict, losing_trades: list[dict]) -> str:
    trades_lines = []
    for idx, rt in enumerate(losing_trades, start=1):
        entry_price = rt["entry_price"]
        exit_price = rt["exit_price"]
        direction = rt["direction"]
        pnl_usdt = rt["pnl_usdt"]
        entry_rsi = rt.get("entry_rsi_14")
        entry_ema21 = rt.get("entry_ema_21")

        if entry_price != 0:
            pnl_pct = ((exit_price - entry_price) / entry_price * 100.0) if direction == "LONG" else (
                (entry_price - exit_price) / entry_price * 100.0
            )
        else:
            pnl_pct = 0.0

        trades_lines.append(
            f"#{idx} | Direction={direction} | EntryTime={rt['entry_time']} | ExitTime={rt['exit_time']} | "
            f"EntryPrice={entry_price:.2f} | ExitPrice={exit_price:.2f} | QtyBTC={rt['qty_btc']:.8f} | "
            f"PnL_USDT={pnl_usdt:.4f} | Approx_PnL_%={pnl_pct:.2f} | "
            f"Entry_RSI={entry_rsi if entry_rsi is not None else 'NA'} | "
            f"Entry_EMA21={entry_ema21 if entry_ema21 is not None else 'NA'}"
        )

    trades_text = "\n".join(trades_lines)
    allowed = sorted(list(ALLOWED_PARAMS.get(strategy, set())))

    prompt = f"""
        You are an experienced quantitative trading assistant.

        Return ONLY a JSON object (no markdown, no extra text) with schema:
        {{
        "symbol": "{symbol}",
        "strategy": "{strategy}",
        "interval": "{interval}",
        "params": {{ ... }},
        "notes": "Short explanation in English."
        }}

        Rules:
        - Only include these allowed params for this strategy: {allowed}
        - ENTRY_BUFFER_PCT / TREND_FILTER_PCT / TREND_BUFFER / MIN_BB_WIDTH_PCT are FRACTIONS (e.g. 0.002 = 0.2%).
        - STOP_LOSS_PCT / TAKE_PROFIT_PCT / DAILY_MAX_LOSS_PCT are PERCENT values (e.g. 0.8 = 0.8%).
        - Keep values realistic.

        Metrics:
        {json.dumps(metrics, indent=2)}

        Losing roundtrips (latest up to {len(losing_trades)}):
        {trades_text}
        """
    return prompt


def run_ai_tuning_for_pair(symbol: str, strategy: str, interval: str):
    if openai_client is None:
        return

    metrics, losing_trades = fetch_metrics_and_losers(symbol, strategy, interval)

    if not losing_trades or metrics.get("trades", 0) < 20:
        logging.info(
            "AI tuner: skipping %s %s (trades=%s, losing_trades=%s)",
            symbol, strategy, metrics.get("trades", 0), len(losing_trades),
        )
        return

    baseline_params = load_current_params(symbol, strategy, interval)
    prompt = build_ai_prompt(symbol, strategy, interval, metrics, losing_trades)

    try:
        resp = openai_client.responses.create(
            model=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini"),
            input=prompt,
        )
    except Exception as e:
        logging.exception("AI tuner: OpenAI error for %s %s: %s", symbol, strategy, e)
        return

    text = resp.output_text

    try:
        data = json.loads(text)
    except Exception as e:
        logging.exception(
            "AI tuner: failed to parse JSON for %s %s, raw text=%r, error=%s",
            symbol, strategy, text, e,
        )
        return

    params = data.get("params", {})
    if not isinstance(params, dict) or not params:
        logging.warning("AI tuner: empty/invalid params for %s %s: %r", symbol, strategy, params)
        return

    allowed_for_strategy = ALLOWED_PARAMS.get(strategy, set())
    filtered_params: Dict[str, float] = {}
    for name, value in params.items():
        if name not in allowed_for_strategy:
            continue
        try:
            filtered_params[name] = float(value)
        except (TypeError, ValueError):
            continue

    if not filtered_params:
        logging.warning("AI tuner: after filtering, no valid params for %s %s", symbol, strategy)
        return

    final_params = apply_human_rules(strategy, baseline_params, filtered_params)

    logging.info(
        "AI tuner: applying params for %s %s: %s (baseline=%s, raw_ai=%s)",
        symbol, strategy, final_params, baseline_params, filtered_params,
    )
    upsert_strategy_params(symbol, strategy, interval, final_params, source="AI")


def ai_auto_tuner_loop():
    if openai_client is None:
        logging.warning("AI tuner: OPENAI_API_KEY not configured, skipping.")
        return
    if not ALLOW_AI_DB_WRITES:
        logging.info(
            "AI tuner: auto-apply disabled (ENVIRONMENT=%s TRADING_MODE=%s AI_AUTO_APPLY=%s) – tuner disabled.",
            ENVIRONMENT, TRADING_MODE, AI_AUTO_APPLY
        )
        return

    logging.info("AI tuner: starting auto-tuner loop, interval=%d minutes", AI_TUNE_INTERVAL_MINUTES)

    # mały delay, żeby API na pewno już działało zanim zaczniemy requests do siebie
    time.sleep(3)

    while True:
        start = time.perf_counter()
        try:
            for symbol in ALL_SYMBOLS:
                for strategy in ALL_STRATEGIES:
                    for interval in ALL_INTERVALS:
                        logging.info("AI tuner: processing %s %s %s", symbol, strategy, interval)
                        run_ai_tuning_for_pair(symbol, strategy, interval)
        except Exception as e:
            logging.exception("AI tuner: error in tuning cycle: %s", e)

        elapsed = time.perf_counter() - start
        logging.info("AI tuner: cycle finished in %.2f s, sleeping %d minutes", elapsed, AI_TUNE_INTERVAL_MINUTES)
        time.sleep(AI_TUNE_INTERVAL_MINUTES * 60)


@contextmanager
def db_cursor():
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        yield conn, cur
    finally:
        if cur is not None:
            try: cur.close()
            except Exception: pass
        if conn is not None:
            try: conn.close()
            except Exception: pass



def get_client_ip(request: Request) -> Optional[str]:
    forwarded_for = request.headers.get("x-forwarded-for") if request else None
    if forwarded_for:
        return forwarded_for.split(",")[0].strip() or None
    real_ip = request.headers.get("x-real-ip") if request else None
    if real_ip:
        return real_ip.strip() or None
    return request.client.host if request and request.client else None


def get_user_agent(request: Request) -> Optional[str]:
    return request.headers.get("user-agent") if request else None


def record_auth_event(
    *,
    action: str,
    result: str,
    request: Request,
    user_id: Optional[int] = None,
    username: Optional[str] = None,
    reason: Optional[str] = None,
) -> None:
    try:
        with db_cursor() as (conn, cur):
            cur.execute(
                """
                INSERT INTO auth_login_events (
                  created_at, user_id, username, action, result, ip, user_agent, reason
                )
                VALUES (now(), %s, %s, %s, %s, %s, %s, %s)
                """,
                (user_id, username, action, result, get_client_ip(request), get_user_agent(request), reason),
            )
            conn.commit()
    except Exception as exc:
        logging.warning("auth event write failed: %s", exc)


def count_recent_failed_logins(*, request: Request, username: str) -> int:
    client_ip = get_client_ip(request)
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT count(*)
            FROM auth_login_events
            WHERE action = 'LOGIN_FAILED'
              AND result = 'FAIL'
              AND created_at > now() - (%s || ' minutes')::interval
              AND (lower(username) = lower(%s) OR ip = %s)
            """,
            (LOGIN_LOCK_WINDOW_MINUTES, username, client_ip),
        )
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0


def assert_login_not_locked(*, request: Request, username: str) -> None:
    if count_recent_failed_logins(request=request, username=username) >= LOGIN_LOCK_MAX_FAILED:
        record_auth_event(
            action="LOGIN_FAILED",
            result="FAIL",
            request=request,
            username=username,
            reason="TEMPORARY_LOCK",
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="too many failed login attempts; try again later",
        )


def hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def verify_password(password: str, password_hash: str) -> bool:
    with db_cursor() as (_conn, cur):
        cur.execute("SELECT crypt(%s, %s) = %s", (password, password_hash, password_hash))
        row = cur.fetchone()
        return bool(row[0]) if row else False


def hash_password(password: str) -> str:
    with db_cursor() as (_conn, cur):
        cur.execute("SELECT crypt(%s, gen_salt('bf', 12))", (password,))
        row = cur.fetchone()
        if not row or not row[0]:
            raise HTTPException(status_code=500, detail="password hashing failed")
        return str(row[0])


def create_session(*, user_id: int, request: Request) -> tuple[str, datetime]:
    raw_token = secrets.token_urlsafe(48)
    token_hash = hash_session_token(raw_token)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=SESSION_TTL_HOURS)

    client_ip = get_client_ip(request)
    user_agent = get_user_agent(request)

    with db_cursor() as (conn, cur):
        cur.execute(
            """
            INSERT INTO auth_sessions (
              user_id, session_token_hash, created_at, expires_at, revoked_at, last_seen_at, ip, user_agent
            )
            VALUES (%s, %s, now(), %s, NULL, now(), %s, %s)
            """,
            (user_id, token_hash, expires_at, client_ip, user_agent),
        )
        conn.commit()

    return raw_token, expires_at


def set_session_cookie(response: Response, raw_token: str, expires_at: datetime) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=raw_token,
        httponly=True,
        secure=SESSION_COOKIE_SECURE,
        samesite=SESSION_COOKIE_SAMESITE,
        expires=expires_at,
        path="/",
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
    )


def get_current_user_from_session_token(raw_token: Optional[str]) -> Optional[CurrentUser]:
    if not raw_token:
        return None

    token_hash = hash_session_token(raw_token)

    with db_cursor() as (conn, cur):
        cur.execute(
            """
            SELECT
              u.id,
              u.username,
              u.is_active,
              u.is_admin,
              u.must_change_password,
              s.id
            FROM auth_sessions s
            JOIN users u
              ON u.id = s.user_id
            WHERE s.session_token_hash = %s
              AND s.revoked_at IS NULL
              AND s.expires_at > now()
            LIMIT 1
            """,
            (token_hash,),
        )
        row = cur.fetchone()

        if not row:
            return None

        session_id = row[5]

        cur.execute(
            """
            UPDATE auth_sessions
            SET last_seen_at = now()
            WHERE id = %s
            """,
            (session_id,),
        )
        conn.commit()

        if not bool(row[2]):
            return None

        return CurrentUser(
            id=int(row[0]),
            username=str(row[1]),
            is_active=bool(row[2]),
            is_admin=bool(row[3]),
            must_change_password=bool(row[4]),
        )


def revoke_session(raw_token: Optional[str]) -> None:
    if not raw_token:
        return

    token_hash = hash_session_token(raw_token)

    with db_cursor() as (conn, cur):
        cur.execute(
            """
            UPDATE auth_sessions
            SET revoked_at = now()
            WHERE session_token_hash = %s
              AND revoked_at IS NULL
            """,
            (token_hash,),
        )
        conn.commit()


def revoke_other_sessions(*, user_id: int, current_raw_token: Optional[str]) -> None:
    current_hash = hash_session_token(current_raw_token) if current_raw_token else None

    with db_cursor() as (conn, cur):
        if current_hash:
            cur.execute(
                """
                UPDATE auth_sessions
                SET revoked_at = now()
                WHERE user_id = %s
                  AND revoked_at IS NULL
                  AND session_token_hash <> %s
                """,
                (user_id, current_hash),
            )
        else:
            cur.execute(
                """
                UPDATE auth_sessions
                SET revoked_at = now()
                WHERE user_id = %s
                  AND revoked_at IS NULL
                """,
                (user_id,),
            )
        conn.commit()


def require_auth(
    request: Request,
    session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME),
) -> CurrentUser:
    user = get_current_user_from_session_token(session_token)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="authentication required",
        )
    request.state.current_user = user
    return user


def require_admin(
    request: Request,
    user: CurrentUser = Depends(require_auth),
) -> CurrentUser:
    if not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="admin required",
        )
    request.state.current_user = user
    return user


def require_auth_or_internal(
    request: Request,
    session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME),
    x_internal_token: Optional[str] = Header(default=None, alias="X-Internal-Token"),
) -> CurrentUser | None:
    user = get_current_user_from_session_token(session_token)
    if user is not None:
        request.state.current_user = user
        return user

    internal_token = os.environ.get("INTERNAL_API_TOKEN")
    if internal_token and x_internal_token == internal_token:
        request.state.current_user = None
        return None

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="authentication required",
    )


@app.get("/health")
def health():
    try:
        with db_cursor() as (_conn, _cur):
            pass
        return {"status": "ok", "build": API_BUILD, "db": DB_NAME, "env": ENVIRONMENT, "mode": TRADING_MODE}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/auth/login", response_model=AuthMeResponse)
def auth_login(payload: LoginRequest, request: Request, response: Response):
    username = (payload.username or "").strip()

    if not username or not payload.password:
        record_auth_event(
            action="LOGIN_FAILED",
            result="FAIL",
            request=request,
            username=username or None,
            reason="MISSING_USERNAME_OR_PASSWORD",
        )
        raise HTTPException(status_code=400, detail="username and password are required")

    assert_login_not_locked(request=request, username=username)

    with db_cursor() as (conn, cur):
        cur.execute(
            """
            SELECT id, username, password_hash, is_active, is_admin, must_change_password
            FROM users
            WHERE username = %s
            LIMIT 1
            """,
            (username,),
        )
        row = cur.fetchone()

    if not row:
        record_auth_event(
            action="LOGIN_FAILED",
            result="FAIL",
            request=request,
            username=username,
            reason="INVALID_CREDENTIALS",
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid credentials")

    user_id = int(row[0])
    db_username = str(row[1])
    password_hash = str(row[2])
    is_active = bool(row[3])
    is_admin = bool(row[4])
    must_change_password = bool(row[5])

    if not is_active:
        record_auth_event(
            action="LOGIN_FAILED",
            result="FAIL",
            request=request,
            user_id=user_id,
            username=db_username,
            reason="USER_INACTIVE",
        )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="user inactive")

    if not verify_password(payload.password, password_hash):
        record_auth_event(
            action="LOGIN_FAILED",
            result="FAIL",
            request=request,
            user_id=user_id,
            username=db_username,
            reason="INVALID_CREDENTIALS",
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid credentials")

    raw_token, expires_at = create_session(user_id=user_id, request=request)
    set_session_cookie(response, raw_token, expires_at)

    with db_cursor() as (conn, cur):
        cur.execute(
            """
            UPDATE users
            SET last_login_at = now()
            WHERE id = %s
            """,
            (user_id,),
        )
        conn.commit()

    user = CurrentUser(
        id=user_id,
        username=db_username,
        is_active=is_active,
        is_admin=is_admin,
        must_change_password=must_change_password,
    )
    request.state.current_user = user
    record_auth_event(
        action="LOGIN_SUCCESS",
        result="OK",
        request=request,
        user_id=user_id,
        username=db_username,
        reason=None,
    )

    return AuthMeResponse(authenticated=True, user=user)


@app.post("/auth/logout")
def auth_logout(
    request: Request,
    response: Response,
    session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME),
):
    user = get_current_user_from_session_token(session_token)
    revoke_session(session_token)
    clear_session_cookie(response)
    if user is not None:
        record_auth_event(
            action="LOGOUT",
            result="OK",
            request=request,
            user_id=user.id,
            username=user.username,
            reason=None,
        )
    request.state.current_user = None
    return {"ok": True}


@app.get("/auth/me", response_model=AuthMeResponse)
def auth_me(
    request: Request,
    session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME),
):
    user = get_current_user_from_session_token(session_token)
    if user is None:
        return AuthMeResponse(authenticated=False, user=None)

    request.state.current_user = user
    return AuthMeResponse(authenticated=True, user=user)


@app.post("/auth/change-password")
def auth_change_password(
    payload: ChangePasswordRequest,
    request: Request,
    user: CurrentUser = Depends(require_auth),
    session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME),
):
    if not payload.old_password or not payload.new_password:
        record_auth_event(action="PASSWORD_CHANGE_FAILED", result="FAIL", request=request, user_id=user.id, username=user.username, reason="MISSING_PASSWORD")
        raise HTTPException(status_code=400, detail="old_password and new_password are required")

    if len(payload.new_password) < 10:
        record_auth_event(action="PASSWORD_CHANGE_FAILED", result="FAIL", request=request, user_id=user.id, username=user.username, reason="NEW_PASSWORD_TOO_SHORT")
        raise HTTPException(status_code=400, detail="new_password must be at least 10 characters")

    if payload.new_password == payload.old_password:
        record_auth_event(action="PASSWORD_CHANGE_FAILED", result="FAIL", request=request, user_id=user.id, username=user.username, reason="NEW_PASSWORD_SAME_AS_OLD")
        raise HTTPException(status_code=400, detail="new_password must be different")

    with db_cursor() as (conn, cur):
        cur.execute(
            """
            SELECT password_hash
            FROM users
            WHERE id = %s
            LIMIT 1
            """,
            (user.id,),
        )
        row = cur.fetchone()

    if not row:
        record_auth_event(action="PASSWORD_CHANGE_FAILED", result="FAIL", request=request, user_id=user.id, username=user.username, reason="USER_NOT_FOUND")
        raise HTTPException(status_code=404, detail="user not found")

    current_hash = str(row[0])
    if not verify_password(payload.old_password, current_hash):
        record_auth_event(action="PASSWORD_CHANGE_FAILED", result="FAIL", request=request, user_id=user.id, username=user.username, reason="INVALID_OLD_PASSWORD")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid old_password")

    new_hash = hash_password(payload.new_password)

    with db_cursor() as (conn, cur):
        cur.execute(
            """
            UPDATE users
            SET password_hash = %s,
                must_change_password = FALSE,
                password_changed_at = now(),
                updated_at = now()
            WHERE id = %s
            """,
            (new_hash, user.id),
        )
        conn.commit()

    revoke_other_sessions(user_id=user.id, current_raw_token=session_token)
    request.state.current_user = CurrentUser(
        id=user.id,
        username=user.username,
        is_active=user.is_active,
        is_admin=user.is_admin,
        must_change_password=False,
    )
    record_auth_event(action="PASSWORD_CHANGED", result="OK", request=request, user_id=user.id, username=user.username, reason=None)

    return {"ok": True, "must_change_password": False}


@app.get("/auth/security-summary")
def auth_security_summary(user: CurrentUser = Depends(require_auth)):
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT last_login_at, password_changed_at
            FROM users
            WHERE id = %s
            LIMIT 1
            """,
            (user.id,),
        )
        user_row = cur.fetchone()

        cur.execute(
            """
            SELECT created_at, ip, user_agent
            FROM auth_login_events
            WHERE user_id = %s
              AND action = 'LOGIN_SUCCESS'
              AND result = 'OK'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (user.id,),
        )
        last_success = cur.fetchone()

        cur.execute(
            """
            SELECT created_at, username, ip, user_agent, reason
            FROM auth_login_events
            WHERE action = 'LOGIN_FAILED'
              AND (user_id = %s OR lower(username) = lower(%s))
            ORDER BY created_at DESC
            LIMIT 10
            """,
            (user.id, user.username),
        )
        failed_rows = cur.fetchall() or []

        cur.execute(
            """
            SELECT count(*)
            FROM auth_sessions
            WHERE user_id = %s
              AND revoked_at IS NULL
              AND expires_at > now()
            """,
            (user.id,),
        )
        active_sessions_row = cur.fetchone()

    return {
        "user_id": user.id,
        "username": user.username,
        "last_login_at": user_row[0] if user_row else None,
        "password_changed_at": user_row[1] if user_row else None,
        "last_successful_login": {
            "created_at": last_success[0] if last_success else None,
            "ip": last_success[1] if last_success else None,
            "user_agent": last_success[2] if last_success else None,
        },
        "failed_logins": [
            {
                "created_at": r[0],
                "username": r[1],
                "ip": r[2],
                "user_agent": r[3],
                "reason": r[4],
            }
            for r in failed_rows
        ],
        "active_sessions": int(active_sessions_row[0] or 0) if active_sessions_row else 0,
    }



@app.get("/candles/latest", response_model=List[Candle])
def get_latest_candles(
    symbol: str = Query(DEFAULT_SYMBOL),
    interval: str = Query("1m"),
    limit: int = Query(50, ge=1, le=500),
    user: CurrentUser = Depends(require_auth),
):
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT open_time, close, ema_21, rsi_14
            FROM candles
            WHERE symbol = %s AND interval = %s
            ORDER BY open_time DESC
            LIMIT %s
            """,
            (symbol, interval, limit),
        )
        rows = cur.fetchall()

        return [
            Candle(
                open_time=row[0],
                close=float(row[1]),
                ema_21=float(row[2]) if row[2] is not None else None,
                rsi_14=float(row[3]) if row[3] is not None else None,
            )
            for row in rows
        ]
    

@app.get("/simulated/orders", response_model=SimulatedOrderPage)
def get_simulated_orders(
    symbol: str = Query(DEFAULT_SYMBOL),
    interval: str = Query("1m"),
    limit: int = Query(50, ge=1, le=50),
    offset: int = Query(0, ge=0),
    strategy: str = Query("RSI"),
    user: CurrentUser = Depends(require_auth),
):
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT COUNT(*)
            FROM simulated_orders
            WHERE symbol = %s AND interval = %s AND strategy = %s
            """,
            (symbol, interval, strategy),
        )
        total = cur.fetchone()[0]

        cur.execute(
            """
            SELECT
                id, created_at, symbol, interval, side, price,
                quantity_btc, reason, rsi_14, ema_21, candle_open_time
            FROM simulated_orders
            WHERE symbol = %s AND interval = %s AND strategy = %s
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (symbol, interval, strategy, limit, offset),
        )
        rows = cur.fetchall()
        items = [
            SimulatedOrder(
                id=row[0],
                created_at=row[1],
                symbol=row[2],
                interval=row[3],
                side=row[4],
                price=float(row[5]),
                quantity_btc=float(row[6]),
                reason=row[7],
                rsi_14=float(row[8]) if row[8] is not None else None,
                ema_21=float(row[9]) if row[9] is not None else None,
                candle_open_time=row[10],
            )
            for row in rows
        ]

        return SimulatedOrderPage(total=total, items=items)


@app.get("/simulated/trades", response_model=List[TradeWithPnl])
def get_simulated_trades(
    symbol: str = Query(DEFAULT_SYMBOL),
    interval: str = Query("1m"),
    strategy: str = Query("RSI"),
    user: CurrentUser = Depends(require_auth),
):
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT
                id, created_at, symbol, interval, side,
                price, quantity_btc, reason, rsi_14, ema_21, candle_open_time
            FROM simulated_orders
            WHERE symbol = %s AND interval = %s AND strategy = %s
            ORDER BY created_at ASC
            """,
            (symbol, interval, strategy),
        )
        rows = cur.fetchall()

        trades: List[TradeWithPnl] = []
        if not rows:
            return trades

        cash = PAPER_START_USDT
        btc = 0.0
        pnl_cum = 0.0

        for row in rows:
            (
                id_,
                created_at,
                symbol,
                interval,
                side,
                price,
                qty_btc,
                _reason,
                rsi_14,
                ema_21,
                candle_open_time,
            ) = row

            price_f = float(price)
            qty_f = float(qty_btc)
            equity_before = cash + btc * price_f

            if side.upper() == "BUY":
                cash -= qty_f * price_f
                btc += qty_f
            elif side.upper() == "SELL":
                cash += qty_f * price_f
                btc -= qty_f

            equity_after = cash + btc * price_f
            pnl_trade = equity_after - equity_before
            pnl_cum += pnl_trade

            trades.append(
                TradeWithPnl(
                    id=id_,
                    created_at=created_at,
                    symbol=symbol,
                    interval=interval,
                    side=side,
                    price=price_f,
                    quantity_btc=qty_f,
                    rsi_14=float(rsi_14) if rsi_14 is not None else None,
                    ema_21=float(ema_21) if ema_21 is not None else None,
                    candle_open_time=candle_open_time,
                    equity_after=equity_after,
                    pnl_trade=pnl_trade,
                    pnl_cumulative=pnl_cum,
                )
            )

        return trades


@app.get("/simulated/roundtrips", response_model=RoundtripPage)
def get_simulated_roundtrips(
    symbol: str = Query(DEFAULT_SYMBOL),
    interval: str = Query("1m"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    losers_only: bool = Query(True),
    strategy: str = Query("RSI"),
    user: CurrentUser | None = Depends(require_auth_or_internal),
):
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT
                id, created_at, symbol, interval, side,
                price, quantity_btc, rsi_14, ema_21, candle_open_time
            FROM simulated_orders
            WHERE symbol = %s AND interval = %s AND strategy = %s
            ORDER BY created_at ASC
            """,
            (symbol, interval, strategy),
        )
        rows = cur.fetchall()

        open_longs: List[Dict] = []
        open_shorts: List[Dict] = []
        roundtrips: List[RoundtripTrade] = []
        rt_id = 1

        for row in rows:
            (
                order_id,
                created_at,
                _symbol,
                _interval,
                side,
                price,
                qty_btc,
                rsi_14,
                ema_21,
                _candle_open_time,
            ) = row

            price_f = float(price)
            qty_f = float(qty_btc)
            side_u = side.upper()

            if side_u == "BUY":
                remaining = qty_f
                while remaining > 0 and open_shorts:
                    o = open_shorts[0]
                    close_qty = min(remaining, o["qty_btc"])
                    remaining -= close_qty
                    o["qty_btc"] -= close_qty

                    entry_price = o["price"]
                    exit_price = price_f
                    pnl = (entry_price - exit_price) * close_qty

                    roundtrips.append(
                        RoundtripTrade(
                            id=rt_id,
                            direction="SHORT",
                            entry_order_id=o["order_id"],
                            exit_order_id=order_id,
                            entry_time=o["created_at"],
                            exit_time=created_at,
                            qty_btc=close_qty,
                            entry_price=entry_price,
                            exit_price=exit_price,
                            pnl_usdt=pnl,
                            entry_rsi_14=o["rsi_14"],
                            entry_ema_21=o["ema_21"],
                        )
                    )
                    rt_id += 1

                    if o["qty_btc"] <= 1e-12:
                        open_shorts.pop(0)

                if remaining > 1e-12:
                    open_longs.append(
                        {
                            "order_id": order_id,
                            "created_at": created_at,
                            "price": price_f,
                            "qty_btc": remaining,
                            "rsi_14": float(rsi_14) if rsi_14 is not None else None,
                            "ema_21": float(ema_21) if ema_21 is not None else None,
                        }
                    )

            elif side_u == "SELL":
                remaining = qty_f
                while remaining > 0 and open_longs:
                    o = open_longs[0]
                    close_qty = min(remaining, o["qty_btc"])
                    remaining -= close_qty
                    o["qty_btc"] -= close_qty

                    entry_price = o["price"]
                    exit_price = price_f
                    pnl = (exit_price - entry_price) * close_qty

                    roundtrips.append(
                        RoundtripTrade(
                            id=rt_id,
                            direction="LONG",
                            entry_order_id=o["order_id"],
                            exit_order_id=order_id,
                            entry_time=o["created_at"],
                            exit_time=created_at,
                            qty_btc=close_qty,
                            entry_price=entry_price,
                            exit_price=exit_price,
                            pnl_usdt=pnl,
                            entry_rsi_14=o["rsi_14"],
                            entry_ema_21=o["ema_21"],
                        )
                    )
                    rt_id += 1

                    if o["qty_btc"] <= 1e-12:
                        open_longs.pop(0)

                if remaining > 1e-12:
                    open_shorts.append(
                        {
                            "order_id": order_id,
                            "created_at": created_at,
                            "price": price_f,
                            "qty_btc": remaining,
                            "rsi_14": float(rsi_14) if rsi_14 is not None else None,
                            "ema_21": float(ema_21) if ema_21 is not None else None,
                        }
                    )

        if losers_only:
            roundtrips = [rt for rt in roundtrips if rt.pnl_usdt < 0]

        roundtrips.sort(key=lambda rt: rt.entry_time, reverse=True)
        total = len(roundtrips)
        items = roundtrips[offset: offset + limit]
        return RoundtripPage(total=total, items=items)


@app.get("/simulated/pnl", response_model=PnLSummary)
def get_simulated_pnl(
    symbol: str = Query(DEFAULT_SYMBOL),
    interval: str = Query("1m"),
    strategy: str = Query("RSI"),
    user: CurrentUser = Depends(require_auth),
):
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT created_at, side, price, quantity_btc
            FROM simulated_orders
            WHERE symbol = %s AND interval = %s AND strategy = %s
            ORDER BY created_at ASC
            """,
            (symbol, interval, strategy),
        )
        rows = cur.fetchall()

        start_balance = PAPER_START_USDT
        cash = start_balance
        btc = 0.0
        fees_est = 0.0
        slip_est = 0.0
        history: List[EquityPoint] = []

        if not rows:
            return PnLSummary(
                start_balance_usdt=start_balance,
                final_balance_usdt=start_balance,
                pnl_usdt=0.0,
                pnl_pct=0.0,
                trades=0,
                history=[],
                fees_usdt_est=0.0,
                slippage_usdt_est=0.0,
                pnl_net_usdt_est=0.0,
                pnl_net_pct_est=0.0,
                final_balance_net_usdt_est=start_balance,
            )

        for created_at, side, price, qty_btc in rows:
            price_f = float(price)
            qty_f = float(qty_btc)

            if side.upper() == "BUY":
                cash -= qty_f * price_f
                btc += qty_f
                notional = abs(qty_f * price_f)
                fees_est += notional * FEE_RATE
                slip_est += notional * SLIPPAGE_RATE
            elif side.upper() == "SELL":
                cash += qty_f * price_f
                btc -= qty_f
                notional = abs(qty_f * price_f)
                fees_est += notional * FEE_RATE
                slip_est += notional * SLIPPAGE_RATE

            equity = cash + btc * price_f
            history.append(EquityPoint(time=created_at, equity_usdt=equity))

        final_equity = history[-1].equity_usdt
        pnl_abs = final_equity - start_balance
        pnl_pct = (pnl_abs / start_balance) * 100.0 if start_balance > 0 else 0.0
        pnl_net = pnl_abs - fees_est - slip_est
        final_net = start_balance + pnl_net
        pnl_net_pct = (pnl_net / start_balance) * 100.0 if start_balance > 0 else 0.0

        history_trimmed = history[-50:]

        return PnLSummary(
            start_balance_usdt=start_balance,
            final_balance_usdt=final_equity,
            pnl_usdt=pnl_abs,
            pnl_pct=pnl_pct,
            trades=len(rows),
            history=history_trimmed,
            fees_usdt_est=fees_est,
            slippage_usdt_est=slip_est,
            pnl_net_usdt_est=pnl_net,
            pnl_net_pct_est=pnl_net_pct,
            final_balance_net_usdt_est=final_net,
        )


@app.get("/simulated/metrics", response_model=StrategyMetrics)
def get_strategy_metrics(
    symbol: str = Query(DEFAULT_SYMBOL),
    interval: str = Query("1m"),
    strategy: str = Query("RSI"),
    user: CurrentUser | None = Depends(require_auth_or_internal),
):
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT created_at, side, price, quantity_btc
            FROM simulated_orders
            WHERE symbol = %s AND interval = %s AND strategy = %s
            ORDER BY created_at ASC
            """,
            (symbol, interval, strategy),
        )
        rows = cur.fetchall()

        trades = len(rows)
        if trades == 0:
            return StrategyMetrics(
                symbol=symbol,
                interval=interval,
                strategy=strategy,
                trades=0,
                winning_trades=0,
                losing_trades=0,
                winrate_pct=0.0,
                total_pnl_usdt=0.0,
                avg_pnl_usdt=0.0,
                max_drawdown_pct=0.0,
                sharpe_ratio=None,
                z_score=None,
            )

        cash = PAPER_START_USDT
        btc = 0.0

        equity_curve: List[float] = []
        returns: List[float] = []
        wins = 0
        losses = 0

        for created_at, side, price, qty_btc in rows:
            price_f = float(price)
            qty_f = float(qty_btc)

            equity_before = cash + btc * price_f

            if side.upper() == "BUY":
                cash -= qty_f * price_f
                btc += qty_f
            elif side.upper() == "SELL":
                cash += qty_f * price_f
                btc -= qty_f

            equity_after = cash + btc * price_f
            pnl_trade = equity_after - equity_before

            equity_curve.append(equity_after)

            if pnl_trade > 0:
                wins += 1
            elif pnl_trade < 0:
                losses += 1

            notional = abs(qty_f * price_f)
            if notional > 0:
                returns.append(pnl_trade / notional)

        final_equity = equity_curve[-1]
        total_pnl_usdt = final_equity - PAPER_START_USDT
        avg_pnl_usdt = total_pnl_usdt / trades
        winrate_pct = (wins / trades) * 100.0 if trades > 0 else 0.0

        max_drawdown_pct = compute_max_drawdown(equity_curve)
        sharpe_ratio = compute_sharpe(returns) if len(returns) >= 2 else None
        z_score = compute_z_score(wins, losses) if (wins + losses) > 0 else None

        return StrategyMetrics(
            symbol=symbol,
            interval=interval,
            strategy=strategy,
            trades=trades,
            winning_trades=wins,
            losing_trades=losses,
            winrate_pct=winrate_pct,
            total_pnl_usdt=total_pnl_usdt,
            avg_pnl_usdt=avg_pnl_usdt,
            max_drawdown_pct=max_drawdown_pct,
            sharpe_ratio=sharpe_ratio,
            z_score=z_score,
        )


@app.get("/candles/summary", response_model=CandleSummary)
def get_summary(
    symbol: str = Query(DEFAULT_SYMBOL),
    interval: str = Query("1m"),
    user: CurrentUser = Depends(require_auth),
):
    with db_cursor() as (_conn, cur):
        cur.execute(
        """
        SELECT symbol, interval, open_time, close, ema_21, rsi_14,
            atr_14, supertrend, supertrend_direction
        FROM candles
        WHERE symbol = %s AND interval = %s
        ORDER BY open_time DESC
        LIMIT 1
        """,
        (symbol, interval),
        )
        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="No candles found")

        return CandleSummary(
            symbol=row[0],
            interval=row[1],
            open_time=row[2],
            close=float(row[3]),
            ema_21=float(row[4]) if row[4] is not None else None,
            rsi_14=float(row[5]) if row[5] is not None else None,
            atr_14=float(row[6]) if row[6] is not None else None,
            supertrend=float(row[7]) if row[7] is not None else None,
            supertrend_direction=int(row[8]) if row[8] is not None else None,
        )


def _load_account_summary() -> AccountSummary:
    if binance_client is None:
        raise HTTPException(status_code=500, detail="Binance API keys not configured for API service")

    try:
        info = binance_client.get_account()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Binance error: {e}")

    balances = info.get("balances", [])
    prices_cache: dict[str, float] = {}

    def get_price_usdt(asset: str) -> float:
        if asset == QUOTE_ASSET:
            return 1.0
        symbol = f"{asset}{QUOTE_ASSET}"
        if symbol in prices_cache:
            return prices_cache[symbol]
        ticker = binance_client.get_symbol_ticker(symbol=symbol)
        price = float(ticker["price"])
        prices_cache[symbol] = price
        return price

    tracked_assets = {"BTC", "ETH", "BNB", QUOTE_ASSET, "SOL"}

    total_usdt = 0.0
    result_balances: list[AssetBalance] = []

    for b in balances:
        asset = b.get("asset")
        if asset not in tracked_assets:
            continue

        free = float(b.get("free", 0.0))
        locked = float(b.get("locked", 0.0))
        total = free + locked
        if total <= 0:
            continue

        try:
            px = get_price_usdt(asset)
        except Exception:
            px = 1.0 if asset == QUOTE_ASSET else 0.0

        value_usdt = total * px
        total_usdt += value_usdt

        result_balances.append(
            AssetBalance(
                asset=asset,
                free=free,
                locked=locked,
                total=total,
                total_usdt=value_usdt,
            )
        )

    return AccountSummary(total_usdt=total_usdt, balances=result_balances)


@app.get("/account/summary", response_model=AccountSummary)
def get_account_summary(user: CurrentUser = Depends(require_auth)):
    return _load_account_summary()


@app.get("/ui/account", response_model=UIAccountSummary)
def ui_account_summary(user: CurrentUser = Depends(require_auth)):
    tracked_assets = [QUOTE_ASSET, "BTC", "ETH", "BNB", "SOL"]
    assets = {asset: 0.0 for asset in tracked_assets}
    asset_values_usdc = {asset: 0.0 for asset in tracked_assets}

    # LIVE: real Binance balances
    if binance_client is not None:
        summary = _load_account_summary()

        for balance in summary.balances:
            if balance.asset not in assets:
                continue
            assets[balance.asset] = float(balance.total)
            asset_values_usdc[balance.asset] = float(balance.total_usdt)

        return UIAccountSummary(
            total_account_value_usdc=float(summary.total_usdt),
            quote_asset=QUOTE_ASSET,
            assets=assets,
            asset_values_usdc=asset_values_usdc,
            updated_at=datetime.now(timezone.utc),
        )

    # PAPER: synthetic account snapshot from DB
    try:
        with db_cursor() as (_conn, cur):
            cur.execute("""
              WITH closed_realized AS (
                SELECT
                  COALESCE(SUM(
                    CASE
                      WHEN UPPER(COALESCE(side, 'LONG')) IN ('SELL', 'SHORT')
                        THEN (entry_price - exit_price) * qty
                      ELSE (exit_price - entry_price) * qty
                    END
                  ), 0)::double precision AS realized_pnl
                FROM positions
                WHERE status = 'CLOSED'
                  AND exit_price IS NOT NULL
                  AND entry_price IS NOT NULL
              ),
              open_unrealized AS (
                SELECT
                  COALESCE(SUM(
                    CASE
                      WHEN UPPER(COALESCE(p.side, 'LONG')) IN ('SELL', 'SHORT')
                        THEN (p.entry_price - COALESCE(c.close, p.entry_price)) * p.qty
                      ELSE (COALESCE(c.close, p.entry_price) - p.entry_price) * p.qty
                    END
                  ), 0)::double precision AS unrealized_pnl
                FROM positions p
                LEFT JOIN LATERAL (
                  SELECT close
                  FROM candles c
                  WHERE c.symbol = p.symbol
                    AND c.interval = p.interval
                  ORDER BY c.open_time DESC
                  LIMIT 1
                ) c ON TRUE
                WHERE p.status = 'OPEN'
              )
              SELECT
                COALESCE((SELECT realized_pnl FROM closed_realized), 0),
                COALESCE((SELECT unrealized_pnl FROM open_unrealized), 0)
            """)
            row = cur.fetchone()

        realized_pnl = float(row[0] or 0.0)
        unrealized_pnl = float(row[1] or 0.0)
        total_usdc = float(PAPER_START_USDT + realized_pnl + unrealized_pnl)

        assets[QUOTE_ASSET] = total_usdc
        asset_values_usdc[QUOTE_ASSET] = total_usdc

        return UIAccountSummary(
            total_account_value_usdc=total_usdc,
            quote_asset=QUOTE_ASSET,
            assets=assets,
            asset_values_usdc=asset_values_usdc,
            updated_at=datetime.now(timezone.utc),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ui/account failed in PAPER mode: {e}")


@app.get("/ui/trading-24h", response_model=UITrading24hSummary)
def ui_trading_24h(user: CurrentUser = Depends(require_auth)):
    try:
        with db_cursor() as (_conn, cur):
            cur.execute("""
              SELECT
                COALESCE(SUM(
                  CASE
                    WHEN UPPER(COALESCE(side, 'LONG')) IN ('SELL', 'SHORT')
                      THEN (entry_price - exit_price) * qty
                    ELSE (exit_price - entry_price) * qty
                  END
                ), 0)::double precision AS closed_pnl_24h,
                COUNT(*)::int AS trades_24h,
                COALESCE(SUM(
                  CASE
                    WHEN (
                      CASE
                        WHEN UPPER(COALESCE(side, 'LONG')) IN ('SELL', 'SHORT')
                          THEN (entry_price - exit_price) * qty
                        ELSE (exit_price - entry_price) * qty
                      END
                    ) > 0 THEN 1 ELSE 0
                  END
                ), 0)::int AS wins_24h,
                COALESCE(SUM(
                  CASE
                    WHEN (
                      CASE
                        WHEN UPPER(COALESCE(side, 'LONG')) IN ('SELL', 'SHORT')
                          THEN (entry_price - exit_price) * qty
                        ELSE (exit_price - entry_price) * qty
                      END
                    ) <= 0 THEN 1 ELSE 0
                  END
                ), 0)::int AS losses_24h,
                COALESCE(
                  ROUND(
                    (
                      100.0 * SUM(
                        CASE
                          WHEN (
                            CASE
                              WHEN UPPER(COALESCE(side, 'LONG')) IN ('SELL', 'SHORT')
                                THEN (entry_price - exit_price) * qty
                              ELSE (exit_price - entry_price) * qty
                            END
                          ) > 0 THEN 1 ELSE 0
                        END
                      )::numeric / NULLIF(COUNT(*), 0)
                    ),
                    2
                  ),
                  0
                )::double precision AS win_rate_24h
              FROM positions
              WHERE status = 'CLOSED'
                AND exit_time IS NOT NULL
                AND exit_time >= now() - INTERVAL '24 hours'
            """)
            row = cur.fetchone()

        return UITrading24hSummary(
            closed_pnl_24h=_safe_float(row[0]) or 0.0,
            trades_24h=_safe_int(row[1]),
            wins_24h=_safe_int(row[2]),
            losses_24h=_safe_int(row[3]),
            win_rate_24h=_safe_float(row[4]) or 0.0,
            updated_at=datetime.now(timezone.utc),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ui/trading-24h failed: {e}")


@app.on_event("startup")
def start_ai_tuner():
    t = threading.Thread(target=ai_auto_tuner_loop, daemon=True)
    t.start()
    logging.info("AI tuner thread started.")


@app.post("/ai/analyze-strategy", response_model=AIAnalyzeResponse)
def analyze_strategy_with_ai(
    req: AIAnalyzeRequest,
    user: CurrentUser = Depends(require_admin),
):
    if openai_client is None:
        raise HTTPException(status_code=500, detail="OpenAI API key not configured on backend")

    try:
        response = openai_client.responses.create(
            model=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini"),
            input=req.prompt,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OpenAI error: {e}")

    return AIAnalyzeResponse(analysis=response.output_text)


@app.get("/watchdog/events", response_model=WatchdogEventPage)
def get_watchdog_events(
    symbol: str = Query(DEFAULT_SYMBOL),
    interval: str = Query("1m"),
    strategy: str = Query("RSI"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: CurrentUser = Depends(require_auth),
):
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT COUNT(*)
            FROM watchdog_events
            WHERE symbol = %s AND interval = %s AND strategy = %s
            """,
            (symbol, interval, strategy),
        )
        total = cur.fetchone()[0]

        cur.execute(
            """
            SELECT id, created_at, symbol, interval, strategy, severity, event, details
            FROM watchdog_events
            WHERE symbol = %s AND interval = %s AND strategy = %s
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (symbol, interval, strategy, limit, offset),
        )
        rows = cur.fetchall()

        items = []
        for r in rows:
            items.append(
                WatchdogEvent(
                    id=r[0],
                    created_at=r[1],
                    symbol=r[2],
                    interval=r[3],
                    strategy=r[4],
                    severity=r[5],
                    event=r[6],
                    details=r[7] if r[7] is not None else None,
                )
            )

        return WatchdogEventPage(total=total, items=items)


@app.get("/regime/latest", response_model=RegimePoint)
def get_regime_latest(
    symbol: str = Query(DEFAULT_SYMBOL),
    interval: str = Query("1m"),
    user: CurrentUser = Depends(require_auth),
):
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT symbol, interval, ts, regime, vol_regime, trend_dir,
                trend_strength_pct, atr_pct, shock_z
            FROM market_regime
            WHERE symbol=%s AND interval=%s
            ORDER BY ts DESC
            LIMIT 1
            """,
            (symbol, interval),
        )
        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="No regime data")
        return RegimePoint(
            symbol=row[0], interval=row[1], ts=row[2],
            regime=row[3], vol_regime=row[4], trend_dir=row[5],
            trend_strength_pct=row[6], atr_pct=row[7], shock_z=row[8],
        )


@app.get("/regime/history", response_model=List[RegimePoint])
def get_regime_history(
    symbol: str = Query(DEFAULT_SYMBOL),
    interval: str = Query("1m"),
    limit: int = Query(200, ge=1, le=5000),
    user: CurrentUser = Depends(require_auth),
):
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT symbol, interval, ts, regime, vol_regime, trend_dir,
                trend_strength_pct, atr_pct, shock_z
            FROM market_regime
            WHERE symbol=%s AND interval=%s
            ORDER BY ts DESC
            LIMIT %s
            """,
            (symbol, interval, limit),
        )
        rows = cur.fetchall()

        return [
            RegimePoint(
                symbol=r[0], interval=r[1], ts=r[2],
                regime=r[3], vol_regime=r[4], trend_dir=r[5],
                trend_strength_pct=r[6], atr_pct=r[7], shock_z=r[8],
            )
            for r in rows
        ]


@app.get("/safety/status")
def safety_status(user: CurrentUser = Depends(require_auth)):
    return {
        "environment": os.environ.get("ENVIRONMENT"),
        "trading_mode": os.environ.get("TRADING_MODE"),
        "live_orders_enabled": os.environ.get("LIVE_ORDERS_ENABLED"),
        "panic_disable_trading": os.environ.get("PANIC_DISABLE_TRADING"),
    }


@app.get("/bots/active")
def bots_active(
    ttl_seconds: int = 600,
    user: CurrentUser = Depends(require_auth),
):
    try:
        with db_cursor() as (_conn, cur):
            cur.execute("""
            SELECT symbol, strategy, interval, last_seen, info
            FROM bot_heartbeat
            WHERE last_seen > now() - (%s || ' seconds')::interval
            ORDER BY last_seen DESC
            """, (ttl_seconds,))
            rows = cur.fetchall()

            return jsonable_encoder({
                "ttl_seconds": ttl_seconds,
                "total": len(rows),
                "items": [
                    {
                        "symbol": r[0],
                        "strategy": r[1],
                        "interval": r[2],
                        "last_seen": r[3],
                        "info": r[4],
                    }
                    for r in rows
                ],
            })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ops/positions/open")
def ops_positions_open(user: CurrentUser = Depends(require_admin)):
    try:
        with db_cursor() as (_conn, cur):
            cur.execute("""
              SELECT
                id, symbol, interval, strategy, side,
                entry_time, entry_price, qty,
                now() - entry_time AS age
              FROM positions
              WHERE status = 'OPEN'
              ORDER BY entry_time ASC
            """)
            rows = cur.fetchall()

        return jsonable_encoder({
            "total": len(rows),
            "items": [
              {
                "id": r[0],
                "symbol": r[1],
                "interval": r[2],
                "strategy": r[3],
                "side": r[4],
                "entry_time": r[5],
                "entry_price": float(r[6]),
                "qty": float(r[7]),
                "age_seconds": r[8].total_seconds(),
              }
              for r in rows
            ]
        })

    except Exception as e:
        return {
            "total": 0,
            "items": [],
            "error_type": type(e).__name__,
            "error": str(e),
            "note": "ops/positions/open not available in this environment",
        }


@app.get("/ops/live-attempts")
def ops_live_attempts(
    minutes: int = 120,
    user: CurrentUser = Depends(require_admin),
):
    try:
        with db_cursor() as (_conn, cur):
            cur.execute("""
              SELECT
                symbol,
                interval,
                strategy,
                reason,
                COUNT(*) AS n,
                MIN(created_at) AS first_at,
                MAX(created_at) AS last_at
              FROM strategy_events
              WHERE created_at >= now() - (%s || ' minutes')::interval
                AND reason LIKE 'LIVE_%%'
              GROUP BY symbol, interval, strategy, reason
              ORDER BY last_at DESC
            """, (minutes,))

            rows = cur.fetchall()

        # Jeśli rows puste -> normalny payload
        if not rows:
            return {
                "window_minutes": minutes,
                "total": 0,
                "items": [],
            }

        # Walidacja kształtu
        bad = [r for r in rows if not isinstance(r, (tuple, list)) or len(r) < 7]
        if bad:
            return {
                "window_minutes": minutes,
                "total": 0,
                "items": [],
                "error_type": "BadRowShape",
                "error": "strategy_events query returned unexpected row shape",
                "row_lens": sorted(list({len(r) for r in rows if isinstance(r, (tuple, list))})),
                "sample_row": jsonable_encoder(rows[0]),
                "note": "Check you are running the expected container/image for this port.",
            }

        items = []
        for r in rows:
            items.append({
                "symbol": r[0],
                "interval": r[1],
                "strategy": r[2],
                "reason": r[3],
                "count": int(r[4]),
                "first_at": r[5],
                "last_at": r[6],
            })

        return jsonable_encoder({
            "window_minutes": minutes,
            "total": len(items),
            "items": items,
        })

    except Exception as e:
        return {
            "window_minutes": minutes,
            "total": 0,
            "items": [],
            "error_type": type(e).__name__,
            "error": str(e),
            "note": "ops/live-attempts not available in this environment",
        }


@app.get("/ops/bot-control")
def ops_bot_control(user: CurrentUser = Depends(require_admin)):
    try:
        with db_cursor() as (_conn, cur):
            cur.execute("""
              SELECT
                symbol, interval, strategy,
                enabled, live_orders_enabled,
                regime_enabled, regime_mode,
                updated_at
              FROM bot_control
              ORDER BY symbol, interval, strategy
            """)
            rows = cur.fetchall()

        return jsonable_encoder({
            "total": len(rows),
            "items": [
              {
                "symbol": r[0],
                "interval": r[1],
                "strategy": r[2],
                "enabled": r[3],
                "live_orders_enabled": r[4],
                "regime_enabled": r[5],
                "regime_mode": r[6],
                "updated_at": r[7],
              }
              for r in rows
            ]
        })

    except Exception as e:
        return {
            "total": 0,
            "items": [],
            "error_type": type(e).__name__,
            "error": str(e),
            "note": "ops/bot-control not available in this environment",
        }


@app.get("/ops/environment")
def ops_environment(user: CurrentUser = Depends(require_admin)):
    return {
        "environment": ENVIRONMENT,
        "trading_mode": TRADING_MODE,
        "db_name": DB_NAME,
        "quote_asset": QUOTE_ASSET,
    }

@app.get("/ui/live-summary")
def ui_live_summary(user: CurrentUser = Depends(require_auth)):
    try:
        with db_cursor() as (_conn, cur):
            cur.execute("""
                WITH panic AS (
                  SELECT panic_enabled, reason, updated_at
                  FROM panic_state
                  WHERE id = true
                ),
                open_pos AS (
                  SELECT
                    p.id,
                    p.symbol,
                    p.interval,
                    p.strategy,
                    p.side,
                    p.qty::double precision AS qty,
                    p.entry_price::double precision AS entry_price,
                    lc.close_price::double precision AS current_price
                  FROM positions p
                  LEFT JOIN LATERAL (
                    SELECT c.close AS close_price
                    FROM candles c
                    WHERE c.symbol = p.symbol
                      AND c.interval = p.interval
                    ORDER BY c.open_time DESC
                    LIMIT 1
                  ) lc ON TRUE
                  WHERE p.status = 'OPEN'
                ),
                open_agg AS (
                  SELECT
                    COUNT(*) AS open_positions_count,
                    COALESCE(SUM(COALESCE(current_price, entry_price) * qty), 0)::double precision AS open_market_value,
                    COALESCE(SUM(
                      CASE
                        WHEN UPPER(COALESCE(side, 'LONG')) IN ('SELL', 'SHORT')
                          THEN (entry_price - COALESCE(current_price, entry_price)) * qty
                        ELSE (COALESCE(current_price, entry_price) - entry_price) * qty
                      END
                    ), 0)::double precision AS open_unrealized_pnl_usdc
                  FROM open_pos
                ),
                bc AS (
                  SELECT
                    COUNT(*) AS slots_total,
                    COUNT(*) FILTER (WHERE enabled) AS enabled_count,
                    COUNT(*) FILTER (WHERE live_orders_enabled) AS live_orders_enabled_count,
                    COUNT(*) FILTER (WHERE regime_enabled) AS regime_enabled_count,
                    COUNT(*) FILTER (WHERE regime_mode = 'ENFORCE') AS enforce_count,
                    COUNT(*) FILTER (WHERE enabled AND live_orders_enabled AND regime_enabled AND regime_mode = 'ENFORCE') AS effective_live_count,
                    MAX(updated_at) AS last_bot_control_update
                  FROM bot_control
                ),
                hb AS (
                  SELECT
                    COUNT(*) AS heartbeat_total,
                    COUNT(*) FILTER (WHERE last_seen >= now() - INTERVAL '10 minutes') AS heartbeat_fresh_count,
                    COUNT(*) FILTER (WHERE last_seen < now() - INTERVAL '10 minutes') AS heartbeat_stale_count,
                    MAX(last_seen) AS latest_heartbeat_at
                  FROM bot_heartbeat
                ),
                candle_freshness AS (
                  SELECT MAX(close_time) AS latest_mark_price_at
                  FROM candles
                )
                SELECT
                  %s AS environment,
                  %s AS trading_mode,
                  COALESCE((SELECT panic_enabled FROM panic), false) AS panic_enabled,
                  COALESCE((SELECT reason FROM panic), '') AS panic_reason,
                  (SELECT updated_at FROM panic) AS panic_updated_at,
                  COALESCE((SELECT slots_total FROM bc), 0),
                  COALESCE((SELECT enabled_count FROM bc), 0),
                  COALESCE((SELECT live_orders_enabled_count FROM bc), 0),
                  COALESCE((SELECT regime_enabled_count FROM bc), 0),
                  COALESCE((SELECT enforce_count FROM bc), 0),
                  COALESCE((SELECT effective_live_count FROM bc), 0),
                  COALESCE((SELECT open_positions_count FROM open_agg), 0),
                  COALESCE((SELECT open_market_value FROM open_agg), 0),
                  COALESCE((SELECT open_unrealized_pnl_usdc FROM open_agg), 0),
                  (SELECT latest_heartbeat_at FROM hb),
                  COALESCE((SELECT heartbeat_total FROM hb), 0),
                  COALESCE((SELECT heartbeat_fresh_count FROM hb), 0),
                  COALESCE((SELECT heartbeat_stale_count FROM hb), 0),
                  (SELECT latest_mark_price_at FROM candle_freshness),
                  (SELECT last_bot_control_update FROM bc),
                  now() AS snapshot_at
            """, (ENVIRONMENT, TRADING_MODE))
            r = cur.fetchone()

        return jsonable_encoder({
            "environment": r[0],
            "trading_mode": r[1],
            "panic": {
                "enabled": bool(r[2]),
                "reason": r[3],
                "updated_at": r[4],
            },
            "slot_counts": {
                "total": _safe_int(r[5]),
                "enabled": _safe_int(r[6]),
                "live_orders_enabled": _safe_int(r[7]),
                "regime_enabled": _safe_int(r[8]),
                "enforce": _safe_int(r[9]),
                "effective_live": _safe_int(r[10]),
            },
            "open_positions": {
                "count": _safe_int(r[11]),
                "market_value_usdc": _safe_float(r[12]) or 0.0,
                "unrealized_pnl_usdc": _safe_float(r[13]) or 0.0,
            },
            "heartbeats": {
                "latest_at": r[14],
                "total": _safe_int(r[15]),
                "fresh": _safe_int(r[16]),
                "stale": _safe_int(r[17]),
            },
            "market_data": {
                "latest_mark_price_at": r[18],
            },
            "bot_control": {
                "last_updated_at": r[19],
            },
            "snapshot_at": r[20],
        })

    except Exception as e:
        return {
            "environment": ENVIRONMENT,
            "trading_mode": TRADING_MODE,
            "error_type": type(e).__name__,
            "error": str(e),
            "note": "ui/live-summary failed",
        }


@app.get("/ui/open-positions")
def ui_open_positions(user: CurrentUser = Depends(require_auth)):
    try:
        with db_cursor() as (_conn, cur):
            cur.execute("""
              SELECT
                p.id,
                p.symbol,
                p.interval,
                p.strategy,
                p.side,
                p.entry_time,
                p.entry_price::double precision AS entry_price,
                p.qty::double precision AS qty,
                EXTRACT(EPOCH FROM (now() - p.entry_time))::double precision AS age_seconds,
                lc.close_price::double precision AS current_price,
                (COALESCE(lc.close_price, p.entry_price) * p.qty)::double precision AS market_value,
                CASE
                  WHEN UPPER(COALESCE(p.side, 'LONG')) IN ('SELL', 'SHORT')
                    THEN ((p.entry_price - COALESCE(lc.close_price, p.entry_price)) * p.qty)::double precision
                  ELSE ((COALESCE(lc.close_price, p.entry_price) - p.entry_price) * p.qty)::double precision
                END AS unrealized_pnl_usdc,
                CASE
                  WHEN p.entry_price IS NULL OR p.entry_price = 0 THEN NULL
                  WHEN UPPER(COALESCE(p.side, 'LONG')) IN ('SELL', 'SHORT')
                    THEN (((p.entry_price - COALESCE(lc.close_price, p.entry_price)) / p.entry_price) * 100.0)::double precision
                  ELSE (((COALESCE(lc.close_price, p.entry_price) - p.entry_price) / p.entry_price) * 100.0)::double precision
                END AS unrealized_pnl_pct,
                lc.mark_open_time
              FROM positions p
              LEFT JOIN LATERAL (
                SELECT
                  c.close AS close_price,
                  c.open_time AS mark_open_time
                FROM candles c
                WHERE c.symbol = p.symbol
                  AND c.interval = p.interval
                ORDER BY c.open_time DESC
                LIMIT 1
              ) lc ON TRUE
              WHERE p.status = 'OPEN'
              ORDER BY p.entry_time ASC
            """)
            rows = cur.fetchall()

        items = []
        for r in rows:
            items.append({
                "id": r[0],
                "symbol": r[1],
                "interval": r[2],
                "strategy": r[3],
                "side": r[4],
                "entry_time": r[5],
                "entry_price": _safe_float(r[6]),
                "qty": _safe_float(r[7]),
                "age_seconds": _safe_float(r[8]) or 0.0,
                "current_price": _safe_float(r[9]),
                "market_value": _safe_float(r[10]),
                "unrealized_pnl_usdc": _safe_float(r[11]),
                "unrealized_pnl_pct": _safe_float(r[12]),
                "mark_open_time": r[13],
            })

        return jsonable_encoder({
            "total": len(items),
            "items": items,
        })

    except Exception as e:
        return {
            "total": 0,
            "items": [],
            "error_type": type(e).__name__,
            "error": str(e),
            "note": "ui/open-positions failed",
        }


@app.get("/ui/recent-closed")
def ui_recent_closed(
    limit: int = Query(default=10, ge=1, le=100),
    user: CurrentUser = Depends(require_auth),
):
    try:
        with db_cursor() as (_conn, cur):
            cur.execute("""
              SELECT
                id,
                exit_time,
                symbol,
                interval,
                strategy,
                side,
                entry_time,
                entry_price::double precision AS entry_price,
                exit_price::double precision AS exit_price,
                qty::double precision AS qty,
                CASE
                  WHEN UPPER(COALESCE(side, 'LONG')) IN ('SELL', 'SHORT')
                    THEN ((entry_price - exit_price) * qty)::double precision
                  ELSE ((exit_price - entry_price) * qty)::double precision
                END AS pnl_usdc,
                CASE
                  WHEN entry_price IS NULL OR entry_price = 0 THEN NULL
                  WHEN UPPER(COALESCE(side, 'LONG')) IN ('SELL', 'SHORT')
                    THEN (((entry_price - exit_price) / entry_price) * 100.0)::double precision
                  ELSE (((exit_price - entry_price) / entry_price) * 100.0)::double precision
                END AS pnl_pct,
                exit_reason
              FROM positions
              WHERE status = 'CLOSED'
                AND exit_time IS NOT NULL
              ORDER BY exit_time DESC
              LIMIT %s
            """, (limit,))
            rows = cur.fetchall()

        items = []
        for r in rows:
            pnl_usdc = _safe_float(r[10]) or 0.0
            items.append({
                "id": r[0],
                "exit_time": r[1],
                "symbol": r[2],
                "interval": r[3],
                "strategy": r[4],
                "side": r[5],
                "entry_time": r[6],
                "entry_price": _safe_float(r[7]),
                "exit_price": _safe_float(r[8]),
                "qty": _safe_float(r[9]),
                "pnl_usdc": pnl_usdc,
                "pnl_pct": _safe_float(r[11]),
                "exit_reason": r[12],
                "win_loss": "WIN" if pnl_usdc > 0 else "LOSS" if pnl_usdc < 0 else "FLAT",
            })

        return jsonable_encoder({
            "limit": limit,
            "total": len(items),
            "items": items,
        })

    except Exception as e:
        return {
            "limit": limit,
            "total": 0,
            "items": [],
            "error_type": type(e).__name__,
            "error": str(e),
            "note": "ui/recent-closed failed",
        }


@app.post("/ui/control/panic", response_model=UIControlResponse)
def ui_control_panic(
    req: UIPanicControlRequest,
    request: Request,
    user: CurrentUser = Depends(require_admin),
):
    actor, actor_role, source = get_request_actor(request)
    applied_at = datetime.now(timezone.utc)
    with db_cursor() as (conn, cur):
        cur.execute("""
          CREATE TABLE IF NOT EXISTS panic_state (
            id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id = TRUE),
            panic_enabled BOOLEAN NOT NULL DEFAULT FALSE,
            reason TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
          );
        """)
        ensure_ui_audit_table(cur)
        cur.execute("SELECT panic_enabled, reason, updated_at FROM panic_state WHERE id = TRUE")
        before = cur.fetchone()
        before_state = {
            "enabled": bool(before[0]) if before else False,
            "reason": before[1] if before else None,
            "updated_at": before[2] if before else None,
        }
        cur.execute("""
          INSERT INTO panic_state (id, panic_enabled, reason, updated_at)
          VALUES (TRUE, %s, %s, %s)
          ON CONFLICT (id)
          DO UPDATE SET
            panic_enabled = EXCLUDED.panic_enabled,
            reason = EXCLUDED.reason,
            updated_at = EXCLUDED.updated_at
        """, (req.enabled, req.reason, applied_at))
        after_state = {
            "enabled": bool(req.enabled),
            "reason": req.reason,
            "updated_at": applied_at,
        }
        log_ui_action(
            cur,
            actor=actor,
            actor_role=actor_role,
            action="panic.update",
            target_type="panic_state",
            target_key="global",
            before_json=before_state,
            after_json=after_state,
            source=source,
            note=req.reason,
        )
        conn.commit()

    return UIControlResponse(
        ok=True,
        message="panic state updated",
        applied_at=applied_at,
        new_state=after_state,
    )



@app.get("/ui/slots")
def ui_slots(user: CurrentUser = Depends(require_auth)):
    try:
        with db_cursor() as (_conn, cur):
            cur.execute("""
              SELECT
                bc.symbol,
                bc.interval,
                bc.strategy,
                bc.enabled,
                bc.live_orders_enabled,
                bc.regime_enabled,
                bc.regime_mode,
                bc.reason,
                bc.updated_at,
                bc.control_mode,
                bc.control_source,
                bc.manual_override_reason,
                bc.manual_override_updated_at,
                op.id AS open_position_id,
                op.side AS open_position_side,
                op.entry_time AS open_position_entry_time,
                hb.last_seen,
                CASE
                  WHEN hb.last_seen IS NULL THEN TRUE
                  WHEN hb.last_seen < now() - INTERVAL '10 minutes' THEN TRUE
                  ELSE FALSE
                END AS heartbeat_stale,
                se.created_at AS last_event_at,
                se.event_type AS last_event_type,
                se.decision AS last_event_decision,
                se.reason AS last_event_reason
              FROM bot_control bc
              LEFT JOIN LATERAL (
                SELECT p.id, p.side, p.entry_time
                FROM positions p
                WHERE p.symbol = bc.symbol
                  AND p.interval = bc.interval
                  AND p.strategy = bc.strategy
                  AND p.status = 'OPEN'
                ORDER BY p.entry_time DESC
                LIMIT 1
              ) op ON TRUE
              LEFT JOIN bot_heartbeat hb
                ON hb.symbol = bc.symbol
               AND hb.interval = bc.interval
               AND hb.strategy = bc.strategy
              LEFT JOIN LATERAL (
                SELECT se.created_at, se.event_type, se.decision, se.reason
                FROM strategy_events se
                WHERE se.symbol = bc.symbol
                  AND se.interval = bc.interval
                  AND se.strategy = bc.strategy
                ORDER BY se.created_at DESC
                LIMIT 1
              ) se ON TRUE
              ORDER BY bc.symbol, bc.interval, bc.strategy
            """)
            rows = cur.fetchall()

        items = []
        for r in rows:
            items.append({
                "symbol": r[0],
                "interval": r[1],
                "strategy": r[2],
                "enabled": bool(r[3]),
                "live_orders_enabled": bool(r[4]),
                "regime_enabled": bool(r[5]),
                "regime_mode": r[6],
                "reason": r[7],
                "updated_at": r[8],
                "control_mode": r[9] or "AUTO",
                "control_source": r[10] or "SYSTEM",
                "manual_override_reason": r[11],
                "manual_override_updated_at": r[12],
                "open_position": {
                    "exists": r[13] is not None,
                    "id": _safe_int(r[13], default=None),
                    "side": r[14],
                    "entry_time": r[15],
                },
                "heartbeat": {
                    "last_seen": r[16],
                    "stale": bool(r[17]),
                },
                "last_event": {
                    "at": r[18],
                    "event_type": r[19],
                    "decision": r[20],
                    "reason": r[21],
                },
            })

        return jsonable_encoder({
            "total": len(items),
            "items": items,
        })

    except Exception as e:
        return {
            "total": 0,
            "items": [],
            "error_type": type(e).__name__,
            "error": str(e),
            "note": "ui/slots failed",
        }


@app.get("/ui/health")
def ui_health(user: CurrentUser = Depends(require_auth)):
    try:
        with db_cursor() as (_conn, cur):
            cur.execute("""
              WITH db_now AS (
                SELECT now() AS ts
              ),
              hb AS (
                SELECT
                  COUNT(*) AS total,
                  COUNT(*) FILTER (WHERE last_seen >= now() - INTERVAL '10 minutes') AS fresh,
                  COUNT(*) FILTER (WHERE last_seen < now() - INTERVAL '10 minutes') AS stale,
                  MAX(last_seen) AS latest_at
                FROM bot_heartbeat
              ),
              candles_agg AS (
                SELECT
                  MAX(close_time) AS latest_candle_close_at,
                  COUNT(DISTINCT symbol || ':' || interval) AS tracked_pairs
                FROM candles
              ),
              orchestrator AS (
                SELECT
                  MAX(created_at) AS latest_event_at,
                  COUNT(*) FILTER (WHERE created_at >= now() - INTERVAL '15 minutes') AS events_last_15m
                FROM strategy_events
              ),
              panic AS (
                SELECT panic_enabled, updated_at
                FROM panic_state
                WHERE id = TRUE
              )
              SELECT
                (SELECT ts FROM db_now),
                COALESCE((SELECT total FROM hb), 0),
                COALESCE((SELECT fresh FROM hb), 0),
                COALESCE((SELECT stale FROM hb), 0),
                (SELECT latest_at FROM hb),
                (SELECT latest_candle_close_at FROM candles_agg),
                COALESCE((SELECT tracked_pairs FROM candles_agg), 0),
                (SELECT latest_event_at FROM orchestrator),
                COALESCE((SELECT events_last_15m FROM orchestrator), 0),
                COALESCE((SELECT panic_enabled FROM panic), FALSE),
                (SELECT updated_at FROM panic),
                now() AS snapshot_at
            """)
            r = cur.fetchone()

        return jsonable_encoder({
            "api": {
                "ok": True,
                "environment": ENVIRONMENT,
                "trading_mode": TRADING_MODE,
                "snapshot_at": r[11],
            },
            "db": {
                "ok": r[0] is not None,
                "now": r[0],
            },
            "bot_heartbeats": {
                "latest_at": r[4],
                "total": _safe_int(r[1]),
                "fresh": _safe_int(r[2]),
                "stale": _safe_int(r[3]),
            },
            "market_data": {
                "latest_candle_close_at": r[5],
                "tracked_pairs": _safe_int(r[6]),
            },
            "orchestrator": {
                "latest_event_at": r[7],
                "events_last_15m": _safe_int(r[8]),
            },
            "panic_state": {
                "enabled": bool(r[9]),
                "updated_at": r[10],
            },
        })

    except Exception as e:
        return {
            "api": {
                "ok": False,
                "environment": ENVIRONMENT,
                "trading_mode": TRADING_MODE,
                "snapshot_at": datetime.now(timezone.utc),
            },
            "error_type": type(e).__name__,
            "error": str(e),
            "note": "ui/health failed",
        }


@app.post("/ui/control/slot", response_model=UIControlResponse)
def ui_control_slot(
    req: UISlotControlRequest,
    request: Request,
    user: CurrentUser = Depends(require_admin),
):
    if req.enabled is None and req.live_orders_enabled is None:
        raise HTTPException(status_code=400, detail="enabled or live_orders_enabled must be provided")

    actor, actor_role, source = get_request_actor(request)
    applied_at = datetime.now(timezone.utc)
    target_key = f"{req.symbol}:{req.interval}:{req.strategy}"
    with db_cursor() as (conn, cur):
        ensure_ui_audit_table(cur)
        cur.execute(
            """
            SELECT enabled, live_orders_enabled, regime_enabled, regime_mode, reason, updated_at
            FROM bot_control
            WHERE symbol=%s AND interval=%s AND strategy=%s
            """,
            (req.symbol, req.interval, req.strategy),
        )
        before = cur.fetchone()
        before_state = {
            "enabled": bool(before[0]) if before else True,
            "live_orders_enabled": bool(before[1]) if before else False,
            "regime_enabled": bool(before[2]) if before else False,
            "regime_mode": before[3] if before else "DRY_RUN",
            "reason": before[4] if before else None,
            "updated_at": before[5] if before else None,
        }
        new_enabled = req.enabled if req.enabled is not None else before_state["enabled"]
        new_live = req.live_orders_enabled if req.live_orders_enabled is not None else before_state["live_orders_enabled"]
        note = req.reason or "slot control update"
        cur.execute(
            """
            INSERT INTO bot_control (
              symbol, strategy, interval, enabled, mode, reason, live_orders_enabled, regime_enabled, regime_mode, updated_at
            )
            VALUES (%s, %s, %s, %s, 'NORMAL', %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, strategy, interval)
            DO UPDATE SET
              enabled = EXCLUDED.enabled,
              reason = EXCLUDED.reason,
              live_orders_enabled = EXCLUDED.live_orders_enabled,
              regime_enabled = COALESCE(bot_control.regime_enabled, EXCLUDED.regime_enabled),
              regime_mode = COALESCE(bot_control.regime_mode, EXCLUDED.regime_mode),
              updated_at = EXCLUDED.updated_at
            """,
            (
                req.symbol,
                req.strategy,
                req.interval,
                new_enabled,
                note,
                new_live,
                before_state["regime_enabled"],
                before_state["regime_mode"],
                applied_at,
            ),
        )
        after_state = {
            "enabled": bool(new_enabled),
            "live_orders_enabled": bool(new_live),
            "regime_enabled": bool(before_state["regime_enabled"]),
            "regime_mode": before_state["regime_mode"],
            "reason": note,
            "updated_at": applied_at,
        }
        log_ui_action(
            cur,
            actor=actor,
            actor_role=actor_role,
            action="slot.update",
            target_type="bot_control",
            target_key=target_key,
            before_json=before_state,
            after_json=after_state,
            source=source,
            note=note,
        )
        conn.commit()

    return UIControlResponse(ok=True, message="slot updated", applied_at=applied_at, new_state=after_state)


@app.post("/ui/control/slot/manual", response_model=UIControlResponse)
def ui_control_slot_manual(
    req: UISlotManualControlRequest,
    request: Request,
    user: CurrentUser = Depends(require_admin),
):
    actor, actor_role, source = get_request_actor(request)
    applied_at = datetime.now(timezone.utc)
    normalized_mode = _normalize_regime_mode(req.regime_mode)
    target_key = f"{req.symbol}:{req.interval}:{req.strategy}"
    with db_cursor() as (conn, cur):
        ensure_ui_audit_table(cur)
        cur.execute(
            """
            SELECT enabled, live_orders_enabled, regime_enabled, regime_mode, reason, updated_at,
                   control_mode, control_source, manual_override_reason, manual_override_updated_at
            FROM bot_control
            WHERE symbol=%s AND interval=%s AND strategy=%s
            """,
            (req.symbol, req.interval, req.strategy),
        )
        before = cur.fetchone()
        before_state = {
            "enabled": bool(before[0]) if before else True,
            "live_orders_enabled": bool(before[1]) if before else False,
            "regime_enabled": bool(before[2]) if before else False,
            "regime_mode": before[3] if before else "DRY_RUN",
            "reason": before[4] if before else None,
            "updated_at": before[5] if before else None,
            "control_mode": before[6] if before and before[6] else "AUTO",
            "control_source": before[7] if before and before[7] else "SYSTEM",
            "manual_override_reason": before[8] if before else None,
            "manual_override_updated_at": before[9] if before else None,
        }
        note = req.reason or "manual slot override"
        cur.execute(
            """
            INSERT INTO bot_control (
              symbol, strategy, interval, enabled, mode, reason, live_orders_enabled, regime_enabled, regime_mode,
              updated_at, control_mode, control_source, manual_override_reason, manual_override_updated_at
            )
            VALUES (%s, %s, %s, %s, 'NORMAL', %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, strategy, interval)
            DO UPDATE SET
              enabled = EXCLUDED.enabled,
              reason = EXCLUDED.reason,
              live_orders_enabled = EXCLUDED.live_orders_enabled,
              regime_enabled = EXCLUDED.regime_enabled,
              regime_mode = EXCLUDED.regime_mode,
              updated_at = EXCLUDED.updated_at,
              control_mode = EXCLUDED.control_mode,
              control_source = EXCLUDED.control_source,
              manual_override_reason = EXCLUDED.manual_override_reason,
              manual_override_updated_at = EXCLUDED.manual_override_updated_at
            """,
            (
                req.symbol,
                req.strategy,
                req.interval,
                req.enabled,
                note,
                req.live_orders_enabled,
                req.regime_enabled,
                normalized_mode,
                applied_at,
                "MANUAL",
                "USER",
                note,
                applied_at,
            ),
        )
        after_state = {
            "enabled": bool(req.enabled),
            "live_orders_enabled": bool(req.live_orders_enabled),
            "regime_enabled": bool(req.regime_enabled),
            "regime_mode": normalized_mode,
            "reason": note,
            "updated_at": applied_at,
            "control_mode": "MANUAL",
            "control_source": "USER",
            "manual_override_reason": note,
            "manual_override_updated_at": applied_at,
        }
        log_ui_action(
            cur,
            actor=actor,
            actor_role=actor_role,
            action="slot.manual_override",
            target_type="bot_control",
            target_key=target_key,
            before_json=before_state,
            after_json=after_state,
            source=source,
            note=note,
        )
        conn.commit()

    return UIControlResponse(ok=True, message="slot set to manual", applied_at=applied_at, new_state=after_state)


@app.post("/ui/control/slot/auto", response_model=UIControlResponse)
def ui_control_slot_auto(
    req: UISlotAutoControlRequest,
    request: Request,
    user: CurrentUser = Depends(require_admin),
):
    actor, actor_role, source = get_request_actor(request)
    applied_at = datetime.now(timezone.utc)
    target_key = f"{req.symbol}:{req.interval}:{req.strategy}"
    with db_cursor() as (conn, cur):
        ensure_ui_audit_table(cur)
        cur.execute(
            """
            SELECT enabled, live_orders_enabled, regime_enabled, regime_mode, reason, updated_at,
                   control_mode, control_source, manual_override_reason, manual_override_updated_at
            FROM bot_control
            WHERE symbol=%s AND interval=%s AND strategy=%s
            """,
            (req.symbol, req.interval, req.strategy),
        )
        before = cur.fetchone()
        before_state = {
            "enabled": bool(before[0]) if before else True,
            "live_orders_enabled": bool(before[1]) if before else False,
            "regime_enabled": bool(before[2]) if before else False,
            "regime_mode": before[3] if before else "DRY_RUN",
            "reason": before[4] if before else None,
            "updated_at": before[5] if before else None,
            "control_mode": before[6] if before and before[6] else "AUTO",
            "control_source": before[7] if before and before[7] else "SYSTEM",
            "manual_override_reason": before[8] if before else None,
            "manual_override_updated_at": before[9] if before else None,
        }
        note = req.reason or "slot returned to auto"
        cur.execute(
            """
            INSERT INTO bot_control (
              symbol, strategy, interval, enabled, mode, reason, live_orders_enabled, regime_enabled, regime_mode,
              updated_at, control_mode, control_source, manual_override_reason, manual_override_updated_at
            )
            VALUES (%s, %s, %s, %s, 'NORMAL', %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, strategy, interval)
            DO UPDATE SET
              enabled = COALESCE(bot_control.enabled, EXCLUDED.enabled),
              reason = EXCLUDED.reason,
              live_orders_enabled = COALESCE(bot_control.live_orders_enabled, EXCLUDED.live_orders_enabled),
              regime_enabled = COALESCE(bot_control.regime_enabled, EXCLUDED.regime_enabled),
              regime_mode = COALESCE(bot_control.regime_mode, EXCLUDED.regime_mode),
              updated_at = EXCLUDED.updated_at,
              control_mode = EXCLUDED.control_mode,
              control_source = EXCLUDED.control_source,
              manual_override_reason = EXCLUDED.manual_override_reason,
              manual_override_updated_at = EXCLUDED.manual_override_updated_at
            """,
            (
                req.symbol,
                req.strategy,
                req.interval,
                before_state["enabled"],
                note,
                before_state["live_orders_enabled"],
                before_state["regime_enabled"],
                before_state["regime_mode"],
                applied_at,
                "AUTO",
                "USER",
                None,
                None,
            ),
        )
        after_state = {
            "enabled": bool(before_state["enabled"]),
            "live_orders_enabled": bool(before_state["live_orders_enabled"]),
            "regime_enabled": bool(before_state["regime_enabled"]),
            "regime_mode": before_state["regime_mode"],
            "reason": note,
            "updated_at": applied_at,
            "control_mode": "AUTO",
            "control_source": "ORC",
            "manual_override_reason": None,
            "manual_override_updated_at": None,
        }
        log_ui_action(
            cur,
            actor=actor,
            actor_role=actor_role,
            action="slot.return_to_auto",
            target_type="bot_control",
            target_key=target_key,
            before_json=before_state,
            after_json=after_state,
            source=source,
            note=note,
        )
        conn.commit()

    return UIControlResponse(ok=True, message="slot returned to auto", applied_at=applied_at, new_state=after_state)


@app.post("/ui/control/regime", response_model=UIControlResponse)
def ui_control_regime(
    req: UIRegimeControlRequest,
    request: Request,
    user: CurrentUser = Depends(require_admin),
):
    actor, actor_role, source = get_request_actor(request)
    applied_at = datetime.now(timezone.utc)
    normalized_mode = _normalize_regime_mode(req.regime_mode)
    target_key = f"{req.symbol}:{req.interval}:{req.strategy}"
    with db_cursor() as (conn, cur):
        ensure_ui_audit_table(cur)
        cur.execute(
            """
            SELECT enabled, live_orders_enabled, regime_enabled, regime_mode, reason, updated_at
            FROM bot_control
            WHERE symbol=%s AND interval=%s AND strategy=%s
            """,
            (req.symbol, req.interval, req.strategy),
        )
        before = cur.fetchone()
        before_state = {
            "enabled": bool(before[0]) if before else True,
            "live_orders_enabled": bool(before[1]) if before else False,
            "regime_enabled": bool(before[2]) if before else False,
            "regime_mode": before[3] if before else "DRY_RUN",
            "reason": before[4] if before else None,
            "updated_at": before[5] if before else None,
        }
        note = req.reason or "regime control update"
        cur.execute(
            """
            INSERT INTO bot_control (
              symbol, strategy, interval, enabled, mode, reason, live_orders_enabled, regime_enabled, regime_mode, updated_at
            )
            VALUES (%s, %s, %s, %s, 'NORMAL', %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, strategy, interval)
            DO UPDATE SET
              enabled = COALESCE(bot_control.enabled, EXCLUDED.enabled),
              reason = EXCLUDED.reason,
              live_orders_enabled = COALESCE(bot_control.live_orders_enabled, EXCLUDED.live_orders_enabled),
              regime_enabled = EXCLUDED.regime_enabled,
              regime_mode = EXCLUDED.regime_mode,
              updated_at = EXCLUDED.updated_at
            """,
            (
                req.symbol,
                req.strategy,
                req.interval,
                before_state["enabled"],
                note,
                before_state["live_orders_enabled"],
                req.regime_enabled,
                normalized_mode,
                applied_at,
            ),
        )
        after_state = {
            "enabled": bool(before_state["enabled"]),
            "live_orders_enabled": bool(before_state["live_orders_enabled"]),
            "regime_enabled": bool(req.regime_enabled),
            "regime_mode": normalized_mode,
            "reason": note,
            "updated_at": applied_at,
        }
        log_ui_action(
            cur,
            actor=actor,
            actor_role=actor_role,
            action="regime.update",
            target_type="bot_control",
            target_key=target_key,
            before_json=before_state,
            after_json=after_state,
            source=source,
            note=note,
        )
        conn.commit()

    return UIControlResponse(ok=True, message="regime updated", applied_at=applied_at, new_state=after_state)


@app.get("/ui/orc-dashboard")
def ui_orc_dashboard(user: CurrentUser = Depends(require_auth)):
    try:
        with db_cursor() as (_conn, cur):
            cur.execute(
                """
                SELECT
                  to_regclass('public.v_orc_picks_v5')::text,
                  to_regclass('public.v_orc_candidates_v5c')::text,
                  to_regclass('public.v_order_sizing_v1')::text,
                  to_regclass('public.bot_control')::text,
                  to_regclass('public.strategy_events')::text,
                  to_regclass('public.slot_capital_policy')::text
                """
            )
            reg = cur.fetchone()

            has_v_orc_picks_v5 = reg[0] is not None
            has_v_orc_candidates_v5c = reg[1] is not None
            has_v_order_sizing_v1 = reg[2] is not None
            has_bot_control = reg[3] is not None
            has_strategy_events = reg[4] is not None
            has_slot_capital_policy = reg[5] is not None

            required_missing = []
            if not has_v_orc_picks_v5:
                required_missing.append("v_orc_picks_v5")
            if not has_v_orc_candidates_v5c:
                required_missing.append("v_orc_candidates_v5c")

            if required_missing:
                return {
                    "total": 0,
                    "items": [],
                    "error_type": "MissingRequiredRelations",
                    "error": f"Missing required truth sources: {', '.join(required_missing)}",
                    "available_sources": {
                        "v_orc_picks_v5": has_v_orc_picks_v5,
                        "v_orc_candidates_v5c": has_v_orc_candidates_v5c,
                        "v_order_sizing_v1": has_v_order_sizing_v1,
                        "bot_control": has_bot_control,
                        "strategy_events": has_strategy_events,
                        "slot_capital_policy": has_slot_capital_policy,
                    },
                }

            latest_blocked_cte = ""
            latest_blocked_join = ""
            latest_blocked_cols = """
                  NULL::text AS blocked_reason,
                  NULL::timestamptz AS blocked_at
            """
            if has_strategy_events:
                latest_blocked_cte = """
                WITH latest_blocked AS (
                  SELECT
                    x.symbol,
                    x.interval,
                    x.strategy,
                    x.reason AS blocked_reason,
                    x.created_at AS blocked_at
                  FROM (
                    SELECT
                      se.symbol,
                      se.interval,
                      se.strategy,
                      se.reason,
                      se.created_at,
                      ROW_NUMBER() OVER (
                        PARTITION BY se.symbol, se.interval, se.strategy
                        ORDER BY se.created_at DESC
                      ) AS rn
                    FROM strategy_events se
                    WHERE se.event_type = 'BLOCKED'
                      AND se.created_at >= NOW() - INTERVAL '24 hours'
                  ) x
                  WHERE x.rn = 1
                )
                """
                latest_blocked_join = """
                LEFT JOIN latest_blocked lb
                  ON lb.symbol = p.symbol
                 AND lb.interval = p.interval
                 AND lb.strategy = p.strategy
                """
                latest_blocked_cols = """
                  lb.blocked_reason,
                  lb.blocked_at
                """

            bot_control_join = ""
            bot_control_cols = """
                  NULL::boolean AS enabled,
                  NULL::boolean AS live_orders_enabled,
                  NULL::boolean AS regime_enabled,
                  NULL::text AS regime_mode,
                  NULL::text AS bot_reason,
                  NULL::timestamptz AS bot_updated_at
            """
            if has_bot_control:
                bot_control_join = """
                LEFT JOIN bot_control bc
                  ON bc.symbol = p.symbol
                 AND bc.interval = p.interval
                 AND bc.strategy = p.strategy
                """
                bot_control_cols = """
                  bc.enabled,
                  bc.live_orders_enabled,
                  bc.regime_enabled,
                  bc.regime_mode,
                  bc.reason AS bot_reason,
                  bc.updated_at AS bot_updated_at
                """

            sizing_join = ""
            sizing_cols = """
                  NULL::numeric AS base_order_size,
                  NULL::numeric AS additional_usdc,
                  NULL::numeric AS adjusted_order_size,
                  NULL::bigint AS closed_trades_last3,
                  NULL::bigint AS wins_last3,
                  NULL::int AS last_trade_was_loss
            """
            if has_v_order_sizing_v1:
                sizing_join = """
                LEFT JOIN v_order_sizing_v1 os
                  ON os.symbol = p.symbol
                 AND os.interval = p.interval
                 AND os.strategy = p.strategy
                """
                sizing_cols = """
                  os.base_order_size,
                  os.additional_usdc,
                  os.adjusted_order_size,
                  os.closed_trades_last3,
                  os.wins_last3,
                  os.last_trade_was_loss
                """

            capital_join = ""
            capital_cols = """
                  NULL::numeric AS target_notional_usdc,
                  NULL::text AS capital_reason,
                  NULL::timestamptz AS capital_last_checked
            """
            if has_slot_capital_policy:
                capital_join = """
                LEFT JOIN slot_capital_policy scp
                  ON scp.symbol = p.symbol
                 AND scp.interval = p.interval
                 AND scp.strategy = p.strategy
                """
                capital_cols = """
                  scp.target_notional_usdc,
                  scp.reason AS capital_reason,
                  scp.last_checked AS capital_last_checked
                """

            sql = f"""
                {latest_blocked_cte}
                SELECT
                  p.symbol,
                  p.interval,
                  p.strategy,
                  CASE
                    WHEN c.eligible_pick_v5 THEN 'TIER1_PROFITABILITY'
                    WHEN c.eligible_bootstrap_v5 THEN 'TIER2_BOOTSTRAP'
                    WHEN c.eligible_signal_v5 THEN 'TIER3_SIGNAL'
                    WHEN c.eligible_activity_v5 THEN 'TIER4_ACTIVITY'
                    WHEN c.eligible_softfill_v5 THEN 'TIER5_SOFTFILL'
                    ELSE 'NO_PATH'
                  END AS picked_via,

                  {bot_control_cols},

                  {capital_cols},

                  {sizing_cols},

                  c.n_signal_15m,
                  c.last_signal_ts,
                  c.n_buy_24h,
                  c.n_runs_24h,
                  c.n_filter_block_24h,
                  c.filter_block_rate_24h,
                  c.n_trades_3d,
                  c.net_sum_3d,
                  c.profit_factor_3d,
                  c.last_exit_ts_3d,
                  c.last_ts_24h,

                  c.eligible_pick_v5,
                  c.eligible_bootstrap_v5,
                  c.eligible_signal_v5,
                  c.eligible_activity_v5,
                  c.eligible_softfill_v5,

                  {latest_blocked_cols}

                FROM v_orc_picks_v5 p
                JOIN v_orc_candidates_v5c c
                  ON c.symbol = p.symbol
                 AND c.interval = p.interval
                 AND c.strategy = p.strategy

                {bot_control_join}
                {capital_join}
                {sizing_join}
                {latest_blocked_join}

                ORDER BY
                  p.symbol,
                  p.interval,
                  p.strategy
            """

            cur.execute(sql)
            rows = cur.fetchall()

        items = []
        for r in rows:
            items.append({
                "symbol": r[0],
                "interval": r[1],
                "strategy": r[2],
                "picked_via": r[3],

                "enabled": r[4],
                "live_orders_enabled": r[5],
                "regime_enabled": r[6],
                "regime_mode": r[7],
                "bot_reason": r[8],
                "bot_updated_at": r[9],

                "target_notional_usdc": float(r[10]) if r[10] is not None else None,
                "capital_reason": r[11],
                "capital_last_checked": r[12],

                "base_order_size": float(r[13]) if r[13] is not None else None,
                "additional_usdc": float(r[14]) if r[14] is not None else None,
                "adjusted_order_size": float(r[15]) if r[15] is not None else None,
                "closed_trades_last3": int(r[16]) if r[16] is not None else 0,
                "wins_last3": int(r[17]) if r[17] is not None else 0,
                "last_trade_was_loss": int(r[18]) if r[18] is not None else 0,

                "n_signal_15m": int(r[19]) if r[19] is not None else 0,
                "last_signal_ts": r[20],
                "n_buy_24h": int(r[21]) if r[21] is not None else 0,
                "n_runs_24h": int(r[22]) if r[22] is not None else 0,
                "n_filter_block_24h": int(r[23]) if r[23] is not None else 0,
                "filter_block_rate_24h": float(r[24]) if r[24] is not None else 0.0,
                "n_trades_3d": int(r[25]) if r[25] is not None else 0,
                "net_sum_3d": float(r[26]) if r[26] is not None else 0.0,
                "profit_factor_3d": float(r[27]) if r[27] is not None else 0.0,
                "last_exit_ts_3d": r[28],
                "last_ts_24h": r[29],

                "eligible_pick_v5": bool(r[30]),
                "eligible_bootstrap_v5": bool(r[31]),
                "eligible_signal_v5": bool(r[32]),
                "eligible_activity_v5": bool(r[33]),
                "eligible_softfill_v5": bool(r[34]),

                "blocked_reason": r[35],
                "blocked_at": r[36],
            })

        return jsonable_encoder({
            "total": len(items),
            "items": items,
            "available_sources": {
                "v_orc_picks_v5": has_v_orc_picks_v5,
                "v_orc_candidates_v5c": has_v_orc_candidates_v5c,
                "v_order_sizing_v1": has_v_order_sizing_v1,
                "bot_control": has_bot_control,
                "strategy_events": has_strategy_events,
                "slot_capital_policy": has_slot_capital_policy,
            },
        })

    except Exception as e:
        return {
            "total": 0,
            "items": [],
            "error_type": type(e).__name__,
            "error": str(e),
            "note": "ui/orc-dashboard failed",
        }

# --- INTERNAL: promotions upsert (v1) ---

# --- INTERNAL: promotions upsert (v1) ---
class UIUserSettingsResponse(BaseModel):
    user_id: int | None = None
    configured_min_entry_usdc: float
    system_min_entry_usdc: float
    effective_min_entry_usdc: float
    base_runtime_notional_usdc: float
    manual_entry_addon_usdc: float
    three_win_boost_usdc: float
    normal_entry_preview_usdc: float
    boosted_entry_preview_usdc: float
    mode: str
    updated_at: datetime | None = None


class UIUserSettingsUpdateRequest(BaseModel):
    min_entry_usdc: float | None = None
    manual_entry_addon_usdc: float | None = None
    three_win_boost_usdc: float | None = None
    mode: str | None = None


class RestoreDefaultsResponse(BaseModel):
    ok: bool
    settings: UIUserSettingsResponse


@app.get("/settings/user", response_model=UIUserSettingsResponse)
def get_settings_user(user: CurrentUser = Depends(require_auth)):
    return UIUserSettingsResponse(**get_user_settings_snapshot())


@app.put("/settings/user", response_model=UIUserSettingsResponse)
def put_settings_user(
    payload: UIUserSettingsUpdateRequest,
    user: CurrentUser = Depends(require_auth),
):
    mode = payload.mode.upper() if payload.mode else None
    if mode is not None and mode not in {"AUTO", "MANUAL"}:
        raise HTTPException(status_code=400, detail="mode must be AUTO or MANUAL")

    if payload.min_entry_usdc is not None and payload.min_entry_usdc < 0:
        raise HTTPException(status_code=400, detail="min_entry_usdc must be >= 0")
    if payload.manual_entry_addon_usdc is not None and payload.manual_entry_addon_usdc < 0:
        raise HTTPException(status_code=400, detail="manual_entry_addon_usdc must be >= 0")
    if payload.three_win_boost_usdc is not None and payload.three_win_boost_usdc < 0:
        raise HTTPException(status_code=400, detail="three_win_boost_usdc must be >= 0")

    return UIUserSettingsResponse(**upsert_user_settings(
        min_entry_usdc=payload.min_entry_usdc,
        manual_entry_addon_usdc=payload.manual_entry_addon_usdc,
        three_win_boost_usdc=payload.three_win_boost_usdc,
        mode=mode,
    ))


@app.post("/settings/user/restore-defaults", response_model=RestoreDefaultsResponse)
def post_settings_user_restore_defaults(user: CurrentUser = Depends(require_auth)):
    settings = upsert_user_settings(
        manual_entry_addon_usdc=0.0,
        three_win_boost_usdc=10.0,
    )
    return RestoreDefaultsResponse(ok=True, settings=UIUserSettingsResponse(**settings))


@app.get("/ui/advanced-summary", response_model=UIUserSettingsResponse)
def get_ui_advanced_summary(user: CurrentUser = Depends(require_auth)):
    return UIUserSettingsResponse(**get_user_settings_snapshot())


class PromotionRow(BaseModel):
    symbol: str
    interval: str
    strategy: str
    paper_score: float
    n_trades: Optional[int] = None
    win_rate: Optional[float] = None
    net_sum: Optional[float] = None
    window_name: Optional[str] = None
    policy_version: Optional[str] = None
    source_ts: Optional[datetime] = None
    meta: Optional[dict] = None
    eligible_live: Optional[bool] = None
    elig_reason: Optional[str] = None


class PromotionsUpsertRequest(BaseModel):
    policy_version: str
    window_name: str
    source_ts: datetime
    hash: str
    rows: List[PromotionRow]


@app.post("/internal/promotions/upsert")
def internal_promotions_upsert(
    req: PromotionsUpsertRequest,
    x_internal_token: Optional[str] = Header(default=None, alias="X-Internal-Token"),
):
    # Minimal auth (optional) – if you set INTERNAL_API_TOKEN in env, require it via header X-Internal-Token.
    internal_token = os.environ.get("INTERNAL_API_TOKEN")
    if internal_token:
        if not x_internal_token or x_internal_token != internal_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid internal token",
            )

    # Basic sanity
    if not req.rows:
        raise HTTPException(status_code=400, detail="rows empty")

    # Idempotency: if hash already seen, return OK (no-op)
    with db_cursor() as (conn, cur):
        cur.execute(
            "SELECT 1 FROM promotion_events WHERE hash=%s LIMIT 1",
            (req.hash,),
        )
        if cur.fetchone():
            return {"status": "ok", "idempotent": True, "hash": req.hash}
        
        batch_published_at = datetime.now(timezone.utc).replace(microsecond=0)

        # Upsert promoted candidates
        upsert_sql = """
        INSERT INTO promoted_candidates (
            symbol, interval, strategy,
            paper_score, n_trades, win_rate, net_sum,
            window_name, policy_version, source_ts,
            published_at, meta,
            eligible_live, elig_reason
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s::jsonb, %s, %s)
        ON CONFLICT (symbol, interval, strategy)
        DO UPDATE SET
            paper_score=EXCLUDED.paper_score,
            n_trades=EXCLUDED.n_trades,
            win_rate=EXCLUDED.win_rate,
            net_sum=EXCLUDED.net_sum,
            window_name=EXCLUDED.window_name,
            policy_version=EXCLUDED.policy_version,
            source_ts=EXCLUDED.source_ts,
            published_at=EXCLUDED.published_at,
            meta=EXCLUDED.meta,
            eligible_live=EXCLUDED.eligible_live,
            elig_reason=EXCLUDED.elig_reason
        """
        n = 0
        for r in req.rows:
            meta = r.meta if isinstance(r.meta, dict) else {}
            eligible_live = bool(r.eligible_live) if r.eligible_live is not None else False
            elig_reason = r.elig_reason

            cur.execute(
            upsert_sql,
            (
                r.symbol, r.interval, r.strategy,
                float(r.paper_score),
                r.n_trades,
                r.win_rate,
                r.net_sum,
                r.window_name or req.window_name,
                r.policy_version or req.policy_version,
                r.source_ts or req.source_ts,
                batch_published_at,
                json.dumps(meta),
                eligible_live,
                elig_reason,
            ),
            )
            n += 1

        # Insert audit event
        cur.execute(
            """
            INSERT INTO promotion_events (source_ts, window_name, policy_version, n_rows, hash, meta)
            VALUES (%s,%s,%s,%s,%s,%s::jsonb)
            """,
            (req.source_ts, req.window_name, req.policy_version, n, req.hash, json.dumps({"rows": n})),
        )
        conn.commit()

    return {"status": "ok", "idempotent": False, "inserted": n, "hash": req.hash}
