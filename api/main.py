import os
import math
from datetime import datetime
from typing import List, Optional, Dict

import psycopg2
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from binance.client import Client

import logging
import time
import json
import threading
import requests
from openai import OpenAI


DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "trading")
DB_USER = os.environ.get("DB_USER", "botuser")
DB_PASS = os.environ.get("DB_PASS", "botpass")
PAPER_START_USDT = float(os.environ.get("PAPER_START_USDT", "1000"))

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

ALL_STRATEGIES: list[str] = ["RSI", "TREND", "BBRANGE", "SUPER_TREND"]

ALLOWED_ORIGINS = [
    # PAPER
    "http://192.168.101.10:3000",
    "http://localhost:3000",

    # LIVE (jeśli chcesz, może zostać – nie szkodzi)
    "http://192.168.101.10:3001",
    "http://localhost:3001",
]

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
        "ENTRY_BUFFER_PCT",      # ułamek np. 0.002 = 0.2%
        "TREND_FILTER_PCT",      # ułamek
    },
    "BBRANGE": {
        "STOP_LOSS_PCT",
        "TAKE_PROFIT_PCT",
        "MAX_POSITION_MINUTES",
        "DAILY_MAX_LOSS_PCT",
        "BB_PERIOD",
        "BB_STD",
        "MIN_BB_WIDTH_PCT",      # ułamek np. 0.0025 = 0.25%
        # (opcjonalnie) jeśli chcesz stroić te filtry:
        "RSI_LONG_MAX",
        "RSI_SHORT_MIN",
        "RSI_BLOCK_EXTREME_LOW",
        "RSI_BLOCK_EXTREME_HIGH",
    },
    "SUPER_TREND": {
        "ATR_PERIOD",
        "ST_MULTIPLIER",
        "STOP_LOSS_PCT",
        "TAKE_PROFIT_PCT",
        "MAX_POSITION_MINUTES",
        "DAILY_MAX_LOSS_PCT",
        "MIN_ATR_PCT",           # w Twoim workerze używasz "0.25" jako % (ale liczysz ATR% i porównujesz do 0.25)
        "TREND_BUFFER",          # ułamek np. 0.001 = 0.1%
        "RSI_LONG_MAX",
        "RSI_SHORT_MIN",
        "RSI_BLOCK_EXTREME_LOW",
        "RSI_BLOCK_EXTREME_HIGH",
        # jeśli używasz wersji workera z ORDER_QTY_BTC:
        "ORDER_QTY_BTC",
    },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

AI_TUNE_INTERVAL_MINUTES = int(os.environ.get("AI_TUNE_INTERVAL_MINUTES", "1440"))
AI_AUTO_APPLY = os.environ.get("AI_AUTO_APPLY", "false").lower() == "true"

ENVIRONMENT = os.environ.get("ENVIRONMENT", "").lower()
TRADING_MODE = os.environ.get("TRADING_MODE", "").upper()

# Twardy bezpiecznik: pozwalamy auto-zapis tylko w PAPER+PAPER.
ALLOW_AI_DB_WRITES = (ENVIRONMENT == "paper" and TRADING_MODE == "PAPER" and AI_AUTO_APPLY)

binance_client = (
    Client(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
    if BINANCE_API_KEY and BINANCE_API_SECRET
    else None
)

app = FastAPI(title="Trading Bot API", version="0.1.0")

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


def load_current_params(symbol: str, strategy: str) -> Dict[str, float]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT param_name, param_value
        FROM strategy_params
        WHERE symbol = %s AND strategy = %s
        """,
        (symbol, strategy),
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


def upsert_strategy_params(symbol: str, strategy: str, params: Dict[str, float], source: str = "AI"):
    if source == "AI" and not ALLOW_AI_DB_WRITES:
        logging.warning(
            "AI tuner: blocked DB write (ENVIRONMENT=%s TRADING_MODE=%s AI_AUTO_APPLY=%s) for %s %s params=%s",
            ENVIRONMENT, TRADING_MODE, AI_AUTO_APPLY, symbol, strategy, list(params.keys())
        )
        return
    conn = get_conn()
    cur = conn.cursor()

    for name, value in params.items():
        cur.execute(
            """
            SELECT param_value FROM strategy_params
            WHERE symbol = %s AND strategy = %s AND param_name = %s
            """,
            (symbol, strategy, name),
        )
        row = cur.fetchone()
        old_value = float(row[0]) if row else None

        cur.execute(
            """
            INSERT INTO strategy_params (symbol, strategy, param_name, param_value)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, strategy, param_name)
            DO UPDATE SET param_value = EXCLUDED.param_value,
                          updated_at = now()
            """,
            (symbol, strategy, name, float(value)),
        )

        cur.execute(
            """
            INSERT INTO strategy_params_history (
                symbol, strategy, param_name, old_value, new_value, source
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (symbol, strategy, name, old_value, float(value), source),
        )

    conn.commit()
    cur.close()
    conn.close()


def fetch_metrics_and_losers(symbol: str, strategy: str, interval: str):
    params = {"symbol": symbol, "interval": interval, "strategy": strategy}

    m_resp = requests.get(f"{INTERNAL_API_BASE}/simulated/metrics", params=params, timeout=10)
    m_resp.raise_for_status()
    metrics = m_resp.json()

    rt_params = {**params, "losers_only": True, "limit": 20, "offset": 0}
    r_resp = requests.get(f"{INTERNAL_API_BASE}/simulated/roundtrips", params=rt_params, timeout=10)
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

    baseline_params = load_current_params(symbol, strategy)
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
    upsert_strategy_params(symbol, strategy, final_params, source="AI")


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


@app.get("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/candles/latest", response_model=List[Candle])
def get_latest_candles(
    symbol: str = Query(DEFAULT_SYMBOL),
    interval: str = Query("1m"),
    limit: int = Query(50, ge=1, le=500),
):
    conn = get_conn()
    cur = conn.cursor()
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
    cur.close()
    conn.close()

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
):
    conn = get_conn()
    cur = conn.cursor()

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
    cur.close()
    conn.close()

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
):
    conn = get_conn()
    cur = conn.cursor()
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
    cur.close()
    conn.close()

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
):
    conn = get_conn()
    cur = conn.cursor()
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
    cur.close()
    conn.close()

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
):
    conn = get_conn()
    cur = conn.cursor()
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
    cur.close()
    conn.close()

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
):
    conn = get_conn()
    cur = conn.cursor()
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
    cur.close()
    conn.close()

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
):
    conn = get_conn()
    cur = conn.cursor()
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
    cur.close()
    conn.close()

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


@app.get("/account/summary", response_model=AccountSummary)
def get_account_summary():
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

    tracked_assets = {"BTC", "ETH", "BNB", "USDC", "SOL"}

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
            px = 1.0

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


@app.on_event("startup")
def start_ai_tuner():
    t = threading.Thread(target=ai_auto_tuner_loop, daemon=True)
    t.start()
    logging.info("AI tuner thread started.")


@app.post("/ai/analyze-strategy", response_model=AIAnalyzeResponse)
def analyze_strategy_with_ai(req: AIAnalyzeRequest):
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
):
    conn = get_conn()
    cur = conn.cursor()

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
    cur.close()
    conn.close()

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
def get_regime_latest(symbol: str = Query(DEFAULT_SYMBOL), interval: str = Query("1m")):
    conn = get_conn()
    cur = conn.cursor()
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
    cur.close()
    conn.close()
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
):
    conn = get_conn()
    cur = conn.cursor()
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
    cur.close()
    conn.close()
    return [
        RegimePoint(
            symbol=r[0], interval=r[1], ts=r[2],
            regime=r[3], vol_regime=r[4], trend_dir=r[5],
            trend_strength_pct=r[6], atr_pct=r[7], shock_z=r[8],
        )
        for r in rows
    ]


@app.get("/safety/status")
def safety_status():
    return {
        "environment": os.environ.get("ENVIRONMENT"),
        "trading_mode": os.environ.get("TRADING_MODE"),
        "live_orders_enabled": os.environ.get("LIVE_ORDERS_ENABLED"),
        "panic_disable_trading": os.environ.get("PANIC_DISABLE_TRADING"),
    }


@app.get("/bots/active")
def bots_active(ttl_seconds: int = 600):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
      SELECT symbol, strategy, interval, last_seen, info
      FROM bot_heartbeat
      WHERE last_seen > now() - (%s || ' seconds')::interval
      ORDER BY last_seen DESC
    """, (ttl_seconds,))
    rows = cur.fetchall()
    