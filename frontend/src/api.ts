import axios from "axios";

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL,
});

// 🔹 dodaliśmy SUPER_TREND
export type Strategy = "RSI" | "TREND" | "BBRANGE" | "SUPER_TREND";

export interface CandleSummary {
  symbol: string;
  interval: string;
  open_time: string;
  close: number;
  ema_21: number | null;
  rsi_14: number | null;

  // 🔹 Dodatkowe pola, żeby frontend mógł pokazać SuperTrend
  atr_14?: number | null;
  supertrend?: number | null;
  supertrend_direction?: number | null; // 1 = UP, -1 = DOWN
}

export interface SimulatedOrder {
  id: number;
  created_at: string;
  symbol: string;
  interval: string;
  side: string;
  price: number;
  quantity_btc: number;
  reason: string | null;
  rsi_14: number | null;
  ema_21: number | null;
  candle_open_time: string;
}

export interface EquityPoint {
  time: string;
  equity_usdt: number;
}

export interface PnLSummary {
  start_balance_usdt: number;
  final_balance_usdt: number;
  pnl_usdt: number;
  pnl_pct: number;
  trades: number;
  history: EquityPoint[];
}

export interface RoundtripTrade {
  id: number;
  direction: "LONG" | "SHORT";
  entry_order_id: number;
  exit_order_id: number;
  entry_time: string;
  exit_time: string;
  qty_btc: number;
  entry_price: number;
  exit_price: number;
  pnl_usdt: number;
  entry_rsi_14: number | null;
  entry_ema_21: number | null;
}

export interface RoundtripPage {
  total: number;
  items: RoundtripTrade[];
}

export interface SimulatedOrderPage {
  total: number;
  items: SimulatedOrder[];
}

export interface AssetBalance {
  asset: string;
  free: number;
  locked: number;
  total: number;
  total_usdt: number;
}

export interface AccountSummary {
  total_usdt: number;
  balances: AssetBalance[];
}

export type SymbolPair = "BTCUSDT" | "ETHUSDT" | "SOLUSDT" | "BNBUSDT";

/** 🔹 Metryki strategii dla (symbol, interval, strategy) */
export interface StrategyMetrics {
  symbol: string;
  interval: string;
  strategy: Strategy;
  trades: number;
  winning_trades: number;
  losing_trades: number;
  winrate_pct: number;
  total_pnl_usdt: number;
  avg_pnl_usdt: number;
  max_drawdown_pct: number;
  sharpe_ratio: number | null;
  z_score: number | null;
}

export interface AIAnalyzeResponse {
  analysis: string;
}

// (opcjonalnie, przyda się w App.tsx)
export const ALL_STRATEGIES: Strategy[] = [
  "RSI",
  "TREND",
  "BBRANGE",
  "SUPER_TREND",
];

export async function getAccountSummary() {
  return (await api.get<AccountSummary>("/account/summary")).data;
}

export async function getSummary(
  symbol: SymbolPair = "BTCUSDT",
  interval = "1m"
) {
  return (
    await api.get<CandleSummary>("/candles/summary", {
      params: { symbol, interval },
    })
  ).data;
}

export async function getOrders(
  symbol: SymbolPair = "BTCUSDT",
  page = 1,
  pageSize = 50,
  interval = "1m",
  strategy: Strategy = "RSI"
) {
  const offset = (page - 1) * pageSize;
  return (
    await api.get<SimulatedOrderPage>("/simulated/orders", {
      params: { symbol, interval, limit: pageSize, offset, strategy },
    })
  ).data;
}

export async function getPnL(
  symbol: SymbolPair = "BTCUSDT",
  interval = "1m",
  strategy: Strategy = "RSI"
) {
  return (
    await api.get<PnLSummary>("/simulated/pnl", {
      params: { symbol, interval, strategy },
    })
  ).data;
}

export async function getRoundtrips(
  symbol: SymbolPair = "BTCUSDT",
  page = 1,
  pageSize = 50,
  losersOnly = true,
  interval = "1m",
  strategy: Strategy = "RSI"
) {
  const offset = (page - 1) * pageSize;
  return (
    await api.get<RoundtripPage>("/simulated/roundtrips", {
      params: {
        symbol,
        interval,
        limit: pageSize,
        offset,
        losers_only: losersOnly,
        strategy,
      },
    })
  ).data;
}

/** 🔹 API do pobrania metryk (winrate, DD, Sharpe, Z-score...) */
export async function getMetrics(
  symbol: SymbolPair = "BTCUSDT",
  interval = "1m",
  strategy: Strategy = "RSI"
) {
  return (
    await api.get<StrategyMetrics>("/simulated/metrics", {
      params: { symbol, interval, strategy },
    })
  ).data;
}

export async function analyzeStrategyWithAI(
  prompt: string
): Promise<string> {
  try {
    const res = await api.post<AIAnalyzeResponse>("/ai/analyze-strategy", {
      prompt,
    });
    return res.data.analysis;
  } catch (err: any) {
    if (err.response) {
      throw new Error(
        `AI analyze error: ${err.response.status} ${JSON.stringify(
          err.response.data
        )}`
      );
    }
    throw err;
  }
}