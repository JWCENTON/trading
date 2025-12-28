import axios from "axios";

declare global {
  interface Window {
    __ENV__?: Record<string, any>;
  }
}

function getEnv(name: string): string | undefined {
  return window?.__ENV__?.[name];
}

export const API_BASE_URL =
  getEnv("VITE_API_BASE_URL") ||
  import.meta.env.VITE_API_BASE_URL ||
  "http://localhost:8000";

export const QUOTE_ASSET =
  (getEnv("VITE_QUOTE_ASSET") ||
    import.meta.env.VITE_QUOTE_ASSET ||
    "USDC").toUpperCase();

if (!API_BASE_URL) {
  throw new Error("Missing API base URL (VITE_API_BASE_URL).");
}

export const api = axios.create({ baseURL: API_BASE_URL });

export type Strategy = "RSI" | "TREND" | "BBRANGE" | "SUPER_TREND";

export type SymbolPair = string;

export const BASE_ASSETS = ["BTC", "ETH", "SOL", "BNB"] as const;
export type BaseAsset = typeof BASE_ASSETS[number];

export function makeSymbol(base: BaseAsset): SymbolPair {
  return `${base}${QUOTE_ASSET}`;
}


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
  fees_usdt_est?: number;
  slippage_usdt_est?: number;
  pnl_net_usdt_est?: number;
  pnl_net_pct_est?: number;
  final_balance_net_usdt_est?: number;
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

export interface WatchdogEvent {
  id: number;
  created_at: string;
  symbol: string;
  interval?: string | null;
  strategy?: Strategy | null;
  severity: string;
  event: string;
  details?: any;
}

export interface WatchdogEventPage {
  total: number;
  items: WatchdogEvent[];
}


export interface SafetyStatus {
  environment?: string | null;
  trading_mode?: string | null;
  live_orders_enabled?: string | null; // env jest string
  panic_disable_trading?: string | null;
}


export async function getSafetyStatus() {
  return (await api.get<SafetyStatus>("/safety/status")).data;
}


export interface RegimePoint {
  symbol: string;
  interval: string;
  ts: string;
  regime?: string | null;
  vol_regime?: string | null;
  trend_dir?: number | null;
  trend_strength_pct?: number | null;
  atr_pct?: number | null;
  shock_z?: number | null;
}

export async function getRegimeLatest(symbol: SymbolPair = makeSymbol("BTC"), interval="1m") {
  return (await api.get<RegimePoint>("/regime/latest", { params: { symbol, interval }})).data;
}

export async function getWatchdogEvents(
  symbol: SymbolPair = makeSymbol("BTC"),
  interval = "1m",
  strategy: Strategy = "RSI",
  limit = 50,
  offset = 0
) {
  return (
    await api.get<WatchdogEventPage>("/watchdog/events", {
      params: { symbol, interval, strategy, limit, offset },
    })
  ).data;
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
  symbol: SymbolPair = makeSymbol("BTC"),
  interval = "1m"
) {
  return (
    await api.get<CandleSummary>("/candles/summary", {
      params: { symbol, interval },
    })
  ).data;
}

export async function getOrders(
  symbol: SymbolPair = makeSymbol("BTC"),
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
  symbol: SymbolPair = makeSymbol("BTC"),
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
  symbol: SymbolPair = makeSymbol("BTC"),
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
  symbol: SymbolPair = makeSymbol("BTC"),
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