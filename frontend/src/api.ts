import axios from "axios";

declare global {
  interface Window {
    __ENV__?: Record<string, any>;
  }
}

function getEnv(name: string): string | undefined {
  return window?.__ENV__?.[name];
}

export type UiEnvironment = "LIVE" | "PAPER";

export const LIVE_API_BASE_URL =
  getEnv("VITE_API_BASE_URL_LIVE") ||
  import.meta.env.VITE_API_BASE_URL_LIVE ||
  "http://localhost:8001";

export const PAPER_API_BASE_URL =
  getEnv("VITE_API_BASE_URL_PAPER") ||
  import.meta.env.VITE_API_BASE_URL_PAPER ||
  getEnv("VITE_API_BASE_URL") ||
  import.meta.env.VITE_API_BASE_URL ||
  "http://localhost:8000";

export const QUOTE_ASSET =
  (getEnv("VITE_QUOTE_ASSET") ||
    import.meta.env.VITE_QUOTE_ASSET ||
    "USDC").toUpperCase();

const UI_ENV_STORAGE_KEY = "trading-ui-environment";

export function getUiEnvironment(): UiEnvironment {
  const raw = typeof window !== "undefined" ? window.localStorage.getItem(UI_ENV_STORAGE_KEY) : null;
  return raw === "PAPER" ? "PAPER" : "LIVE";
}

export function setUiEnvironment(env: UiEnvironment) {
  if (typeof window !== "undefined") {
    window.localStorage.setItem(UI_ENV_STORAGE_KEY, env);
  }
}

export function getApiBaseUrlForEnvironment(env: UiEnvironment): string {
  return env === "LIVE" ? LIVE_API_BASE_URL : PAPER_API_BASE_URL;
}

export function getCurrentApiBaseUrl(): string {
  return getApiBaseUrlForEnvironment(getUiEnvironment());
}

function getApi() {
  return axios.create({ baseURL: getCurrentApiBaseUrl() });
}

export type Strategy = "RSI" | "TREND" | "BBRANGE" | "SUPERTREND";
export type SymbolPair = string;
export const BASE_ASSETS = ["BTC", "ETH", "SOL", "BNB"] as const;
export type BaseAsset = typeof BASE_ASSETS[number];
export const ALL_STRATEGIES: Strategy[] = ["RSI", "TREND", "BBRANGE", "SUPERTREND"];

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
  atr_14?: number | null;
  supertrend?: number | null;
  supertrend_direction?: number | null;
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
  live_orders_enabled?: string | null;
  panic_disable_trading?: string | null;
}

export interface OpsPositionOpen {
  id: number;
  symbol: string;
  interval: string;
  strategy: Strategy;
  side: string;
  entry_time: string;
  entry_price: number;
  qty: number;
  age_seconds: number;
}

export interface OpsPositionsOpenResponse {
  total: number;
  items: OpsPositionOpen[];
  error_type?: string;
  error?: string;
  note?: string;
}

export interface OpsLiveAttemptRow {
  symbol: string;
  interval: string;
  strategy: Strategy;
  reason: string;
  count: number;
  first_at: string;
  last_at: string;
}

export interface OpsLiveAttemptsResponse {
  window_minutes: number;
  total: number;
  items: OpsLiveAttemptRow[];
  error_type?: string;
  error?: string;
  note?: string;
}

export interface OpsBotControlRow {
  symbol: string;
  interval: string;
  strategy: Strategy;
  enabled: boolean;
  live_orders_enabled: boolean;
  regime_enabled: boolean;
  regime_mode: string | null;
  updated_at: string;
}

export interface OpsBotControlResponse {
  total: number;
  items: OpsBotControlRow[];
  error_type?: string;
  error?: string;
  note?: string;
}

export interface BotsActiveRow {
  symbol: string;
  strategy: Strategy;
  interval: string;
  last_seen: string;
  info: any;
}

export interface BotsActiveResponse {
  ttl_seconds: number;
  total: number;
  items: BotsActiveRow[];
  error_type?: string;
  error?: string;
}

export interface OpsEnvironmentResponse {
  environment: string;
  trading_mode: string;
  db_name: string;
  quote_asset: string;
}

export interface UiOrcDashboardRow {
  symbol: string;
  interval: string;
  strategy: Strategy;
  picked_via: string;
  enabled: boolean | null;
  live_orders_enabled: boolean | null;
  regime_enabled: boolean | null;
  regime_mode: string | null;
  bot_reason: string | null;
  bot_updated_at: string | null;
  target_notional_usdc: number | null;
  capital_reason: string | null;
  capital_last_checked: string | null;
  base_order_size: number | null;
  additional_usdc: number | null;
  adjusted_order_size: number | null;
  closed_trades_last3: number;
  wins_last3: number;
  last_trade_was_loss: number;
  n_signal_15m: number;
  last_signal_ts: string | null;
  n_buy_24h: number;
  n_runs_24h: number;
  n_filter_block_24h: number;
  filter_block_rate_24h: number;
  n_trades_3d: number;
  net_sum_3d: number;
  profit_factor_3d: number;
  last_exit_ts_3d: string | null;
  last_ts_24h: string | null;
  eligible_pick_v5: boolean;
  eligible_bootstrap_v5: boolean;
  eligible_signal_v5: boolean;
  eligible_activity_v5: boolean;
  eligible_softfill_v5: boolean;
  blocked_reason: string | null;
  blocked_at: string | null;
}

export interface UiOrcDashboardResponse {
  total: number;
  items: UiOrcDashboardRow[];
  available_sources?: {
    v_orc_picks_v5: boolean;
    v_orc_candidates_v5c: boolean;
    v_order_sizing_v1: boolean;
    bot_control: boolean;
    strategy_events: boolean;
    slot_capital_policy: boolean;
  };
  error_type?: string;
  error?: string;
  note?: string;
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

export interface UiLiveSummary {
  environment: string;
  trading_mode: string;
  panic: {
    enabled: boolean;
    reason: string;
    updated_at: string | null;
  };
  slot_counts: {
    total: number;
    enabled: number;
    live_orders_enabled: number;
    regime_enabled: number;
    enforce: number;
    effective_live: number;
  };
  open_positions: {
    count: number;
    market_value_usdc: number;
    unrealized_pnl_usdc: number;
  };
  heartbeats: {
    latest_at: string | null;
    total: number;
    fresh: number;
    stale: number;
  };
  market_data: {
    latest_mark_price_at: string | null;
  };
  bot_control: {
    last_updated_at: string | null;
  };
  snapshot_at: string;
  error_type?: string;
  error?: string;
  note?: string;
}

export interface UiOpenPosition {
  id: number;
  symbol: string;
  interval: string;
  strategy: Strategy;
  side: string;
  entry_time: string;
  entry_price: number | null;
  qty: number | null;
  age_seconds: number;
  current_price: number | null;
  market_value: number | null;
  unrealized_pnl_usdc: number | null;
  unrealized_pnl_pct: number | null;
  mark_open_time: string | null;
}

export interface UiOpenPositionsResponse {
  total: number;
  items: UiOpenPosition[];
  error_type?: string;
  error?: string;
  note?: string;
}

export interface UiRecentClosedPosition {
  id: number;
  exit_time: string;
  symbol: string;
  interval: string;
  strategy: Strategy;
  side: string;
  entry_time: string;
  entry_price: number | null;
  exit_price: number | null;
  qty: number | null;
  pnl_usdc: number;
  pnl_pct: number | null;
  exit_reason: string | null;
  win_loss: "WIN" | "LOSS" | "FLAT";
}

export interface UiRecentClosedResponse {
  limit: number;
  total: number;
  items: UiRecentClosedPosition[];
  error_type?: string;
  error?: string;
  note?: string;
}


export interface UiSlotRow {
  symbol: string;
  interval: string;
  strategy: Strategy;
  enabled: boolean;
  live_orders_enabled: boolean;
  regime_enabled: boolean;
  regime_mode: string | null;
  reason: string | null;
  updated_at: string | null;
  open_position: {
    exists: boolean;
    id: number | null;
    side: string | null;
    entry_time: string | null;
  };
  heartbeat: {
    last_seen: string | null;
    stale: boolean;
  };
  last_event: {
    at: string | null;
    event_type: string | null;
    decision: string | null;
    reason: string | null;
  };
}

export interface UiSlotsResponse {
  total: number;
  items: UiSlotRow[];
  error_type?: string;
  error?: string;
  note?: string;
}

export interface UiHealthResponse {
  api: {
    ok: boolean;
    environment: string;
    trading_mode: string;
    snapshot_at: string;
  };
  db: {
    ok: boolean;
    now: string | null;
  };
  bot_heartbeats: {
    latest_at: string | null;
    total: number;
    fresh: number;
    stale: number;
  };
  market_data: {
    latest_candle_close_at: string | null;
    tracked_pairs: number;
  };
  orchestrator: {
    latest_event_at: string | null;
    events_last_15m: number;
  };
  panic_state: {
    enabled: boolean;
    updated_at: string | null;
  };
  error_type?: string;
  error?: string;
  note?: string;
}

export interface UiAccountSummary {
  total_account_value_usdc: number;
  quote_asset: string;
  assets: Record<string, number>;
  asset_values_usdc: Record<string, number>;
  updated_at: string;
}

export interface UiTrading24hSummary {
  closed_pnl_24h: number;
  trades_24h: number;
  wins_24h: number;
  losses_24h: number;
  win_rate_24h: number;
  updated_at: string;
}

export interface UiSlotControlPayload {
  symbol: string;
  interval: string;
  strategy: Strategy;
  enabled?: boolean;
  live_orders_enabled?: boolean;
  reason?: string;
}

export interface UiRegimeControlPayload {
  symbol: string;
  interval: string;
  strategy: Strategy;
  regime_enabled: boolean;
  regime_mode: string;
  reason?: string;
}
export interface UiControlResponse {
  ok: boolean;
  message: string;
  applied_at: string;
  new_state: Record<string, unknown>;
}

export async function getSafetyStatus() {
  return (await getApi().get<SafetyStatus>("/safety/status")).data;
}

export async function getOpsPositionsOpen() {
  return (await getApi().get<OpsPositionsOpenResponse>("/ops/positions/open")).data;
}

export async function getOpsLiveAttempts(minutes = 120) {
  return (
    await getApi().get<OpsLiveAttemptsResponse>("/ops/live-attempts", { params: { minutes } })
  ).data;
}

export async function getOpsBotControl() {
  return (await getApi().get<OpsBotControlResponse>("/ops/bot-control")).data;
}

export async function getBotsActive(ttlSeconds = 600) {
  return (
    await getApi().get<BotsActiveResponse>("/bots/active", { params: { ttl_seconds: ttlSeconds } })
  ).data;
}

export async function getOpsEnvironment() {
  return (await getApi().get<OpsEnvironmentResponse>("/ops/environment")).data;
}

export async function getUiOrcDashboard() {
  return (await getApi().get<UiOrcDashboardResponse>("/ui/orc-dashboard")).data;
}

export async function getUiLiveSummary() {
  return (await getApi().get<UiLiveSummary>("/ui/live-summary")).data;
}

export async function getUiAccount() {
  return (await getApi().get<UiAccountSummary>("/ui/account")).data;
}

export async function getUiTrading24h() {
  return (await getApi().get<UiTrading24hSummary>("/ui/trading-24h")).data;
}

export async function getUiOpenPositions() {
  return (await getApi().get<UiOpenPositionsResponse>("/ui/open-positions")).data;
}

export async function getUiRecentClosed(limit = 10) {
  return (await getApi().get<UiRecentClosedResponse>("/ui/recent-closed", { params: { limit } })).data;
}

export async function updatePanicState(enabled: boolean, reason: string) {
  return (await getApi().post<UiControlResponse>("/ui/control/panic", { enabled, reason })).data;
}


export async function getUiSlots() {
  return (await getApi().get<UiSlotsResponse>("/ui/slots")).data;
}

export async function getUiHealth() {
  return (await getApi().get<UiHealthResponse>("/ui/health")).data;
}

export async function updateSlotControl(payload: UiSlotControlPayload) {
  return (await getApi().post<UiControlResponse>("/ui/control/slot", payload)).data;
}

export async function updateRegimeControl(payload: UiRegimeControlPayload) {
  return (await getApi().post<UiControlResponse>("/ui/control/regime", payload)).data;
}

export function envBool(v: string | null | undefined): boolean {
  if (!v) return false;
  const s = String(v).trim().toLowerCase();
  return s === "1" || s === "true" || s === "yes" || s === "on";
}

export async function getRegimeLatest(symbol: SymbolPair = makeSymbol("BTC"), interval = "1m") {
  return (await getApi().get<RegimePoint>("/regime/latest", { params: { symbol, interval } })).data;
}

export async function getWatchdogEvents(
  symbol: SymbolPair = makeSymbol("BTC"),
  interval = "1m",
  strategy: Strategy = "RSI",
  limit = 50,
  offset = 0,
) {
  return (
    await getApi().get<WatchdogEventPage>("/watchdog/events", {
      params: { symbol, interval, strategy, limit, offset },
    })
  ).data;
}

export async function getAccountSummary() {
  return (await getApi().get<AccountSummary>("/account/summary")).data;
}

export async function getSummary(symbol: SymbolPair = makeSymbol("BTC"), interval = "1m") {
  return (
    await getApi().get<CandleSummary>("/candles/summary", {
      params: { symbol, interval },
    })
  ).data;
}

export async function getOrders(
  symbol: SymbolPair = makeSymbol("BTC"),
  page = 1,
  pageSize = 50,
  interval = "1m",
  strategy: Strategy = "RSI",
) {
  const offset = (page - 1) * pageSize;
  return (
    await getApi().get<SimulatedOrderPage>("/simulated/orders", {
      params: { symbol, interval, limit: pageSize, offset, strategy },
    })
  ).data;
}

export async function getPnL(
  symbol: SymbolPair = makeSymbol("BTC"),
  interval = "1m",
  strategy: Strategy = "RSI",
) {
  return (
    await getApi().get<PnLSummary>("/simulated/pnl", {
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
  strategy: Strategy = "RSI",
) {
  const offset = (page - 1) * pageSize;
  return (
    await getApi().get<RoundtripPage>("/simulated/roundtrips", {
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

export async function getMetrics(
  symbol: SymbolPair = makeSymbol("BTC"),
  interval = "1m",
  strategy: Strategy = "RSI",
) {
  return (
    await getApi().get<StrategyMetrics>("/simulated/metrics", {
      params: { symbol, interval, strategy },
    })
  ).data;
}

export async function analyzeStrategyWithAI(prompt: string): Promise<string> {
  try {
    const res = await getApi().post<AIAnalyzeResponse>("/ai/analyze-strategy", {
      prompt,
    });
    return res.data.analysis;
  } catch (err: any) {
    if (err.response) {
      throw new Error(`AI analyze error: ${err.response.status} ${JSON.stringify(err.response.data)}`);
    }
    throw err;
  }
}
