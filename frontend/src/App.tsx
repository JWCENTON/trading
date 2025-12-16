import { useEffect, useState } from "react";
import {
  type CandleSummary,
  type SimulatedOrder,
  type PnLSummary,
  type EquityPoint,
  type RoundtripTrade,
  type SymbolPair,
  type AccountSummary,
  type Strategy,
  type StrategyMetrics,
  type WatchdogEvent,
  type RegimePoint,
  getWatchdogEvents,
  getRegimeLatest,
  getAccountSummary,
  getSummary,
  getOrders,
  getPnL,
  getRoundtrips,
  getMetrics,
  analyzeStrategyWithAI,
  ALL_STRATEGIES, // 🔹 używamy wspólnej listy strategii z api.ts (RSI, TREND, BBRANGE, SUPER_TREND)
} from "./api";

function formatDate(d: string) {
  return new Date(d).toLocaleString();
}

function getPnlPrimary(p: PnLSummary) {
  const hasNet =
    typeof p.pnl_net_usdt_est === "number" &&
    typeof p.pnl_net_pct_est === "number" &&
    typeof p.final_balance_net_usdt_est === "number";

  return {
    hasNet,
    pnlUsdt: hasNet ? (p.pnl_net_usdt_est as number) : p.pnl_usdt,
    pnlPct: hasNet ? (p.pnl_net_pct_est as number) : p.pnl_pct,
    finalBalance: hasNet
      ? (p.final_balance_net_usdt_est as number)
      : p.final_balance_usdt,
    fees: p.fees_usdt_est ?? 0,
    slippage: p.slippage_usdt_est ?? 0,
  };
}

function EquityChart({ history }: { history: EquityPoint[] }) {
  if (!history || history.length < 2) {
    return (
      <div style={{ opacity: 0.7 }}>Not enough data for equity chart yet.</div>
    );
  }

  const width = 100;
  const height = 40;

  const values = history.map((p) => p.equity_usdt);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;

  const points = history.map((p, idx) => {
    const x = (idx / (history.length - 1)) * width;
    const y = height - ((p.equity_usdt - min) / span) * height;
    return { x, y };
  });

  const pathD = points
    .map((pt, idx) => `${idx === 0 ? "M" : "L"} ${pt.x} ${pt.y}`)
    .join(" ");

  return (
    <div
      style={{
        marginTop: 12,
        paddingTop: 8,
        borderTop: "1px solid #1f2937",
      }}
    >
      <div style={{ fontSize: 12, opacity: 0.7, marginBottom: 4 }}>
        Equity history
      </div>
      <div style={{ width: "100%", maxWidth: 600 }}>
        <svg
          viewBox={`0 0 ${width} ${height}`}
          preserveAspectRatio="none"
          style={{
            width: "100%",
            height: "160px",
          }}
        >
          <rect x="0" y="0" width={width} height={height} fill="#020617" />
          <path d={pathD} fill="none" stroke="#22c55e" strokeWidth="0.8" />
          {points.length > 0 && (
            <circle
              cx={points[points.length - 1].x}
              cy={points[points.length - 1].y}
              r="1.2"
              fill="#eab308"
            />
          )}
        </svg>
      </div>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          fontSize: 11,
          marginTop: 4,
          opacity: 0.7,
        }}
      >
        <span>min: {min.toFixed(2)} USDT</span>
        <span>max: {max.toFixed(2)} USDT</span>
      </div>
    </div>
  );
}

function AiAnalyzer({
  symbol,
  strategy,
  metrics,
  losingTrades,
}: {
  symbol: SymbolPair;
  strategy: Strategy;
  metrics: StrategyMetrics | null;
  losingTrades: RoundtripTrade[];
}) {
  const [copied, setCopied] = useState(false);

  const [aiLoading, setAiLoading] = useState(false);
  const [aiError, setAiError] = useState<string | null>(null);
  const [aiAnswer, setAiAnswer] = useState<string | null>(null);

  if (!metrics || losingTrades.length === 0) {
    return (
      <div
        style={{
          background: "#020617",
          padding: "16px",
          borderRadius: 12,
          boxShadow: "0 10px 30px rgba(0,0,0,0.7)",
          marginTop: 24,
        }}
      >
        <h2 style={{ fontSize: "18px", marginBottom: 8 }}>
          🤖 AI Analyzer – Etap A
        </h2>
        <div style={{ fontSize: 13, opacity: 0.8 }}>
          Czekam na metryki i/lub stratne transakcje. Jak tylko pojawią się
          losing trades dla tego bota, tutaj wygeneruje się prompt do AI.
        </div>
      </div>
    );
  }

  const maxTrades = 20;
  const selected = losingTrades.slice(0, maxTrades);

  const tradesText = selected
    .map((rt, idx) => {
      const pnlPct =
        rt.entry_price !== 0
          ? ((rt.exit_price - rt.entry_price) / rt.entry_price) *
            (rt.direction === "LONG" ? 100 : -100)
          : 0;
      return [
        `#${idx + 1}`,
        `Direction=${rt.direction}`,
        `EntryTime=${rt.entry_time}`,
        `ExitTime=${rt.exit_time}`,
        `EntryPrice=${rt.entry_price.toFixed(2)}`,
        `ExitPrice=${rt.exit_price.toFixed(2)}`,
        `QtyBTC=${rt.qty_btc.toFixed(8)}`,
        `PnL_USDT=${rt.pnl_usdt.toFixed(4)}`,
        `Approx_PnL_%=${pnlPct.toFixed(2)}`,
        `Entry_RSI=${
          rt.entry_rsi_14 !== null ? rt.entry_rsi_14.toFixed(2) : "NA"
        }`,
        `Entry_EMA21=${
          rt.entry_ema_21 !== null ? rt.entry_ema_21.toFixed(2) : "NA"
        }`,
      ].join(" | ");
    })
    .join("\n");

  const prompt = `You are an experienced quantitative trading assistant.

Analyze the following trading bot performance and losing roundtrips.
Suggest concrete, actionable improvements to the strategy parameters and/or entry/exit logic.
Focus on simple changes I can implement in code (thresholds, filters, timeouts, take-profit/stop-loss levels, etc.).

Bot:
- Symbol: ${symbol}
- Strategy: ${strategy}
- Interval: 1m

Metrics:
- Total trades: ${metrics.trades}
- Winning trades: ${metrics.winning_trades}
- Losing trades: ${metrics.losing_trades}
- Winrate: ${metrics.winrate_pct.toFixed(2)}%
- Total PnL (USDT): ${metrics.total_pnl_usdt.toFixed(4)}
- Average PnL per trade (USDT): ${metrics.avg_pnl_usdt.toFixed(6)}
- Max drawdown (%): ${metrics.max_drawdown_pct.toFixed(3)}
- Sharpe ratio: ${
    metrics.sharpe_ratio !== null
      ? metrics.sharpe_ratio.toFixed(4)
      : "NA"
  }
- Z-score (win/loss streaks): ${
    metrics.z_score !== null ? metrics.z_score.toFixed(4) : "NA"
  }

Losing roundtrips (latest, up to ${maxTrades} trades):
${tradesText}

Please:
1) Identify the main problems of this strategy based on the above data.
2) Propose parameter changes (with concrete numbers) that might improve performance.
3) Suggest additional simple filters or conditions (e.g. time of day, RSI range, trend filters) that could avoid the worst entries.
4) If something looks overfitted or statistically weak, mention it explicitly.`;

  async function handleCopy() {
    try {
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(prompt);
      } else {
        const textArea = document.createElement("textarea");
        textArea.value = prompt;
        textArea.style.position = "fixed";
        textArea.style.left = "-9999px";
        textArea.style.top = "-9999px";
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        try {
          document.execCommand("copy");
        } finally {
          document.body.removeChild(textArea);
        }
      }

      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (e) {
      console.error("Clipboard error", e);
    }
  }

  async function handleAskBuiltInAI() {
    try {
      setAiError(null);
      setAiAnswer(null);
      setAiLoading(true);
      const answer = await analyzeStrategyWithAI(prompt);
      setAiAnswer(answer);
    } catch (e: any) {
      console.error(e);
      setAiError(e?.message || "Error calling built-in AI");
    } finally {
      setAiLoading(false);
    }
  }

  return (
    <div
      style={{
        background: "#020617",
        padding: "16px",
        borderRadius: 12,
        boxShadow: "0 10px 30px rgba(0,0,0,0.7)",
        marginTop: 24,
      }}
    >
      <h2 style={{ fontSize: "18px", marginBottom: 8 }}>
        🤖 AI Analyzer – Etap A
      </h2>
      <p style={{ fontSize: 13, opacity: 0.8, marginBottom: 8 }}>
        Poniżej masz gotowy prompt, który możesz wkleić do ChatGPT (lub innego
        AI). Jest zbudowany z metryk strategii i ostatnich stratnych roundtripów
        dla bota <b>{symbol.replace("USDT", "")}</b> / <b>{strategy}</b>.
        <br />
        Dodatkowo możesz użyć przycisku{" "}
        <b>„Zapytaj wbudowane AI (OpenAI)”</b>, żeby backend sam wysłał ten
        prompt do API.
      </p>

      <div
        style={{
          display: "flex",
          gap: 8,
          marginBottom: 8,
          flexWrap: "wrap",
        }}
      >
        <button
          onClick={handleCopy}
          style={{
            padding: "6px 12px",
            borderRadius: 8,
            border: "1px solid #1f2937",
            background: "#1f2937",
            color: "#e5e7eb",
            fontSize: 13,
            cursor: "pointer",
          }}
        >
          {copied ? "✅ Copied!" : "📋 Copy prompt"}
        </button>

        <button
          onClick={handleAskBuiltInAI}
          disabled={aiLoading}
          style={{
            padding: "6px 12px",
            borderRadius: 8,
            border: "1px solid #0ea5e9",
            background: aiLoading ? "#1e293b" : "#0ea5e9",
            color: "#02110a",
            fontSize: 13,
            cursor: aiLoading ? "default" : "pointer",
          }}
        >
          {aiLoading ? "⏳ Asking OpenAI..." : "🚀 Zapytaj wbudowane AI (OpenAI)"}
        </button>
      </div>

      <textarea
        readOnly
        value={prompt}
        style={{
          width: "100%",
          minHeight: 220,
          background: "#020617",
          color: "#e5e7eb",
          borderRadius: 8,
          border: "1px solid #1f2937",
          padding: 10,
          fontSize: 12,
          fontFamily: "monospace",
          resize: "vertical",
        }}
      />

      {aiError && (
        <div
          style={{
            marginTop: 12,
            padding: 8,
            borderRadius: 8,
            background: "#7f1d1d",
            fontSize: 12,
          }}
        >
          ❌ AI error: {aiError}
        </div>
      )}

      {aiAnswer && (
        <div
          style={{
            marginTop: 12,
            padding: 10,
            borderRadius: 8,
            background: "#020617",
            border: "1px solid #1f2937",
            fontSize: 13,
            whiteSpace: "pre-wrap",
          }}
        >
          <div
            style={{
              fontSize: 12,
              opacity: 0.7,
              marginBottom: 4,
            }}
          >
            Odpowiedź wbudowanego AI (OpenAI):
          </div>
          {aiAnswer}
        </div>
      )}
    </div>
  );
}

const ALL_SYMBOLS: SymbolPair[] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"];
// 🔹 usunięte lokalne const ALL_STRATEGIES – korzystamy z importu z api.ts

function App() {
  const [activeTab, setActiveTab] = useState<"HOME" | "BOT">("HOME");

  const [symbol, setSymbol] = useState<SymbolPair>("BTCUSDT");
  const [strategy, setStrategy] = useState<Strategy>("RSI");

  const [summary, setSummary] = useState<CandleSummary | null>(null);
  const [account, setAccount] = useState<AccountSummary | null>(null);

  const [pnl, setPnl] = useState<PnLSummary | null>(null);
  const [portfolioPnl, setPortfolioPnl] = useState<PnLSummary | null>(null);

  const [allPnL, setAllPnL] = useState<Record<string, PnLSummary>>({});
  const [metrics, setMetrics] = useState<Record<string, StrategyMetrics>>({});

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [roundtrips, setRoundtrips] = useState<RoundtripTrade[]>([]);
  const [losingTotal, setLosingTotal] = useState(0);
  const [tradesPage, setTradesPage] = useState(1);
  const TRADES_PAGE_SIZE = 50;

  const [ordersPage, setOrdersPage] = useState<SimulatedOrder[]>([]);
  const [ordersTotal, setOrdersTotal] = useState(0);
  const [ordersPageNum, setOrdersPageNum] = useState(1);
  const ORDERS_PAGE_SIZE = 50;

  const [regime, setRegime] = useState<RegimePoint | null>(null);
  const [watchdog, setWatchdog] = useState<WatchdogEvent[]>([]);
  const [watchdogTotal, setWatchdogTotal] = useState(0);

  const ordersTotalPages =
    ordersTotal === 0 ? 1 : Math.ceil(ordersTotal / ORDERS_PAGE_SIZE);
  const currentOrdersPage = Math.min(ordersPageNum, ordersTotalPages);

  const totalLosingPages =
    losingTotal === 0 ? 1 : Math.ceil(losingTotal / TRADES_PAGE_SIZE);
  const currentPage = Math.min(tradesPage, totalLosingPages);

  function handleOrdersPrev() {
    setOrdersPageNum((p) => Math.max(1, p - 1));
  }

  function handleOrdersNext() {
    setOrdersPageNum((p) => Math.min(ordersTotalPages, p + 1));
  }

  function handlePrevPage() {
    setTradesPage((p) => Math.max(1, p - 1));
  }

  function handleNextPage() {
    setTradesPage((p) => Math.min(totalLosingPages, p + 1));
  }

  useEffect(() => {
    setTradesPage(1);
    setOrdersPageNum(1);
  }, [symbol, strategy]);

  async function loadData(currentSymbol: SymbolPair, page: number) {
    try {
      setError(null);
      setLoading(true);

      const pnlPromises: Promise<PnLSummary>[] = [];
      const metricsPromises: Promise<StrategyMetrics>[] = [];
      const comboKeys: string[] = [];

      for (const sym of ALL_SYMBOLS) {
        for (const strat of ALL_STRATEGIES) {
          pnlPromises.push(getPnL(sym, "1m", strat));
          metricsPromises.push(getMetrics(sym, "1m", strat));
          comboKeys.push(`${sym}-${strat}`);
        }
      }

      const [s, oPage, rtPage, acct, pnlResults, metricsResults, reg, wd] =
        await Promise.all([
          getSummary(currentSymbol),
          getOrders(
            currentSymbol,
            currentOrdersPage,
            ORDERS_PAGE_SIZE,
            "1m",
            strategy
          ),
          getRoundtrips(
            currentSymbol,
            page,
            TRADES_PAGE_SIZE,
            true,
            "1m",
            strategy
          ),
          getAccountSummary(),
          Promise.all(pnlPromises),
          Promise.all(metricsPromises),

          // 🔹 NOWE
          getRegimeLatest(currentSymbol, "1m"),
          getWatchdogEvents(currentSymbol, "1m", strategy, 50, 0),
        ]);

      setSummary(s);
      setOrdersPage(oPage.items);
      setOrdersTotal(oPage.total);
      setRoundtrips(rtPage.items);
      setLosingTotal(rtPage.total);
      setAccount(acct);
      setRegime(reg);
      setWatchdog(wd.items);
      setWatchdogTotal(wd.total);

      const pnlsMap: Record<string, PnLSummary> = {};
      const metricsMap: Record<string, StrategyMetrics> = {};
      comboKeys.forEach((key, idx) => {
        pnlsMap[key] = pnlResults[idx];
        metricsMap[key] = metricsResults[idx];
      });
      setAllPnL(pnlsMap);
      setMetrics(metricsMap);

      const currentPnl = pnlsMap[`${currentSymbol}-${strategy}`];
      setPnl(currentPnl || null);

      const parts = ALL_SYMBOLS.map(
        (sym) => pnlsMap[`${sym}-${strategy}`]
      ).filter((p): p is PnLSummary => !!p);

      if (parts.length === 0) {
        setPortfolioPnl(null);
      } else {
        const start = parts.reduce(
          (sum, p) => sum + p.start_balance_usdt,
          0
        );
        const final = parts.reduce((sum, p) => sum + getPnlPrimary(p).finalBalance, 0);          
        const pnlUsdt = final - start;
        const pnlPct = start > 0 ? (pnlUsdt / start) * 100 : 0;
        const tradesTotal = parts.reduce((sum, p) => sum + p.trades, 0);

        const len = Math.min(...parts.map((p) => p.history.length));
        const portfolioHistory: EquityPoint[] = [];

        for (let i = 0; i < len; i++) {
          let equitySum = 0;
          let latestTime: string | null = null;

          for (const p of parts) {
            const h = p.history[i];
            equitySum += h.equity_usdt;

            if (!latestTime || new Date(h.time) > new Date(latestTime)) {
              latestTime = h.time;
            }
          }

          if (latestTime) {
            portfolioHistory.push({
              time: latestTime,
              equity_usdt: equitySum,
            });
          }
        }

        setPortfolioPnl({
          start_balance_usdt: start,
          final_balance_usdt: final,
          pnl_usdt: pnlUsdt,
          pnl_pct: pnlPct,
          trades: tradesTotal,
          history: portfolioHistory,
        });
      }
    } catch (e: any) {
      console.error(e);
      setError(e?.message || "Error loading data");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData(symbol, currentPage);
    const interval = setInterval(() => loadData(symbol, currentPage), 60_000);
    return () => clearInterval(interval);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbol, currentPage, currentOrdersPage, strategy]);

  const pageStartIndex = (currentPage - 1) * TRADES_PAGE_SIZE;
  const ordersRowStartIndex = (currentOrdersPage - 1) * ORDERS_PAGE_SIZE;

  const allPnLValues = Object.values(allPnL);
  let globalPnL: PnLSummary | null = null;

  if (allPnLValues.length > 0) {
    const start = allPnLValues.reduce(
      (sum, p) => sum + p.start_balance_usdt,
      0
    );
    const final = allPnLValues.reduce((sum, p) => {
      const prim = getPnlPrimary(p);
      return sum + prim.finalBalance;
    }, 0);
    const pnlUsdt = final - start;
    const pnlPct = start > 0 ? (pnlUsdt / start) * 100 : 0;
    const tradesTotal = allPnLValues.reduce((sum, p) => sum + p.trades, 0);

    const minLen = Math.min(...allPnLValues.map((p) => p.history.length));
    const history: EquityPoint[] = [];

    if (minLen > 0 && Number.isFinite(minLen)) {
      for (let i = 0; i < minLen; i++) {
        let equitySum = 0;
        let latestTime: string | null = null;

        for (const p of allPnLValues) {
          const h = p.history[i];
          equitySum += h.equity_usdt;
          if (!latestTime || new Date(h.time) > new Date(latestTime)) {
            latestTime = h.time;
          }
        }

        if (latestTime) {
          history.push({
            time: latestTime,
            equity_usdt: equitySum,
          });
        }
      }
    }

    globalPnL = {
      start_balance_usdt: start,
      final_balance_usdt: final,
      pnl_usdt: pnlUsdt,
      pnl_pct: pnlPct,
      trades: tradesTotal,
      history,
    };
  }

  const feesTotal = allPnLValues.reduce((sum, p) => sum + (p.fees_usdt_est ?? 0), 0);
  const slipTotal = allPnLValues.reduce((sum, p) => sum + (p.slippage_usdt_est ?? 0), 0);
  const currentKey = `${symbol}-${strategy}`;
  const currentMetrics: StrategyMetrics | null = metrics[currentKey] ?? null;

  return (
    <div
      className="app-root"
      style={{
        fontFamily: "system-ui, sans-serif",
        padding: "20px",
        background: "#0f172a",
        minHeight: "100vh",
        color: "#e5e7eb",
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-end",
          marginBottom: "16px",
          gap: 12,
          flexWrap: "wrap",
        }}
      >
        <div>
          <h1 style={{ fontSize: "24px", margin: 0 }}>
            {activeTab === "HOME"
              ? "📊 Trading Bots – Home"
              : `🧠 ${symbol} Bot Dashboard (${strategy})`}
          </h1>
          <div
            style={{
              marginTop: 8,
              display: "inline-flex",
              borderRadius: 999,
              border: "1px solid #1f2937",
              overflow: "hidden",
            }}
          >
            <button
              onClick={() => setActiveTab("HOME")}
              style={{
                padding: "6px 14px",
                fontSize: 13,
                border: "none",
                background: activeTab === "HOME" ? "#22c55e" : "transparent",
                color: activeTab === "HOME" ? "#02110a" : "#e5e7eb",
                cursor: "pointer",
              }}
            >
              HOME
            </button>
            <button
              onClick={() => setActiveTab("BOT")}
              style={{
                padding: "6px 14px",
                fontSize: 13,
                border: "none",
                background: activeTab === "BOT" ? "#38bdf8" : "transparent",
                color: activeTab === "BOT" ? "#02110a" : "#e5e7eb",
                cursor: "pointer",
              }}
            >
              BOT VIEW
            </button>
          </div>
        </div>

        {activeTab === "BOT" && (
          <div
            style={{
              display: "flex",
              gap: 12,
              flexWrap: "wrap",
              alignItems: "center",
            }}
          >
            <div
              style={{
                display: "inline-flex",
                borderRadius: 999,
                border: "1px solid #1f2937",
                overflow: "hidden",
              }}
            >
              {ALL_SYMBOLS.map((s) => {
                const active = s === symbol;
                return (
                  <button
                    key={s}
                    onClick={() => setSymbol(s)}
                    style={{
                      padding: "6px 14px",
                      fontSize: 13,
                      border: "none",
                      background: active ? "#22c55e" : "transparent",
                      color: active ? "#02110a" : "#e5e7eb",
                      cursor: "pointer",
                    }}
                  >
                    {s.replace("USDT", "")}
                  </button>
                );
              })}
            </div>

            <div
              style={{
                display: "inline-flex",
                borderRadius: 999,
                border: "1px solid #1f2937",
                overflow: "hidden",
              }}
            >
              {ALL_STRATEGIES.map((st) => {
                const active = st === strategy;
                return (
                  <button
                    key={st}
                    onClick={() => setStrategy(st)}
                    style={{
                      padding: "6px 14px",
                      fontSize: 13,
                      border: "none",
                      background: active ? "#38bdf8" : "transparent",
                      color: active ? "#02110a" : "#e5e7eb",
                      cursor: "pointer",
                    }}
                  >
                    {st}
                  </button>
                );
              })}
            </div>
          </div>
        )}
      </div>

      {error && (
        <div
          style={{
            background: "#b91c1c",
            padding: "8px 12px",
            borderRadius: 8,
            marginBottom: 16,
          }}
        >
          Error: {error}
        </div>
      )}

      {loading && <div>Loading...</div>}

      {!loading && activeTab === "HOME" && (
        <>
          {account && (
            <div
              style={{
                background: "#111827",
                padding: "16px",
                borderRadius: 12,
                marginBottom: 24,
                boxShadow: "0 10px 30px rgba(0,0,0,0.5)",
              }}
            >
              <h2 style={{ fontSize: "18px", marginBottom: 8 }}>
                🔐 Real Binance account
              </h2>

              <div style={{ marginBottom: 12 }}>
                <div style={{ fontSize: 12, opacity: 0.7 }}>Total value</div>
                <div style={{ fontSize: 18, fontWeight: 600 }}>
                  {account.total_usdt.toFixed(2)} USDT
                </div>
              </div>

              <div
                style={{
                  fontSize: 13,
                  opacity: 0.8,
                  marginBottom: 8,
                }}
              >
                Breakdown:
              </div>

              <div style={{ overflowX: "auto" }}>
                <table
                  style={{
                    width: "100%",
                    borderCollapse: "collapse",
                    fontSize: 13,
                  }}
                >
                  <thead>
                    <tr
                      style={{
                        textAlign: "left",
                        borderBottom: "1px solid #1f2937",
                      }}
                    >
                      <th style={{ padding: "6px 8px" }}>Asset</th>
                      <th style={{ padding: "6px 8px" }}>Free</th>
                      <th style={{ padding: "6px 8px" }}>Locked</th>
                      <th style={{ padding: "6px 8px" }}>Total</th>
                      <th style={{ padding: "6px 8px" }}>Value (USDT)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {account.balances.map((b) => (
                      <tr
                        key={b.asset}
                        style={{ borderBottom: "1px solid #111827" }}
                      >
                        <td style={{ padding: "6px 8px" }}>{b.asset}</td>
                        <td style={{ padding: "6px 8px" }}>
                          {b.free.toFixed(8)}
                        </td>
                        <td style={{ padding: "6px 8px" }}>
                          {b.locked.toFixed(8)}
                        </td>
                        <td style={{ padding: "6px 8px" }}>
                          {b.total.toFixed(8)}
                        </td>
                        <td style={{ padding: "6px 8px" }}>
                          {b.total_usdt.toFixed(2)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {globalPnL && (
            <div
              style={{
                background: "#111827",
                padding: "16px",
                borderRadius: 12,
                marginBottom: 24,
                boxShadow: "0 10px 30px rgba(0,0,0,0.5)",
              }}
            >
              <h2 style={{ fontSize: "18px", marginBottom: 8 }}>
                🌍 Global virtual PnL (all bots)
              </h2>
              <div style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>
                    Start balance
                  </div>
                  <div>{globalPnL.start_balance_usdt.toFixed(2)} USDT</div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>
                    Current balance
                  </div>
                  <div>{globalPnL.final_balance_usdt.toFixed(2)} USDT</div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>PnL</div>
                  <div
                    style={{
                      color:
                        globalPnL.pnl_usdt >= 0 ? "#22c55e" : "#ef4444",
                      fontWeight: 600,
                    }}
                  >
                    {globalPnL.pnl_usdt.toFixed(2)} USDT (
                    {globalPnL.pnl_pct.toFixed(2)}%)
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>
                    Trades (total)
                  </div>
                  <div>{globalPnL.trades}</div>
                  <div>
                    <div style={{ fontSize: 12, opacity: 0.7 }}>Fees (est.)</div>
                    <div>{feesTotal.toFixed(2)} USDT</div>
                  </div>
                  <div>
                    <div style={{ fontSize: 12, opacity: 0.7 }}>Slippage (est.)</div>
                    <div>{slipTotal.toFixed(2)} USDT</div>
                  </div>
                </div>
              </div>
              <EquityChart history={globalPnL.history} />
            </div>
          )}

          <div
            style={{
              background: "#020617",
              padding: "16px",
              borderRadius: 12,
              boxShadow: "0 10px 30px rgba(0,0,0,0.7)",
              marginBottom: 24,
            }}
          >
            <h2 style={{ fontSize: "18px", marginBottom: 12 }}>
              Bots overview (by symbol & strategy)
            </h2>

            <div style={{ overflowX: "auto" }}>
              <table
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                  fontSize: 14,
                }}
              >
                <thead>
                  <tr
                    style={{
                      textAlign: "left",
                      borderBottom: "1px solid #1f2937",
                    }}
                  >
                    <th style={{ padding: "6px 8px" }}>Symbol</th>
                    <th style={{ padding: "6px 8px" }}>Strategy</th>
                    <th style={{ padding: "6px 8px" }}>Start</th>
                    <th style={{ padding: "6px 8px" }}>Current</th>
                    <th style={{ padding: "6px 8px" }}>PnL (USDT)</th>
                    <th style={{ padding: "6px 8px" }}>PnL (%)</th>
                    <th style={{ padding: "6px 8px" }}>Trades</th>
                    <th style={{ padding: "6px 8px" }}>Winrate</th>
                    <th style={{ padding: "6px 8px" }}>Max DD</th>
                    <th style={{ padding: "6px 8px" }}>Sharpe</th>
                  </tr>
                </thead>
                <tbody>
                  {ALL_SYMBOLS.map((sym) =>
                    ALL_STRATEGIES.map((st) => {
                      const key = `${sym}-${st}`;
                      const stats = allPnL[key];
                      const sPrim = stats ? getPnlPrimary(stats) : null;
                      const m = metrics[key];

                      return (
                        <tr
                          key={key}
                          style={{
                            borderBottom: "1px solid #111827",
                          }}
                        >
                          <td style={{ padding: "6px 8px" }}>
                            {sym.replace("USDT", "")}
                          </td>
                          <td style={{ padding: "6px 8px" }}>{st}</td>
                          {stats ? (
                            <>
                              <td style={{ padding: "6px 8px" }}>
                                {stats.start_balance_usdt.toFixed(2)}
                              </td>
                              <td style={{ padding: "6px 8px" }}>
                                {sPrim ? sPrim.finalBalance.toFixed(2) : "-"}
                              </td>

                              <td
                                style={{
                                  padding: "6px 8px",
                                  color: sPrim && sPrim.pnlUsdt >= 0 ? "#22c55e" : "#ef4444",
                                  fontWeight: 600,
                                }}
                              >
                                {sPrim ? sPrim.pnlUsdt.toFixed(2) : "-"}
                              </td>

                              <td style={{ padding: "6px 8px" }}>
                                {sPrim ? sPrim.pnlPct.toFixed(2) : "-"}%
                              </td>

                              <td style={{ padding: "6px 8px" }}>
                                {stats.trades}
                              </td>
                            </>
                          ) : (
                            <>
                              <td style={{ padding: "6px 8px" }}>-</td>
                              <td style={{ padding: "6px 8px" }}>-</td>
                              <td style={{ padding: "6px 8px" }}>-</td>
                              <td style={{ padding: "6px 8px" }}>-</td>
                              <td style={{ padding: "6px 8px" }}>-</td>
                            </>
                          )}

                          {m ? (
                            <>
                              <td style={{ padding: "6px 8px" }}>
                                {m.winrate_pct.toFixed(1)}%
                              </td>
                              <td style={{ padding: "6px 8px" }}>
                                -{m.max_drawdown_pct.toFixed(2)}%
                              </td>
                              <td style={{ padding: "6px 8px" }}>
                                {m.sharpe_ratio !== null
                                  ? m.sharpe_ratio.toFixed(2)
                                  : "-"}
                              </td>
                            </>
                          ) : (
                            <>
                              <td style={{ padding: "6px 8px" }}>-</td>
                              <td style={{ padding: "6px 8px" }}>-</td>
                              <td style={{ padding: "6px 8px" }}>-</td>
                            </>
                          )}
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}

      {!loading && activeTab === "BOT" && (
        <>
          {portfolioPnl && (
            <div
              style={{
                background: "#111827",
                padding: "16px",
                borderRadius: 12,
                marginBottom: 24,
                boxShadow: "0 10px 30px rgba(0,0,0,0.5)",
              }}
            >
              <h2 style={{ fontSize: "18px", marginBottom: 8 }}>
                Portfolio PnL (
                {ALL_SYMBOLS.map((s) => s.replace("USDT", "")).join(" + ")}
                , {strategy})
              </h2>
              <div style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>
                    Start balance
                  </div>
                  <div>
                    {portfolioPnl.start_balance_usdt.toFixed(2)} USDT
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>
                    Current balance
                  </div>
                  <div>
                    {portfolioPnl.final_balance_usdt.toFixed(2)} USDT
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>PnL</div>
                  <div
                    style={{
                      color:
                        portfolioPnl.pnl_usdt >= 0 ? "#22c55e" : "#ef4444",
                      fontWeight: 600,
                    }}
                  >
                    {portfolioPnl.pnl_usdt.toFixed(2)} USDT (
                    {portfolioPnl.pnl_pct.toFixed(2)}%)
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>
                    Trades (total)
                  </div>
                  <div>{portfolioPnl.trades}</div>
                </div>
              </div>
              <EquityChart history={portfolioPnl.history} />
            </div>
          )}

          {pnl && (() => {
            const pPrim = getPnlPrimary(pnl);

            return (
              <div
                style={{
                  background: "#111827",
                  padding: "16px",
                  borderRadius: 12,
                  marginBottom: 24,
                  boxShadow: "0 10px 30px rgba(0,0,0,0.5)",
                }}
              >
                <h2 style={{ fontSize: "18px", marginBottom: 8 }}>
                  Virtual account PnL ({symbol.replace("USDT", "")}, {strategy})
                </h2>

                <div style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
                  <div>
                    <div style={{ fontSize: 12, opacity: 0.7 }}>Start balance</div>
                    <div>{pnl.start_balance_usdt.toFixed(2)} USDT</div>
                  </div>

                  <div>
                    <div style={{ fontSize: 12, opacity: 0.7 }}>
                      Current balance{pPrim.hasNet ? " (NET est.)" : ""}
                    </div>
                    <div>{pPrim.finalBalance.toFixed(2)} USDT</div>
                  </div>

                  <div>
                    <div style={{ fontSize: 12, opacity: 0.7 }}>
                      PnL{pPrim.hasNet ? " (NET est.)" : ""}
                    </div>
                    <div
                      style={{
                        color: pPrim.pnlUsdt >= 0 ? "#22c55e" : "#ef4444",
                        fontWeight: 600,
                      }}
                    >
                      {pPrim.pnlUsdt.toFixed(2)} USDT ({pPrim.pnlPct.toFixed(2)}%)
                    </div>
                  </div>

                  <div>
                    <div style={{ fontSize: 12, opacity: 0.7 }}>Trades</div>
                    <div>{pnl.trades}</div>
                  </div>

                  {pPrim.hasNet && (
                    <>
                      <div>
                        <div style={{ fontSize: 12, opacity: 0.7 }}>Fees (est.)</div>
                        <div>{pPrim.fees.toFixed(2)} USDT</div>
                      </div>
                      <div>
                        <div style={{ fontSize: 12, opacity: 0.7 }}>Slippage (est.)</div>
                        <div>{pPrim.slippage.toFixed(2)} USDT</div>
                      </div>
                    </>
                  )}

                  {currentMetrics && (
                    <>
                      <div>
                        <div style={{ fontSize: 12, opacity: 0.7 }}>Winrate</div>
                        <div>{currentMetrics.winrate_pct.toFixed(1)}%</div>
                      </div>
                      <div>
                        <div style={{ fontSize: 12, opacity: 0.7 }}>Max drawdown</div>
                        <div>-{currentMetrics.max_drawdown_pct.toFixed(2)}%</div>
                      </div>
                      <div>
                        <div style={{ fontSize: 12, opacity: 0.7 }}>Sharpe</div>
                        <div>
                          {currentMetrics.sharpe_ratio !== null
                            ? currentMetrics.sharpe_ratio.toFixed(2)
                            : "-"}
                        </div>
                      </div>
                      <div>
                        <div style={{ fontSize: 12, opacity: 0.7 }}>Z-score (vs 50%)</div>
                        <div>
                          {currentMetrics.z_score !== null
                            ? currentMetrics.z_score.toFixed(2)
                            : "-"}
                        </div>
                      </div>
                    </>
                  )}
                </div>

                <EquityChart history={pnl.history} />
                {pPrim.hasNet && (
                  <div style={{ marginTop: 6, fontSize: 11, opacity: 0.65 }}>
                    Note: equity chart is gross; headline values are NET (est.).
                  </div>
                )}
              </div>
            );
          })()}

          {summary && (
            <div
              style={{
                background: "#111827",
                padding: "16px",
                borderRadius: 12,
                marginBottom: 24,
                boxShadow: "0 10px 30px rgba(0,0,0,0.5)",
              }}
            >
              <h2 style={{ fontSize: "18px", marginBottom: 8 }}>
                Last candle (summary)
              </h2>
              <div style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>Symbol</div>
                  <div style={{ fontWeight: 600 }}>{summary.symbol}</div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>
                    Interval
                  </div>
                  <div>{summary.interval}</div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>
                    Open time
                  </div>
                  <div>{formatDate(summary.open_time)}</div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>Close</div>
                  <div>{summary.close.toFixed(2)}</div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>EMA 21</div>
                  <div>
                    {summary.ema_21 !== null
                      ? summary.ema_21.toFixed(2)
                      : "-"}
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>RSI 14</div>
                  <div>
                    {summary.rsi_14 !== null
                      ? summary.rsi_14.toFixed(2)
                      : "-"}
                  </div>
                </div>

                {/* 🔹 Dodatkowe pola dla SuperTrend – pokazujemy gdy są dostępne */}
                {summary.atr_14 !== undefined && (
                  <div>
                    <div style={{ fontSize: 12, opacity: 0.7 }}>ATR 14</div>
                    <div>
                      {summary.atr_14 !== null
                        ? summary.atr_14.toFixed(4)
                        : "-"}
                    </div>
                  </div>
                )}
                {summary.supertrend !== undefined && (
                  <div>
                    <div style={{ fontSize: 12, opacity: 0.7 }}>
                      SuperTrend
                    </div>
                    <div>
                      {summary.supertrend !== null
                        ? summary.supertrend.toFixed(2)
                        : "-"}
                    </div>
                  </div>
                )}
                {summary.supertrend_direction !== undefined && (
                  <div>
                    <div style={{ fontSize: 12, opacity: 0.7 }}>
                      ST Direction
                    </div>
                    <div>
                      {summary.supertrend_direction === 1
                        ? "UP"
                        : summary.supertrend_direction === -1
                        ? "DOWN"
                        : "-"}
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {regime && (
            <div
              style={{
                background: "#111827",
                padding: "16px",
                borderRadius: 12,
                marginBottom: 24,
                boxShadow: "0 10px 30px rgba(0,0,0,0.5)",
              }}
            >
              <h2 style={{ fontSize: "18px", marginBottom: 8 }}>
                Market regime (latest)
              </h2>

              <div style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>Regime</div>
                  <div style={{ fontWeight: 600 }}>{regime.regime ?? "-"}</div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>Vol</div>
                  <div>{regime.vol_regime ?? "-"}</div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>Trend dir</div>
                  <div>
                    {regime.trend_dir === 1
                      ? "UP"
                      : regime.trend_dir === -1
                      ? "DOWN"
                      : "-"}
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>
                    Trend strength %
                  </div>
                  <div>
                    {typeof regime.trend_strength_pct === "number"
                      ? regime.trend_strength_pct.toFixed(3)
                      : "-"}
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>ATR %</div>
                  <div>
                    {typeof regime.atr_pct === "number"
                      ? regime.atr_pct.toFixed(3)
                      : "-"}
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>Shock z</div>
                  <div>
                    {typeof regime.shock_z === "number"
                      ? regime.shock_z.toFixed(2)
                      : "-"}
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 12, opacity: 0.7 }}>TS</div>
                  <div>{formatDate(regime.ts)}</div>
                </div>
              </div>
            </div>
          )}

          <div
            style={{
              background: "#020617",
              padding: "16px",
              borderRadius: 12,
              boxShadow: "0 10px 30px rgba(0,0,0,0.7)",
              marginBottom: 24,
            }}
          >
            <h2 style={{ fontSize: "18px", marginBottom: 12 }}>
              Watchdog events (latest) — total {watchdogTotal}
            </h2>

            {watchdog.length === 0 ? (
              <div style={{ opacity: 0.7 }}>No events.</div>
            ) : (
              <div style={{ overflowX: "auto" }}>
                <table
                  style={{
                    width: "100%",
                    borderCollapse: "collapse",
                    fontSize: 13,
                  }}
                >
                  <thead>
                    <tr
                      style={{
                        textAlign: "left",
                        borderBottom: "1px solid #1f2937",
                      }}
                    >
                      <th style={{ padding: "6px 8px" }}>Time</th>
                      <th style={{ padding: "6px 8px" }}>Severity</th>
                      <th style={{ padding: "6px 8px" }}>Event</th>
                      <th style={{ padding: "6px 8px" }}>Details</th>
                    </tr>
                  </thead>
                  <tbody>
                    {watchdog.map((e) => (
                      <tr
                        key={e.id}
                        style={{ borderBottom: "1px solid #111827" }}
                      >
                        <td style={{ padding: "6px 8px" }}>
                          {formatDate(e.created_at)}
                        </td>
                        <td style={{ padding: "6px 8px" }}>{e.severity}</td>
                        <td style={{ padding: "6px 8px" }}>{e.event}</td>
                        <td style={{ padding: "6px 8px" }}>
                          {e.details ? JSON.stringify(e.details) : "-"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          <div
            style={{
              background: "#020617",
              padding: "16px",
              borderRadius: 12,
              boxShadow: "0 10px 30px rgba(0,0,0,0.7)",
            }}
          >
            <h2 style={{ fontSize: "18px", marginBottom: 12 }}>
              Simulated orders (page {currentOrdersPage} /{" "}
              {ordersTotalPages})
            </h2>

            {ordersTotal === 0 ? (
              <div style={{ opacity: 0.7 }}>No simulated orders yet.</div>
            ) : (
              <>
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    marginBottom: 8,
                    fontSize: 13,
                  }}
                >
                  <div>Total orders: {ordersTotal}</div>
                  <div
                    style={{
                      display: "flex",
                      gap: 8,
                      alignItems: "center",
                    }}
                  >
                    <button
                      onClick={handleOrdersPrev}
                      disabled={currentOrdersPage <= 1}
                      style={{
                        padding: "4px 8px",
                        borderRadius: 6,
                        border: "1px solid #1f2937",
                        background:
                          currentOrdersPage <= 1 ? "#111827" : "#1f2937",
                        color: "#e5e7eb",
                        cursor:
                          currentOrdersPage <= 1
                            ? "default"
                            : "pointer",
                      }}
                    >
                      ‹ Prev
                    </button>
                    <span>
                      Page {currentOrdersPage} / {ordersTotalPages}
                    </span>
                    <button
                      onClick={handleOrdersNext}
                      disabled={currentOrdersPage >= ordersTotalPages}
                      style={{
                        padding: "4px 8px",
                        borderRadius: 6,
                        border: "1px solid #1f2937",
                        background:
                          currentOrdersPage >= ordersTotalPages
                            ? "#111827"
                            : "#1f2937",
                        color: "#e5e7eb",
                        cursor:
                          currentOrdersPage >= ordersTotalPages
                            ? "default"
                            : "pointer",
                      }}
                    >
                      Next ›
                    </button>
                  </div>
                </div>

                <div style={{ overflowX: "auto" }}>
                  <table
                    style={{
                      width: "100%",
                      borderCollapse: "collapse",
                      fontSize: 14,
                    }}
                  >
                    <thead>
                      <tr
                        style={{
                          textAlign: "left",
                          borderBottom: "1px solid #1f2937",
                        }}
                      >
                        <th style={{ padding: "6px 8px" }}>#</th>
                        <th style={{ padding: "6px 8px" }}>Time</th>
                        <th style={{ padding: "6px 8px" }}>Side</th>
                        <th style={{ padding: "6px 8px" }}>Price</th>
                        <th style={{ padding: "6px 8px" }}>Qty BTC</th>
                        <th style={{ padding: "6px 8px" }}>RSI</th>
                        <th style={{ padding: "6px 8px" }}>EMA21</th>
                        <th style={{ padding: "6px 8px" }}>Reason</th>
                      </tr>
                    </thead>
                    <tbody>
                      {ordersPage.map((o, idx) => (
                        <tr
                          key={o.id}
                          style={{
                            borderBottom: "1px solid #111827",
                          }}
                        >
                          <td style={{ padding: "6px 8px" }}>
                            {ordersRowStartIndex + idx + 1}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {formatDate(o.created_at)}
                          </td>
                          <td
                            style={{
                              padding: "6px 8px",
                              color:
                                o.side === "BUY" ? "#22c55e" : "#ef4444",
                              fontWeight: 600,
                            }}
                          >
                            {o.side}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {o.price.toFixed(2)}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {o.quantity_btc.toFixed(8)}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {o.rsi_14 !== null
                              ? o.rsi_14.toFixed(2)
                              : "-"}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {o.ema_21 !== null
                              ? o.ema_21.toFixed(2)
                              : "-"}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {o.reason ?? "-"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>

          <AiAnalyzer
            symbol={symbol}
            strategy={strategy}
            metrics={currentMetrics}
            losingTrades={roundtrips}
          />

          <div
            style={{
              background: "#020617",
              padding: "16px",
              borderRadius: 12,
              boxShadow: "0 10px 30px rgba(0,0,0,0.7)",
              marginTop: 24,
            }}
          >
            <h2 style={{ fontSize: "18px", marginBottom: 12 }}>
              Trades analysis (losing roundtrips)
            </h2>

            {losingTotal === 0 ? (
              <div style={{ opacity: 0.7 }}>
                No losing closed trades yet 🎉
              </div>
            ) : (
              <>
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    marginBottom: 8,
                    fontSize: 13,
                  }}
                >
                  <div>Total losing trades: {losingTotal}</div>
                  <div
                    style={{
                      display: "flex",
                      gap: 8,
                      alignItems: "center",
                    }}
                  >
                    <button
                      onClick={handlePrevPage}
                      disabled={currentPage <= 1}
                      style={{
                        padding: "4px 8px",
                        borderRadius: 6,
                        border: "1px solid #1f2937",
                        background:
                          currentPage <= 1 ? "#111827" : "#1f2937",
                        color: "#e5e7eb",
                        cursor:
                          currentPage <= 1 ? "default" : "pointer",
                      }}
                    >
                      ‹ Prev
                    </button>
                    <span>
                      Page {currentPage} / {totalLosingPages}
                    </span>
                    <button
                      onClick={handleNextPage}
                      disabled={currentPage >= totalLosingPages}
                      style={{
                        padding: "4px 8px",
                        borderRadius: 6,
                        border: "1px solid #1f2937",
                        background:
                          currentPage >= totalLosingPages
                            ? "#111827"
                            : "#1f2937",
                        color: "#e5e7eb",
                        cursor:
                          currentPage >= totalLosingPages
                            ? "default"
                            : "pointer",
                      }}
                    >
                      Next ›
                    </button>
                  </div>
                </div>

                <div style={{ overflowX: "auto" }}>
                  <table
                    style={{
                      width: "100%",
                      borderCollapse: "collapse",
                      fontSize: 14,
                    }}
                  >
                    <thead>
                      <tr
                        style={{
                          textAlign: "left",
                          borderBottom: "1px solid #1f2937",
                        }}
                      >
                        <th style={{ padding: "6px 8px" }}>#</th>
                        <th style={{ padding: "6px 8px" }}>Dir</th>
                        <th style={{ padding: "6px 8px" }}>
                          Entry time
                        </th>
                        <th style={{ padding: "6px 8px" }}>
                          Exit time
                        </th>
                        <th style={{ padding: "6px 8px" }}>
                          Entry price
                        </th>
                        <th style={{ padding: "6px 8px" }}>
                          Exit price
                        </th>
                        <th style={{ padding: "6px 8px" }}>
                          Qty BTC
                        </th>
                        <th style={{ padding: "6px 8px" }}>
                          PnL (USDT)
                        </th>
                        <th style={{ padding: "6px 8px" }}>
                          RSI in
                        </th>
                        <th style={{ padding: "6px 8px" }}>
                          EMA21 in
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {roundtrips.map((rt, idx) => (
                        <tr
                          key={rt.id}
                          style={{
                            borderBottom: "1px solid #111827",
                          }}
                        >
                          <td style={{ padding: "6px 8px" }}>
                            {pageStartIndex + idx + 1}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {rt.direction}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {formatDate(rt.entry_time)}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {formatDate(rt.exit_time)}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {rt.entry_price.toFixed(2)}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {rt.exit_price.toFixed(2)}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {rt.qty_btc.toFixed(8)}
                          </td>
                          <td
                            style={{
                              padding: "6px 8px",
                              color: "#ef4444",
                              fontWeight: 600,
                            }}
                          >
                            {rt.pnl_usdt.toFixed(4)}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {rt.entry_rsi_14 !== null
                              ? rt.entry_rsi_14.toFixed(2)
                              : "-"}
                          </td>
                          <td style={{ padding: "6px 8px" }}>
                            {rt.entry_ema_21 !== null
                              ? rt.entry_ema_21.toFixed(2)
                              : "-"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        </>
      )}
    </div>
  );
}

export default App;