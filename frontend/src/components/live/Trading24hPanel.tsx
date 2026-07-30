import type { UiTrading24hSummary } from "../../api";

interface Trading24hPanelProps {
  trading24h: UiTrading24hSummary | null;
}

function fmt(value: number | null | undefined, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toFixed(digits);
}

function clampPct(value: number) {
  return Math.max(4, Math.min(100, value));
}

export function Trading24hPanel({ trading24h }: Trading24hPanelProps) {
  const pnl = trading24h?.closed_pnl_24h ?? 0;
  const trades = trading24h?.trades_24h ?? 0;
  const wins = trading24h?.wins_24h ?? 0;
  const losses = trading24h?.losses_24h ?? 0;
  const winRate = trading24h?.win_rate_24h ?? 0;
  const flats = trading24h?.flats ?? 0;
  const unresolved = trading24h?.unresolved_trades ?? 0;
  const coverage = (trading24h?.coverage_ratio ?? 0) * 100;
  const pnlTone = pnl > 0 ? "positive" : pnl < 0 ? "negative" : "neutral";

  const maxCount = Math.max(wins, losses, trades, 1);

  return (
    <section className="panel dashboard-performance-panel">
      <div className="panel-header">
        <div>
          <h2>Performance 24h</h2>
          <span className="panel-meta">Closed trades only · net after fees</span>
        </div>
        <strong className={`dashboard-pnl ${pnlTone}`}>{fmt(pnl)} USDC</strong>
      </div>

      <div className="dashboard-chart-card">
        <div className="dashboard-chart-header">
          <span>Trading result</span>
          <strong className={pnlTone}>{pnl >= 0 ? "+" : ""}{fmt(pnl)} USDC</strong>
        </div>

        <div className="dashboard-bars" aria-label="24h trading chart">
          <div className="dashboard-bar-row">
            <span>Trades</span>
            <div><i style={{ width: `${clampPct((trades / maxCount) * 100)}%` }} /></div>
            <strong>{trades}</strong>
          </div>
          <div className="dashboard-bar-row">
            <span>Wins</span>
            <div><i className="bar-positive" style={{ width: `${clampPct((wins / maxCount) * 100)}%` }} /></div>
            <strong>{wins}</strong>
          </div>
          <div className="dashboard-bar-row">
            <span>Losses</span>
            <div><i className="bar-negative" style={{ width: `${clampPct((losses / maxCount) * 100)}%` }} /></div>
            <strong>{losses}</strong>
          </div>
          <div className="dashboard-bar-row">
            <span>Flats</span>
            <div><i className="bar-accent" style={{ width: `${clampPct((flats / maxCount) * 100)}%` }} /></div>
            <strong>{flats}</strong>
          </div>
          <div className="dashboard-bar-row">
            <span>Unresolved</span>
            <div><i style={{ width: `${clampPct((unresolved / maxCount) * 100)}%` }} /></div>
            <strong>{unresolved}</strong>
          </div>
          <div className="dashboard-bar-row">
            <span>Win rate</span>
            <div><i className="bar-accent" style={{ width: `${clampPct(winRate)}%` }} /></div>
            <strong>{fmt(winRate)}%</strong>
          </div>
        </div>
      </div>

      <div className="stats-grid dashboard-kpi-grid">
        <article className="info-tile">
          <span className="status-label">Net PnL</span>
          <strong className={`status-value ${pnlTone}`}>{fmt(pnl)} USDC</strong>
          <span className="status-meta">Realized after fees</span>
        </article>

        <article className="info-tile">
          <span className="status-label">Trades</span>
          <strong className="status-value">{trades}</strong>
          <span className="status-meta">Closed positions</span>
        </article>

        <article className="info-tile">
          <span className="status-label">W / L / Flat / Unresolved</span>
          <strong className="status-value">{wins} / {losses} / {flats} / {unresolved}</strong>
          <span className="status-meta">Coverage {fmt(coverage)}%</span>
        </article>

        <article className="info-tile">
          <span className="status-label">Win rate</span>
          <strong className="status-value">{fmt(winRate)}%</strong>
          <span className="status-meta">Closed-trade hit rate</span>
        </article>
      </div>
    </section>
  );
}
