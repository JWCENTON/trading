import type { UiTrading24hSummary } from "../../api";

interface Trading24hPanelProps {
  trading24h: UiTrading24hSummary | null;
}

function formatNumber(value: number | null | undefined, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toFixed(digits);
}

export function Trading24hPanel({ trading24h }: Trading24hPanelProps) {
  const pnl = trading24h?.closed_pnl_24h ?? 0;
  const pnlTone = pnl > 0 ? "positive" : pnl < 0 ? "negative" : "neutral";

  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Trading 24h</h2>
        <span className="panel-meta">Closed trades only</span>
      </div>

      <div className="stats-grid">
        <article className="info-tile">
          <span className="status-label">Closed PnL 24h</span>
          <strong className={`status-value ${pnlTone}`}>{formatNumber(trading24h?.closed_pnl_24h)} USDC</strong>
          <span className="status-meta">Czy system zarabia dzisiaj</span>
        </article>

        <article className="info-tile">
          <span className="status-label">Trades 24h</span>
          <strong className="status-value">{trading24h?.trades_24h ?? "—"}</strong>
          <span className="status-meta">Closed positions in last 24h</span>
        </article>

        <article className="info-tile">
          <span className="status-label">Wins / Losses</span>
          <strong className="status-value">
            {trading24h ? `${trading24h.wins_24h} / ${trading24h.losses_24h}` : "—"}
          </strong>
          <span className="status-meta">Win / loss split</span>
        </article>

        <article className="info-tile">
          <span className="status-label">Win rate</span>
          <strong className="status-value">{formatNumber(trading24h?.win_rate_24h)}%</strong>
          <span className="status-meta">24h closed-trade hit rate</span>
        </article>
      </div>
    </section>
  );
}
