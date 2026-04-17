import type { UiLiveSummary } from '../../api';

interface TopStatusBarProps {
  summary: UiLiveSummary | null;
  onRefresh: () => Promise<void> | void;
  refreshBusy: boolean;
}

function formatNumber(value: number | null | undefined, digits = 4) {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return value.toFixed(digits);
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return '—';
  return new Date(value).toLocaleString();
}

export function TopStatusBar({ summary, onRefresh, refreshBusy }: TopStatusBarProps) {
  const statusCards = [
    {
      label: 'Panic',
      value: summary?.panic.enabled ? 'ON' : 'OFF',
      tone: summary?.panic.enabled ? 'negative' : 'positive',
      meta: summary?.panic.reason || '—',
    },
    {
      label: 'Environment',
      value: summary ? `${summary.environment} / ${summary.trading_mode}` : '—',
      tone: 'neutral',
      meta: 'Runtime target',
    },
    {
      label: 'Open Unrealized PnL',
      value: summary ? `${formatNumber(summary.open_positions.unrealized_pnl_usdc)} USDC` : '—',
      tone:
        (summary?.open_positions.unrealized_pnl_usdc ?? 0) > 0
          ? 'positive'
          : (summary?.open_positions.unrealized_pnl_usdc ?? 0) < 0
            ? 'negative'
            : 'neutral',
      meta: 'Truth from positions + candles',
    },
    {
      label: 'Open Positions',
      value: summary ? String(summary.open_positions.count) : '—',
      tone: 'neutral',
      meta: 'Currently open',
    },
    {
      label: 'Effective Live',
      value: summary ? `${summary.slot_counts.effective_live} / ${summary.slot_counts.total}` : '—',
      tone: summary && summary.slot_counts.effective_live > 0 ? 'positive' : 'neutral',
      meta: 'enabled + live_orders + regime ENFORCE',
    },
    {
      label: 'Open Market Value',
      value: summary ? `${formatNumber(summary.open_positions.market_value_usdc)} USDC` : '—',
      tone: 'neutral',
      meta: 'Mark value',
    },
    {
      label: 'Heartbeats',
      value: summary ? `${summary.heartbeats.fresh} fresh / ${summary.heartbeats.stale} stale` : '—',
      tone: summary && summary.heartbeats.stale > 0 ? 'negative' : 'positive',
      meta: `Latest: ${formatDateTime(summary?.heartbeats.latest_at)}`,
    },
    {
      label: 'Last Refresh',
      value: formatDateTime(summary?.snapshot_at),
      tone: 'neutral',
      meta: summary?.market_data.latest_mark_price_at
        ? `Market data: ${formatDateTime(summary.market_data.latest_mark_price_at)}`
        : 'Market data: —',
      action: true,
    },
  ];

  return (
    <section className="status-grid">
      {statusCards.map((card) => (
        <article key={card.label} className={`status-card ${card.tone}`}>
          <span className="status-label">{card.label}</span>
          <strong className="status-value">{card.value}</strong>
          <span className="status-meta">{card.meta}</span>
          {card.action ? (
            <div className="status-card-actions">
              <button
                type="button"
                className="action-button action-button--small"
                onClick={() => void onRefresh()}
                disabled={refreshBusy}
              >
                {refreshBusy ? 'Refreshing...' : 'Refresh'}
              </button>
            </div>
          ) : null}
        </article>
      ))}
    </section>
  );
}
