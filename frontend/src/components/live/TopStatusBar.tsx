import type { UiLiveSummary } from '../../api';

interface TopStatusBarProps {
  summary: UiLiveSummary | null;
}

function fmtNumber(value: number | null | undefined, digits = 2): string {
  if (value == null || Number.isNaN(value)) return '—';
  return Number(value).toFixed(digits);
}

function fmtDate(value: string | null | undefined): string {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

export function TopStatusBar({ summary }: TopStatusBarProps) {
  const panicClass = summary?.panic.enabled ? 'negative' : 'positive';
  const pnl = summary?.open_positions.unrealized_pnl_usdc ?? null;
  const pnlClass = pnl == null ? 'neutral' : pnl > 0 ? 'positive' : pnl < 0 ? 'negative' : 'neutral';

  return (
    <section className="status-grid">
      <div className={`status-card ${panicClass}`}>
        <span className="status-label">Panic</span>
        <span className="status-value">{summary ? (summary.panic.enabled ? 'ON' : 'OFF') : '—'}</span>
      </div>
      <div className="status-card neutral">
        <span className="status-label">Environment</span>
        <span className="status-value">{summary?.environment ?? '—'} / {summary?.trading_mode ?? '—'}</span>
      </div>
      <div className={`status-card ${pnlClass}`}>
        <span className="status-label">Open Unrealized PnL</span>
        <span className="status-value">{fmtNumber(pnl, 4)} USDC</span>
      </div>
      <div className="status-card neutral">
        <span className="status-label">Open Positions</span>
        <span className="status-value">{summary?.open_positions.count ?? '—'}</span>
      </div>
      <div className="status-card neutral">
        <span className="status-label">Effective Live</span>
        <span className="status-value">{summary?.slot_counts.effective_live ?? '—'} / {summary?.slot_counts.total ?? '—'}</span>
      </div>
      <div className="status-card neutral">
        <span className="status-label">Open Market Value</span>
        <span className="status-value">{fmtNumber(summary?.open_positions.market_value_usdc, 4)} USDC</span>
      </div>
      <div className="status-card neutral">
        <span className="status-label">Heartbeats</span>
        <span className="status-value">{summary?.heartbeats.fresh ?? '—'} fresh / {summary?.heartbeats.stale ?? '—'} stale</span>
      </div>
      <div className="status-card neutral">
        <span className="status-label">Last Refresh</span>
        <span className="status-value">{fmtDate(summary?.snapshot_at)}</span>
      </div>
    </section>
  );
}
