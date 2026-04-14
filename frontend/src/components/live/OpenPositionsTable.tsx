import type { UiOpenPosition } from '../../api';

interface OpenPositionsTableProps {
  items: UiOpenPosition[];
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return '—';
  return new Date(value).toLocaleString();
}

function formatNumber(value: number | null | undefined, digits = 4) {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return value.toFixed(digits);
}

function formatPercent(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return `${value.toFixed(2)}%`;
}

function formatAge(seconds: number | null | undefined) {
  if (!seconds || seconds < 0) return '0m';
  const totalMinutes = Math.floor(seconds / 60);
  const days = Math.floor(totalMinutes / 1440);
  const hours = Math.floor((totalMinutes % 1440) / 60);
  const minutes = totalMinutes % 60;
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

export function OpenPositionsTable({ items }: OpenPositionsTableProps) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Open positions</h2>
        <span className="panel-meta">{items.length} items</span>
      </div>

      {items.length === 0 ? (
        <div className="empty-state">No open positions.</div>
      ) : (
        <>
          <div className="mobile-cards">
            {items.map((item) => {
              const pnlTone = (item.unrealized_pnl_usdc ?? 0) > 0 ? 'positive' : (item.unrealized_pnl_usdc ?? 0) < 0 ? 'negative' : 'neutral';
              return (
                <article key={item.id} className="mobile-card">
                  <div className="mobile-card-header">
                    <div>
                      <h3>{item.symbol}</h3>
                      <p>{item.interval} • {item.strategy} • {item.side}</p>
                    </div>
                    <span className={`pill ${pnlTone}`}>{formatNumber(item.unrealized_pnl_usdc)} USDC</span>
                  </div>
                  <div className="mobile-kv-grid">
                    <div><span>Entry</span><strong>{formatNumber(item.entry_price)}</strong></div>
                    <div><span>Current</span><strong>{formatNumber(item.current_price)}</strong></div>
                    <div><span>Qty</span><strong>{formatNumber(item.qty, 6)}</strong></div>
                    <div><span>Age</span><strong>{formatAge(item.age_seconds)}</strong></div>
                    <div><span>Market value</span><strong>{formatNumber(item.market_value)}</strong></div>
                    <div><span>UPnL %</span><strong className={pnlTone}>{formatPercent(item.unrealized_pnl_pct)}</strong></div>
                  </div>
                  <div className="mobile-card-footer">Entry time: {formatDateTime(item.entry_time)}</div>
                </article>
              );
            })}
          </div>

          <div className="table-wrap desktop-table">
            <table>
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>Interval</th>
                  <th>Strategy</th>
                  <th>Side</th>
                  <th>Entry time</th>
                  <th>Age</th>
                  <th>Entry</th>
                  <th>Current</th>
                  <th>Qty</th>
                  <th>Market value</th>
                  <th>UPnL</th>
                  <th>UPnL %</th>
                </tr>
              </thead>
              <tbody>
                {items.map((item) => {
                  const pnlTone = (item.unrealized_pnl_usdc ?? 0) > 0 ? 'positive' : (item.unrealized_pnl_usdc ?? 0) < 0 ? 'negative' : 'neutral';
                  return (
                    <tr key={item.id}>
                      <td>{item.symbol}</td>
                      <td>{item.interval}</td>
                      <td>{item.strategy}</td>
                      <td>{item.side}</td>
                      <td>{formatDateTime(item.entry_time)}</td>
                      <td>{formatAge(item.age_seconds)}</td>
                      <td>{formatNumber(item.entry_price)}</td>
                      <td>{formatNumber(item.current_price)}</td>
                      <td>{formatNumber(item.qty, 6)}</td>
                      <td>{formatNumber(item.market_value)}</td>
                      <td className={pnlTone}>{formatNumber(item.unrealized_pnl_usdc)}</td>
                      <td className={pnlTone}>{formatPercent(item.unrealized_pnl_pct)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  );
}
