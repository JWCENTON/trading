import type { UiRecentClosedPosition } from '../../api';

interface RecentClosedTableProps {
  items: UiRecentClosedPosition[];
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
  return value.toFixed(2);
}

export function RecentClosedTable({ items }: RecentClosedTableProps) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Last closed positions</h2>
        <span className="panel-meta">{items.length} items</span>
      </div>

      {items.length === 0 ? (
        <div className="empty-state">No closed positions yet.</div>
      ) : (
        <>
          <div className="mobile-cards">
            {items.map((item) => {
              const tone = item.win_loss === 'WIN' ? 'positive' : item.win_loss === 'LOSS' ? 'negative' : 'neutral';
              return (
                <article key={item.id} className="mobile-card">
                  <div className="mobile-card-header">
                    <div>
                      <h3>{item.symbol}</h3>
                      <p>{item.interval} • {item.strategy} • {item.side}</p>
                    </div>
                    <span className={`pill ${tone}`}>{item.win_loss}</span>
                  </div>
                  <div className="mobile-kv-grid">
                    <div><span>Exit</span><strong>{formatDateTime(item.exit_time)}</strong></div>
                    <div><span>PnL</span><strong className={tone}>{formatNumber(item.pnl_usdc)}</strong></div>
                    <div><span>PnL %</span><strong className={tone}>{formatPercent(item.pnl_pct)}%</strong></div>
                    <div><span>Reason</span><strong>{item.exit_reason || '—'}</strong></div>
                    <div><span>Entry</span><strong>{formatNumber(item.entry_price)}</strong></div>
                    <div><span>Exit price</span><strong>{formatNumber(item.exit_price)}</strong></div>
                  </div>
                </article>
              );
            })}
          </div>

          <div className="table-wrap desktop-table">
            <table>
              <thead>
                <tr>
                  <th>Exit time</th>
                  <th>Symbol</th>
                  <th>Interval</th>
                  <th>Strategy</th>
                  <th>Side</th>
                  <th>Entry</th>
                  <th>Exit</th>
                  <th>Qty</th>
                  <th>PnL</th>
                  <th>PnL %</th>
                  <th>Exit reason</th>
                  <th>Result</th>
                </tr>
              </thead>
              <tbody>
                {items.map((item) => {
                  const tone = item.win_loss === 'WIN' ? 'positive' : item.win_loss === 'LOSS' ? 'negative' : 'neutral';
                  return (
                    <tr key={item.id}>
                      <td>{formatDateTime(item.exit_time)}</td>
                      <td>{item.symbol}</td>
                      <td>{item.interval}</td>
                      <td>{item.strategy}</td>
                      <td>{item.side}</td>
                      <td>{formatNumber(item.entry_price)}</td>
                      <td>{formatNumber(item.exit_price)}</td>
                      <td>{formatNumber(item.qty, 6)}</td>
                      <td className={tone}>{formatNumber(item.pnl_usdc)}</td>
                      <td className={tone}>{formatPercent(item.pnl_pct)}%</td>
                      <td>{item.exit_reason || '—'}</td>
                      <td><span className={`pill ${tone}`}>{item.win_loss}</span></td>
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
