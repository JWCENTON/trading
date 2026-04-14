import type { UiRecentClosedPosition } from '../../api';

interface RecentClosedTableProps {
  items: UiRecentClosedPosition[];
}

function fmt(value: number | null | undefined, digits = 4) {
  if (value == null || Number.isNaN(value)) return '—';
  return Number(value).toFixed(digits);
}

function fmtDate(value: string | null | undefined) {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

export function RecentClosedTable({ items }: RecentClosedTableProps) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Last closed positions</h2>
        <span className="panel-meta">{items.length} items</span>
      </div>
      <div className="table-wrap">
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
            {items.length === 0 ? (
              <tr>
                <td colSpan={12} className="empty-state">No closed positions.</td>
              </tr>
            ) : items.map((item) => {
              const cls = item.win_loss === 'WIN' ? 'positive' : item.win_loss === 'LOSS' ? 'negative' : 'neutral';
              return (
                <tr key={item.id}>
                  <td>{fmtDate(item.exit_time)}</td>
                  <td>{item.symbol}</td>
                  <td>{item.interval}</td>
                  <td>{item.strategy}</td>
                  <td>{item.side}</td>
                  <td>{fmt(item.entry_price)}</td>
                  <td>{fmt(item.exit_price)}</td>
                  <td>{fmt(item.qty, 6)}</td>
                  <td className={cls}>{fmt(item.pnl_usdc)}</td>
                  <td className={cls}>{fmt(item.pnl_pct, 2)}</td>
                  <td>{item.exit_reason ?? '—'}</td>
                  <td><span className={`pill ${cls}`}>{item.win_loss}</span></td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
