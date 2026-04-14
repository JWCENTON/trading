import type { UiOpenPosition } from '../../api';

interface OpenPositionsTableProps {
  items: UiOpenPosition[];
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

function fmtAge(seconds: number | null | undefined) {
  if (seconds == null || Number.isNaN(seconds)) return '—';
  const total = Math.max(0, Math.floor(seconds));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

export function OpenPositionsTable({ items }: OpenPositionsTableProps) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Open positions</h2>
        <span className="panel-meta">{items.length} items</span>
      </div>
      <div className="table-wrap">
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
            {items.length === 0 ? (
              <tr>
                <td colSpan={12} className="empty-state">No open positions.</td>
              </tr>
            ) : items.map((item) => {
              const pnlClass = (item.unrealized_pnl_usdc ?? 0) > 0 ? 'positive' : (item.unrealized_pnl_usdc ?? 0) < 0 ? 'negative' : 'neutral';
              return (
                <tr key={item.id}>
                  <td>{item.symbol}</td>
                  <td>{item.interval}</td>
                  <td>{item.strategy}</td>
                  <td>{item.side}</td>
                  <td>{fmtDate(item.entry_time)}</td>
                  <td>{fmtAge(item.age_seconds)}</td>
                  <td>{fmt(item.entry_price)}</td>
                  <td>{fmt(item.current_price)}</td>
                  <td>{fmt(item.qty, 6)}</td>
                  <td>{fmt(item.market_value)}</td>
                  <td className={pnlClass}>{fmt(item.unrealized_pnl_usdc)}</td>
                  <td className={pnlClass}>{fmt(item.unrealized_pnl_pct, 2)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
