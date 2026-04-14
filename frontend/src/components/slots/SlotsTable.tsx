import type { UiSlotRow } from '../../api';

interface SlotsTableProps {
  items: UiSlotRow[];
}

function fmtDate(value: string | null | undefined) {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function pillClass(value: boolean) {
  return value ? 'positive' : 'negative';
}

export function SlotsTable({ items }: SlotsTableProps) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Slots</h2>
        <span className="panel-meta">{items.length} items</span>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Interval</th>
              <th>Strategy</th>
              <th>Enabled</th>
              <th>Live orders</th>
              <th>Regime</th>
              <th>Regime mode</th>
              <th>Open position</th>
              <th>Heartbeat</th>
              <th>Last event</th>
              <th>Reason</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>
            {items.length === 0 ? (
              <tr>
                <td colSpan={12} className="empty-state">No slots.</td>
              </tr>
            ) : items.map((item) => (
              <tr key={`${item.symbol}:${item.interval}:${item.strategy}`}>
                <td>{item.symbol}</td>
                <td>{item.interval}</td>
                <td>{item.strategy}</td>
                <td><span className={`pill ${pillClass(item.enabled)}`}>{item.enabled ? 'ON' : 'OFF'}</span></td>
                <td><span className={`pill ${pillClass(item.live_orders_enabled)}`}>{item.live_orders_enabled ? 'ON' : 'OFF'}</span></td>
                <td><span className={`pill ${pillClass(item.regime_enabled)}`}>{item.regime_enabled ? 'ON' : 'OFF'}</span></td>
                <td>{item.regime_mode ?? '—'}</td>
                <td>{item.open_position.exists ? `${item.open_position.side ?? 'OPEN'} #${item.open_position.id ?? '—'}` : '—'}</td>
                <td className={item.heartbeat.stale ? 'negative' : 'neutral'}>{fmtDate(item.heartbeat.last_seen)}</td>
                <td>{item.last_event.event_type ?? '—'}{item.last_event.decision ? ` / ${item.last_event.decision}` : ''}</td>
                <td>{item.reason ?? item.last_event.reason ?? '—'}</td>
                <td>{fmtDate(item.updated_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
