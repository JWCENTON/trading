import type { UiSlotRow } from '../../api';

interface SlotsTableProps {
  items: UiSlotRow[];
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return '—';
  return new Date(value).toLocaleString();
}

function boolTone(value: boolean) {
  return value ? 'positive' : 'neutral';
}

export function SlotsTable({ items }: SlotsTableProps) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Slots</h2>
        <span className="panel-meta">{items.length} rows</span>
      </div>

      <div className="mobile-cards">
        {items.map((item) => (
          <article key={`${item.symbol}-${item.interval}-${item.strategy}`} className="mobile-card">
            <div className="mobile-card-header">
              <div>
                <h3>{item.symbol}</h3>
                <p>{item.interval} • {item.strategy}</p>
              </div>
              <span className={`pill ${item.live_orders_enabled ? 'positive' : 'neutral'}`}>
                {item.live_orders_enabled ? 'LIVE' : 'OFF'}
              </span>
            </div>
            <div className="mobile-kv-grid">
              <div><span>Enabled</span><strong className={boolTone(item.enabled)}>{String(item.enabled)}</strong></div>
              <div><span>Regime</span><strong>{item.regime_mode || '—'}</strong></div>
              <div><span>Regime on</span><strong className={boolTone(item.regime_enabled)}>{String(item.regime_enabled)}</strong></div>
              <div><span>Open pos</span><strong>{item.open_position.exists ? item.open_position.side || 'YES' : 'No'}</strong></div>
              <div><span>Heartbeat</span><strong className={item.heartbeat.stale ? 'negative' : 'positive'}>{item.heartbeat.stale ? 'STALE' : 'FRESH'}</strong></div>
              <div><span>Last event</span><strong>{item.last_event.event_type || '—'}</strong></div>
            </div>
            <div className="mobile-card-footer">{item.reason || 'No reason'} • {formatDateTime(item.last_event.at)}</div>
          </article>
        ))}
      </div>

      <div className="table-wrap desktop-table">
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Interval</th>
              <th>Strategy</th>
              <th>Enabled</th>
              <th>Live orders</th>
              <th>Regime enabled</th>
              <th>Regime mode</th>
              <th>Open position</th>
              <th>Heartbeat</th>
              <th>Last event</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={`${item.symbol}-${item.interval}-${item.strategy}`}>
                <td>{item.symbol}</td>
                <td>{item.interval}</td>
                <td>{item.strategy}</td>
                <td><span className={`pill ${boolTone(item.enabled)}`}>{item.enabled ? 'ON' : 'OFF'}</span></td>
                <td><span className={`pill ${boolTone(item.live_orders_enabled)}`}>{item.live_orders_enabled ? 'ON' : 'OFF'}</span></td>
                <td><span className={`pill ${boolTone(item.regime_enabled)}`}>{item.regime_enabled ? 'ON' : 'OFF'}</span></td>
                <td>{item.regime_mode || '—'}</td>
                <td>{item.open_position.exists ? `${item.open_position.side || 'OPEN'} • ${formatDateTime(item.open_position.entry_time)}` : 'No'}</td>
                <td className={item.heartbeat.stale ? 'negative' : 'positive'}>{item.heartbeat.stale ? 'STALE' : 'FRESH'}</td>
                <td>
                  <div>{item.last_event.event_type || '—'}</div>
                  <div className="cell-subtext">{item.last_event.reason || item.reason || '—'}</div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
