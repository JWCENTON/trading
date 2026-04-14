import type { UiHealthResponse } from '../../api';

interface HealthPanelProps {
  health: UiHealthResponse | null;
}

function fmtDate(value: string | null | undefined) {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

export function HealthPanel({ health }: HealthPanelProps) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h2>System health</h2>
        <span className="panel-meta">Truth-only</span>
      </div>
      {!health ? (
        <div className="empty-state">No health snapshot loaded yet.</div>
      ) : (
        <div className="status-grid">
          <div className={`status-card ${health.api.ok ? 'positive' : 'negative'}`}>
            <span className="status-label">API</span>
            <span className="status-value">{health.api.ok ? 'OK' : 'FAIL'}</span>
          </div>
          <div className={`status-card ${health.db.ok ? 'positive' : 'negative'}`}>
            <span className="status-label">DB</span>
            <span className="status-value">{health.db.ok ? 'OK' : 'FAIL'}</span>
          </div>
          <div className="status-card neutral">
            <span className="status-label">Heartbeats</span>
            <span className="status-value">{health.bot_heartbeats.fresh} fresh / {health.bot_heartbeats.stale} stale</span>
          </div>
          <div className="status-card neutral">
            <span className="status-label">Market data</span>
            <span className="status-value">{health.market_data.tracked_pairs} pairs</span>
          </div>
          <div className="status-card neutral">
            <span className="status-label">Orchestrator</span>
            <span className="status-value">{health.orchestrator.events_last_15m} events / 15m</span>
          </div>
          <div className={`status-card ${health.panic_state.enabled ? 'negative' : 'positive'}`}>
            <span className="status-label">Panic state</span>
            <span className="status-value">{health.panic_state.enabled ? 'ON' : 'OFF'}</span>
          </div>
          <div className="status-card neutral">
            <span className="status-label">Latest heartbeat</span>
            <span className="status-value">{fmtDate(health.bot_heartbeats.latest_at)}</span>
          </div>
          <div className="status-card neutral">
            <span className="status-label">Latest candle</span>
            <span className="status-value">{fmtDate(health.market_data.latest_candle_close_at)}</span>
          </div>
        </div>
      )}
    </section>
  );
}
