import type { UiHealthResponse } from '../../api';

interface HealthPanelProps {
  health: UiHealthResponse | null;
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return '—';
  return new Date(value).toLocaleString();
}

function formatAgeSeconds(value: number | null | undefined) {
  if (value == null) return '—';
  return `${Math.max(value, 0)}s`;
}

export function HealthPanel({ health }: HealthPanelProps) {
  const cards = [
    {
      title: 'API',
      value: health?.api.ok ? 'OK' : 'N/A',
      tone: health?.api.ok ? 'positive' : 'neutral',
      meta: health ? `${health.api.environment} / ${health.api.trading_mode}` : '—',
    },
    {
      title: 'DB',
      value: health?.db.ok ? 'OK' : 'N/A',
      tone: health?.db.ok ? 'positive' : 'neutral',
      meta: formatDateTime(health?.db.now),
    },
    {
      title: 'Active bot heartbeats',
      value: health ? `${health.bot_heartbeats.fresh} fresh / ${health.bot_heartbeats.stale} stale` : '—',
      tone: (health?.bot_heartbeats.stale ?? 0) > 0 ? 'negative' : 'positive',
      meta: health
        ? `Latest: ${formatDateTime(health.bot_heartbeats.latest_at)}${health.bot_heartbeats.legacy_old ? ` · Legacy inactive: ${health.bot_heartbeats.legacy_old}` : ''}`
        : '—',
    },
    {
      title: 'Workers',
      value: health?.worker_heartbeats
        ? `${health.worker_heartbeats.healthy} OK / ${health.worker_heartbeats.degraded + health.worker_heartbeats.stale + health.worker_heartbeats.dead} issue`
        : '—',
      tone: ((health?.worker_heartbeats?.degraded ?? 0) + (health?.worker_heartbeats?.stale ?? 0) + (health?.worker_heartbeats?.dead ?? 0)) > 0 ? 'negative' : 'positive',
      meta: health?.worker_heartbeats ? `${health.worker_heartbeats.total} reporting` : '—',
    },
    {
      title: 'Market data',
      value: health ? String(health.market_data.tracked_pairs) : '—',
      tone: 'neutral',
      meta: `Latest: ${formatDateTime(health?.market_data.latest_candle_close_at)}`,
    },
    {
      title: 'Orchestrator',
      value: health ? String(health.orchestrator.events_last_15m) : '—',
      tone: 'neutral',
      meta: `Latest: ${formatDateTime(health?.orchestrator.latest_event_at)}`,
    },
    {
      title: 'Panic state',
      value: health?.panic_state.enabled ? 'ON' : 'OFF',
      tone: health?.panic_state.enabled ? 'negative' : 'positive',
      meta: `Updated: ${formatDateTime(health?.panic_state.updated_at)}`,
    },
    {
      title: 'Semantic / authority',
      value: health?.conformance?.overall_readiness ? 'PASS' : 'NOT ACCEPTED',
      tone: health?.conformance?.overall_readiness ? 'positive' : 'negative',
      meta: health?.conformance
        ? `${health.conformance.regime_source} · ${health.conformance.policy_coverage} · ${health.conformance.effective_regime_mode ?? '—'} · ${health.conformance.actual_entry_authority}`
        : 'No conformance evidence',
    },
    {
      title: 'Runtime revision',
      value: health?.conformance?.runtime_revision?.slice(0, 12) || 'UNKNOWN',
      tone: health?.conformance?.runtime_revision ? 'positive' : 'negative',
      meta: health?.conformance
        ? `process=${health.conformance.process_health} data=${health.conformance.data_health} semantic=${health.conformance.semantic_health} authority=${health.conformance.authority_health}`
        : '—',
    },
  ];

  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Health overview</h2>
        <span className="panel-meta">Operator checks</span>
      </div>
      <div className="health-grid">
        {cards.map((card) => (
          <article key={card.title} className="health-card">
            <span className="status-label">{card.title}</span>
            <strong className={`status-value ${card.tone}`}>{card.value}</strong>
            <span className="status-meta">{card.meta}</span>
          </article>
        ))}
      </div>

      {health?.worker_heartbeats?.items?.length ? (
        <div className="table-wrap health-workers-table-wrap">
          <table className="data-table health-workers-table">
            <thead>
              <tr>
                <th>Worker</th>
                <th>Status</th>
                <th>Last tick</th>
                <th>Age</th>
                <th>Loop</th>
                <th>Last error</th>
              </tr>
            </thead>
            <tbody>
              {health.worker_heartbeats.items.map((worker) => (
                <tr key={`${worker.service_name}:${worker.environment}`}>
                  <td>{worker.service_name}</td>
                  <td>
                    <span className={`status-pill status-pill--${worker.effective_status}`}>
                      {worker.effective_status}
                    </span>
                  </td>
                  <td>{formatDateTime(worker.last_tick)}</td>
                  <td>{formatAgeSeconds(worker.age_seconds)}</td>
                  <td>{worker.loop_duration_ms == null ? '—' : `${worker.loop_duration_ms}ms`}</td>
                  <td className="muted-cell">{worker.last_error || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </section>
  );
}
