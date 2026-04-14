import type { UiHealthResponse } from '../../api';

interface HealthPanelProps {
  health: UiHealthResponse | null;
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return '—';
  return new Date(value).toLocaleString();
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
      title: 'Heartbeats',
      value: health ? `${health.bot_heartbeats.fresh} fresh / ${health.bot_heartbeats.stale} stale` : '—',
      tone: (health?.bot_heartbeats.stale ?? 0) > 0 ? 'negative' : 'positive',
      meta: `Latest: ${formatDateTime(health?.bot_heartbeats.latest_at)}`,
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
    </section>
  );
}
