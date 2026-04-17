import { useMemo, useState } from 'react';
import type { UiRegimeControlPayload, UiSlotControlPayload, UiSlotRow } from '../../api';

interface SlotActionsPanelProps {
  items: UiSlotRow[];
  actionBusy: boolean;
  onRefresh: () => Promise<void> | void;
  onUpdateSlot: (payload: UiSlotControlPayload) => Promise<void> | void;
  onUpdateRegime: (payload: UiRegimeControlPayload) => Promise<void> | void;
}

export function SlotActionsPanel({ items, actionBusy, onRefresh, onUpdateSlot, onUpdateRegime }: SlotActionsPanelProps) {
  const [selectedKey, setSelectedKey] = useState('');
  const [reason, setReason] = useState('ui operator slot update');

  const selected = useMemo(
    () => items.find((item) => `${item.symbol}:${item.interval}:${item.strategy}` === selectedKey) ?? null,
    [items, selectedKey],
  );

  return (
    <section className="panel quick-actions-panel">
      <div className="panel-header">
        <h2>Slot actions</h2>
        <span className="panel-meta">Manual control</span>
      </div>

      <div className="quick-actions-grid">
        <div className="stack-row">
          <label htmlFor="slot-select" className="field-label">Selected slot</label>
          <select
            id="slot-select"
            className="dark-select"
            value={selectedKey}
            onChange={(e) => setSelectedKey(e.target.value)}
          >
            <option value="">Choose slot…</option>
            {items.map((item) => {
              const key = `${item.symbol}:${item.interval}:${item.strategy}`;
              return (
                <option key={key} value={key}>
                  {item.symbol} • {item.interval} • {item.strategy}
                </option>
              );
            })}
          </select>
        </div>

        <div className="stack-row">
          <label htmlFor="slot-reason" className="field-label">Reason</label>
          <input id="slot-reason" value={reason} onChange={(e) => setReason(e.target.value)} />
        </div>

        {selected ? (
          <div className="selected-slot-summary">
            <span className="status-label">Current</span>
            <strong>
              {selected.enabled ? 'ENABLED' : 'DISABLED'} • {selected.live_orders_enabled ? 'LIVE ON' : 'LIVE OFF'} • {selected.regime_mode || '—'}
            </strong>
            <span className="cell-subtext">{selected.reason || 'No reason'}</span>
          </div>
        ) : null}

        <div className="button-row button-row--stack-mobile">
          <button type="button" className="action-button" onClick={() => void onRefresh()} disabled={actionBusy}>Refresh slots</button>
          <button
            type="button"
            className={`action-button success ${selected?.enabled && selected?.live_orders_enabled ? 'is-active' : ''}`}
            disabled={actionBusy || !selected}
            onClick={() => selected && void onUpdateSlot({ symbol: selected.symbol, interval: selected.interval, strategy: selected.strategy, enabled: true, live_orders_enabled: true, reason })}
            aria-pressed={Boolean(selected?.enabled && selected?.live_orders_enabled)}
          >
            Enable + Live ON
          </button>
          <button
            type="button"
            className={`action-button ${selected?.enabled && !selected?.live_orders_enabled ? 'is-active' : ''}`}
            disabled={actionBusy || !selected}
            onClick={() => selected && void onUpdateSlot({ symbol: selected.symbol, interval: selected.interval, strategy: selected.strategy, enabled: true, live_orders_enabled: false, reason })}
            aria-pressed={Boolean(selected?.enabled && !selected?.live_orders_enabled)}
          >
            Enable + Live OFF
          </button>
          <button
            type="button"
            className={`action-button danger ${selected && !selected.enabled ? 'is-active' : ''}`}
            disabled={actionBusy || !selected}
            onClick={() => selected && void onUpdateSlot({ symbol: selected.symbol, interval: selected.interval, strategy: selected.strategy, enabled: false, live_orders_enabled: false, reason })}
            aria-pressed={Boolean(selected && !selected.enabled)}
          >
            Disable slot
          </button>
          <button
            type="button"
            className={`action-button success ${selected?.regime_enabled && selected?.regime_mode === 'ENFORCE' ? 'is-active' : ''}`}
            disabled={actionBusy || !selected}
            onClick={() => selected && void onUpdateRegime({ symbol: selected.symbol, interval: selected.interval, strategy: selected.strategy, regime_enabled: true, regime_mode: 'ENFORCE', reason })}
            aria-pressed={Boolean(selected?.regime_enabled && selected?.regime_mode === 'ENFORCE')}
          >
            Regime ENFORCE
          </button>
          <button
            type="button"
            className={`action-button ${selected?.regime_enabled && selected?.regime_mode === 'DRY_RUN' ? 'is-active' : ''}`}
            disabled={actionBusy || !selected}
            onClick={() => selected && void onUpdateRegime({ symbol: selected.symbol, interval: selected.interval, strategy: selected.strategy, regime_enabled: true, regime_mode: 'DRY_RUN', reason })}
            aria-pressed={Boolean(selected?.regime_enabled && selected?.regime_mode === 'DRY_RUN')}
          >
            Regime DRY_RUN
          </button>
        </div>
      </div>
    </section>
  );
}
