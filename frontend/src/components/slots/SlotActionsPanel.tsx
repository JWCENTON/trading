import { useMemo, useState } from 'react';
import type { Strategy, UiRegimeControlPayload, UiSlotControlPayload, UiSlotRow } from '../../api';

interface SlotActionsPanelProps {
  items: UiSlotRow[];
  actionBusy?: boolean;
  onRefresh: () => Promise<void> | void;
  onUpdateSlot: (payload: UiSlotControlPayload) => Promise<void> | void;
  onUpdateRegime: (payload: UiRegimeControlPayload) => Promise<void> | void;
}

function parseKey(key: string) {
  const [symbol, interval, strategy] = key.split('|');
  return { symbol, interval, strategy: strategy as Strategy };
}

export function SlotActionsPanel({ items, actionBusy = false, onRefresh, onUpdateSlot, onUpdateRegime }: SlotActionsPanelProps) {
  const [selectedKey, setSelectedKey] = useState('');
  const [reason, setReason] = useState('operator slot action');

  const options = useMemo(
    () => items.map((item) => ({
      key: `${item.symbol}|${item.interval}|${item.strategy}`,
      label: `${item.symbol} / ${item.interval} / ${item.strategy}`,
    })),
    [items],
  );

  const selected = selectedKey ? parseKey(selectedKey) : null;

  return (
    <section className="panel quick-actions-panel">
      <div className="panel-header">
        <h2>Slot actions</h2>
        <span className="panel-meta">Manual control</span>
      </div>

      <div className="quick-actions-grid">
        <div className="panic-block">
          <label htmlFor="slot-select">Selected slot</label>
          <select
            id="slot-select"
            value={selectedKey}
            onChange={(e) => setSelectedKey(e.target.value)}
            style={{ background: '#020617', border: '1px solid #334155', borderRadius: 10, color: '#f8fafc', padding: '10px 12px' }}
          >
            <option value="">Choose slot…</option>
            {options.map((option) => (
              <option key={option.key} value={option.key}>{option.label}</option>
            ))}
          </select>
        </div>

        <div className="panic-block">
          <label htmlFor="slot-reason">Reason</label>
          <input id="slot-reason" value={reason} onChange={(e) => setReason(e.target.value)} />
        </div>

        <div className="button-row">
          <button type="button" className="action-button" onClick={() => void onRefresh()} disabled={actionBusy}>Refresh slots</button>
          <button type="button" className="action-button success" disabled={actionBusy || !selected} onClick={() => selected && void onUpdateSlot({ ...selected, enabled: true, live_orders_enabled: true, reason })}>Enable + live ON</button>
          <button type="button" className="action-button danger" disabled={actionBusy || !selected} onClick={() => selected && void onUpdateSlot({ ...selected, enabled: false, live_orders_enabled: false, reason })}>Disable + live OFF</button>
          <button type="button" className="action-button" disabled={actionBusy || !selected} onClick={() => selected && void onUpdateRegime({ ...selected, regime_enabled: true, regime_mode: 'ENFORCE', reason })}>Regime ENFORCE</button>
          <button type="button" className="action-button" disabled={actionBusy || !selected} onClick={() => selected && void onUpdateRegime({ ...selected, regime_enabled: true, regime_mode: 'DRY_RUN', reason })}>Regime DRY_RUN</button>
        </div>
      </div>
    </section>
  );
}
