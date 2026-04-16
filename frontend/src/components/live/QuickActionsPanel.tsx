import { useEffect, useState } from 'react';
import type { UiLiveSummary, UiUserSettings } from '../../api';

interface QuickActionsPanelProps {
  summary: UiLiveSummary | null;
  onRefresh: () => Promise<void> | void;
  onTogglePanic: (enabled: boolean, reason: string) => Promise<void> | void;
  onSaveMinEntry: (value: number) => Promise<void> | void;
  settings: UiUserSettings | null;
  actionBusy: boolean;
}

export function QuickActionsPanel({ summary, onRefresh, onTogglePanic, onSaveMinEntry, settings, actionBusy }: QuickActionsPanelProps) {
  const [reason, setReason] = useState('ui operator action');
  const [minEntryInput, setMinEntryInput] = useState('6');

  useEffect(() => {
    if (settings) {
      setMinEntryInput(String(settings.configured_min_entry_usdc));
    }
  }, [settings]);

  return (
    <section className="panel quick-actions-panel">
      <div className="panel-header">
        <h2>Quick actions</h2>
        <span className="panel-meta">Manual-first</span>
      </div>

      <div className="quick-actions-grid">
        <div className="stack-row stack-row--split">
          <div className="info-tile">
            <span className="status-label">Current panic</span>
            <strong className={`status-value ${summary?.panic.enabled ? 'negative' : 'positive'}`}>
              {summary?.panic.enabled ? 'ON' : 'OFF'}
            </strong>
          </div>
          <div className="info-tile">
            <span className="status-label">Current reason</span>
            <strong className="status-value text-ellipsis">{summary?.panic.reason || '—'}</strong>
          </div>
        </div>

        <div className="panic-block">
          <label htmlFor="panic-reason">Panic reason</label>
          <input
            id="panic-reason"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder="Podaj reason do audit loga"
          />
        </div>

        <div className="panic-block">
          <label htmlFor="min-entry-usdc">Min entry (USDC)</label>
          <input
            id="min-entry-usdc"
            value={minEntryInput}
            onChange={(e) => setMinEntryInput(e.target.value)}
            inputMode="decimal"
            placeholder="np. 100"
          />
          <div className="stack-row stack-row--split">
            <div className="info-tile">
              <span className="status-label">Configured</span>
              <strong className="status-value">{settings?.configured_min_entry_usdc ?? '-'}</strong>
            </div>
            <div className="info-tile">
              <span className="status-label">Effective</span>
              <strong className="status-value">{settings?.effective_min_entry_usdc ?? '-'}</strong>
            </div>
          </div>
        </div>

        <div className="button-row button-row--stack-mobile">
          <button type="button" className="action-button" onClick={() => void onRefresh()} disabled={actionBusy}>
            Refresh live
          </button>
          <button
            type="button"
            className="action-button"
            onClick={() => void onSaveMinEntry(Number(minEntryInput || '0'))}
            disabled={actionBusy}
          >
            Save min entry
          </button>
          <button type="button" className="action-button danger" onClick={() => void onTogglePanic(true, reason)} disabled={actionBusy}>
            Panic ON
          </button>
          <button type="button" className="action-button success" onClick={() => void onTogglePanic(false, reason)} disabled={actionBusy}>
            Panic OFF
          </button>
        </div>
      </div>
    </section>
  );
}
