import { useState } from 'react';
import type { UiLiveSummary, UiUserSettings } from '../../api';

interface QuickActionsPanelProps {
  summary: UiLiveSummary | null;
  onTogglePanic: (enabled: boolean, reason: string) => Promise<void> | void;
  settings: UiUserSettings | null;
  actionBusy: boolean;
}

export function QuickActionsPanel({ summary, onTogglePanic, settings, actionBusy }: QuickActionsPanelProps) {
  const [reason, setReason] = useState('ui operator action');

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

        <div className="stack-row stack-row--split">
          <div className="info-tile">
            <span className="status-label">Base</span>
            <strong className="status-value">{settings?.base_runtime_notional_usdc ?? '-'}</strong>
          </div>
          <div className="info-tile">
            <span className="status-label">Manual add-on</span>
            <strong className="status-value">{settings?.manual_entry_addon_usdc ?? '-'}</strong>
          </div>
          <div className="info-tile">
            <span className="status-label">3-win boost</span>
            <strong className="status-value">{settings?.three_win_boost_usdc ?? '-'}</strong>
          </div>
        </div>

        <div className="stack-row stack-row--split">
          <div className="info-tile">
            <span className="status-label">Normal entry</span>
            <strong className="status-value">{settings?.normal_entry_preview_usdc ?? '-'}</strong>
          </div>
          <div className="info-tile">
            <span className="status-label">Boosted entry</span>
            <strong className="status-value">{settings?.boosted_entry_preview_usdc ?? '-'}</strong>
          </div>
        </div>

        <div className="button-row button-row--stack-mobile">
          <button
            type="button"
            className="action-button danger"
            onClick={() => void onTogglePanic(true, reason)}
            disabled={actionBusy}
            aria-pressed={summary?.panic.enabled === true}
          >
            Panic ON
          </button>
          <button
            type="button"
            className="action-button success"
            onClick={() => void onTogglePanic(false, reason)}
            disabled={actionBusy}
            aria-pressed={summary?.panic.enabled === false}
          >
            Panic OFF
          </button>
        </div>
      </div>
    </section>
  );
}
