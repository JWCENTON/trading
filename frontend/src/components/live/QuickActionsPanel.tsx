import { useMemo, useState } from 'react';
import type { UiLiveSummary } from '../../api';

interface QuickActionsPanelProps {
  summary: UiLiveSummary | null;
  onRefresh: () => Promise<void> | void;
  onTogglePanic: (enabled: boolean, reason: string) => Promise<void> | void;
  actionBusy?: boolean;
}

export function QuickActionsPanel({ summary, onRefresh, onTogglePanic, actionBusy = false }: QuickActionsPanelProps) {
  const [reason, setReason] = useState('operator manual action');
  const panicEnabled = summary?.panic.enabled ?? false;
  const placeholder = useMemo(
    () => (panicEnabled ? 'reason for disabling panic' : 'reason for enabling panic'),
    [panicEnabled],
  );

  return (
    <section className="panel quick-actions-panel">
      <div className="panel-header">
        <h2>Quick actions</h2>
        <span className="panel-meta">Manual-first</span>
      </div>
      <div className="quick-actions-grid">
        <div className="button-row">
          <button type="button" className="action-button" onClick={() => void onRefresh()} disabled={actionBusy}>
            Refresh live
          </button>
        </div>

        <div className="panic-block">
          <label htmlFor="panic-reason">Panic reason</label>
          <input
            id="panic-reason"
            value={reason}
            placeholder={placeholder}
            onChange={(e) => setReason(e.target.value)}
          />
          <div className="button-row">
            <button
              type="button"
              className="action-button danger"
              onClick={() => void onTogglePanic(true, reason || 'panic enabled from ui')}
              disabled={actionBusy || panicEnabled}
            >
              Panic ON
            </button>
            <button
              type="button"
              className="action-button success"
              onClick={() => void onTogglePanic(false, reason || 'panic disabled from ui')}
              disabled={actionBusy || !panicEnabled}
            >
              Panic OFF
            </button>
          </div>
        </div>
      </div>
    </section>
  );
}
