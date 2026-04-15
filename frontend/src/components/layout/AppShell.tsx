import type { ReactNode } from 'react';
import type { UiEnvironment } from '../../api';

export type AppTab = 'live' | 'slots' | 'health' | 'advanced';

interface AppShellProps {
  title: string;
  subtitle: string;
  activeTab: AppTab;
  onTabChange: (tab: AppTab) => void;
  children: ReactNode;
  environment?: UiEnvironment;
}

const tabs: Array<{ key: AppTab; label: string; shortLabel: string }> = [
  { key: 'live', label: 'Live', shortLabel: 'Live' },
  { key: 'slots', label: 'Slots', shortLabel: 'Slots' },
  { key: 'health', label: 'Health', shortLabel: 'Health' },
  { key: 'advanced', label: 'Advanced', shortLabel: 'Adv' },
];

export function AppShell({ title, subtitle, activeTab, onTabChange, children, environment }: AppShellProps) {
  return (
    <div className="app-shell">
      <aside className="app-sidebar">
        <div className="app-brand-wrap">
          <div className="app-brand">TRADING UI</div>
          <div className="app-brand-subtitle">Operator Dark • manual refresh first</div>
          <div className="app-brand-env">{environment ? `ENV: ${environment}` : "ENV: —"}</div>
        </div>

        <nav className="app-nav" aria-label="Primary navigation">
          {tabs.map((tab) => (
            <button
              key={tab.key}
              type="button"
              className={`nav-button ${activeTab === tab.key ? 'active' : ''}`}
              onClick={() => onTabChange(tab.key)}
            >
              <span className="nav-button-label nav-button-label--full">{tab.label}</span>
              <span className="nav-button-label nav-button-label--short">{tab.shortLabel}</span>
            </button>
          ))}
        </nav>
      </aside>

      <main className="app-main">
        <header className="page-header">
          <h1>{title}</h1>
          <p>{subtitle}</p>
        </header>
        {children}
      </main>
    </div>
  );
}
