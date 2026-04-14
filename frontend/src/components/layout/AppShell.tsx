import type { ReactNode } from 'react';

export type AppTab = 'live' | 'slots' | 'health' | 'advanced';

interface AppShellProps {
  title: string;
  subtitle: string;
  activeTab: AppTab;
  onTabChange: (tab: AppTab) => void;
  children: ReactNode;
}

const tabs: Array<{ key: AppTab; label: string }> = [
  { key: 'live', label: 'Live' },
  { key: 'slots', label: 'Slots' },
  { key: 'health', label: 'Health' },
  { key: 'advanced', label: 'Advanced' },
];

export function AppShell({ title, subtitle, activeTab, onTabChange, children }: AppShellProps) {
  return (
    <div className="app-shell">
      <aside className="app-sidebar">
        <div>
          <div className="app-brand">TRADING UI</div>
          <div className="app-brand-subtitle">Operator Dark</div>
        </div>

        <nav className="app-nav">
          {tabs.map((tab) => (
            <button
              key={tab.key}
              type="button"
              className={`nav-button ${activeTab === tab.key ? 'active' : ''}`}
              onClick={() => onTabChange(tab.key)}
            >
              {tab.label}
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
