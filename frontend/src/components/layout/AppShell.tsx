import { useEffect, useState, type ReactNode } from 'react';
import type { UiEnvironment } from '../../api';
import { NotificationCenter } from './NotificationCenter';

export type AppTab = 'live' | 'slots' | 'health' | 'audit' | 'advanced' | 'security';

interface AppShellProps {
  title: string;
  subtitle: string;
  activeTab: AppTab;
  onTabChange: (tab: AppTab) => void;
  children: ReactNode;
  environment?: UiEnvironment;
  theme?: 'dark' | 'light';
  onThemeToggle?: () => void;
  isAdmin?: boolean;
}

const tabs: Array<{ key: AppTab; label: string; shortLabel: string }> = [
  { key: 'live', label: 'Live', shortLabel: 'Live' },
  { key: 'slots', label: 'Slots', shortLabel: 'Slots' },
  { key: 'health', label: 'Health', shortLabel: 'Health' },
  { key: 'audit', label: 'Audit', shortLabel: 'Audit' },
  { key: 'advanced', label: 'Advanced', shortLabel: 'Adv' },
  { key: 'security', label: 'Security', shortLabel: 'Sec' },
];

export function AppShell({ title, subtitle, activeTab, onTabChange, children, environment, theme = 'dark', onThemeToggle, isAdmin = false }: AppShellProps) {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);

  useEffect(() => {
    setMobileNavOpen(false);
  }, [activeTab]);

  useEffect(() => {
    const handleResize = () => {
      if (window.innerWidth > 900) {
        setMobileNavOpen(false);
      }
    };

    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  const visibleTabs = isAdmin ? tabs : tabs.filter((tab) => tab.key !== 'advanced');
  const isLive = environment === 'LIVE';

  return (
    <div className="app-shell app-shell--premium">
      <aside className="app-sidebar app-topbar">
        <div className="app-brand-wrap">
          <div className="app-brand-row">
            <div className="app-identity">
              <img className="app-logo" src="/waltrade-bot-logo.jpeg" alt="WALTRADE-BOT" />
              <div className="app-brand-copy">
                <div className="app-brand">WALTRADE-BOT</div>
                <div className="app-brand-subtitle">Trade smart • Automate • Grow</div>
              </div>
            </div>

            <div className="app-topbar-center" aria-label="Runtime status">
              <span className={`environment-pill ${isLive ? 'environment-pill--live' : 'environment-pill--paper'}`}>
                {environment ?? '—'}
              </span>
              <span className="connection-pill">
                <span className="connection-dot" />
                Connected
              </span>
            </div>

            <div className="app-topbar-actions">
              <button
                type="button"
                className="theme-toggle theme-toggle--desktop"
                onClick={onThemeToggle}
                aria-label="Toggle dark/light mode"
                title={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
              >
                {theme === 'dark' ? '☀︎  Light' : '☾  Dark'}
              </button>
              <button
                type="button"
                className={`nav-toggle ${mobileNavOpen ? 'active' : ''}`}
                aria-label={mobileNavOpen ? 'Close navigation menu' : 'Open navigation menu'}
                aria-expanded={mobileNavOpen}
                aria-controls="primary-mobile-nav"
                onClick={() => setMobileNavOpen((prev) => !prev)}
              >
                <span />
                <span />
                <span />
              </button>
            </div>
          </div>
        </div>

        <nav
          id="primary-mobile-nav"
          className={`app-nav ${mobileNavOpen ? 'open' : ''}`}
          aria-label="Primary navigation"
        >
          {visibleTabs.map((tab) => (
            <button
              key={tab.key}
              type="button"
              className={`nav-button ${activeTab === tab.key ? 'active' : ''}`}
              onClick={() => onTabChange(tab.key)}
              aria-pressed={activeTab === tab.key}
            >
              <span className="nav-button-label nav-button-label--full">{tab.label}</span>
              <span className="nav-button-label nav-button-label--short">{tab.shortLabel}</span>
            </button>
          ))}

          <button
            type="button"
            className="theme-toggle theme-toggle--mobile"
            onClick={onThemeToggle}
            aria-label="Toggle dark/light mode"
          >
            {theme === 'dark' ? 'Light mode' : 'Dark mode'}
          </button>
        </nav>
      </aside>

      <main className="app-main">
        <header className="page-header">
          <div>
            <h1>{title}</h1>
            <p>{subtitle}</p>
          </div>
          <NotificationCenter canMarkRead={isAdmin} />
        </header>
        {children}
      </main>
    </div>
  );
}
