import { useCallback, useEffect, useMemo, useState } from "react";
import {
  getAuthMe,
  login,
  logout,
  changePassword,
  getSecuritySummary,
  getApiKeyStatus,
  submitApiKeySafetyConfirmation,
  getUiAccount,
  getUiAdvancedSummary,
  getUserSettings,
  getUiHealth,
  getUiLiveSummary,
  getUiOpenPositions,
  getUiRecentClosed,
  getUiSlots,
  getUiTrading24h,
  updatePanicState,
  restoreUserSettingsDefaults,
  returnSlotToAuto,
  updateUserSettings,
  updateRegimeControl,
  updateSlotControl,
  updateSlotManualControl,
  getUiNotificationPreferences,
  updateUiNotificationPreferences,
  type AuthUser,
  type SecuritySummary,
  type ApiKeyStatusResponse,
  type UiAccountSummary,
  type UiEnvironment,
  type UiHealthResponse,
  type UiLiveSummary,
  type UiOpenPosition,
  type UiRecentClosedPosition,
  type UiSlotRow,
  type UiTrading24hSummary,
  type UiUserSettings,
  type UiNotificationPreference,
} from "./api";
import { applyTheme, getInitialTheme, toggleTheme, type ThemeMode } from "./theme";
import { AppShell, type AppTab } from "./components/layout/AppShell";
import { TopStatusBar } from "./components/live/TopStatusBar";
import { EnvironmentSwitch } from "./components/live/EnvironmentSwitch";
import { AccountSnapshotPanel } from "./components/live/AccountSnapshotPanel";
import { Trading24hPanel } from "./components/live/Trading24hPanel";
import { OpenPositionsTable } from "./components/live/OpenPositionsTable";
import { RecentClosedTable } from "./components/live/RecentClosedTable";
import { QuickActionsPanel } from "./components/live/QuickActionsPanel";
import { SlotsTable } from "./components/slots/SlotsTable";
import { SlotActionsPanel } from "./components/slots/SlotActionsPanel";
import { HealthPanel } from "./components/health/HealthPanel";
import "./App.css";

interface PanicConfirmState {
  enabled: boolean;
  reason: string;
}

function detectEnvironmentFromHost(): UiEnvironment {
  if (typeof window === "undefined") return "LIVE";
  return window.location.hostname.startsWith("paper-") ? "PAPER" : "LIVE";
}

function App() {
  const [activeTab, setActiveTab] = useState<AppTab>("live");
  const [environment] = useState<UiEnvironment>(() => detectEnvironmentFromHost());
  const [summary, setSummary] = useState<UiLiveSummary | null>(null);
  const [account, setAccount] = useState<UiAccountSummary | null>(null);
  const [trading24h, setTrading24h] = useState<UiTrading24hSummary | null>(null);
  const [openPositions, setOpenPositions] = useState<UiOpenPosition[]>([]);
  const [recentClosed, setRecentClosed] = useState<UiRecentClosedPosition[]>([]);
  const [theme, setTheme] = useState<ThemeMode>(() => getInitialTheme());
  const [slots, setSlots] = useState<UiSlotRow[]>([]);
  const [health, setHealth] = useState<UiHealthResponse | null>(null);
  const [settings, setSettings] = useState<UiUserSettings | null>(null);
  const [loading, setLoading] = useState(true);
  const [actionBusy, setActionBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [panicConfirm, setPanicConfirm] = useState<PanicConfirmState | null>(null);
  const [authChecked, setAuthChecked] = useState(false);
  const [authenticated, setAuthenticated] = useState(false);
  const [currentUser, setCurrentUser] = useState<AuthUser | null>(null);
  const [loginUsername, setLoginUsername] = useState("");
  const [loginPassword, setLoginPassword] = useState("");
  const [authBusy, setAuthBusy] = useState(false);
  const [oldPassword, setOldPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [notificationPreferences, setNotificationPreferences] = useState<UiNotificationPreference[]>([]);
  const [notificationPrefBusy, setNotificationPrefBusy] = useState(false);

  const [securitySummary, setSecuritySummary] = useState<SecuritySummary | null>(null);
  const [securityLoading, setSecurityLoading] = useState(false);
  const [apiKeyStatus, setApiKeyStatus] = useState<ApiKeyStatusResponse | null>(null);
  const [apiKeyConfirmBusy, setApiKeyConfirmBusy] = useState(false);
  const [apiKeyConfirmDone, setApiKeyConfirmDone] = useState(false);

  const checkAuth = useCallback(async () => {
    setError(null);
    setAuthBusy(true);
    try {
      const me = await getAuthMe();
      const isAuthenticated = Boolean(me.authenticated);
      const nextUser = me.user ?? null;

      setAuthenticated(isAuthenticated);
      setCurrentUser(nextUser);

      if (isAuthenticated && nextUser?.must_change_password) {
        setActiveTab("security");
      }
    } catch (_err) {
      setAuthenticated(false);
      setCurrentUser(null);
    } finally {
      setAuthChecked(true);
      setAuthBusy(false);
    }
  }, []);

  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  useEffect(() => {
    void checkAuth();
  }, [checkAuth, environment]);

  const handleLogin = useCallback(async () => {
    setError(null);
    setAuthBusy(true);
    try {
      const result = await login({
        username: loginUsername.trim(),
        password: loginPassword,
      });
      setAuthenticated(Boolean(result.authenticated));
      setCurrentUser(result.user ?? null);
      setLoginPassword("");
      await checkAuth();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setAuthenticated(false);
      setCurrentUser(null);
    } finally {
      setAuthBusy(false);
    }
  }, [checkAuth, loginPassword, loginUsername]);

  const handleLogout = useCallback(async () => {
    setError(null);
    setAuthBusy(true);
    try {
      await logout();
    } catch (_err) {
      // ignore
    } finally {
      setAuthenticated(false);
      setCurrentUser(null);
      setSummary(null);
      setAccount(null);
      setTrading24h(null);
      setOpenPositions([]);
      setRecentClosed([]);
      setSlots([]);
      setHealth(null);
      setSettings(null);
      setAuthChecked(true);
      setAuthBusy(false);
    }
  }, []);

  const handleChangePassword = useCallback(async () => {
    setError(null);
    setAuthBusy(true);
    try {
      const forceChangeWasRequired = Boolean(currentUser?.must_change_password);

      await changePassword({
        old_password: oldPassword,
        new_password: newPassword,
      });

      setOldPassword("");
      setNewPassword("");
      await checkAuth();

      if (forceChangeWasRequired) {
        setActiveTab("live");
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setAuthBusy(false);
    }
  }, [checkAuth, currentUser?.must_change_password, newPassword, oldPassword]);

  const loadLive = useCallback(async () => {
    setError(null);
    setLoading(true);
    try {
      const [summaryRes, accountRes, trading24hRes, openRes, closedRes, settingsRes] = await Promise.all([
        getUiLiveSummary(),
        getUiAccount(),
        getUiTrading24h(),
        getUiOpenPositions(),
        getUiRecentClosed(10),
        getUserSettings(),
      ]);

      setSummary(summaryRes);
      setAccount(accountRes);
      setTrading24h(trading24hRes);
      setOpenPositions(openRes.items);
      setRecentClosed(closedRes.items);
      setSettings(settingsRes);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setLoading(false);
    }
  }, [environment]);

  const loadSlots = useCallback(async () => {
    setError(null);
    setLoading(true);
    try {
      const slotsRes = await getUiSlots();
      if (slotsRes.error) throw new Error(slotsRes.error);
      setSlots(slotsRes.items);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setLoading(false);
    }
  }, [environment]);

  const loadAdvanced = useCallback(async () => {
    setError(null);
    setLoading(true);
    try {
      const [settingsRes, notificationPrefsRes] = await Promise.all([
        getUiAdvancedSummary(),
        getUiNotificationPreferences(),
      ]);
      setSettings(settingsRes);
      setNotificationPreferences(notificationPrefsRes.items);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setLoading(false);
    }
  }, [environment]);

  const loadSecuritySummary = useCallback(async () => {
    try {
      setSecurityLoading(true);
      const [summaryRes, apiKeyRes] = await Promise.all([
        getSecuritySummary(),
        getApiKeyStatus(),
      ]);
      setSecuritySummary(summaryRes);
      setApiKeyStatus(apiKeyRes);
      setApiKeyConfirmDone(Boolean(apiKeyRes.safety_confirmed));
    } catch (e) {
      console.error("security summary failed", e);
    } finally {
      setSecurityLoading(false);
    }
  }, []);

  const handleApiKeySafetyConfirm = useCallback(async () => {
    try {
      setApiKeyConfirmBusy(true);

      const payload = {
        reading_enabled: true,
        spot_trading_enabled: true,
        withdrawals_disabled: true,
        margin_loan_repay_transfer_disabled: true,
        internal_transfer_disabled: true,
        universal_transfer_disabled: true,
        ip_whitelist_enabled: true,
        risk_accepted: true,
        no_investment_advice_ack: true,
        client_controls_binance_account_ack: true,
      };

      const res = await submitApiKeySafetyConfirmation(payload);

      if (res.ok && res.all_confirmed) {
        setApiKeyConfirmDone(true);
      }

      await loadSecuritySummary();
    } catch (e) {
      console.error("api key safety confirm failed", e);
      setError("API key safety confirmation failed");
    } finally {
      setApiKeyConfirmBusy(false);
    }
  }, [loadSecuritySummary]);

  const loadHealth = useCallback(async () => {
    setError(null);
    setLoading(true);
    try {
      const healthRes = await getUiHealth();
      if (healthRes.error) throw new Error(healthRes.error);
      setHealth(healthRes);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setLoading(false);
    }
  }, [environment]);

  useEffect(() => {
    if (!authChecked || !authenticated) return;

    if (activeTab === "live") {
      void loadLive();
    } else if (activeTab === "slots") {
      void loadSlots();
    } else if (activeTab === "health") {
      void loadHealth();
    } else if (activeTab === "advanced") {
      void loadAdvanced();
    }
  }, [activeTab, environment, authChecked, authenticated, loadAdvanced, loadHealth, loadLive, loadSlots]);

  const handleTogglePanic = useCallback(async (enabled: boolean, reason: string) => {
    setActionBusy(true);
    setError(null);
    try {
      await updatePanicState(enabled, reason);
      await Promise.all([loadLive(), loadHealth()]);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setActionBusy(false);
    }
  }, [loadHealth, loadLive]);

  const handleRequestPanicToggle = useCallback((enabled: boolean, reason: string) => {
    setPanicConfirm({ enabled, reason });
  }, []);

  const handleSaveAdvancedSettings = useCallback(async (manualEntryAddonUsdc: number, threeWinBoostUsdc: number) => {
    setActionBusy(true);
    setError(null);
    try {
      const nextSettings = await updateUserSettings({
        manual_entry_addon_usdc: manualEntryAddonUsdc,
        three_win_boost_usdc: threeWinBoostUsdc,
      });
      setSettings(nextSettings);
      await Promise.all([loadAdvanced(), loadLive()]);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setActionBusy(false);
    }
  }, [loadAdvanced, loadLive]);

  const handleRestoreAdvancedDefaults = useCallback(async () => {
    setActionBusy(true);
    setError(null);
    try {
      const response = await restoreUserSettingsDefaults();
      setSettings(response.settings);
      await Promise.all([loadAdvanced(), loadLive()]);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setActionBusy(false);
    }
  }, [loadAdvanced, loadLive]);

  const handleToggleNotificationPreference = useCallback(async (category: string, enabled: boolean) => {
    setNotificationPrefBusy(true);
    setError(null);
    try {
      const nextPrefs = notificationPreferences.map((item) =>
        item.category === category ? { ...item, enabled } : item,
      );
      const response = await updateUiNotificationPreferences(nextPrefs);
      setNotificationPreferences(response.items);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setNotificationPrefBusy(false);
    }
  }, [notificationPreferences]);

  const handleSlotUpdate = useCallback(async (payload: Parameters<typeof updateSlotControl>[0]) => {
    setActionBusy(true);
    setError(null);
    try {
      await updateSlotControl(payload);
      await Promise.all([loadSlots(), loadLive()]);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setActionBusy(false);
    }
  }, [loadLive, loadSlots]);

  const handleRegimeUpdate = useCallback(async (payload: Parameters<typeof updateRegimeControl>[0]) => {
    setActionBusy(true);
    setError(null);
    try {
      await updateRegimeControl(payload);
      await Promise.all([loadSlots(), loadLive()]);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setActionBusy(false);
    }
  }, [loadLive, loadSlots]);

  const handleSlotManualUpdate = useCallback(async (payload: Parameters<typeof updateSlotManualControl>[0]) => {
    setActionBusy(true);
    setError(null);
    try {
      await updateSlotManualControl(payload);
      await Promise.all([loadSlots(), loadLive()]);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setActionBusy(false);
    }
  }, [loadLive, loadSlots]);

  const handleSlotReturnAuto = useCallback(async (payload: Parameters<typeof returnSlotToAuto>[0]) => {
    setActionBusy(true);
    setError(null);
    try {
      await returnSlotToAuto(payload);
      await Promise.all([loadSlots(), loadLive()]);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setActionBusy(false);
    }
  }, [loadLive, loadSlots]);

  const title = useMemo(() => {
    switch (activeTab) {
      case "slots": return "Slots";
      case "health": return "Health";
      case "advanced": return "Advanced";
      case "security": return "Security";
      default: return "Live";
    }
  }, [activeTab]);

  const [manualAddonInput, setManualAddonInput] = useState("0");
  const [threeWinBoostInput, setThreeWinBoostInput] = useState("10");

  useEffect(() => {
    if (settings) {
      setManualAddonInput(String(settings.manual_entry_addon_usdc ?? 0));
      setThreeWinBoostInput(String(settings.three_win_boost_usdc ?? 10));
    }
  }, [settings]);

  useEffect(() => {
    if (activeTab === "security" && authenticated) {
      void loadSecuritySummary();
    }
  }, [activeTab, authenticated, loadSecuritySummary]);

  const subtitle = useMemo(() => {
    if (activeTab === "security") {
      return "Hasło, sesja operatora i przyszłe security features jak recovery oraz 2FA.";
    }
    if (activeTab === "advanced") {
      return "Advanced zostawiamy jako strefę cięższych rzeczy. Na tym etapie migrujemy najpierw Live, Slots, Health i Security.";
    }
    if (activeTab === "slots") {
      return "Operator slot control: enabled, live orders, regime gating, open position, heartbeat i last event.";
    }
    if (activeTab === "health") {
      return "Zdrowie systemu: API, DB, bot heartbeats, market data freshness, orchestrator freshness i panic state.";
    }
    return "Manual refresh first. Truth-only operatorski widok oparty o panic_state, bot_control, positions, bot_heartbeat i candles.";
  }, [activeTab]);

  if (!authChecked) {
    return <div className="panel">Checking session...</div>;
  }

  if (!authenticated) {
    return (
      <div className="page-grid">
        <section className="panel quick-actions-panel" style={{ maxWidth: 420, margin: "40px auto" }}>
          <div className="panel-header">
            <h2>Login</h2>
            <span className="panel-meta">{environment} environment</span>
          </div>

          {error ? <div className="error-banner">API error: {error}</div> : null}

          <div style={{ display: "grid", gap: 12 }}>
            <label>
              <div>Username</div>
              <input
                value={loginUsername}
                onChange={(e) => setLoginUsername(e.target.value)}
                autoComplete="username"
              />
            </label>

            <label>
              <div>Password</div>
              <input
                type="password"
                value={loginPassword}
                onChange={(e) => setLoginPassword(e.target.value)}
                autoComplete="current-password"
              />
            </label>

            <div className="button-row">
              <button className="action-button" onClick={() => void handleLogin()} disabled={authBusy}>
                {authBusy ? "Logging in..." : `Login to ${environment}`}
              </button>
            </div>

            <div className="live-controls-primary">
              <EnvironmentSwitch environment={environment} />
            </div>
          </div>
        </section>
      </div>
    );
  }

  return (
    <AppShell
      title={title}
      subtitle={subtitle}
      activeTab={activeTab}
      onTabChange={setActiveTab}
      environment={environment}
      theme={theme}
      onThemeToggle={() => setTheme((current) => toggleTheme(current))}
    >
      <div className="button-row" style={{ marginBottom: 12 }}>
        <span style={{ marginRight: 12 }}>
          Logged in as <strong>{currentUser?.username}</strong> ({environment})
        </span>
        <button className="action-button secondary" onClick={() => void handleLogout()} disabled={authBusy}>
          Logout
        </button>
      </div>

      <div className="page-grid">
        {error ? <div className="error-banner">API error: {error}</div> : null}
        {loading && activeTab === "live" && !summary ? <div className="panel">Ładowanie nowego panelu Live…</div> : null}
        {loading && activeTab === "slots" && slots.length === 0 ? <div className="panel">Ładowanie Slots…</div> : null}
        {loading && activeTab === "health" && !health ? <div className="panel">Ładowanie Health…</div> : null}

        {activeTab === "live" ? (
          <div className="live-home-stack">
            <TopStatusBar summary={summary} onRefresh={loadLive} refreshBusy={loading || actionBusy} />

            <div className="live-priority-grid">
              <div className="live-priority-main">
                <AccountSnapshotPanel account={account} />
              </div>
              <div className="live-priority-side">
                <Trading24hPanel trading24h={trading24h} />
              </div>
            </div>

            <div className="live-controls-grid">
              <div className="live-controls-primary">
                <EnvironmentSwitch environment={environment} />
              </div>
              <div className="live-controls-secondary">
                <QuickActionsPanel
                  summary={summary}
                  onTogglePanic={handleRequestPanicToggle}
                  settings={settings}
                  actionBusy={actionBusy}
                />
              </div>
            </div>

            <OpenPositionsTable items={openPositions} />
            <RecentClosedTable items={recentClosed} />
          </div>
        ) : null}

        {activeTab === "slots" ? (
          <>
            <SlotActionsPanel
              items={slots}
              actionBusy={actionBusy}
              onRefresh={loadSlots}
              onUpdateSlot={handleSlotUpdate}
              onUpdateRegime={handleRegimeUpdate}
              onSetManual={handleSlotManualUpdate}
              onReturnAuto={handleSlotReturnAuto}
            />
            <SlotsTable items={slots} />
          </>
        ) : null}

        {activeTab === "health" ? (
          <>
            <section className="panel quick-actions-panel">
              <div className="panel-header">
                <h2>Health actions</h2>
                <span className="panel-meta">Manual refresh</span>
              </div>
              <div className="button-row">
                <button className="action-button" onClick={() => void loadHealth()}>
                  Refresh health
                </button>
              </div>
            </section>
            <HealthPanel health={health} />
          </>
        ) : null}

        {activeTab === "security" ? (
          <section className="panel advanced-placeholder">
            <div className="panel-header">
              <h2>Security</h2>
              <span className="panel-meta">
                {securityLoading ? "Loading security..." : currentUser?.must_change_password ? "Password change required" : "Password & session"}
              </span>
            </div>

            <div className="quick-actions-grid">
              <div className="stack-row stack-row--split">
                <div className="info-tile">
                  <span className="status-label">Current user</span>
                  <strong className="status-value">{currentUser?.username ?? "-"}</strong>
                </div>
                <div className="info-tile">
                  <span className="status-label">Role</span>
                  <strong className="status-value">{currentUser?.is_admin ? "ADMIN" : "USER"}</strong>
                </div>
                <div className="info-tile">
                  <span className="status-label">Password state</span>
                  <strong className="status-value">
                    {currentUser?.must_change_password ? "CHANGE REQUIRED" : "OK"}
                  </strong>
                </div>
              </div>

              <div className="panic-block">
                <label htmlFor="security-old-password">Current password</label>
                <input
                  id="security-old-password"
                  type="password"
                  value={oldPassword}
                  onChange={(e) => setOldPassword(e.target.value)}
                  autoComplete="current-password"
                />
              </div>

              <div className="panic-block">
                <label htmlFor="security-new-password">New password</label>
                <input
                  id="security-new-password"
                  type="password"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  autoComplete="new-password"
                />
              </div>

              <div className="button-row button-row--stack-mobile">
                <button
                  className="action-button"
                  onClick={() => void handleChangePassword()}
                  disabled={authBusy || !oldPassword || !newPassword}
                >
                  {authBusy ? "Updating..." : "Change password"}
                </button>
                <button
                  className="action-button secondary"
                  onClick={() => void handleLogout()}
                  disabled={authBusy}
                >
                  Logout
                </button>
              </div>
            
              {securitySummary && (
                <div className="info-tile" style={{ marginTop: 20 }}>
                  <strong>Last login:</strong> {securitySummary.last_login_at ?? "-"}<br />
                  <strong>Password changed:</strong> {securitySummary.password_changed_at ?? "-"}<br />
                  <strong>Active sessions:</strong> {securitySummary.active_sessions}
                </div>
              )}

              {securitySummary?.last_successful_login && (
                <div className="info-tile" style={{ marginTop: 10 }}>
                  <strong>Last successful login</strong><br />
                  {securitySummary.last_successful_login.created_at}<br />
                  IP: {securitySummary.last_successful_login.ip}<br />
                  UA: {securitySummary.last_successful_login.user_agent}
                </div>
              )}

              {securitySummary?.failed_logins?.length ? (
                <div className="info-tile" style={{ marginTop: 10 }}>
                  <strong>Recent failed logins</strong>
                  <ul>
                    {securitySummary.failed_logins.slice(0,5).map((f, i) => (
                      <li key={i}>
                        {f.created_at} | {f.ip} | {f.reason}
                      </li>
                    ))}
                  </ul>
                </div>
              ) : null}

              <div className="info-tile" style={{ marginTop: 16 }}>
                <strong>Binance API key safety</strong><br />
                Configured: {apiKeyStatus?.configured ? "YES" : "NO"}<br />
                Account read: {apiKeyStatus?.account_read_check ?? "-"}<br />
                Spot trading: {apiKeyStatus?.spot_trading_check ?? "-"}<br />
                Secret exposed: {apiKeyStatus?.secrets_exposed ? "YES - CHECK IMMEDIATELY" : "NO"}<br />
                Withdraw permission: must be disabled in Binance API Management.<br />
                IP whitelist: required/recommended.
              </div>

              <div className="info-tile" style={{ marginTop: 10 }}>
                <strong>Required confirmation</strong>
                <ul>
                  <li>Reading permission is enabled.</li>
                  <li>Spot trading permission is enabled.</li>
                  <li>Withdrawal permission is disabled.</li>
                  <li>Margin loan/repay/transfer is disabled.</li>
                  <li>Internal transfer is disabled.</li>
                  <li>Universal transfer is disabled.</li>
                  <li>IP whitelist is enabled for this VPS.</li>
                  <li>I understand crypto trading risk and possible losses.</li>
                  <li>This software is not investment advice.</li>
                  <li>I control the Binance account and API key.</li>
                </ul>

                <button
                  className="action-button"
                  onClick={() => void handleApiKeySafetyConfirm()}
                  disabled={apiKeyConfirmBusy || apiKeyConfirmDone || !apiKeyStatus?.configured}
                >
                  {apiKeyConfirmBusy
                    ? "Saving confirmation..."
                    : apiKeyConfirmDone
                      ? "Confirmed"
                      : "Confirm API key safety checklist"}
                </button>
              </div>


            </div>
          </section>
        ) : null}

        {panicConfirm ? (
          <div className="confirm-modal-backdrop" role="presentation">
            <div
              className="confirm-modal"
              role="dialog"
              aria-modal="true"
              aria-labelledby="panic-confirm-title"
            >
              <div className="confirm-modal-header">
                <h2 id="panic-confirm-title">Confirm panic change</h2>
                <span className="panel-meta">Safety confirmation</span>
              </div>

              <div className="confirm-modal-body">
                <p>
                  {panicConfirm.enabled
                    ? 'Are you sure you want to switch PANIC ON? This should immediately block trading actions for this runtime.'
                    : 'Are you sure you want to switch PANIC OFF? Make sure the runtime is safe before re-enabling trading actions.'}
                </p>

                <div className="selected-slot-summary">
                  <span className="status-label">Reason</span>
                  <strong>{panicConfirm.reason || '—'}</strong>
                </div>
              </div>

              <div className="button-row button-row--modal">
                <button
                  type="button"
                  className="action-button"
                  onClick={() => setPanicConfirm(null)}
                  disabled={actionBusy}
                >
                  Cancel
                </button>
                <button
                  type="button"
                  className={`action-button ${panicConfirm.enabled ? 'danger' : 'success'}`}
                  onClick={async () => {
                    await handleTogglePanic(panicConfirm.enabled, panicConfirm.reason);
                    setPanicConfirm(null);
                  }}
                  disabled={actionBusy}
                >
                  {actionBusy ? 'Applying...' : panicConfirm.enabled ? 'Confirm PANIC ON' : 'Confirm PANIC OFF'}
                </button>
              </div>
            </div>
          </div>
        ) : null}

        {activeTab === "advanced" ? (
          <section className="panel advanced-placeholder">
            <div className="panel-header">
              <h2>Advanced</h2>
              <span className="panel-meta">Sizing controls</span>
            </div>
            <div className="quick-actions-grid">
              <div className="stack-row stack-row--split">
                <div className="info-tile">
                  <span className="status-label">Base runtime notional</span>
                  <strong className="status-value">{settings?.base_runtime_notional_usdc ?? '-'}</strong>
                </div>
                <div className="info-tile">
                  <span className="status-label">Normal entry preview</span>
                  <strong className="status-value">{settings?.normal_entry_preview_usdc ?? '-'}</strong>
                </div>
                <div className="info-tile">
                  <span className="status-label">Boosted entry preview</span>
                  <strong className="status-value">{settings?.boosted_entry_preview_usdc ?? '-'}</strong>
                </div>
              </div>

              <div className="panic-block">
                <label htmlFor="manual-addon-usdc">Manual add-on (USDC)</label>
                <input
                  id="manual-addon-usdc"
                  value={manualAddonInput}
                  onChange={(e) => setManualAddonInput(e.target.value)}
                  inputMode="decimal"
                  placeholder="np. 10"
                />
              </div>

              <div className="panic-block">
                <label htmlFor="three-win-boost-usdc">3-win boost (USDC)</label>
                <input
                  id="three-win-boost-usdc"
                  value={threeWinBoostInput}
                  onChange={(e) => setThreeWinBoostInput(e.target.value)}
                  inputMode="decimal"
                  placeholder="np. 10"
                />
              </div>

              <div className="stack-row stack-row--split">
                <div className="info-tile">
                  <span className="status-label">Mode</span>
                  <strong className="status-value">{settings?.mode ?? '-'}</strong>
                </div>
                <div className="info-tile">
                  <span className="status-label">Updated at</span>
                  <strong className="status-value text-ellipsis">{settings?.updated_at ?? '-'}</strong>
                </div>
              </div>


              <div className="advanced-section-title">
                <strong>Sizing controls actions</strong>
                <span>These actions apply only to manual add-on and 3-win boost settings.</span>
              </div>

              <div className="button-row button-row--stack-mobile advanced-section-actions">
                <button
                  className="action-button"
                  onClick={() => void handleSaveAdvancedSettings(Number(manualAddonInput || '0'), Number(threeWinBoostInput || '0'))}
                  disabled={actionBusy}
                >
                  Save sizing controls
                </button>
                <button
                  className="action-button"
                  onClick={() => void handleRestoreAdvancedDefaults()}
                  disabled={actionBusy}
                >
                  Restore sizing defaults
                </button>
                <button className="action-button" onClick={() => void loadAdvanced()} disabled={actionBusy}>
                  Refresh sizing controls
                </button>
              </div>

              <div className="section-divider" />

              <div className="info-tile">
                <strong>Notification preferences</strong><br />
                Default is conservative: CRITICAL only. TRADING and INFO can be enabled when the signal/noise ratio is acceptable.
              </div>

              <div className="stack-row stack-row--split">
                {notificationPreferences.map((pref) => (
                  <label key={pref.category} className="info-tile notification-pref-tile">
                    <span className="status-label">{pref.category}</span>
                    <strong className="status-value">{pref.enabled ? "ENABLED" : "DISABLED"}</strong>
                    <input
                      type="checkbox"
                      checked={pref.enabled}
                      disabled={notificationPrefBusy}
                      onChange={(e) => void handleToggleNotificationPreference(pref.category, e.target.checked)}
                    />
                  </label>
                ))}
              </div>


            
            </div>
          </section>
        ) : null}
      </div>
    </AppShell>
  );
}

export default App;
