import { useCallback, useEffect, useMemo, useState } from "react";
import {
  getUiHealth,
  getUiLiveSummary,
  getUiOpenPositions,
  getUiRecentClosed,
  getUiSlots,
  updatePanicState,
  updateRegimeControl,
  updateSlotControl,
  type UiHealthResponse,
  type UiLiveSummary,
  type UiOpenPosition,
  type UiRecentClosedPosition,
  type UiSlotRow,
} from "./api";
import { AppShell, type AppTab } from "./components/layout/AppShell";
import { TopStatusBar } from "./components/live/TopStatusBar";
import { OpenPositionsTable } from "./components/live/OpenPositionsTable";
import { RecentClosedTable } from "./components/live/RecentClosedTable";
import { QuickActionsPanel } from "./components/live/QuickActionsPanel";
import { SlotsTable } from "./components/slots/SlotsTable";
import { SlotActionsPanel } from "./components/slots/SlotActionsPanel";
import { HealthPanel } from "./components/health/HealthPanel";
import "./App.css";

function App() {
  const [activeTab, setActiveTab] = useState<AppTab>("live");
  const [summary, setSummary] = useState<UiLiveSummary | null>(null);
  const [openPositions, setOpenPositions] = useState<UiOpenPosition[]>([]);
  const [recentClosed, setRecentClosed] = useState<UiRecentClosedPosition[]>([]);
  const [slots, setSlots] = useState<UiSlotRow[]>([]);
  const [health, setHealth] = useState<UiHealthResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [actionBusy, setActionBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadLive = useCallback(async () => {
    setError(null);
    setLoading(true);
    try {
      const [summaryRes, openRes, closedRes] = await Promise.all([
        getUiLiveSummary(),
        getUiOpenPositions(),
        getUiRecentClosed(10),
      ]);

      if (summaryRes.error || openRes.error || closedRes.error) {
        throw new Error(summaryRes.error || openRes.error || closedRes.error || "Unknown API error");
      }

      setSummary(summaryRes);
      setOpenPositions(openRes.items);
      setRecentClosed(closedRes.items);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setLoading(false);
    }
  }, []);

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
  }, []);

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
  }, []);

  useEffect(() => {
    if (activeTab === "live") {
      void loadLive();
    } else if (activeTab === "slots") {
      void loadSlots();
    } else if (activeTab === "health") {
      void loadHealth();
    }
  }, [activeTab, loadHealth, loadLive, loadSlots]);

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

  const title = useMemo(() => {
    switch (activeTab) {
      case "slots": return "Slots";
      case "health": return "Health";
      case "advanced": return "Advanced";
      default: return "Live";
    }
  }, [activeTab]);

  const subtitle = useMemo(() => {
    if (activeTab === "advanced") {
      return "Advanced zostawiamy jako strefę cięższych rzeczy. Na tym etapie migrujemy najpierw Live, Slots i Health.";
    }
    if (activeTab === "slots") {
      return "Operator slot control: enabled, live orders, regime gating, open position, heartbeat i last event.";
    }
    if (activeTab === "health") {
      return "Zdrowie systemu: API, DB, bot heartbeats, market data freshness, orchestrator freshness i panic state.";
    }
    return "Manual refresh first. Truth-only operatorski widok oparty o panic_state, bot_control, positions, bot_heartbeat i candles.";
  }, [activeTab]);

  return (
    <AppShell
      title={title}
      subtitle={subtitle}
      activeTab={activeTab}
      onTabChange={setActiveTab}
    >
      <div className="page-grid">
        {error ? <div className="error-banner">API error: {error}</div> : null}
        {loading && activeTab === "live" && !summary ? <div className="panel">Ładowanie nowego panelu Live…</div> : null}
        {loading && activeTab === "slots" && slots.length === 0 ? <div className="panel">Ładowanie Slots…</div> : null}
        {loading && activeTab === "health" && !health ? <div className="panel">Ładowanie Health…</div> : null}

        {activeTab === "live" ? (
          <>
            <TopStatusBar summary={summary} />
            <QuickActionsPanel summary={summary} onRefresh={loadLive} onTogglePanic={handleTogglePanic} actionBusy={actionBusy} />
            <OpenPositionsTable items={openPositions} />
            <RecentClosedTable items={recentClosed} />
          </>
        ) : null}

        {activeTab === "slots" ? (
          <>
            <SlotActionsPanel
              items={slots}
              actionBusy={actionBusy}
              onRefresh={loadSlots}
              onUpdateSlot={handleSlotUpdate}
              onUpdateRegime={handleRegimeUpdate}
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

        {activeTab === "advanced" ? (
          <section className="panel advanced-placeholder">
            <div className="panel-header">
              <h2>Advanced</h2>
              <span className="panel-meta">Next step</span>
            </div>
            <p>
              Tu przeniesiemy ORC, watchdog i cięższe debug/analysis views po dokończeniu operatorskiego v1.
            </p>
          </section>
        ) : null}
      </div>
    </AppShell>
  );
}

export default App;
