import { useEffect, useRef, useState } from "react";
import {
  createTestUiNotification,
  getUiNotifications,
  markAllUiNotificationsRead,
  markUiNotificationRead,
  type UiNotification,
} from "../../api";

function formatNotificationTime(value: string): string {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString();
}

export function NotificationCenter() {
  const [open, setOpen] = useState(false);
  const [items, setItems] = useState<UiNotification[]>([]);
  const [unread, setUnread] = useState(0);
  const [busy, setBusy] = useState(false);
  const wrapRef = useRef<HTMLDivElement | null>(null);

  async function loadNotifications() {
    try {
      const data = await getUiNotifications(20, false);
      setItems(data.items);
      setUnread(data.unread);
    } catch {
      // Do not break the shell if notifications are unavailable.
    }
  }

  useEffect(() => {
    void loadNotifications();
    const timer = window.setInterval(() => void loadNotifications(), 30000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    function onDocumentClick(event: MouseEvent) {
      if (!wrapRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    }

    document.addEventListener("mousedown", onDocumentClick);
    return () => document.removeEventListener("mousedown", onDocumentClick);
  }, []);

  async function handleMarkRead(id: number) {
    await markUiNotificationRead(id);
    await loadNotifications();
  }

  function togglePanel() {
    setOpen((v) => !v);
  }

  async function handleMarkAllRead() {
    setBusy(true);
    try {
      await markAllUiNotificationsRead();
      await loadNotifications();
    } finally {
      setBusy(false);
    }
  }

  async function handleCreateTest() {
    setBusy(true);
    try {
      await createTestUiNotification();
      await loadNotifications();
      setOpen(true);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="notification-center" ref={wrapRef}>
      <button
        type="button"
        className={`notification-bell ${open ? "active" : ""}`}
        onClick={togglePanel}
        aria-label="Open notifications"
      >
        <span className="notification-bell-icon">🔔</span>
        {unread > 0 ? <span className="notification-badge">{unread > 99 ? "99+" : unread}</span> : null}
      </button>

      {open ? (
        <div className="notification-panel">
          <div className="notification-panel-header">
            <div>
              <div className="notification-title">Notifications</div>
              <div className="notification-subtitle">{unread} unread</div>
            </div>
            <div className="notification-actions">
              <button type="button" onClick={() => void handleCreateTest()} disabled={busy}>
                Test
              </button>
              <button type="button" onClick={() => void handleMarkAllRead()} disabled={busy || unread === 0}>
                Mark all read
              </button>
            </div>
          </div>

          <div className="notification-list">
            {items.length === 0 ? (
              <div className="notification-empty">No notifications yet.</div>
            ) : (
              items.map((item) => (
                <button
                  key={item.id}
                  type="button"
                  className={`notification-item notification-item--${item.severity} ${item.read_at ? "" : "unread"}`}
                  onClick={() => void handleMarkRead(item.id)}
                >
                  <div className="notification-item-top">
                    <span className="notification-item-title">{item.title}</span>
                    <span className="notification-item-time">{formatNotificationTime(item.created_at)}</span>
                  </div>
                  <div className="notification-item-message">{item.message}</div>
                  <div className="notification-item-source">{item.source || item.event_type}</div>
                </button>
              ))
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}
