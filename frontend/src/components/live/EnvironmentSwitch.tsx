import type { UiEnvironment } from "../../api";

interface EnvironmentSwitchProps {
  environment: UiEnvironment;
  canSwitch?: boolean;
}

function isMobileDevice(): boolean {
  if (typeof window === "undefined") return false;
  return /Android|iPhone|iPad|iPod|Mobile/i.test(window.navigator.userAgent);
}

function getEnvironmentUrl(env: UiEnvironment): string {
  if (typeof window === "undefined") return "#";

  const protocol = window.location.protocol;
  const currentHost = window.location.hostname;

  const isPaper = currentHost.startsWith("paper-");

  let liveHost: string;
  let paperHost: string;

  if (isPaper) {
    liveHost = currentHost.replace(/^paper-/, "");
    paperHost = currentHost;
  } else {
    liveHost = currentHost;
    paperHost = `paper-${currentHost}`;
  }

  return env === "LIVE"
    ? `${protocol}//${liveHost}`
    : `${protocol}//${paperHost}`;
}

export function EnvironmentSwitch({ environment, canSwitch = true }: EnvironmentSwitchProps) {
  const openEnvironment = (target: UiEnvironment) => {
    if (typeof window === "undefined") return;
    if (target === environment) return;
    if (!canSwitch) return;

    const url = getEnvironmentUrl(target);

    if (isMobileDevice()) {
      window.location.href = url;
      return;
    }

    window.open(url, "_blank", "noopener,noreferrer");
  };

  return (
    <section className="panel quick-actions-panel">
      <div className="panel-header">
        <h2>Environment</h2>
        <span className="panel-meta">{canSwitch ? "One UI • two runtimes" : "Read-only environment"}</span>
      </div>

      <div className="environment-switch">
        <button
          type="button"
          className={`env-button ${environment === "LIVE" ? "active" : ""}`}
          onClick={() => openEnvironment("LIVE")}
          aria-pressed={environment === "LIVE"}
          disabled={!canSwitch || environment === "LIVE"}
        >
          <span className="env-button-title">LIVE</span>
          <span className="env-button-meta">
            {environment === "LIVE" ? "Current host" : canSwitch ? "Open live" : "Admin only"}
          </span>
        </button>

        <button
          type="button"
          className={`env-button ${environment === "PAPER" ? "active" : ""}`}
          onClick={() => openEnvironment("PAPER")}
          aria-pressed={environment === "PAPER"}
          disabled={!canSwitch || environment === "PAPER"}
        >
          <span className="env-button-title">PAPER</span>
          <span className="env-button-meta">
            {environment === "PAPER" ? "Current host" : canSwitch ? "Open paper" : "Admin only"}
          </span>
        </button>
      </div>
    </section>
  );
}