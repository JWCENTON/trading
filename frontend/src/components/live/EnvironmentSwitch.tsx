import type { UiEnvironment } from "../../api";

interface EnvironmentSwitchProps {
  environment: UiEnvironment;
}

function isMobileDevice(): boolean {
  if (typeof window === "undefined") return false;
  return /Android|iPhone|iPad|iPod|Mobile/i.test(window.navigator.userAgent);
}

function isLocalHost(hostname: string): boolean {
  return (
    hostname === "localhost" ||
    hostname === "127.0.0.1" ||
    hostname === "::1" ||
    hostname.endsWith(".local") ||
    /^192\.168\.\d{1,3}\.\d{1,3}$/.test(hostname) ||
    /^10\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(hostname) ||
    /^172\.(1[6-9]|2\d|3[0-1])\.\d{1,3}\.\d{1,3}$/.test(hostname)
  );
}

function getEnvironmentUrl(env: UiEnvironment): string {
  if (typeof window === "undefined") return "#";

  const { protocol, hostname, port } = window.location;

  if (isLocalHost(hostname)) {
    const localBase = port ? `${protocol}//${hostname}:${port}` : `${protocol}//${hostname}`;
    return localBase;
  }

  const isPaperHost = hostname.startsWith("paper.");
  const baseHost = isPaperHost ? hostname.slice("paper.".length) : hostname;

  const targetHost = env === "LIVE" ? baseHost : `paper.${baseHost}`;

  return `${protocol}//${targetHost}`;
}

export function EnvironmentSwitch({ environment }: EnvironmentSwitchProps) {
  const openEnvironment = (target: UiEnvironment) => {
    if (typeof window === "undefined") return;
    if (target === environment) return;

    const url = getEnvironmentUrl(target);

    // Desktop: nowa karta
    // Mobile: to samo okno
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
        <span className="panel-meta">One UI • two runtimes</span>
      </div>

      <div className="environment-switch">
        <button
          type="button"
          className={`env-button ${environment === "LIVE" ? "active" : ""}`}
          onClick={() => openEnvironment("LIVE")}
          aria-pressed={environment === "LIVE"}
        >
          <span className="env-button-title">LIVE</span>
          <span className="env-button-meta">
            {environment === "LIVE" ? "Current host" : "Open live"}
          </span>
        </button>

        <button
          type="button"
          className={`env-button ${environment === "PAPER" ? "active" : ""}`}
          onClick={() => openEnvironment("PAPER")}
          aria-pressed={environment === "PAPER"}
        >
          <span className="env-button-title">PAPER</span>
          <span className="env-button-meta">
            {environment === "PAPER" ? "Current host" : "Open paper"}
          </span>
        </button>
      </div>
    </section>
  );
}