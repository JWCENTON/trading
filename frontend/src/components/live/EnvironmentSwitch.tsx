import type { UiEnvironment } from "../../api";

interface EnvironmentSwitchProps {
  environment: UiEnvironment;
  onChange: (env: UiEnvironment) => void;
}

export function EnvironmentSwitch({ environment, onChange }: EnvironmentSwitchProps) {
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
          onClick={() => onChange("LIVE")}
        >
          <span className="env-button-title">LIVE</span>
          <span className="env-button-meta">API :8001</span>
        </button>

        <button
          type="button"
          className={`env-button ${environment === "PAPER" ? "active" : ""}`}
          onClick={() => onChange("PAPER")}
        >
          <span className="env-button-title">PAPER</span>
          <span className="env-button-meta">API :8000</span>
        </button>
      </div>
    </section>
  );
}
