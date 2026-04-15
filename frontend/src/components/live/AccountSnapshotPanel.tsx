import type { UiAccountSummary } from "../../api";

interface AccountSnapshotPanelProps {
  account: UiAccountSummary | null;
}

function formatAsset(value: number | null | undefined, digits = 6) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  if (Math.abs(value) >= 1000) return value.toFixed(2);
  return value.toFixed(digits).replace(/\.?0+$/, "");
}

function formatUsd(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return `${value.toFixed(2)} USDC`;
}

export function AccountSnapshotPanel({ account }: AccountSnapshotPanelProps) {
  const quoteAsset = account?.quote_asset || "USDC";
  const assetOrder = [quoteAsset, "BTC", "ETH", "BNB", "SOL"];

  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Account snapshot</h2>
        <span className="panel-meta">Binance truth</span>
      </div>

      <div className="account-total-card">
        <span className="status-label">Total account value</span>
        <strong className="account-total-value">{formatUsd(account?.total_account_value_usdc)}</strong>
        <span className="status-meta">Tracked assets: {assetOrder.join(" / ")}</span>
      </div>

      <div className="account-grid">
        {assetOrder.map((asset) => (
          <article key={asset} className="info-tile account-asset-card">
            <span className="status-label">{asset}</span>
            <strong className="status-value">{formatAsset(account?.assets?.[asset])}</strong>
            <span className="status-meta">{formatUsd(account?.asset_values_usdc?.[asset])}</span>
          </article>
        ))}
      </div>
    </section>
  );
}
