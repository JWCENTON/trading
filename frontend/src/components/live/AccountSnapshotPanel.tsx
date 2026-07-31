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
  const isExchangeTruth = account?.calculation_method === "LIVE_EXCHANGE_BALANCES_MARKED_TO_USDC";
  const accountLabel = isExchangeTruth
    ? "Total account value"
    : account?.account_value_status === "RECONSTRUCTED_COMPLETE"
      ? "Reconstructed account value"
      : account?.account_value_status === "RECONSTRUCTED_PARTIAL"
        ? "Partial reconstructed estimate"
        : account?.account_value_status === "CANONICAL"
          ? "Canonical account value"
          : "Unavailable";

  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Account snapshot</h2>
        <span className="panel-meta">{isExchangeTruth ? "Exchange truth" : accountLabel}</span>
      </div>

      <div className="account-total-card">
        <span className="status-label">{accountLabel}</span>
        <strong className="account-total-value">{formatUsd(account?.total_account_value_usdc)}</strong>
        <span className="status-meta">
          {isExchangeTruth
            ? `Tracked assets: ${assetOrder.join(" / ")}`
            : `Realized coverage: ${account?.realized_coverage_count ?? 0}/${account?.closed_positions_count ?? 0} (${(account?.realized_coverage_pct ?? 0).toFixed(2)}%)`}
        </span>
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
