import { useMemo, useState } from "react";
import type { UiAccountSummary, UiEquityHistoryResponse } from "../../api";

interface AccountSnapshotPanelProps {
  account: UiAccountSummary | null;
  equity: UiEquityHistoryResponse | null;
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

function formatChange(value: number | null | undefined, pct: number | null | undefined) {
  if (value === null || value === undefined || pct === null || pct === undefined) return "—";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)} (${sign}${pct.toFixed(2)}%)`;
}

export function AccountSnapshotPanel({ account, equity }: AccountSnapshotPanelProps) {
  const [equityRange, setEquityRange] = useState<"7D" | "30D" | "ALL">("30D");
  const quoteAsset = account?.quote_asset || "USDC";
  const assetOrder = [quoteAsset, "BTC", "ETH", "BNB", "SOL"];
  const isExchangeTruth = account?.calculation_method === "LIVE_EXCHANGE_BALANCES_MARKED_TO_USDC";
  const accountLabel = isExchangeTruth
    ? "Total account value"
    : account?.account_value_status?.startsWith("RECONSTRUCTED_COMPLETE")
      ? "Reconstructed account value"
      : account?.account_value_status?.startsWith("RECONSTRUCTED_PARTIAL")
        ? "Partial reconstructed estimate"
        : account?.account_value_status === "CANONICAL"
          ? "Canonical account value"
          : "Unavailable";
  const equityItems = useMemo(() => {
    if (!equity) return [];
    if (equityRange === "ALL") return equity.items;
    const days = equityRange === "7D" ? 7 : 30;
    const cutoff = new Date();
    cutoff.setUTCDate(cutoff.getUTCDate() - days);
    cutoff.setUTCHours(0, 0, 0, 0);
    return equity.items.filter((item) => new Date(`${item.snapshot_date}T00:00:00Z`) >= cutoff);
  }, [equity, equityRange]);
  const chartPoints = equityItems.filter((item) => item.waltrade_managed_equity_usdc !== null);
  const values = chartPoints.map((item) => item.waltrade_managed_equity_usdc as number);
  const min = values.length ? Math.min(...values) : 0;
  const max = values.length ? Math.max(...values) : 0;
  const span = Math.max(max - min, 0.000001);
  const polyline = chartPoints.map((item, index) => {
    const x = chartPoints.length === 1 ? 50 : (index / (chartPoints.length - 1)) * 100;
    const y = 38 - (((item.waltrade_managed_equity_usdc as number) - min) / span) * 34;
    return `${x},${y}`;
  }).join(" ");
  const latest = equity?.items.at(-1);

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
            : `Resolved: ${account?.resolved_outcome_count ?? 0}/${account?.closed_positions_count ?? 0} · High assurance: ${account?.high_assurance_count ?? 0} · Legacy compatible: ${account?.legacy_compatible_count ?? 0} · Unresolved: ${account?.unresolved_outcome_count ?? 0}`}
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

      <div className="equity-section">
        <div className="equity-section-header">
          <div>
            <h3>WalTrade Equity</h3>
            <span className="status-meta">{latest?.evidence_status ?? "No snapshot yet"}</span>
          </div>
          <div className="equity-range" aria-label="Equity chart range">
            {(["7D", "30D", "ALL"] as const).map((range) => (
              <button key={range} type="button" className={equityRange === range ? "active" : ""} onClick={() => setEquityRange(range)}>{range}</button>
            ))}
          </div>
        </div>
        <div className="equity-metrics-grid">
          <span><small>Current WalTrade Equity</small><strong>{formatUsd(equity?.metrics.current_waltrade_equity)}</strong></span>
          <span><small>Total Account Value</small><strong>{formatUsd(equity?.metrics.current_account_total)}</strong></span>
          <span><small>External / Manual Value</small><strong>{formatUsd(latest?.external_manual_value_usdc)}</strong></span>
          <span><small>7D</small><strong>{formatChange(equity?.metrics.change_7d_abs, equity?.metrics.change_7d_pct)}</strong></span>
          <span><small>30D</small><strong>{formatChange(equity?.metrics.change_30d_abs, equity?.metrics.change_30d_pct)}</strong></span>
          <span><small>This Month</small><strong>{formatChange(equity?.metrics.month_change_abs, equity?.metrics.month_change_pct)}</strong></span>
          <span><small>Current Drawdown</small><strong>{equity?.metrics.drawdown_from_peak_pct == null ? "—" : `${equity.metrics.drawdown_from_peak_pct.toFixed(2)}%`}</strong></span>
          <span><small>Since Baseline</small><strong>{formatChange(equity?.metrics.since_baseline_abs, equity?.metrics.since_baseline_pct)}</strong><small>{equity?.metrics.baseline_date ?? "No complete baseline"}</small></span>
        </div>
        <div className="equity-chart" aria-label="WalTrade Equity over time">
          <span className="status-label">WalTrade Equity over time</span>
          {chartPoints.length < 2 ? (
            <div className="equity-chart-empty">Not enough history yet</div>
          ) : (
            <svg viewBox="0 0 100 42" preserveAspectRatio="none" role="img">
              <polyline points={polyline} fill="none" vectorEffect="non-scaling-stroke" />
            </svg>
          )}
        </div>
      </div>
    </section>
  );
}
