from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, localcontext
from enum import Enum
from functools import wraps
import hashlib
import json
from typing import Iterable

from common.exchange_symbols import resolve_canonical_instrument
from common.inventory_quantity import (
    ExitInventoryClassification,
    ExitInventoryStatus,
)


CALCULATION_VERSION = "FINANCIAL_TRUTH_CALCULATION_V3"
ARITHMETIC_CONTRACT_VERSION = "FINANCIAL_TRUTH_ARITHMETIC_V1"
PRECISION_CONTRACT_VERSION = "FINANCIAL_TRUTH_DECIMAL_PRECISION_V1"
DECIMAL_CONTEXT_PRECISION = 120
DECIMAL_CONTEXT_ROUNDING = ROUND_HALF_UP
ALLOCATION_RATIO_SCALE = 20
ALLOCATION_RATIO_QUANTUM = Decimal("1e-20")


def _canonical_decimal_context(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        with localcontext() as context:
            context.prec = DECIMAL_CONTEXT_PRECISION
            context.rounding = DECIMAL_CONTEXT_ROUNDING
            return function(*args, **kwargs)
    return wrapped


@_canonical_decimal_context
def canonical_allocation_ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    """PostgreSQL NUMERIC-compatible V1 ratio with an explicit scale boundary."""
    if denominator <= 0 or numerator <= 0:
        return Decimal("0")
    if numerator >= denominator:
        return Decimal("1")
    return (numerator / denominator).quantize(
        ALLOCATION_RATIO_QUANTUM,
        rounding=DECIMAL_CONTEXT_ROUNDING,
    )


class NonCanonicalFinancialTruthIssue(str, Enum):
    POSITION_LIFECYCLE_NOT_CLOSED = "POSITION_LIFECYCLE_NOT_CLOSED"


def is_noncanonical_financial_truth_issue(value: object) -> bool:
    return any(value == issue.value for issue in NonCanonicalFinancialTruthIssue)


def _d(value) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _json_value(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value


@dataclass(frozen=True)
class FillEvidence:
    fill_id: str
    order_id: str
    position_id: int
    purpose: str
    side: str
    symbol: str
    quantity: Decimal
    price: Decimal
    notional: Decimal
    fee_quantity: Decimal | None
    fee_asset: str | None
    authoritative_fee_usdc: Decimal | None
    estimated_fee_usdc: Decimal | None
    event_time: datetime
    source_authority: str
    source_exchange: str
    source_environment: str
    source_deployment_id: str
    account_identity_fingerprint: str | None
    instrument_metadata_fingerprint: str | None
    step_size: Decimal | None
    base_asset: str | None
    quote_asset: str | None
    source_version: str


@dataclass(frozen=True)
class FinancialTruthCalculation:
    position_id: int
    position_status: str
    financial_truth_status: str
    gross_entry_qty: Decimal | None
    gross_exit_qty: Decimal | None
    base_asset_entry_fee_qty: Decimal | None
    base_asset_exit_fee_qty: Decimal | None
    net_entry_inventory_qty: Decimal | None
    net_exit_inventory_reduction_qty: Decimal | None
    gross_remaining_execution_qty: Decimal | None
    remaining_inventory_qty: Decimal | None
    authoritative_entry_notional: Decimal | None
    authoritative_exit_notional: Decimal | None
    authoritative_entry_fees_usdc: Decimal | None
    authoritative_exit_fees_usdc: Decimal | None
    authoritative_fees_usdc: Decimal | None
    authoritative_gross_pnl: Decimal | None
    authoritative_net_pnl: Decimal | None
    estimated_gross_pnl: Decimal | None
    estimated_fees_usdc: Decimal | None
    estimated_net_pnl: Decimal | None
    entry_fill_count: int
    exit_fill_count: int
    first_entry_fill_at: datetime | None
    last_entry_fill_at: datetime | None
    first_exit_fill_at: datetime | None
    last_exit_fill_at: datetime | None
    source_authority: str | None
    source_exchange: str | None
    source_environment: str | None
    source_deployment_id: str | None
    source_account_identity_fingerprint: str | None
    source_order_ids: tuple[str, ...]
    source_fill_ids: tuple[str, ...]
    source_fingerprint: str
    calculation_version: str
    failure_code: str | None
    failure_detail: str | None

    @property
    def arithmetic_contract_version(self) -> str:
        return ARITHMETIC_CONTRACT_VERSION

    @property
    def precision_contract_version(self) -> str:
        return PRECISION_CONTRACT_VERSION

    def semantic_values(self) -> dict:
        values = {
            key: _json_value(value)
            for key, value in asdict(self).items()
            if key not in {"position_id"}
        }
        values["arithmetic_contract_version"] = self.arithmetic_contract_version
        values["precision_contract_version"] = self.precision_contract_version
        return values


def source_fingerprint(fills: Iterable[FillEvidence]) -> str:
    payload = []
    for fill in sorted(fills, key=lambda item: (item.event_time, item.fill_id)):
        payload.append(
            {
                key: _json_value(value)
                for key, value in asdict(fill).items()
            }
        )
    raw = json.dumps({
        "arithmetic_contract_version": ARITHMETIC_CONTRACT_VERSION,
        "precision_contract_version": PRECISION_CONTRACT_VERSION,
        "fills": payload,
    }, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def classify_fee_asset_role(
    fee_asset: str | None,
    base_asset: str | None,
    quote_asset: str | None,
) -> str:
    fee = str(fee_asset).strip().upper() if fee_asset else None
    base = str(base_asset).strip().upper() if base_asset else None
    quote = str(quote_asset).strip().upper() if quote_asset else None
    if not fee or fee in {"NONE", "NULL", "UNKNOWN", "N/A"}:
        return "UNKNOWN"
    if base and fee == base:
        return "BASE"
    if quote and fee == quote:
        return "QUOTE"
    if base and quote:
        return "THIRD_ASSET"
    return "UNKNOWN"


@dataclass(frozen=True)
class FeeAssetRoleResolution:
    fee_asset_role: str
    fee_asset_role_source: str
    instrument_resolution_status: str
    instrument_resolution_source: str
    base_asset: str | None
    quote_asset: str | None


def resolve_fee_asset_role(
    *,
    fee_asset: str | None,
    symbol: str | None,
    base_asset: str | None,
    quote_asset: str | None,
) -> FeeAssetRoleResolution:
    fee = str(fee_asset).strip().upper() if fee_asset else None
    stored_base = str(base_asset).strip().upper() if base_asset else None
    stored_quote = str(quote_asset).strip().upper() if quote_asset else None
    canonical = resolve_canonical_instrument(symbol)

    if bool(stored_base) != bool(stored_quote):
        return FeeAssetRoleResolution(
            "UNKNOWN", "UNKNOWN", "CONFLICT",
            "STORED_INSTRUMENT_SNAPSHOT", None, None,
        )
    if stored_base and stored_quote:
        if (
            canonical.status == "RESOLVED"
            and (
                canonical.base_asset != stored_base
                or canonical.quote_asset != stored_quote
            )
        ):
            return FeeAssetRoleResolution(
                "UNKNOWN", "UNKNOWN", "CONFLICT",
                "STORED_INSTRUMENT_SNAPSHOT", None, None,
            )
        role = classify_fee_asset_role(fee, stored_base, stored_quote)
        return FeeAssetRoleResolution(
            role,
            "STORED_INSTRUMENT_SNAPSHOT" if role != "UNKNOWN" else "UNKNOWN",
            "RESOLVED", "STORED_INSTRUMENT_SNAPSHOT",
            stored_base, stored_quote,
        )
    if canonical.status != "RESOLVED":
        return FeeAssetRoleResolution(
            "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", None, None,
        )
    role = classify_fee_asset_role(
        fee, canonical.base_asset, canonical.quote_asset
    )
    return FeeAssetRoleResolution(
        role,
        "CANONICAL_SYMBOL_RESOLUTION" if role != "UNKNOWN" else "UNKNOWN",
        "RESOLVED", "CANONICAL_SYMBOL_RESOLUTION",
        canonical.base_asset, canonical.quote_asset,
    )


@_canonical_decimal_context
def calculate_financial_truth(
    *,
    position_id: int,
    position_status: str,
    fills: Iterable[FillEvidence],
    estimated_gross_pnl: Decimal | None = None,
    estimated_fees_usdc: Decimal | None = None,
    estimated_net_pnl: Decimal | None = None,
    position_symbol: str | None = None,
    inventory_classification: ExitInventoryClassification | None = None,
) -> FinancialTruthCalculation:
    evidence = tuple(fills)
    fingerprint = source_fingerprint(evidence)
    if inventory_classification is not None:
        inventory_payload = {
            "fill_fingerprint": fingerprint,
            "status": inventory_classification.status.value,
            "remaining_inventory_qty": _json_value(
                inventory_classification.remaining_inventory_qty
            ),
            "executable_inventory_qty": _json_value(
                inventory_classification.executable_inventory_qty
            ),
            "dust_qty": _json_value(inventory_classification.dust_qty),
            "terminal_reason": inventory_classification.terminal_reason,
        }
        fingerprint = hashlib.sha256(
            json.dumps(
                inventory_payload, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()
    if not evidence:
        return FinancialTruthCalculation(
            position_id, position_status, "UNKNOWN",
            None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None,
            _d(estimated_gross_pnl), _d(estimated_fees_usdc),
            _d(estimated_net_pnl), 0, 0, None, None, None, None,
            None, None, None, None, None, (), (), fingerprint,
            CALCULATION_VERSION, None, None,
        )

    def failed(code: str, detail: str) -> FinancialTruthCalculation:
        first = evidence[0]
        return FinancialTruthCalculation(
            position_id, position_status, "FAILED",
            None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None,
            _d(estimated_gross_pnl), _d(estimated_fees_usdc),
            _d(estimated_net_pnl), 0, 0, None, None, None, None,
            first.source_authority, first.source_exchange,
            first.source_environment, first.source_deployment_id, None,
            tuple(sorted({f.order_id for f in evidence})),
            tuple(sorted({f.fill_id for f in evidence})), fingerprint,
            CALCULATION_VERSION, code, detail,
        )

    if any(fill.position_id != position_id for fill in evidence):
        return failed("POSITION_LINKAGE_CONFLICT", "fill linked to another position")
    identities = {f.account_identity_fingerprint for f in evidence if f.account_identity_fingerprint}
    if len(identities) > 1:
        return failed("ACCOUNT_IDENTITY_CONFLICT", "verified UID fingerprints differ")
    authorities = {f.source_authority for f in evidence}
    exchanges = {f.source_exchange for f in evidence}
    environments = {f.source_environment for f in evidence}
    deployments = {f.source_deployment_id for f in evidence}
    symbols = {f.symbol for f in evidence}
    if any(len(values) > 1 for values in (authorities, exchanges, environments, deployments, symbols)):
        return failed("SOURCE_PROVENANCE_CONFLICT", "source provenance is inconsistent")
    if (
        position_symbol is not None
        and {str(position_symbol).strip().upper()} != {
            str(value).strip().upper() for value in symbols
        }
    ):
        return failed(
            "SOURCE_PROVENANCE_CONFLICT",
            "position and fill symbols are inconsistent",
        )
    if any(f.quantity <= 0 or f.price < 0 or f.notional < 0 for f in evidence):
        return failed("INVALID_EXECUTION_VALUE", "negative or zero execution value")

    entries = tuple(f for f in evidence if f.purpose == "ENTRY")
    exits = tuple(f for f in evidence if f.purpose == "EXIT")
    if len(entries) + len(exits) != len(evidence):
        return failed("INVALID_ORDER_PURPOSE", "purpose must be ENTRY or EXIT")

    gross_entry = sum((f.quantity for f in entries), Decimal("0"))
    gross_exit = sum((f.quantity for f in exits), Decimal("0"))
    fee_role_resolutions = tuple(
        resolve_fee_asset_role(
            fee_asset=f.fee_asset, symbol=f.symbol,
            base_asset=f.base_asset, quote_asset=f.quote_asset,
        )
        for f in evidence
    )
    entry_base_fee = sum(
        (f.fee_quantity or Decimal("0"))
        for f, resolution in zip(evidence, fee_role_resolutions)
        if f.purpose == "ENTRY" and resolution.fee_asset_role == "BASE"
    )
    exit_base_fee = sum(
        (f.fee_quantity or Decimal("0"))
        for f, resolution in zip(evidence, fee_role_resolutions)
        if f.purpose == "EXIT" and resolution.fee_asset_role == "BASE"
    )
    net_entry = gross_entry - entry_base_fee
    net_exit_reduction = gross_exit + exit_base_fee
    gross_remaining = gross_entry - gross_exit
    raw_remaining_inventory = net_entry - net_exit_reduction
    canonical_terminal = bool(
        inventory_classification is not None
        and inventory_classification.status in {
            ExitInventoryStatus.FULLY_EXECUTED_CLOSE,
            ExitInventoryStatus.TERMINAL_DUST_CLOSE,
        }
    )
    remaining_inventory = (
        inventory_classification.remaining_inventory_qty
        if inventory_classification is not None
        else raw_remaining_inventory
    )
    quantity_conflict = min(net_entry, gross_remaining) < 0 or (
        raw_remaining_inventory < 0 and not canonical_terminal
    )
    lifecycle_conflict = (
        bool(exits) and str(position_status).upper() != "CLOSED"
    )
    if quantity_conflict and not lifecycle_conflict:
        return failed(
            "EXIT_QUANTITY_EXCEEDS_ENTRY", "exit inventory exceeds entry"
        )

    entry_notional = sum((f.notional for f in entries), Decimal("0")) if entries else None
    exit_notional = sum((f.notional for f in exits), Decimal("0")) if exits else None
    entry_fee_values = [f.authoritative_fee_usdc for f in entries]
    exit_fee_values = [f.authoritative_fee_usdc for f in exits]
    entry_fees = (
        sum((value for value in entry_fee_values if value is not None), Decimal("0"))
        if entries and all(value is not None for value in entry_fee_values) else None
    )
    exit_fees = (
        sum((value for value in exit_fee_values if value is not None), Decimal("0"))
        if exits and all(value is not None for value in exit_fee_values) else None
    )
    total_fees = (
        entry_fees + exit_fees
        if entry_fees is not None and exit_fees is not None else None
    )
    estimated_fee_values = [
        f.estimated_fee_usdc for f in evidence if f.estimated_fee_usdc is not None
    ]
    estimated_fees = (
        sum(estimated_fee_values, Decimal("0"))
        if estimated_fee_values else _d(estimated_fees_usdc)
    )

    gross_pnl = None
    net_pnl = None
    if gross_entry > 0 and gross_exit > 0 and entry_notional is not None:
        exited_ratio = canonical_allocation_ratio(gross_exit, gross_entry)
        allocated_entry_notional = entry_notional * exited_ratio
        gross_pnl = exit_notional - allocated_entry_notional
        inventory_exit_ratio = canonical_allocation_ratio(
            net_exit_reduction, net_entry,
        )
        allocated_entry_fees = (
            entry_fees * inventory_exit_ratio
            if entry_fees is not None else None
        )
        if allocated_entry_fees is not None and exit_fees is not None:
            net_pnl = gross_pnl - allocated_entry_fees - exit_fees

    missing = []
    if not entries:
        missing.append("MISSING_ENTRY_FILLS")
    if not identities:
        missing.append("MISSING_ACCOUNT_PROVENANCE")
    if any(f.instrument_metadata_fingerprint is None for f in evidence):
        if remaining_inventory != 0:
            missing.append("MISSING_INSTRUMENT_SNAPSHOT")
    if any(f.authoritative_fee_usdc is None for f in evidence):
        missing.append("MISSING_AUTHORITATIVE_FEE")
    fee_roles = tuple(
        resolution.fee_asset_role for resolution in fee_role_resolutions
    )
    if any(role == "UNKNOWN" for role in fee_roles):
        missing.append("FEE_ASSET_ROLE_UNKNOWN")
    if any(
        (
            f.base_asset is None
            or f.quote_asset is None
            or f.instrument_metadata_fingerprint is None
        )
        and resolution.instrument_resolution_source
        != "CANONICAL_SYMBOL_RESOLUTION"
        for f, resolution in zip(evidence, fee_role_resolutions)
    ):
        missing.append("MISSING_INSTRUMENT_METADATA")
    if any(
        f.authoritative_fee_usdc is None and f.estimated_fee_usdc is not None
        for f in evidence
    ):
        missing.append("FEE_VALUATION_ESTIMATED")
    if any(role == "THIRD_ASSET" for role in fee_roles):
        missing.append("THIRD_ASSET_FEE_ESTIMATED")
    if exit_base_fee:
        missing.append("BASE_EXIT_FEE_SEMANTICS_UNSUPPORTED")
    if not exits:
        missing.append("MISSING_EXIT_FILLS")
    elif lifecycle_conflict:
        missing.insert(
            0,
            NonCanonicalFinancialTruthIssue.POSITION_LIFECYCLE_NOT_CLOSED.value,
        )
    if quantity_conflict:
        missing.append("EXIT_QUANTITY_EXCEEDS_ENTRY")
    steps = {f.step_size for f in evidence if f.step_size is not None}
    if len(steps) > 1:
        return failed("INSTRUMENT_METADATA_CONFLICT", "step size snapshots differ")
    tolerance = next(iter(steps)) if steps else None
    inventory_complete = canonical_terminal or remaining_inventory == 0 or (
        tolerance is not None and remaining_inventory <= tolerance
    )
    if not inventory_complete:
        missing.append("REMAINING_INVENTORY")
    if net_pnl is None:
        missing.append("MISSING_AUTHORITATIVE_NET_PNL")

    status = "COMPLETE" if not missing else "INCOMPLETE"
    first = evidence[0]
    times_entry = [f.event_time for f in entries]
    times_exit = [f.event_time for f in exits]
    return FinancialTruthCalculation(
        position_id, position_status, status,
        gross_entry, gross_exit, entry_base_fee, exit_base_fee,
        net_entry, net_exit_reduction, gross_remaining, remaining_inventory,
        entry_notional, exit_notional, entry_fees, exit_fees, total_fees,
        gross_pnl, net_pnl, _d(estimated_gross_pnl), estimated_fees,
        _d(estimated_net_pnl), len(entries), len(exits),
        min(times_entry) if times_entry else None,
        max(times_entry) if times_entry else None,
        min(times_exit) if times_exit else None,
        max(times_exit) if times_exit else None,
        first.source_authority, first.source_exchange,
        first.source_environment, first.source_deployment_id,
        next(iter(identities)) if identities else None,
        tuple(sorted({f.order_id for f in evidence})),
        tuple(sorted({f.fill_id for f in evidence})), fingerprint,
        CALCULATION_VERSION, missing[0] if missing else None,
        ",".join(missing) if missing else None,
    )
