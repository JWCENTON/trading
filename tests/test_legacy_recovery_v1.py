from datetime import datetime, timezone
from decimal import Decimal

from common.legacy_recovery import (
    FeeValuationStatus,
    FillApplicationProof,
    IngestionApplicationStatus,
    LegacyFillEvidence,
    LegacyPositionEvidence,
    LegacyPositionRecomputationService,
    LegacyRecoveryPlanner,
    PrecisionPolicy,
    PrecisionStatus,
    ProvenanceSource,
    RecomputationStatus,
    canonical_semantic_bytes,
    classify_fill_application,
    resolve_provenance,
    semantic_repair_fingerprint,
    semantic_repair_state,
    value_fee,
)


D = Decimal


def policy() -> PrecisionPolicy:
    return PrecisionPolicy(D("0.000001"), 8, 9, D("0.000000001"), "OKX_LOT_SZ")


def fill(
    *, fill_id="1", side="BUY", qty="0.035152", fee="0.000123032",
    fee_asset="BNB", price="566.1",
):
    valuation = value_fee(
        quantity=D(fee), asset=fee_asset, base_asset="BNB",
        quote_asset="USDC", fill_price=D(price),
    )
    return LegacyFillEvidence(
        fill_id, f"order-{fill_id}", f"trade-{fill_id}", side,
        D(qty), D(price), D(fee), fee_asset, valuation,
    )


def evidence(entry, exit_, precision=policy()):
    return LegacyPositionEvidence(
        3080, "BNBUSDC", "BNB", "USDC", (entry,), (exit_,), precision,
    )


def test_fee_aware_inventory_normalizes_3080_shape_closed():
    result = LegacyPositionRecomputationService().recompute(
        evidence(
            fill(),
            fill(
                fill_id="2", side="SELL", qty="0.035029",
                fee="0.06952731065", fee_asset="USDC", price="567.1",
            ),
        )
    )
    assert result.raw_remaining_qty == D("-0.000000032")
    assert result.normalized_remaining_qty == 0
    assert result.precision_status is PrecisionStatus.OVER_EXIT_WITHIN_PRECISION
    assert result.recomputation_status is RecomputationStatus.COMPLETE_CLOSED
    assert result.financial_truth_eligibility


def test_real_residual_remains_open():
    result = LegacyPositionRecomputationService().recompute(
        evidence(
            fill(qty="1", fee="0", fee_asset="USDC"),
            fill(fill_id="2", side="SELL", qty="0.9", fee="0", fee_asset="USDC"),
        )
    )
    assert result.normalized_remaining_qty == D("0.1")
    assert result.precision_status is PrecisionStatus.REAL_REMAINING_INVENTORY
    assert result.recomputation_status is RecomputationStatus.COMPLETE_OPEN


def test_over_exit_within_precision_retains_raw_audit_delta():
    result = LegacyPositionRecomputationService().recompute(
        evidence(
            fill(qty="1", fee="0", fee_asset="USDC"),
            fill(
                fill_id="2", side="SELL", qty="1.00000003",
                fee="0", fee_asset="USDC",
            ),
        )
    )
    assert result.raw_remaining_qty == D("-0.00000003")
    assert result.normalized_remaining_qty == 0
    assert result.normalization_reason == "WITHIN_FORMAL_INSTRUMENT_PRECISION"


def test_over_exit_outside_precision_is_conflict():
    result = LegacyPositionRecomputationService().recompute(
        evidence(
            fill(qty="1", fee="0", fee_asset="USDC"),
            fill(fill_id="2", side="SELL", qty="1.01", fee="0", fee_asset="USDC"),
        )
    )
    assert result.precision_status is PrecisionStatus.OVER_EXIT_CONFLICT
    assert result.recomputation_status is RecomputationStatus.CONFLICT
    assert not result.financial_truth_eligibility


def test_missing_base_fee_valuation_is_incomplete():
    entry = fill()
    unknown = entry.fee_valuation.__class__(
        entry.fee_quantity, "BNB", None, None, None, FeeValuationStatus.UNKNOWN,
    )
    entry = entry.__class__(
        entry.fill_id, entry.order_id, entry.trade_id, entry.side,
        entry.quantity, entry.price, entry.fee_quantity, entry.fee_asset, unknown,
    )
    result = LegacyPositionRecomputationService().recompute(
        evidence(
            entry,
            fill(fill_id="2", side="SELL", qty="0.035029", fee="0", fee_asset="USDC"),
        )
    )
    assert result.recomputation_status is RecomputationStatus.INCOMPLETE
    assert "MISSING_AUTHORITATIVE_FEE_VALUATION" in result.blocking_reasons


def test_quote_and_base_fee_valuation_preserve_provenance():
    quote = value_fee(
        quantity=D("0.07"), asset="USDC", base_asset="BNB",
        quote_asset="USDC", fill_price=D("500"),
    )
    base = value_fee(
        quantity=D("0.001"), asset="BNB", base_asset="BNB",
        quote_asset="USDC", fill_price=D("500"),
    )
    assert quote.status is FeeValuationStatus.AUTHORITATIVE_QUOTE_FEE
    assert base.status is FeeValuationStatus.AUTHORITATIVE_BASE_FEE_WITH_FILL_PRICE
    assert base.valued_fee_usdc == D("0.5")
    assert base.valuation_source == "SAME_FILL_EXECUTION_PRICE"


def proof(*, local="fill:1", applied="abc", semantic="abc"):
    return FillApplicationProof(
        "okx", "trade", "order", semantic, local, applied,
        datetime.now(timezone.utc) if applied else None,
    )


def test_duplicate_requires_complete_application_proof():
    assert classify_fill_application(
        observed_fingerprint="abc", proof=proof(),
    ) is IngestionApplicationStatus.TRUE_DUPLICATE_APPLIED
    assert classify_fill_application(
        observed_fingerprint="abc", proof=proof(local=None, applied=None),
    ) is IngestionApplicationStatus.OBSERVED_NOT_APPLIED
    assert classify_fill_application(
        observed_fingerprint="different", proof=proof(),
    ) is IngestionApplicationStatus.IDEMPOTENCY_CONFLICT


def test_semantic_fingerprint_excludes_operational_fields_and_tracks_economics():
    first = {
        "fill": {"qty": D("1.00"), "fee": D("0.01")},
        "last_seen_at": "first", "attempt_count": 1,
        "nested": {"last_polled_at": "first", "status": "OPEN"},
    }
    later = {
        **first, "last_seen_at": "later", "attempt_count": 99,
        "nested": {"last_polled_at": "later", "status": "OPEN"},
    }
    assert semantic_repair_fingerprint(semantic_repair_state(first)) == (
        semantic_repair_fingerprint(semantic_repair_state(later))
    )
    assert semantic_repair_fingerprint(
        semantic_repair_state({**first, "fill": {"qty": D("1.01"), "fee": D("0.01")}})
    ) != semantic_repair_fingerprint(semantic_repair_state(first))
    assert semantic_repair_fingerprint(
        semantic_repair_state({**first, "fill": {"qty": D("1"), "fee": D("0.02")}})
    ) != semantic_repair_fingerprint(semantic_repair_state(first))
    assert canonical_semantic_bytes({"value": None}).endswith(b"\n")
    assert b'"value":null' in canonical_semantic_bytes({"value": None})


def test_provenance_priority_and_read_only_planner_contract():
    source, payload = resolve_provenance({
        ProvenanceSource.CANONICAL_SYMBOL_RESOLVER: {"base": "BNB"},
        ProvenanceSource.EXCHANGE_PAYLOAD: {"instId": "BNB-USDC"},
    })
    assert source is ProvenanceSource.EXCHANGE_PAYLOAD
    assert payload == {"instId": "BNB-USDC"}
    result = LegacyPositionRecomputationService().recompute(
        evidence(
            fill(),
            fill(
                fill_id="2", side="SELL", qty="0.035029",
                fee="0.06952731065", fee_asset="USDC", price="567.1",
            ),
        )
    )
    plan = LegacyRecoveryPlanner().position_plan(result)
    assert plan.eligible_actions == LegacyRecoveryPlanner.POSITION_ACTIONS
    assert plan.semantic_fingerprint == result.evidence_fingerprint
    assert "second_execution=NO_OP" in plan.post_state_invariants
