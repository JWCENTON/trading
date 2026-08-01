from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import json
from typing import Any, Mapping


ZERO = Decimal("0")


class HistoricalExitIntentClassification(str, Enum):
    NO_EXIT_INTENTS = "NO_EXIT_INTENTS"
    BENIGN_UNFILLED_LEGACY_EXIT_INTENTS = (
        "BENIGN_UNFILLED_LEGACY_EXIT_INTENTS"
    )
    EXECUTED_OR_AMBIGUOUS_EXIT_EVIDENCE = (
        "EXECUTED_OR_AMBIGUOUS_EXIT_EVIDENCE"
    )


def _safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): _safe(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (tuple, list)):
        return [_safe(item) for item in value]
    return value


def _hash(value: Any) -> str:
    encoded = json.dumps(
        _safe(value), sort_keys=True, separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class HistoricalExitIntentGate:
    classification: HistoricalExitIntentClassification
    retirement_allowed: bool
    reason: str | None
    source_table: str
    derived_allowed_status: str
    intent_count: int
    first_intent_id: int | None
    last_intent_id: int | None
    first_created_at: datetime | None
    last_created_at: datetime | None
    filled_intent_count: int
    fill_count: int
    filled_quantity: Decimal
    inventory_reduction_quantity: Decimal
    intent_lifecycle_count: int
    terminal_lifecycle_count: int
    terminal_financial_truth_count: int
    external_identity_count: int
    conflicting_position_ids: tuple[int, ...]
    duplicate_identity_conflict_count: int
    status_distribution: tuple[tuple[str, int], ...]
    quantity_distribution: tuple[tuple[str, int], ...]
    intent_identity_hash: str
    quantity_distribution_hash: str
    intent_ids: tuple[int, ...]
    fill_ids: tuple[int, ...]

    def public_payload(self) -> Mapping[str, Any]:
        return _safe({
            "classification": self.classification.value,
            "retirement_allowed": self.retirement_allowed,
            "reason": self.reason,
            "source_table": self.source_table,
            "derived_allowed_status": self.derived_allowed_status,
            "intent_count": self.intent_count,
            "first_intent_id": self.first_intent_id,
            "last_intent_id": self.last_intent_id,
            "first_created_at": self.first_created_at,
            "last_created_at": self.last_created_at,
            "filled_intent_count": self.filled_intent_count,
            "fill_count": self.fill_count,
            "filled_quantity": self.filled_quantity,
            "inventory_reduction_quantity": self.inventory_reduction_quantity,
            "intent_lifecycle_count": self.intent_lifecycle_count,
            "terminal_lifecycle_count": self.terminal_lifecycle_count,
            "terminal_financial_truth_count": (
                self.terminal_financial_truth_count
            ),
            "external_identity_count": self.external_identity_count,
            "conflicting_position_ids": self.conflicting_position_ids,
            "duplicate_identity_conflict_count": (
                self.duplicate_identity_conflict_count
            ),
            "status_distribution": dict(self.status_distribution),
            "quantity_distribution": dict(self.quantity_distribution),
            "intent_identity_hash": self.intent_identity_hash,
            "quantity_distribution_hash": self.quantity_distribution_hash,
        })

    def fingerprint_payload(self) -> Mapping[str, Any]:
        return self.public_payload()


class HistoricalExitIntentGateRepository:
    SOURCE_TABLE = "simulated_orders"
    ALLOWED_DERIVED_STATUS = "PAPER_SIMULATED_UNFILLED_INTENT"

    @staticmethod
    def _rows(cur, position: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
        cur.execute(
            """
            SELECT so.id,so.created_at,so.symbol,so."interval" AS interval,
                   so.strategy,so.side,so.price,so.quantity_btc,so.reason,
                   so.candle_open_time,so.is_exit,
                   COALESCE(f.fill_count,0) AS fill_count,
                   COALESCE(f.filled_quantity,0) AS filled_quantity,
                   COALESCE(f.fill_ids,ARRAY[]::bigint[]) AS fill_ids,
                   COALESCE(f.fill_position_ids,ARRAY[]::bigint[])
                     AS fill_position_ids,
                   COALESCE(l.lifecycle_count,0) AS lifecycle_count
            FROM simulated_orders so
            LEFT JOIN LATERAL (
              SELECT count(*) AS fill_count,
                     COALESCE(sum(sf.fill_qty),0) AS filled_quantity,
                     array_agg(sf.id ORDER BY sf.id) AS fill_ids,
                     array_agg(DISTINCT sf.position_id)
                       FILTER (WHERE sf.position_id IS NOT NULL)
                       AS fill_position_ids
              FROM simulated_execution_fills_v1 sf
              WHERE sf.simulated_order_id=so.id
            ) f ON true
            LEFT JOIN LATERAL (
              SELECT count(*) AS lifecycle_count
              FROM position_lifecycle_events_c2_2 event
              WHERE event.order_id=so.id::text
            ) l ON true
            WHERE upper(so.symbol)=upper(%s) AND so."interval"=%s
              AND so.strategy=%s AND so.created_at>=%s
              AND (so.is_exit OR upper(so.side)='SELL')
            ORDER BY so.id
            """,
            (
                position["symbol"], position["interval"],
                position["strategy"], position["entry_time"],
            ),
        )
        names = tuple(item[0] for item in cur.description)
        return tuple(dict(zip(names, row)) for row in cur.fetchall())

    @staticmethod
    def _conflicting_positions(
        cur,
        *,
        position: Mapping[str, Any],
        rows: tuple[Mapping[str, Any], ...],
    ) -> tuple[int, ...]:
        if not rows or position.get("qty") is None:
            return ()
        first = min(row["created_at"] for row in rows)
        last = max(row["created_at"] for row in rows)
        cur.execute(
            """
            SELECT id,entry_time,exit_time
            FROM positions
            WHERE id<>%s AND upper(symbol)=upper(%s)
              AND "interval"=%s AND strategy=%s AND qty=%s
              AND entry_time<=%s AND (exit_time IS NULL OR exit_time>=%s)
            ORDER BY id
            """,
            (
                int(position["id"]), position["symbol"], position["interval"],
                position["strategy"], position["qty"], last, first,
            ),
        )
        conflicts = []
        for other_id, entry_time, exit_time in cur.fetchall():
            if any(
                entry_time <= row["created_at"]
                and (exit_time is None or row["created_at"] <= exit_time)
                and Decimal(str(row["quantity_btc"]))
                == Decimal(str(position["qty"]))
                for row in rows
            ):
                conflicts.append(int(other_id))
        return tuple(conflicts)

    @classmethod
    def classify(
        cls,
        cur,
        *,
        position: Mapping[str, Any],
        resolved_exit_order_count: int,
        source_conflict_count: int,
        position_exit_fill_count: int,
        terminal_lifecycle_count: int,
        terminal_financial_truth_count: int,
    ) -> HistoricalExitIntentGate:
        rows = cls._rows(cur, position)
        conflicts = cls._conflicting_positions(
            cur, position=position, rows=rows
        )
        intent_ids = tuple(int(row["id"]) for row in rows)
        fill_ids = tuple(
            sorted({
                int(fill_id)
                for row in rows for fill_id in (row["fill_ids"] or ())
            })
        )
        filled_intent_count = sum(
            1 for row in rows
            if int(row["fill_count"]) > 0
            or Decimal(str(row["filled_quantity"])) > ZERO
        )
        fill_count = sum(int(row["fill_count"]) for row in rows)
        filled_quantity = sum(
            (Decimal(str(row["filled_quantity"])) for row in rows), ZERO
        )
        intent_lifecycle_count = sum(
            int(row["lifecycle_count"]) for row in rows
        )
        inventory_reduction = max(
            Decimal(str(position.get("cumulative_exit_executed_qty") or 0)),
            Decimal(str(position.get("exit_inventory_reduction_qty") or 0)),
        )
        external_identity_count = (
            int(position.get("exit_order_id") is not None)
            + int(position.get("exit_client_order_id") is not None)
            + int(resolved_exit_order_count)
        )

        status_counts: Counter[str] = Counter()
        quantity_counts: Counter[str] = Counter()
        natural_identities: dict[tuple[Any, ...], str] = {}
        duplicate_conflicts = 0
        wrong_fill_position = False
        invalid_side_or_purpose = False
        invalid_value = False
        quantity_mismatch = False
        quantity_oversize = False
        position_qty = (
            Decimal(str(position["qty"]))
            if position.get("qty") is not None else ZERO
        )
        identity_rows = []
        for row in rows:
            quantity = Decimal(str(row["quantity_btc"]))
            price = Decimal(str(row["price"]))
            quantity_counts[format(quantity, "f")] += 1
            wrong_fill_position = wrong_fill_position or any(
                int(fill_position_id) != int(position["id"])
                for fill_position_id in (row["fill_position_ids"] or ())
            )
            valid_shape = (
                str(row["side"]).upper() == "SELL"
                and bool(row["is_exit"])
            )
            invalid_side_or_purpose = invalid_side_or_purpose or not valid_shape
            invalid_value = invalid_value or quantity <= ZERO or price <= ZERO
            quantity_mismatch = quantity_mismatch or quantity != position_qty
            quantity_oversize = quantity_oversize or quantity > position_qty
            if int(row["fill_count"]) > 0 or Decimal(
                str(row["filled_quantity"])
            ) > ZERO:
                derived_status = "SIMULATED_INTENT_EXECUTED_OR_FILLED"
            elif not valid_shape or quantity <= ZERO or price <= ZERO:
                derived_status = "SIMULATED_INTENT_AMBIGUOUS"
            else:
                derived_status = cls.ALLOWED_DERIVED_STATUS
            status_counts[derived_status] += 1
            natural_key = (
                str(row["symbol"]).upper(), str(row["interval"]),
                str(row["strategy"]), row["candle_open_time"],
                bool(row["is_exit"]),
            )
            content_hash = _hash({
                key: row[key] for key in (
                    "id", "created_at", "symbol", "interval", "strategy",
                    "side", "price", "quantity_btc", "reason",
                    "candle_open_time", "is_exit", "fill_count",
                    "filled_quantity", "fill_ids", "fill_position_ids",
                    "lifecycle_count",
                )
            })
            prior = natural_identities.get(natural_key)
            if prior is not None and prior != content_hash:
                duplicate_conflicts += 1
            natural_identities[natural_key] = content_hash
            identity_rows.append((int(row["id"]), content_hash))

        reasons = []
        if str(position.get("status") or "").upper() != "OPEN":
            reasons.append("POSITION_NOT_OPEN")
        if source_conflict_count:
            reasons.append("EXIT_SOURCE_CONFLICT")
        if position_exit_fill_count or fill_count or filled_quantity > ZERO:
            reasons.append("EXIT_FILL_EXISTS")
        if wrong_fill_position:
            reasons.append("EXIT_FILL_POSITION_LINKAGE_CONFLICT")
        if inventory_reduction > ZERO:
            reasons.append("INVENTORY_REDUCTION_EXISTS")
        if intent_lifecycle_count or terminal_lifecycle_count:
            reasons.append("TERMINAL_OR_LINKED_EXIT_LIFECYCLE_EXISTS")
        if terminal_financial_truth_count:
            reasons.append("TERMINAL_FINANCIAL_TRUTH_EXISTS")
        if external_identity_count:
            reasons.append("EXTERNAL_OR_AUTHORITATIVE_EXIT_IDENTITY")
        if invalid_side_or_purpose:
            reasons.append("UNKNOWN_INTENT_STATUS")
        if invalid_value:
            reasons.append("INVALID_EXIT_INTENT_VALUE")
        if quantity_oversize:
            reasons.append("EXIT_INTENT_QUANTITY_EXCEEDS_INVENTORY")
        elif quantity_mismatch:
            reasons.append("EXIT_INTENT_QUANTITY_MISMATCH")
        if conflicts:
            reasons.append("EXIT_INTENT_POSITION_LINKAGE_CONFLICT")
        if duplicate_conflicts:
            reasons.append("DUPLICATE_EXIT_INTENT_IDENTITY_CONFLICT")

        if reasons:
            classification = (
                HistoricalExitIntentClassification
                .EXECUTED_OR_AMBIGUOUS_EXIT_EVIDENCE
            )
            allowed = False
            reason = reasons[0]
        elif rows:
            classification = (
                HistoricalExitIntentClassification
                .BENIGN_UNFILLED_LEGACY_EXIT_INTENTS
            )
            allowed = True
            reason = None
        else:
            classification = HistoricalExitIntentClassification.NO_EXIT_INTENTS
            allowed = True
            reason = None

        quantity_distribution = tuple(sorted(quantity_counts.items()))
        status_distribution = tuple(sorted(status_counts.items()))
        return HistoricalExitIntentGate(
            classification=classification,
            retirement_allowed=allowed,
            reason=reason,
            source_table=cls.SOURCE_TABLE,
            derived_allowed_status=cls.ALLOWED_DERIVED_STATUS,
            intent_count=len(rows),
            first_intent_id=intent_ids[0] if intent_ids else None,
            last_intent_id=intent_ids[-1] if intent_ids else None,
            first_created_at=rows[0]["created_at"] if rows else None,
            last_created_at=rows[-1]["created_at"] if rows else None,
            filled_intent_count=filled_intent_count,
            fill_count=fill_count,
            filled_quantity=filled_quantity,
            inventory_reduction_quantity=inventory_reduction,
            intent_lifecycle_count=intent_lifecycle_count,
            terminal_lifecycle_count=int(terminal_lifecycle_count),
            terminal_financial_truth_count=int(terminal_financial_truth_count),
            external_identity_count=external_identity_count,
            conflicting_position_ids=conflicts,
            duplicate_identity_conflict_count=duplicate_conflicts,
            status_distribution=status_distribution,
            quantity_distribution=quantity_distribution,
            intent_identity_hash=_hash(identity_rows),
            quantity_distribution_hash=_hash(quantity_distribution),
            intent_ids=intent_ids,
            fill_ids=fill_ids,
        )
