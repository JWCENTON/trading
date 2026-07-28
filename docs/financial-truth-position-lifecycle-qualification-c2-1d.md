# Financial Truth position lifecycle qualification — C2.1D

Financial Truth COMPLETE requires all existing authoritative execution,
quantity, fee, provenance, identity, instrument, inventory, and PnL evidence
plus `positions.status=CLOSED`.

EXIT evidence represents an execution event. It does not prove that the
standard position lifecycle transition succeeded. EXIT evidence linked to an
OPEN position is the typed `POSITION_LIFECYCLE_NOT_CLOSED` conflict. The result
is INCOMPLETE and non-canonical: shadow and dry-run report it, while apply makes
zero canonical and canonical-audit writes.

An OPEN position with ENTRY evidence only remains naturally INCOMPLETE because
EXIT evidence is missing. A CLOSED position without EXIT evidence is also
INCOMPLETE. Existing behavior for other INCOMPLETE outcomes is unchanged.

BBRANGE operational close exceptions fail closed with `ledger_ok=false`,
`position_close_succeeded=false`, and `POSITION_CLOSE_FAILED`. PAPER successful
EXIT requires an explicit `position_close_succeeded is True`; missing data
never defaults to success.

This is a forward-only code contract. It changes no schema, migration,
historical row, backfill, strategy behavior, execution order, risk, sizing, or
LIVE ACK behavior. C2.1 is local and not deployed pending another combined
final review.
