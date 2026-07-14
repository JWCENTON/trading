from __future__ import annotations


def normalize_exchange_source(value) -> str:
    """Return the canonical exchange identity used by orders and fills."""
    return str(value or "").strip().lower()
