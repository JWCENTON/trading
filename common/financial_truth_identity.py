from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import threading
from typing import Callable


IDENTITY_VERSION = "OKX_ACCOUNT_CONFIG_V1"


def _canonical_hash(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ExchangeAccountIdentity:
    exchange: str
    uid: str
    main_uid: str
    scope: str
    source: str
    version: str
    captured_at: datetime
    fingerprint: str


def okx_account_identity(data: dict, *, captured_at: datetime | None = None) -> ExchangeAccountIdentity:
    rows = data.get("data") if isinstance(data, dict) else None
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        raise ValueError("ACCOUNT_IDENTITY_INVALID_RESPONSE: empty data")
    uid = str(rows[0].get("uid") or "").strip()
    main_uid = str(rows[0].get("mainUid") or "").strip()
    if not uid:
        raise ValueError("ACCOUNT_IDENTITY_INVALID_RESPONSE: missing uid")
    if not main_uid:
        raise ValueError("ACCOUNT_IDENTITY_INVALID_RESPONSE: missing mainUid")
    scope = "MAIN" if uid == main_uid else "SUB_ACCOUNT"
    payload = {
        "exchange": "OKX",
        "uid": uid,
        "main_uid": main_uid,
        "scope": scope,
        "source": "OKX_ACCOUNT_CONFIG",
        "version": IDENTITY_VERSION,
    }
    return ExchangeAccountIdentity(
        exchange="OKX",
        uid=uid,
        main_uid=main_uid,
        scope=scope,
        source="OKX_ACCOUNT_CONFIG",
        version=IDENTITY_VERSION,
        captured_at=captured_at or datetime.now(timezone.utc),
        fingerprint=_canonical_hash(payload),
    )


class AccountIdentityCache:
    def __init__(self, ttl: timedelta = timedelta(hours=24)):
        self.ttl = ttl
        self._values: dict[str, ExchangeAccountIdentity] = {}
        self._lock = threading.Lock()

    def get(
        self,
        credential_scope: str,
        fetch: Callable[[], ExchangeAccountIdentity],
        *,
        now: datetime | None = None,
        refresh: bool = False,
    ) -> tuple[ExchangeAccountIdentity, str]:
        current = now or datetime.now(timezone.utc)
        with self._lock:
            cached = self._values.get(credential_scope)
            if (
                cached is not None
                and not refresh
                and current - cached.captured_at < self.ttl
            ):
                return cached, "ACCOUNT_IDENTITY_CACHE_HIT"
        try:
            value = fetch()
        except Exception:
            with self._lock:
                self._values.pop(credential_scope, None)
            raise
        with self._lock:
            self._values[credential_scope] = value
        return value, (
            "ACCOUNT_IDENTITY_CACHE_REFRESH" if cached is not None
            else "ACCOUNT_IDENTITY_FETCH_OK"
        )
