#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from common.db import read_only_db_conn
from common.portfolio_state import read_portfolio_state


def main() -> None:
    environment = os.environ.get("TRADING_MODE", "")
    deployment_id = os.environ.get("DEPLOYMENT_ID", "")
    as_of = datetime.now(timezone.utc)
    exchange_client = None
    if environment.strip().upper() == "LIVE":
        from common.exchange_client import OkxMarketDataAdapter
        exchange_client = OkxMarketDataAdapter()
    with read_only_db_conn() as conn:
        with conn.cursor() as cur:
            state = read_portfolio_state(
                cur,
                environment=environment,
                deployment_id=deployment_id,
                as_of=as_of,
                runtime_revision=os.environ.get("GIT_SHA"),
                exchange_client=exchange_client,
            )
    print(json.dumps(state.serializable(), sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
