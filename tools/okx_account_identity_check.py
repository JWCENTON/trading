from __future__ import annotations

import argparse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import os

from common.exchange_client import get_market_data_client


def _masked(value: str) -> str:
    value = str(value or "")
    return "MASKED:" + ("*" * max(0, min(len(value) - 4, 8))) + value[-4:]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only OKX account identity diagnostic"
    )
    parser.add_argument("--environment", choices=("paper", "live"), required=True)
    parser.add_argument("--masked-output", action="store_true", required=True)
    parser.add_argument("--probe-balance-first", action="store_true")
    args = parser.parse_args(argv)
    configured = os.environ.get("ENVIRONMENT", "").strip().lower()
    if configured != args.environment:
        raise SystemExit(
            f"CONFIGURATION_ERROR: expected environment={args.environment}"
        )
    if os.environ.get("EXCHANGE", "").strip().upper() != "OKX":
        raise SystemExit("CONFIGURATION_ERROR: EXCHANGE must be OKX")

    client = get_market_data_client()
    balance_meta = None
    if args.probe_balance_first:
        response = client._private_request("GET", "/api/v5/account/balance")
        if str(response.get("code")) != "0":
            raise SystemExit("BALANCE_DIAGNOSTIC_FAILED")
        balance_meta = dict(client._last_private_request_diagnostic)
    first, first_status = client.get_account_identity(refresh=True)
    config_meta = dict(client._last_private_request_diagnostic)
    second, second_status = client.get_account_identity()
    refreshed, refresh_status = client.get_account_identity(refresh=True)
    if not (
        first.fingerprint == second.fingerprint == refreshed.fingerprint
        and first.uid == refreshed.uid
        and first.main_uid == refreshed.main_uid
    ):
        raise SystemExit("ACCOUNT_IDENTITY_UNSTABLE")
    print("endpoint_access=PASS")
    print(f"scope={first.scope}")
    print(f"uid={_masked(first.uid)}")
    print(f"mainUid={_masked(first.main_uid)}")
    print(f"first_lookup={first_status}")
    print(f"second_lookup={second_status}")
    print(f"forced_refresh={refresh_status}")
    print("identity_fingerprint_deterministic=PASS")
    if balance_meta:
        print("balance_request_auth=PASS")
        print(f"balance_http_status={balance_meta.get('http_status')}")
        print(f"balance_okx_code={balance_meta.get('okx_code')}")
        print(f"balance_request_path={balance_meta.get('request_path')}")
    print(f"config_http_status={config_meta.get('http_status')}")
    print(f"config_okx_code={config_meta.get('okx_code')}")
    print(f"config_request_path={config_meta.get('request_path')}")
    print(f"base_url={config_meta.get('base_url')}")
    print(f"method={config_meta.get('method')}")
    print(f"body_length={config_meta.get('body_length')}")
    print(f"signature_length={config_meta.get('signature_length')}")
    print("signer=" + str(config_meta.get("signer")))
    print("http_client=" + str(config_meta.get("http_client")))
    print("demo_header=" + str(config_meta.get("demo_header")))
    required_headers = {
        "Content-Type", "OK-ACCESS-KEY", "OK-ACCESS-PASSPHRASE",
        "OK-ACCESS-SIGN", "OK-ACCESS-TIMESTAMP",
    }
    print(
        "required_headers_present="
        + str(required_headers.issubset(set(config_meta.get("header_names") or ())))
    )
    server_date = config_meta.get("server_date")
    if server_date:
        server_time = parsedate_to_datetime(server_date).astimezone(timezone.utc)
        skew = abs((datetime.now(timezone.utc) - server_time).total_seconds())
        print(f"clock_skew_seconds={skew:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
