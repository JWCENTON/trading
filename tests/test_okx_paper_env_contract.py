from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_EXAMPLE = REPO_ROOT / ".env.okx.paper.example"


def _read_env_example() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def test_local_paper_uses_canonical_okx_environment() -> None:
    env = _read_env_example()

    assert env["ENV_FILE"] == ".env.okx.paper"
    assert env["ENV_FILE"] != ".env.paper"
    assert env["EXCHANGE"] == "OKX"
    assert env["EXCHANGE"] != ""
    assert env["TRADING_MODE"] == "PAPER"
    assert env["OKX_TESTNET"] == "false"
    assert env["LIVE_ORDERS_ENABLED"] == "0"
    assert env["OKX_EXECUTION_ENABLED"] == "0"
    assert env["DB_NAME"] == "trading_paper"
    assert env["DB_HOST"] == "db"
