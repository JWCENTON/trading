import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "db/migrations/20260724_canonical_promotions_schema_parity_v1.sql"
)
API = ROOT / "api/main.py"


def _normalized_sql() -> str:
    return re.sub(r"\s+", " ", MIGRATION.read_text().lower()).strip()


def test_migration_is_additive_idempotent_and_environment_independent():
    sql = _normalized_sql()
    assert sql.startswith("begin;")
    assert sql.endswith("commit;")
    assert sql.count("create table if not exists") == 3
    assert "add column if not exists" in sql
    assert "create index if not exists" in sql
    assert "create unique index if not exists" in sql
    for destructive in ("drop table", "drop column", "truncate", "delete from"):
        assert destructive not in sql
    for cross_environment in (
        "live-api",
        "paper-api",
        "trading_live",
        "dblink",
        "postgres_fdw",
    ):
        assert cross_environment not in sql


def test_legacy_window_is_renamed_fail_closed_without_data_copy():
    sql = MIGRATION.read_text()
    assert (
        'ALTER TABLE public.promoted_candidates RENAME COLUMN "window" TO window_name'
        in sql
    )
    assert (
        'ALTER TABLE public.promotion_events RENAME COLUMN "window" TO window_name'
        in sql
    )
    assert sql.count("refusing ambiguous migration") == 2
    assert "UPDATE " not in sql.upper()


def test_canonical_tables_cover_every_endpoint_column():
    sql = _normalized_sql()
    required = {
        "promoted_candidates": {
            "symbol",
            "interval",
            "strategy",
            "paper_score",
            "n_trades",
            "win_rate",
            "net_sum",
            "window_name",
            "policy_version",
            "source_ts",
            "published_at",
            "meta",
            "eligible_live",
            "elig_reason",
        },
        "promotion_events": {
            "id",
            "created_at",
            "source_ts",
            "window_name",
            "policy_version",
            "n_rows",
            "hash",
            "meta",
        },
        "promoted_regime_candidates": {
            "symbol",
            "interval",
            "strategy",
            "market_regime",
            "paper_score",
            "n_trades",
            "win_rate",
            "net_sum",
            "profit_factor",
            "fee_pressure_pct",
            "window_name",
            "policy_version",
            "source_ts",
            "published_at",
            "meta",
            "eligible_live",
            "elig_reason",
        },
    }
    for table, columns in required.items():
        definition = sql.split(
            f"create table if not exists public.{table} (", 1
        )[1].split(");", 1)[0]
        for column in columns:
            assert re.search(rf"\b{re.escape(column)}\b", definition)


def test_keys_constraints_and_indexes_match_endpoint_operations():
    sql = _normalized_sql()
    assert "primary key (symbol, interval, strategy)" in sql
    assert (
        "primary key (symbol, interval, strategy, market_regime)"
        in sql
    )
    assert "ux_promotion_events_hash" in sql
    assert "promoted_candidates_elig_consistency" in sql
    assert "n_trades > 0" in sql
    assert "policy_version is not null" in sql
    expected_indexes = {
        "ix_promoted_candidates_published_at",
        "ix_promoted_candidates_score",
        "ix_promotion_events_created_at",
        "ix_promoted_regime_candidates_published_at",
        "ix_promoted_regime_candidates_score",
        "ix_promoted_regime_candidates_lookup",
    }
    assert expected_indexes <= set(
        re.findall(r"create (?:unique )?index if not exists (\w+)", sql)
    )


def test_current_api_sql_uses_the_canonical_contract_without_payload_change():
    source = API.read_text()
    assert '@app.post("/internal/promotions/upsert")' in source
    assert '@app.post("/internal/regime-promotions/upsert")' in source
    assert "ON CONFLICT (symbol, interval, strategy)" in source
    assert (
        "ON CONFLICT (symbol, interval, strategy, market_regime)"
        in source
    )
    assert "SELECT 1 FROM promotion_events WHERE hash=%s LIMIT 1" in source
    assert (
        "INSERT INTO promotion_events "
        "(source_ts, window_name, policy_version, n_rows, hash, meta)"
        in re.sub(r"\s+", " ", source)
    )
