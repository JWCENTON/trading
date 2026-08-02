from __future__ import annotations

from dataclasses import replace

import pytest

import common.legacy_fill_equivalence_proof as proof_module
from common.legacy_fill_equivalence_proof import APPLY_ENABLE_ENV
from common.local_live_legacy_residual_repair import resolve_correction_trust
from tests.postgres.database_baseline_fixture import disposable_postgres_v16
from tests.postgres.test_legacy_fill_equivalence_proof_v1_pg import (
    FakeExchange,
    MIGRATION,
    _execute,
    _seed,
    _service,
)


def _ingestion_rows(cur):
    cur.execute(
        "SELECT ingestion_id,correction_revision,source_fingerprint,"
        "applied_fingerprint,applied_at,application_status,adoption_id,"
        "contract_generation,local_fill_id "
        "FROM exchange_fill_ingestion_state_v2 "
        "WHERE ingestion_id=ANY(%s) ORDER BY ingestion_id",
        ([8, 10, 12, 14, 16, 18, 19, 20],),
    )
    names = [column[0] for column in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def test_residual_writer_accepts_only_valid_fresh_equivalence_proof(
    disposable_postgres_v16, monkeypatch,
):
    database = "waltrade_baseline_test_residual_equivalence_v1"
    disposable_postgres_v16.create_database(database)
    pg = replace(disposable_postgres_v16, database=database)
    monkeypatch.setattr(proof_module, "EXPECTED_DATABASE", database)
    evidence, manifest = _seed(pg)
    exchange = FakeExchange(evidence)
    proof_service = _service(pg, exchange, manifest)

    connection = pg.connect()
    try:
        with connection.cursor() as cur:
            with pytest.raises(RuntimeError, match="BLOCKED_BY_MISSING"):
                resolve_correction_trust(cur, _ingestion_rows(cur))
        connection.rollback()
    finally:
        connection.close()

    _execute(pg, MIGRATION)
    monkeypatch.setenv(APPLY_ENABLE_ENV, "1")
    proof_service.apply(
        apply_requested=True, environment="LIVE", deployment_id="local-live",
        database=database, manifest_path="proof-manifest.json",
    )
    connection = pg.connect()
    try:
        with connection.cursor() as cur:
            trust_source, proofs = resolve_correction_trust(
                cur, _ingestion_rows(cur),
            )
        connection.rollback()
    finally:
        connection.close()
    assert trust_source == "LEGACY_EQUIVALENCE_PROOF"
    assert set(proofs) == {8, 10, 12, 14, 16, 18, 19, 20}

    _execute(
        pg, "UPDATE exchange_fill_ingestion_state_v2 SET correction_revision=3 "
        "WHERE ingestion_id=8",
    )
    connection = pg.connect()
    try:
        with connection.cursor() as cur:
            with pytest.raises(RuntimeError, match="BLOCKED_BY_STALE"):
                resolve_correction_trust(cur, _ingestion_rows(cur))
        connection.rollback()
    finally:
        connection.close()
