"""Shared registration for the isolated PostgreSQL 16 baseline fixture."""

from tests.postgres.database_baseline_fixture import disposable_postgres_v16


__all__ = ["disposable_postgres_v16"]
