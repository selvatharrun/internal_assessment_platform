from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.engine import Engine

from postgres_config import PostgresSettings


def create_postgres_engine(settings: PostgresSettings) -> Engine:
    return create_engine(settings.sqlalchemy_url(), pool_pre_ping=True)


def initialize_pgvector(engine: Engine) -> None:
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    except DBAPIError as exc:
        # psycopg exposes SQLSTATE in .sqlstate for server feature errors.
        sqlstate = getattr(getattr(exc, "orig", None), "sqlstate", None)
        message = str(getattr(exc, "orig", exc)).lower()
        if sqlstate == "0A000" and "extension \"vector\" is not available" in message:
            raise RuntimeError(
                "pgvector is not installed on the PostgreSQL server. "
                "Install pgvector on the DB host, then run as a privileged user: "
                "CREATE EXTENSION IF NOT EXISTS vector; "
                "(You can verify availability with: SELECT * FROM pg_available_extensions WHERE name = 'vector';)"
            ) from exc
        raise


def test_connection(engine: Engine) -> None:
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
