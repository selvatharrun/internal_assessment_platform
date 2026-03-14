from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

import yaml
from sqlalchemy.engine import URL, make_url


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str
    collection_name: str

    @classmethod
    def from_sources(cls, secrets_path: str | Path = "secrets.yaml") -> "PostgresSettings":
        secrets = {}
        secrets_file = Path(secrets_path)
        if secrets_file.exists():
            with open(secrets_file, "r", encoding="utf-8") as handle:
                secrets = yaml.safe_load(handle) or {}

        def pick(name: str, default: str = "") -> str:
            return str(os.getenv(name) or secrets.get(name) or default)

        connection_url = pick("POSTGRES_URL") or pick("DATABASE_URL")
        if connection_url:
            parsed = make_url(connection_url)
            sslmode_value = parsed.query.get("sslmode", "prefer")
            if isinstance(sslmode_value, (list, tuple)):
                sslmode_value = sslmode_value[0] if sslmode_value else "prefer"

            return cls(
                host=str(parsed.host or "localhost"),
                port=int(parsed.port or 5432),
                database=str(parsed.database or "postgres"),
                user=str(parsed.username or "postgres"),
                password=str(parsed.password or ""),
                sslmode=str(sslmode_value or "prefer"),
                collection_name=pick("POSTGRES_COLLECTION", "opencodereasoning_qa"),
            )

        return cls(
            host=pick("POSTGRES_HOST", "localhost"),
            port=int(pick("POSTGRES_PORT", "5432")),
            database=pick("POSTGRES_DB", "rag_db"),
            user=pick("POSTGRES_USER", "postgres"),
            password=pick("POSTGRES_PASSWORD", "postgres"),
            sslmode=pick("POSTGRES_SSLMODE", "prefer"),
            collection_name=pick("POSTGRES_COLLECTION", "opencodereasoning_qa"),
        )

    def sqlalchemy_url(self) -> str:
        return URL.create(
            drivername="postgresql+psycopg",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query={"sslmode": self.sslmode},
        ).render_as_string(hide_password=False)
