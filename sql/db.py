from __future__ import annotations

import hashlib
import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

SQL_DIR = Path(__file__).parent


def connect(dsn: str | None = None) -> psycopg.Connection:
    if dsn or os.getenv("DATABASE_URL"):
        return psycopg.connect(dsn or os.environ["DATABASE_URL"], row_factory=dict_row)
    settings = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB", "easyapply"),
        "user": os.getenv("POSTGRES_USER", "easyapply"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
        "connect_timeout": 10,
    }
    return psycopg.connect(**settings, row_factory=dict_row)


def initialize(conn: psycopg.Connection) -> None:
    """Apply versioned SQL once; reject edits to an already-applied migration."""
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(70627001)")
        conn.execute("CREATE SCHEMA IF NOT EXISTS easyapply")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS easyapply.schema_migrations (
                filename text PRIMARY KEY, checksum text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        for path in sorted(SQL_DIR.glob("[0-9][0-9][0-9]_*.sql")):
            body = path.read_bytes()
            checksum = hashlib.sha256(body).hexdigest()
            previous = conn.execute(
                "SELECT checksum FROM easyapply.schema_migrations WHERE filename = %s",
                (path.name,),
            ).fetchone()
            if previous:
                if previous["checksum"] != checksum:
                    raise ValueError(f"Applied migration changed: {path.name}; add a new migration")
                continue
            conn.execute(body.decode("utf-8"))
            conn.execute(
                "INSERT INTO easyapply.schema_migrations (filename, checksum) VALUES (%s, %s)",
                (path.name, checksum),
            )
