"""
query_layer/connection.py — DuckDB connection factory.

Three public entry points:

  get_thread_connection()
      Per-thread in-memory connection for FastAPI.
      Uses a SESSION (non-persistent) S3 secret. Thread-safe via threading.local.

  open_catalog_for_setup()
      Read-write connection to superset_views.duckdb used by the setup script.
      Creates a PERSISTENT S3 secret (stored on disk in duckdb_home/.duckdb/).

  open_catalog_readonly()
      Read-only connection to superset_views.duckdb; useful for ad-hoc inspection.
      Persistent secret is auto-loaded from duckdb_home/.duckdb/stored_secrets/.
"""

from __future__ import annotations

import threading

import duckdb

from query_layer.config import settings

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_EXTENSIONS = ("delta", "httpfs")


def _load_extensions(con: duckdb.DuckDBPyConnection) -> None:
    """Installs (idempotent) and loads Delta Lake + S3 filesystem extensions."""
    for ext in _EXTENSIONS:
        con.execute(f"INSTALL {ext};")
        con.execute(f"LOAD {ext};")


def _create_session_secret(con: duckdb.DuckDBPyConnection) -> None:
    """Creates a SESSION-scoped S3 secret (lost when the connection closes)."""
    con.execute(
        f"""
        CREATE OR REPLACE SECRET minio_s3 (
            TYPE        s3,
            KEY_ID      '{settings.minio_access_key}',
            SECRET      '{settings.minio_secret_key}',
            ENDPOINT    '{settings.minio_endpoint}',
            USE_SSL     false,
            URL_STYLE   'path'
        );
        """
    )


def _create_persistent_secret(con: duckdb.DuckDBPyConnection) -> None:
    """Creates a PERSISTENT S3 secret stored under duckdb_home/.duckdb/stored_secrets/.

    The secret survives across connections — DuckDB auto-loads it from disk
    whenever a connection is opened with the same home_directory.
    """
    con.execute(
        f"""
        CREATE OR REPLACE PERSISTENT SECRET minio_s3 (
            TYPE        s3,
            KEY_ID      '{settings.minio_access_key}',
            SECRET      '{settings.minio_secret_key}',
            ENDPOINT    '{settings.minio_endpoint}',
            USE_SSL     false,
            URL_STYLE   'path'
        );
        """
    )


# ---------------------------------------------------------------------------
# FastAPI — per-thread in-memory connection
# ---------------------------------------------------------------------------

_thread_local = threading.local()


def _make_in_memory_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()  # in-memory; no file lock
    _load_extensions(con)
    _create_session_secret(con)
    return con


def get_thread_connection() -> duckdb.DuckDBPyConnection:
    """Returns a per-thread in-memory DuckDB connection ready to query MinIO.

    The connection is created on the first call per thread and reused thereafter.
    Call delta_scan() or use query_layer.catalog views on this connection.

    Example (FastAPI route):
        con = get_thread_connection()
        rows = con.execute(
            "SELECT * FROM delta_scan('s3://lakehouse/gold/amazon/mart_keyword_daily/')"
            " WHERE ingested_at = ?",
            [date_str]
        ).fetchall()
    """
    if not hasattr(_thread_local, "con"):
        _thread_local.con = _make_in_memory_connection()
    return _thread_local.con


# ---------------------------------------------------------------------------
# Superset catalog connections
# ---------------------------------------------------------------------------


def _catalog_connect(read_only: bool) -> duckdb.DuckDBPyConnection:
    """Opens superset_views.duckdb with a custom home_directory.

    Setting home_directory tells DuckDB where to find (or write) persistent
    secrets in {home_directory}/.duckdb/stored_secrets/.
    autoinstall/autoload_known_extensions ensure delta + httpfs are available
    without needing an explicit LOAD call on the Superset side.
    """
    return duckdb.connect(
        str(settings.catalog_path),
        read_only=read_only,
        config={
            "home_directory": str(settings.duckdb_home),
            "autoinstall_known_extensions": True,
            "autoload_known_extensions": True,
        },
    )


def open_catalog_for_setup() -> duckdb.DuckDBPyConnection:
    """Read-write connection to superset_views.duckdb for the setup script.

    Creates extensions and a PERSISTENT S3 secret so that subsequent
    read-only Superset connections auto-load credentials without extra config.
    """
    con = _catalog_connect(read_only=False)
    _load_extensions(con)
    _create_persistent_secret(con)
    return con


def open_catalog_readonly() -> duckdb.DuckDBPyConnection:
    """Read-only connection to superset_views.duckdb for ad-hoc inspection.

    Does NOT load extensions manually — they are auto-loaded via
    autoload_known_extensions when delta_scan() is first called by a view.
    Persistent S3 secret is auto-loaded from duckdb_home/.duckdb/stored_secrets/.
    """
    return _catalog_connect(read_only=True)
