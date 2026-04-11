#!/usr/bin/env python3
"""
scripts/setup_duckdb_catalog.py
================================
One-time (and re-runnable) setup script that prepares the DuckDB artifacts
consumed by the Superset container.

What it does
------------
  1. Creates ./duckdb_data/ — DuckDB's home directory for this project.
     Persistent secrets are stored in ./duckdb_data/.duckdb/stored_secrets/.

  2. Opens (or creates) ./superset_views.duckdb with duckdb_data as home_directory.

  3. Installs delta + httpfs DuckDB extensions (network download, idempotent).

  4. Creates a PERSISTENT S3 secret pointing to MinIO.
     The secret is stored on disk so Superset containers auto-load it on
     connection startup — no per-session credential setup required.

  5. Creates a DuckDB VIEW for every Gold layer table.
     Views use delta_scan() and read directly from MinIO at query time.

  6. Prints a verification table of all created views.

Re-run this script whenever:
  - MinIO credentials change  → recreates the persistent secret
  - A new Gold table is added → adds its view
  - superset_views.duckdb is deleted/corrupted → rebuilds it from scratch

Usage
-----
  cd /path/to/amz-market-intelligence-lakehouse-platform
  python scripts/setup_duckdb_catalog.py

Prerequisites
-------------
  - MinIO must be running and the Gold layer must have been materialised at
    least once (otherwise delta_scan() path validation will fail).
  - pip install duckdb duckdb-engine  (or uv sync after updating pyproject.toml)
"""

from __future__ import annotations

import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so query_layer is importable
# when this script is called directly (not as a module).
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from query_layer.catalog import describe_views, list_views, setup_views  # noqa: E402
from query_layer.config import settings  # noqa: E402
from query_layer.connection import open_catalog_for_setup  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GREEN = "\033[92m"
_YELLOW = "\033[93m"
_RESET = "\033[0m"
_BOLD = "\033[1m"


def _ok(msg: str) -> None:
    print(f"  {_GREEN}✓{_RESET} {msg}")


def _info(msg: str) -> None:
    print(f"  {_YELLOW}→{_RESET} {msg}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print(f"\n{_BOLD}DuckDB Catalog Setup{_RESET}")
    print("=" * 50)

    # 1. Ensure duckdb_home directory exists
    _info(f"DuckDB home   : {settings.duckdb_home}")
    _info(f"Catalog file  : {settings.catalog_path}")
    _info(f"MinIO endpoint: {settings.minio_endpoint}")
    _info(f"Gold base path: {settings.gold_base_path}")
    print()

    settings.duckdb_home.mkdir(parents=True, exist_ok=True)
    secrets_dir = settings.duckdb_home / ".duckdb" / "stored_secrets"
    secrets_dir.mkdir(parents=True, exist_ok=True)
    _ok(f"Directory ready: {settings.duckdb_home}")

    # 2. Open catalog file (creates it if not exists)
    print("\n[1/3] Opening catalog file and installing extensions...")
    con = open_catalog_for_setup()
    # open_catalog_for_setup() calls _load_extensions + _create_persistent_secret
    _ok("Extensions installed and loaded: delta, httpfs")

    # 3. Persistent secret confirmation
    print("\n[2/3] Persistent S3 secret...")
    _ok(f"Secret 'minio_s3' written to {secrets_dir}")

    # 4. Create views
    print("\n[3/3] Creating Gold layer views...")
    created = setup_views(con)
    for name in created:
        _ok(name)

    # 5. Verification
    visible = list_views(con)
    missing = set(v.name for v in describe_views()) - set(visible)
    if missing:
        print(f"\n  ⚠ Missing views: {missing}")
        sys.exit(1)

    con.close()

    # Print next steps
    print(f"\n{'=' * 50}")
    print(f"{_BOLD}Setup complete.{_RESET}\n")
    print("Files produced:")
    print(f"  {settings.catalog_path}")
    print(f"  {secrets_dir} (contains minio_s3 secret)")
    print()
    print("Next steps:")
    print("  1. Start Superset:")
    print("       docker compose up -d superset")
    print()
    print("  2. Open http://localhost:8088  (admin / admin)")
    print()
    print("  3. Settings → Database Connections → + Database → DuckDB")
    print("     SQLAlchemy URI:")
    print("       duckdb:////app/superset_views.duckdb")
    print()
    print("  4. In 'Advanced → Other → Engine Parameters' paste:")
    print(
        '       {"connect_args": {"config": {"home_directory": "/app/duckdb_data",'
        ' "autoinstall_known_extensions": true, "autoload_known_extensions": true}}}'
    )
    print()
    print("  5. Click 'Test Connection', then Save.")
    print()


if __name__ == "__main__":
    main()
