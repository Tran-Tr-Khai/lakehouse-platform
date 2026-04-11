"""
query_layer/catalog.py — Gold layer view definitions.

Each Gold Delta table is exposed as a named DuckDB VIEW.
Views are created in the superset_views.duckdb catalog file by the setup script
and are also available in FastAPI in-memory connections via setup_views().

Design decisions:
  - dim_product view filters WHERE is_current = true and excludes the internal
    _attr_hash column (SCD2 implementation detail, not for consumption).
  - Fact and mart views expose all columns without filtering — date filtering
    is expected to happen at query time (Superset dashboards, API params).
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from query_layer.config import settings

# ---------------------------------------------------------------------------
# View definitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ViewDef:
    """A named SQL view pointing to a Gold Delta table."""

    name: str
    sql: str
    description: str


def _build_views(gold: str) -> list[_ViewDef]:
    """Returns all Gold view definitions for the given base path."""
    return [
        _ViewDef(
            name="dim_product",
            description=(
                "Current product dimension (SCD2). "
                "One row per active (asin, marketplace). "
                "Internal _attr_hash column excluded."
            ),
            sql=f"""
                SELECT
                    product_sk,
                    asin,
                    marketplace,
                    title,
                    brand,
                    category_path,
                    category_leaf,
                    category_depth,
                    bsr_primary_category,
                    date_first_available,
                    variation_count,
                    size_count,
                    color_count,
                    main_image_url,
                    is_fba,
                    effective_date,
                    expiry_date,
                    is_current,
                    gold_processed_at
                FROM delta_scan('{gold}/dim_product/')
                WHERE is_current = true
            """,
        ),
        _ViewDef(
            name="fact_search_ranking",
            description=(
                "One row per (asin, keyword, marketplace, ingested_at). "
                "Search card impression: price, rating, badges, coupon per keyword."
            ),
            sql=f"SELECT * FROM delta_scan('{gold}/fact_search_ranking/')",
        ),
        _ViewDef(
            name="fact_price_snapshot",
            description=(
                "One row per (asin, marketplace, ingested_at). Canonical daily price: min/max/avg/latest + coupon."
            ),
            sql=f"SELECT * FROM delta_scan('{gold}/fact_price_snapshot/')",
        ),
        _ViewDef(
            name="fact_product_performance",
            description=(
                "One row per (asin, marketplace, ingested_at). "
                "BSR ranks, rating breakdown (pct per star), stock, buybox, FBA."
            ),
            sql=f"SELECT * FROM delta_scan('{gold}/fact_product_performance/')",
        ),
        _ViewDef(
            name="mart_keyword_daily",
            description=(
                "One row per (keyword, marketplace, ingested_at). "
                "Pre-aggregated: product count, badge %, price stats, avg rating."
            ),
            sql=f"SELECT * FROM delta_scan('{gold}/mart_keyword_daily/')",
        ),
        _ViewDef(
            name="mart_brand_competitive",
            description=(
                "One row per (brand, category_leaf, marketplace, ingested_at). "
                "Brand benchmarks: BSR, in-stock rate, FBA %, variation breadth."
            ),
            sql=f"SELECT * FROM delta_scan('{gold}/mart_brand_competitive/')",
        ),
    ]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def setup_views(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Creates or replaces all Gold views in `con`.

    Safe to call on both catalog file connections (setup script) and
    in-memory connections (FastAPI startup). Returns created view names.
    """
    views = _build_views(settings.gold_base_path)
    created: list[str] = []
    for v in views:
        con.execute(f"CREATE OR REPLACE VIEW {v.name} AS {v.sql}")
        created.append(v.name)
    return created


def list_views(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Returns the names of all views currently visible in `con`."""
    rows = con.execute("SELECT table_name FROM information_schema.views ORDER BY table_name").fetchall()
    return [r[0] for r in rows]


def describe_views() -> list[_ViewDef]:
    """Returns all view definitions (for documentation or diagnostics)."""
    return _build_views(settings.gold_base_path)
