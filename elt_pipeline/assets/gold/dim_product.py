import dagster as dg
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ...resources.spark_resource import SparkResource

SILVER_PATH = "s3a://lakehouse/silver/amazon/product_details"
GOLD_PATH = "s3a://lakehouse/gold/amazon/dim_product"

# Attributes that trigger a new SCD2 version when changed.
# Deliberately limited to avoid version proliferation from scraper noise.
_CHANGE_COLS = [
    "title",
    "brand",
    "category_path",
    "category_leaf",
    "category_depth",
    "is_fba",
    "variation_count",
]

daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-03-04")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _add_attr_hash(df: DataFrame) -> DataFrame:
    """Adds `_attr_hash` column: SHA-256 of all SCD2-tracked attributes."""
    return df.withColumn(
        "_attr_hash",
        F.sha2(
            F.concat_ws(
                "~",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit(""))
                    for c in _CHANGE_COLS
                ],
            ),
            256,
        ),
    )

def _build_incoming(silver_df: DataFrame, partition_date: str) -> DataFrame:
    """Selects and shapes the silver data into the dim_product schema."""
    incoming = (
        silver_df
        .select(
            "asin",
            "marketplace",
            "title",
            "brand",
            "category_path",
            "category_leaf",
            "category_depth",
            F.col("best_seller_ranks").getItem(0).getField("category").alias(
                "bsr_primary_category"
            ),
            "date_first_available",
            "variation_count",
            "size_count",
            "color_count",
            "main_image_url",
            "is_fba",
        )
        .withColumn("effective_date", F.lit(partition_date).cast("date"))
        .withColumn("expiry_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
    )
    return _add_attr_hash(incoming)


def _compute_surrogate_key(df: DataFrame) -> DataFrame:
    """Attaches deterministic `product_sk` based on (asin, marketplace, effective_date)."""
    return df.withColumn(
        "product_sk",
        F.sha2(
            F.concat_ws(
                "~",
                F.col("asin"),
                F.col("marketplace"),
                F.col("effective_date").cast("string"),
            ),
            256,
        ),
    )


# ---------------------------------------------------------------------------
# SCD2 upsert
# ---------------------------------------------------------------------------


def _first_run(incoming: DataFrame) -> DataFrame:
    """Shapes incoming into a full initial snapshot with gold audit columns."""
    return (
        _compute_surrogate_key(incoming)
        .withColumn("gold_processed_at", F.current_timestamp())
    )


def _upsert(session: SparkSession, incoming: DataFrame, partition_date: str) -> None:
    """Two-step SCD2:
    1. Expire existing current rows whose tracked attributes changed.
    2. Insert new versions for changed + genuinely new products.
    """
    dim = DeltaTable.forPath(session, GOLD_PATH)

    # Step 1 — Expire rows whose attributes changed
    dim.alias("target").merge(
        incoming.alias("source"),
        "target.asin = source.asin"
        " AND target.marketplace = source.marketplace"
        " AND target.is_current = true",
    ).whenMatchedUpdate(
        condition="target._attr_hash <> source._attr_hash",
        set={
            "expiry_date": F.expr(f"date_sub('{partition_date}', 1)"),
            "is_current": F.lit(False),
        },
    ).execute()

    # Step 2 — Insert new/changed rows (skip products that are still current + unchanged)
    existing_current = (
        session.read.format("delta")
        .load(GOLD_PATH)
        .filter(F.col("is_current") == True)
        .select("asin", "marketplace")
    )

    new_rows = (
        incoming
        .join(existing_current, on=["asin", "marketplace"], how="left_anti")
        .transform(_compute_surrogate_key)
        .withColumn("gold_processed_at", F.current_timestamp())
    )

    (
        new_rows.write
        .format("delta")
        .mode("append")
        .save(GOLD_PATH)
    )


# ---------------------------------------------------------------------------
# Dagster asset
# ---------------------------------------------------------------------------


@dg.asset(
    partitions_def=daily_partitions,
    compute_kind="pyspark",
    group_name="gold",
    key_prefix=["gold", "amazon"],
    name="dim_product",
    deps=[dg.AssetKey(["silver", "amazon", "product_details"])],
    metadata={
        "description": (
            "SCD Type-2 product dimension. One row per (asin, marketplace) version. "
            "New versions are created when tracked attributes (title, brand, category, "
            "is_fba, variation_count) change. Surrogate key is a SHA-256 of "
            "(asin, marketplace, effective_date). "
            "Delta table is NOT date-partitioned; all versions coexist in a single table."
        )
    },
)
def gold_amazon_dim_product(
    context: dg.AssetExecutionContext,
    spark: SparkResource,
) -> dg.Output[None]:
    """Silver product_details → Gold dim_product (SCD2 MERGE)."""
    partition_date: str = context.partition_key
    session: SparkSession = spark.get_session()

    silver_df = (
        session.read.format("delta")
        .load(SILVER_PATH)
        .filter(F.col("ingested_at") == partition_date)
    )

    row_count = silver_df.count()
    if row_count == 0:
        context.log.warning(
            f"Silver product_details partition {partition_date} is empty — skipping."
        )
        return dg.Output(
            value=None,
            metadata={"partition_date": partition_date, "silver_row_count": 0},
        )

    context.log.info(f"Silver rows: {row_count}")

    incoming = _build_incoming(silver_df, partition_date)

    if DeltaTable.isDeltaTable(session, GOLD_PATH):
        context.log.info("dim_product exists — running SCD2 upsert")
        _upsert(session, incoming, partition_date)
    else:
        context.log.info("dim_product does not exist — writing initial snapshot")
        initial = _first_run(incoming)
        initial.write.format("delta").mode("overwrite").save(GOLD_PATH)

    total_rows = session.read.format("delta").load(GOLD_PATH).count()
    current_rows = (
        session.read.format("delta")
        .load(GOLD_PATH)
        .filter(F.col("is_current") == True)
        .count()
    )

    context.log.info(
        f"dim_product: {total_rows} total rows, {current_rows} current rows"
    )

    return dg.Output(
        value=None,
        metadata={
            "partition_date": partition_date,
            "silver_row_count": dg.MetadataValue.int(row_count),
            "dim_total_rows": dg.MetadataValue.int(total_rows),
            "dim_current_rows": dg.MetadataValue.int(current_rows),
            "gold_path": dg.MetadataValue.text(GOLD_PATH),
        },
    )
