import dagster as dg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ...resources.spark_resource import SparkResource

SILVER_PATH = "s3a://lakehouse/silver/amazon/search_results"
DIM_PRODUCT_PATH = "s3a://lakehouse/gold/amazon/dim_product"
GOLD_PATH = "s3a://lakehouse/gold/amazon/fact_search_ranking"

daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-03-04")


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------


def _transform(silver_df, dim_df):
    """Joins search results with current dim_product to resolve product_sk.

    Price, rating and monthly_sales are kept at this grain because they
    reflect the exact values shown on the search result card for each keyword.
    That per-keyword signal is analytically distinct from the single canonical
    daily price in fact_price_snapshot.
    """
    dim_current = (
        dim_df
        .filter(F.col("is_current") == True)
        .select("product_sk", "asin", "marketplace")
    )

    return (
        silver_df
        .join(F.broadcast(dim_current), on=["asin", "marketplace"], how="left")
        .select(
            "product_sk",
            "asin",
            "keyword",
            "marketplace",
            "ingested_at",
            # Price
            "price_amount",
            "original_price_amount",
            "discount_pct",
            # Social proof
            F.col("rating").alias("rating_value"),
            "ratings_count",
            F.col("monthly_sales").alias("monthly_sales_min"),
            # Badges
            "is_sponsored",
            "is_prime",
            "is_best_seller",
            "is_amazons_choice",
            # Coupon
            "has_coupon",
            F.col("coupon").alias("coupon_discount_pct"),
            # Audit
            "source_extracted_at",
            F.current_timestamp().alias("gold_processed_at"),
        )
    )


# ---------------------------------------------------------------------------
# Dagster asset
# ---------------------------------------------------------------------------


@dg.asset(
    partitions_def=daily_partitions,
    compute_kind="pyspark",
    group_name="gold",
    key_prefix=["gold", "amazon"],
    name="fact_search_ranking",
    deps=[
        dg.AssetKey(["silver", "amazon", "search_results"]),
        dg.AssetKey(["gold", "amazon", "dim_product"]),
    ],
    metadata={
        "description": (
            "One row per (asin, keyword, marketplace, ingested_at). "
            "Captures the full search result card impression: price, rating, badges, "
            "and coupon as seen for each keyword on each day. "
            "Partitioned by ingested_at. Written with replaceWhere (idempotent)."
        )
    },
)
def gold_amazon_fact_search_ranking(
    context: dg.AssetExecutionContext,
    spark: SparkResource,
) -> dg.Output[None]:
    """Silver search_results + dim_product → Gold fact_search_ranking."""
    partition_date: str = context.partition_key
    session: SparkSession = spark.get_session()

    silver_df = (
        session.read.format("delta")
        .load(SILVER_PATH)
        .filter(F.col("ingested_at") == partition_date)
    )

    row_count_silver = silver_df.count()
    if row_count_silver == 0:
        context.log.warning(
            f"Silver search_results partition {partition_date} is empty — skipping."
        )
        return dg.Output(
            value=None,
            metadata={"partition_date": partition_date, "silver_row_count": 0},
        )

    dim_df = session.read.format("delta").load(DIM_PRODUCT_PATH)

    fact = _transform(silver_df, dim_df)
    row_count_gold = fact.count()

    context.log.info(
        f"Writing fact_search_ranking: {row_count_gold} rows → {GOLD_PATH}"
    )

    (
        fact.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingested_at = '{partition_date}'")
        .partitionBy("ingested_at")
        .save(GOLD_PATH)
    )

    return dg.Output(
        value=None,
        metadata={
            "partition_date": partition_date,
            "silver_row_count": dg.MetadataValue.int(row_count_silver),
            "gold_row_count": dg.MetadataValue.int(row_count_gold),
            "gold_path": dg.MetadataValue.text(GOLD_PATH),
            "write_mode": dg.MetadataValue.text("overwrite (replaceWhere)"),
        },
    )
