import dagster as dg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ...resources.spark_resource import SparkResource

SILVER_PATH = "s3a://lakehouse/silver/amazon/search_results"
DIM_PRODUCT_PATH = "s3a://lakehouse/gold/amazon/dim_product"
GOLD_PATH = "s3a://lakehouse/gold/amazon/fact_price_snapshot"

daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-03-04")


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------


def _transform(silver_df, dim_df):
    """Aggregates per-keyword rows into a single canonical daily price per ASIN.

    Columns intentionally excluded: monthly_sales, rating_value, ratings_count.
    Those are not price metrics; they belong in fact_search_ranking (raw,
    per-keyword) and fact_product_performance (from the product-detail page).
    """
    # Latest price/coupon snapshot per ASIN (deterministic tie-break on source_extracted_at)
    latest_window = (
        Window.partitionBy("asin", "marketplace", "ingested_at")
        .orderBy(F.col("source_extracted_at").desc())
    )

    latest = (
        silver_df
        .withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .select(
            "asin",
            "marketplace",
            "ingested_at",
            F.col("price_amount").alias("latest_price"),
            F.col("original_price_amount").alias("original_price_latest"),
            F.col("discount_pct").alias("discount_pct_latest"),
            F.col("coupon").alias("coupon_discount_pct_latest"),
        )
    )

    aggs = silver_df.groupBy("asin", "marketplace", "ingested_at").agg(
        F.round(F.min("price_amount"), 2).alias("min_price"),
        F.round(F.max("price_amount"), 2).alias("max_price"),
        F.round(F.avg("price_amount"), 2).alias("avg_price"),
        F.max(F.col("has_coupon").cast("int")).cast("boolean").alias("has_coupon"),
        F.countDistinct("keyword").cast("integer").alias("keyword_appearance_count"),
    )

    dim_current = (
        dim_df
        .filter(F.col("is_current") == True)
        .select("product_sk", "asin", "marketplace")
    )

    return (
        aggs
        .join(latest, on=["asin", "marketplace", "ingested_at"])
        .join(F.broadcast(dim_current), on=["asin", "marketplace"], how="left")
        .select(
            "product_sk",
            "asin",
            "marketplace",
            "ingested_at",
            "min_price",
            "max_price",
            "avg_price",
            "latest_price",
            "original_price_latest",
            "discount_pct_latest",
            "has_coupon",
            "coupon_discount_pct_latest",
            "keyword_appearance_count",
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
    name="fact_price_snapshot",
    deps=[
        dg.AssetKey(["silver", "amazon", "search_results"]),
        dg.AssetKey(["gold", "amazon", "dim_product"]),
    ],
    metadata={
        "description": (
            "One row per (asin, marketplace, ingested_at). "
            "Aggregates all keyword appearances for an ASIN into a single canonical "
            "daily price record: min/max/avg/latest price + coupon status. "
            "Intentionally excludes rating and monthly_sales (wrong grain for this table). "
            "Partitioned by ingested_at. Written with replaceWhere (idempotent)."
        )
    },
)
def gold_amazon_fact_price_snapshot(
    context: dg.AssetExecutionContext,
    spark: SparkResource,
) -> dg.Output[None]:
    """Silver search_results + dim_product → Gold fact_price_snapshot."""
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
        f"Writing fact_price_snapshot: {row_count_gold} rows → {GOLD_PATH}"
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
