import dagster as dg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ...resources.spark_resource import SparkResource

SILVER_PATH = "s3a://lakehouse/silver/amazon/product_details"
DIM_PRODUCT_PATH = "s3a://lakehouse/gold/amazon/dim_product"
GOLD_PATH = "s3a://lakehouse/gold/amazon/fact_product_performance"

daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-03-04")


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------


def _transform(silver_df, dim_df):
    """1:1 with silver product_details grain (one row per asin × marketplace × day).

    Flattens BSR array, keeps rating breakdown, stock/buybox status, and
    variation counts. Joins dim_product for product_sk.

    Note: `rating_value` (numeric score) and `ratings_count` are NOT available
    in the product-detail silver layer — those fields appear on search cards.
    Use fact_search_ranking for per-ASIN rating values.
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
            "marketplace",
            "ingested_at",
            # Best Seller Rank — flatten first two entries from the struct array
            F.col("best_seller_ranks").getItem(0).getField("rank").alias(
                "bsr_primary_rank"
            ),
            F.col("best_seller_ranks").getItem(0).getField("category").alias(
                "bsr_primary_category"
            ),
            F.col("best_seller_ranks").getItem(1).getField("rank").alias(
                "bsr_secondary_rank"
            ),
            F.col("best_seller_ranks").getItem(1).getField("category").alias(
                "bsr_secondary_category"
            ),
            # Rating breakdown from product-detail overlay
            "rating_weighted_avg",
            "rating_pct_5",
            "rating_pct_4",
            "rating_pct_3",
            "rating_pct_2",
            "rating_pct_1",
            # Availability
            "in_stock",
            "buybox_seller",
            "is_fba",
            # Variation breadth (counts per dimension type)
            "variation_count",
            "size_count",
            "color_count",
            "image_count",
            # Audit
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
    name="fact_product_performance",
    deps=[
        dg.AssetKey(["silver", "amazon", "product_details"]),
        dg.AssetKey(["gold", "amazon", "dim_product"]),
    ],
    metadata={
        "description": (
            "One row per (asin, marketplace, ingested_at). "
            "Captures the product-detail page snapshot: BSR ranks, full rating "
            "breakdown (pct per star), stock/buybox status, FBA flag, and "
            "variation/image counts. 1:1 cardinality with silver product_details. "
            "Partitioned by ingested_at. Written with replaceWhere (idempotent)."
        )
    },
)
def gold_amazon_fact_product_performance(
    context: dg.AssetExecutionContext,
    spark: SparkResource,
) -> dg.Output[None]:
    """Silver product_details + dim_product → Gold fact_product_performance."""
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
            f"Silver product_details partition {partition_date} is empty — skipping."
        )
        return dg.Output(
            value=None,
            metadata={"partition_date": partition_date, "silver_row_count": 0},
        )

    dim_df = session.read.format("delta").load(DIM_PRODUCT_PATH)

    fact = _transform(silver_df, dim_df)
    row_count_gold = fact.count()

    context.log.info(
        f"Writing fact_product_performance: {row_count_gold} rows → {GOLD_PATH}"
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
