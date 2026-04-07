# Gold Layer Design — Amazon Market Intelligence

> **Scope:** This document covers the complete gold layer: one SCD2 dimension table, three daily fact tables, and two analytical mart tables, all sourced from `silver/amazon/search_results` and `silver/amazon/product_details`.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Gold Catalogue](#2-gold-catalogue)
3. [Update Strategy Summary](#3-update-strategy-summary)
4. [dim\_product (SCD Type 2)](#4-dim_product-scd-type-2)
5. [fact\_search\_ranking](#5-fact_search_ranking)
6. [fact\_price\_history](#6-fact_price_history)
7. [fact\_product\_performance](#7-fact_product_performance)
8. [mart\_keyword\_daily](#8-mart_keyword_daily)
9. [mart\_brand\_competitive](#9-mart_brand_competitive)
10. [Lineage & Dependency Graph](#10-lineage--dependency-graph)
11. [Surrogate Key Strategy](#11-surrogate-key-strategy)
12. [Folder Structure](#12-folder-structure)
13. [Verification Checklist](#13-verification-checklist)

---

## 1. Architecture Overview

```
┌───────────────────────────────────────────────────── Silver ──┐
│  silver/amazon/search_results     (asin, keyword, ingested_at) │
│  silver/amazon/product_details    (asin, ingested_at)          │
└────────────────────────────────────────────────────────────────┘
         │                          │
         ▼                          ▼
┌───────────────────────────────────────────────────── Gold ────┐
│                                                                 │
│  DIMENSION                                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  dim_product   (SCD2 — title, brand, category, is_fba)  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  FACT  (daily partitions, replaceWhere)                         │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  fact_search_ranking      (asin × keyword × day)        │   │
│  │  fact_price_history       (asin × day)                  │   │
│  │  fact_product_performance (asin × day)                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  MART  (pre-aggregated, replaceWhere)                           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  mart_keyword_daily      (keyword × day)                │   │
│  │  mart_brand_competitive  (brand × category × day)       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Stack: PySpark + Delta Lake on MinIO                           │
│  Path:  s3a://lakehouse/gold/amazon/<table_name>/               │
└─────────────────────────────────────────────────────────────────┘
```

**Gold layer responsibilities:**
- Encode business logic (surrogate keys, SCD2 history, aggregations)
- Provide stable, analyst-ready schemas that BI tools and notebooks can query directly
- Decouple downstream consumers from silver schema evolution

---

## 2. Gold Catalogue

| Table | Type | Grain | Source Silver Tables | Write Mode |
|---|---|---|---|---|
| `dim_product` | Dimension (SCD2) | 1 row per (asin, marketplace) version | `product_details` | Delta MERGE |
| `fact_search_ranking` | Fact | (asin, keyword, marketplace, ingested_at) | `search_results` | replaceWhere |
| `fact_price_history` | Fact | (asin, marketplace, ingested_at) | `search_results` | replaceWhere |
| `fact_product_performance` | Fact | (asin, marketplace, ingested_at) | `product_details` | replaceWhere |
| `mart_keyword_daily` | Mart | (keyword, marketplace, ingested_at) | `fact_search_ranking` + `dim_product` | replaceWhere |
| `mart_brand_competitive` | Mart | (brand, category_leaf, marketplace, ingested_at) | `fact_product_performance` + `dim_product` | replaceWhere |

All gold tables reside under `s3a://lakehouse/gold/amazon/<table_name>/`.

---

## 3. Update Strategy Summary

| Table | Strategy | Rationale |
|---|---|---|
| `dim_product` | Delta `MERGE` (SCD2) | Row-level upsert; new versions inserted, current rows expired when attributes change |
| `fact_*` tables | `replaceWhere` on `ingested_at` | Entire day's grain is rebuilt deterministically from silver; no row-level merging needed |
| `mart_*` tables | `replaceWhere` on `ingested_at` | Aggregations are fully recalculated per day from upstream facts |

**Execution order** within a Dagster run:
```
silver/amazon/product_details
  └─► dim_product             (MERGE)
        └─► fact_search_ranking      (replaceWhere)
        └─► fact_price_history       (replaceWhere)
        └─► fact_product_performance (replaceWhere)
              └─► mart_keyword_daily     (replaceWhere)
              └─► mart_brand_competitive (replaceWhere)
```

> All fact and mart tables join at read time against `dim_product WHERE is_current = true`. The dimension must be materialised before any fact or mart table in the same daily run.

---

## 4. dim\_product (SCD Type 2)

**Path:** `s3a://lakehouse/gold/amazon/dim_product/`  
**Format:** Delta Lake (no date partition — full snapshot managed by SCD2 logic)  
**Source:** `silver/amazon/product_details`

### 4.1 Schema

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `product_sk` | STRING | NO | Surrogate key — SHA-256 of `(asin \|\| '~' \|\| marketplace \|\| '~' \|\| effective_date)` |
| `asin` | STRING | NO | Amazon ASIN — natural business key |
| `marketplace` | STRING | NO | e.g. `"US"` |
| `title` | STRING | YES | Whitespace-trimmed product title |
| `brand` | STRING | YES | Cleaned brand name (no `"Brand: "` prefix) |
| `category_path` | STRING | YES | Full breadcrumb, e.g. `"Clothing > Women > Sweatshirts"` |
| `category_leaf` | STRING | YES | Most specific category node |
| `category_depth` | INTEGER | YES | Depth of the category tree |
| `bsr_primary_category` | STRING | YES | Category of the product's primary BSR entry |
| `date_first_available` | DATE | YES | When the product was first listed on Amazon |
| `variation_count` | INTEGER | YES | Total ASIN variations in this parent listing |
| `size_count` | INTEGER | YES | Number of distinct size options |
| `color_count` | INTEGER | YES | Number of distinct colour options |
| `main_image_url` | STRING | YES | Primary product image URL |
| `is_fba` | BOOLEAN | YES | Fulfilled by Amazon flag |
| `effective_date` | DATE | NO | First `ingested_at` for which this version is valid |
| `expiry_date` | DATE | YES | Last valid date (null → current version) |
| `is_current` | BOOLEAN | NO | `true` for the active version of each `(asin, marketplace)` |
| `gold_processed_at` | TIMESTAMP | NO | When this row was written to the gold layer |

### 4.2 SCD2 Tracked Attributes

Version changes are detected on any difference in **this set** of attributes:

```
title, brand, category_path, category_leaf, category_depth, is_fba, variation_count
```

Attributes deliberately **excluded from change tracking** (still stored, updated in-place on the current row without creating a new version):

```
date_first_available  — immutable once set
size_count, color_count — variation inventory; high churn, low analytical SCD value
main_image_url, bsr_primary_category — operational metadata, not analytical dimensions
```

> **Design note:** Change-tracked attributes define slow-changing business dimensions. Limiting the tracked set avoids false version proliferation from scraper noise.

### 4.3 MERGE Logic

```python
from delta.tables import DeltaTable

def _upsert_dim_product(session, incoming_df, partition_date):
    """SCD2 MERGE for dim_product.

    Steps
    -----
    1. Hash incoming rows on tracked attributes.
    2. Match against existing current rows on (asin, marketplace).
    3. On MATCH + attribute change  → expire the existing row (set expiry_date, is_current=False).
    4. On MATCH + no change          → no-op (avoid unnecessary version writes).
    5. On NOT MATCHED                → insert as new current row.
    6. For expired rows, insert the new version as a separate INSERT pass.
    """
    CHANGE_COLS = ["title", "brand", "category_path", "category_leaf",
                   "category_depth", "is_fba", "variation_count"]

    def _hash(df):
        return df.withColumn(
            "_attr_hash",
            F.sha2(F.concat_ws("~", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in CHANGE_COLS]), 256),
        )

    incoming = _hash(
        incoming_df.withColumn("effective_date", F.lit(partition_date).cast("date"))
                   .withColumn("expiry_date",    F.lit(None).cast("date"))
                   .withColumn("is_current",     F.lit(True))
    )

    dim = DeltaTable.forPath(session, GOLD_DIM_PRODUCT_PATH)

    # --- Step 1: Expire rows whose attributes changed ---
    dim.alias("target").merge(
        incoming.alias("source"),
        "target.asin = source.asin AND target.marketplace = source.marketplace "
        "AND target.is_current = true",
    ).whenMatchedUpdate(
        condition="target._attr_hash <> source._attr_hash",
        set={
            "expiry_date": F.expr(f"date_sub('{partition_date}', 1)"),
            "is_current":  F.lit(False),
        },
    ).execute()

    # --- Step 2: Insert new / changed rows ---
    existing_current = (
        session.read.format("delta").load(GOLD_DIM_PRODUCT_PATH)
               .filter(F.col("is_current") == True)
               .select("asin", "marketplace", "_attr_hash")
    )
    new_rows = incoming.join(
        existing_current,
        on=["asin", "marketplace"],
        how="left_anti",   # not already current with same hash
    ).withColumn(
        "product_sk",
        F.sha2(F.concat_ws("~", F.col("asin"), F.col("marketplace"),
                           F.col("effective_date").cast("string")), 256),
    ).withColumn("gold_processed_at", F.current_timestamp())

    (
        new_rows.drop("_attr_hash")
                .write.format("delta")
                .mode("append")
                .save(GOLD_DIM_PRODUCT_PATH)
    )
```

### 4.4 First-Run Behaviour

When the Delta table does not yet exist, a full-load is performed:

```python
try:
    DeltaTable.forPath(session, GOLD_DIM_PRODUCT_PATH)
except Exception:
    # Table does not exist → write the initial snapshot as current rows
    initial_df.write.format("delta").mode("overwrite").save(GOLD_DIM_PRODUCT_PATH)
```

---

## 5. fact\_search\_ranking

**Path:** `s3a://lakehouse/gold/amazon/fact_search_ranking/`  
**Format:** Delta Lake, partitioned by `ingested_at`  
**Source:** `silver/amazon/search_results` + `dim_product` (for `product_sk` lookup)  
**Grain:** One row per `(asin, keyword, marketplace, ingested_at)` — matches the silver dedup grain exactly.

### 5.1 Schema

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `product_sk` | STRING | YES | FK → `dim_product.product_sk` (current version as of `ingested_at`) |
| `asin` | STRING | NO | Amazon ASIN |
| `keyword` | STRING | NO | Search keyword |
| `marketplace` | STRING | NO | Marketplace code |
| `ingested_at` | DATE | NO | Partition column |
| `price_amount` | DOUBLE | YES | Market price seen in search result |
| `original_price_amount` | DOUBLE | YES | Crossed-out / was-price |
| `discount_pct` | DOUBLE | YES | `(original − price) / original × 100`, rounded 2 dp |
| `rating_value` | DOUBLE | YES | Numeric rating 0–5 |
| `ratings_count` | LONG | YES | Total review count |
| `monthly_sales_min` | LONG | YES | Lower bound of "X+ bought in past month" badge |
| `is_sponsored` | BOOLEAN | YES | Whether the listing is an ad |
| `is_prime` | BOOLEAN | YES | Prime-eligible shipping |
| `is_best_seller` | BOOLEAN | YES | Best Seller badge present |
| `is_amazons_choice` | BOOLEAN | YES | Amazon's Choice badge |
| `has_coupon` | BOOLEAN | NO | Coupon present on listing |
| `coupon_discount_pct` | DOUBLE | YES | Coupon discount percentage |
| `source_extracted_at` | TIMESTAMP | YES | Scraper timestamp from silver |
| `gold_processed_at` | TIMESTAMP | NO | When this gold write ran |

> **`product_sk` resolution:** Left join `silver.search_results` against `dim_product` on `(asin, marketplace) WHERE is_current = true`. A null `product_sk` means the product exists in search results but has not yet been scraped for product details — valid and expected for newly encountered ASINs.

### 5.2 Transformation

```python
silver_sr = session.read.format("delta").load(SILVER_SEARCH_PATH)\
                   .filter(F.col("ingested_at") == partition_date)

dim = session.read.format("delta").load(GOLD_DIM_PRODUCT_PATH)\
             .filter(F.col("is_current") == True)\
             .select("product_sk", "asin", "marketplace")

fact = (
    silver_sr
    .join(F.broadcast(dim), on=["asin", "marketplace"], how="left")
    .select(
        "product_sk", "asin", "keyword", "marketplace", "ingested_at",
        "price_amount", "original_price_amount", "discount_pct",
        F.col("rating").alias("rating_value"),   # silver column name
        "ratings_count", "monthly_sales",
        "is_sponsored", "is_prime", "is_best_seller", "is_amazons_choice",
        "has_coupon", F.col("coupon").alias("coupon_discount_pct"),
        "source_extracted_at",
        F.current_timestamp().alias("gold_processed_at"),
    )
)
```

### 5.3 Write

```python
fact.write.format("delta")\
    .mode("overwrite")\
    .option("replaceWhere", f"ingested_at = '{partition_date}'")\
    .partitionBy("ingested_at")\
    .save(GOLD_FACT_SEARCH_RANKING_PATH)
```

---

## 6. fact\_price\_history

**Path:** `s3a://lakehouse/gold/amazon/fact_price_history/`  
**Format:** Delta Lake, partitioned by `ingested_at`  
**Source:** `silver/amazon/search_results` (aggregated across all keyword appearances per ASIN per day)  
**Grain:** One row per `(asin, marketplace, ingested_at)` — single canonical daily price snapshot per product.

### 6.1 Aggregation Rationale

A single ASIN may appear in multiple keyword searches on the same day with the same or slightly different prices (search caching artefacts). To produce a single authoritative daily price record:

| Aggregation | Field(s) | Rationale |
|---|---|---|
| `MIN` | `price_amount` → `min_price` | Shows the best available offer |
| `MAX` | `price_amount` → `max_price` | Detects price variation across keywords |
| `AVG` | `price_amount` → `avg_price` | Smoothed signal for trend analysis |
| Latest by `source_extracted_at` | `price_amount`, `original_price_amount`, `discount_pct`, `monthly_sales_min`, `ratings_count`, `coupon_*` | Most up-to-date single snapshot |
| `BOOL_OR` | `has_coupon` | True if any keyword appearance had an active coupon |
| `COUNT DISTINCT` | `keyword` → `keyword_appearance_count` | How many keywords this ASIN ranked for today |

### 6.2 Schema

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `product_sk` | STRING | YES | FK → `dim_product.product_sk` (current as of `ingested_at`) |
| `asin` | STRING | NO | |
| `marketplace` | STRING | NO | |
| `ingested_at` | DATE | NO | Partition column |
| `min_price` | DOUBLE | YES | Minimum price seen across all keyword appearances |
| `max_price` | DOUBLE | YES | Maximum price seen |
| `avg_price` | DOUBLE | YES | Average price across appearances, rounded 2 dp |
| `latest_price` | DOUBLE | YES | Price from the latest scrape (by `source_extracted_at`) |
| `original_price_latest` | DOUBLE | YES | Original/was-price from the latest scrape |
| `discount_pct_latest` | DOUBLE | YES | Discount % from the latest scrape |
| `monthly_sales_min` | LONG | YES | Monthly sales lower bound (any keyword appearance) |
| `ratings_count_latest` | LONG | YES | Review count from the latest scrape |
| `rating_value_latest` | DOUBLE | YES | Rating value from the latest scrape |
| `has_coupon` | BOOLEAN | NO | True if any keyword appearance had a coupon |
| `coupon_discount_pct_latest` | DOUBLE | YES | Coupon discount % from the latest scrape |
| `keyword_appearance_count` | INTEGER | NO | Number of distinct keywords this ASIN ranked for |
| `gold_processed_at` | TIMESTAMP | NO | |

### 6.3 Transformation

```python
from pyspark.sql.window import Window

silver_sr = session.read.format("delta").load(SILVER_SEARCH_PATH)\
                   .filter(F.col("ingested_at") == partition_date)

# --- Latest snapshot per ASIN across keywords ---
latest_window = Window.partitionBy("asin", "marketplace", "ingested_at")\
                      .orderBy(F.col("source_extracted_at").desc())

latest = (
    silver_sr
    .withColumn("_rn", F.row_number().over(latest_window))
    .filter(F.col("_rn") == 1)
    .select(
        "asin", "marketplace", "ingested_at",
        F.col("price_amount").alias("latest_price"),
        F.col("original_price_amount").alias("original_price_latest"),
        F.col("discount_pct").alias("discount_pct_latest"),
        F.col("ratings_count").alias("ratings_count_latest"),
        F.col("rating").alias("rating_value_latest"),
        F.col("monthly_sales").alias("monthly_sales_min"),
        F.col("coupon").alias("coupon_discount_pct_latest"),
        F.col("source_extracted_at"),
    )
)

# --- Global aggregates per ASIN ---
aggs = silver_sr.groupBy("asin", "marketplace", "ingested_at").agg(
    F.round(F.min("price_amount"), 2).alias("min_price"),
    F.round(F.max("price_amount"), 2).alias("max_price"),
    F.round(F.avg("price_amount"), 2).alias("avg_price"),
    F.max(F.col("has_coupon").cast("int")).cast("boolean").alias("has_coupon"),
    F.countDistinct("keyword").alias("keyword_appearance_count"),
)

fact = (
    aggs.join(latest, on=["asin", "marketplace", "ingested_at"])
        .join(F.broadcast(dim), on=["asin", "marketplace"], how="left")
        .withColumn("gold_processed_at", F.current_timestamp())
)
```

---

## 7. fact\_product\_performance

**Path:** `s3a://lakehouse/gold/amazon/fact_product_performance/`  
**Format:** Delta Lake, partitioned by `ingested_at`  
**Source:** `silver/amazon/product_details` + `dim_product` (for `product_sk`)  
**Grain:** One row per `(asin, marketplace, ingested_at)` — 1:1 cardinality with the silver product_details grain.

### 7.1 Schema

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `product_sk` | STRING | YES | FK → `dim_product.product_sk` |
| `asin` | STRING | NO | |
| `marketplace` | STRING | NO | |
| `ingested_at` | DATE | NO | Partition column |
| `bsr_primary_rank` | INTEGER | YES | `best_seller_ranks[0].rank` |
| `bsr_primary_category` | STRING | YES | `best_seller_ranks[0].category` |
| `bsr_secondary_rank` | INTEGER | YES | `best_seller_ranks[1].rank`, nullable |
| `bsr_secondary_category` | STRING | YES | `best_seller_ranks[1].category`, nullable |
| `rating_weighted_avg` | DOUBLE | YES | Σ(star × pct) / Σ(pct) from rating_overview |
| `rating_pct_5` | DOUBLE | YES | % of 5-star reviews |
| `rating_pct_4` | DOUBLE | YES | |
| `rating_pct_3` | DOUBLE | YES | |
| `rating_pct_2` | DOUBLE | YES | |
| `rating_pct_1` | DOUBLE | YES | |
| `in_stock` | BOOLEAN | YES | `stock_status == "in stock"` |
| `buybox_seller` | STRING | YES | Current Buy Box owner |
| `is_fba` | BOOLEAN | YES | |
| `variation_count` | INTEGER | YES | Total ASIN count in parent listing |
| `size_count` | INTEGER | YES | |
| `color_count` | INTEGER | YES | |
| `image_count` | INTEGER | YES | |
| `gold_processed_at` | TIMESTAMP | NO | |

### 7.2 Transformation

```python
silver_pd = session.read.format("delta").load(SILVER_PRODUCT_PATH)\
                   .filter(F.col("ingested_at") == partition_date)

fact = (
    silver_pd
    .join(F.broadcast(dim), on=["asin", "marketplace"], how="left")
    .select(
        "product_sk", "asin", "marketplace", "ingested_at",
        # BSR — flatten first two entries from the array column
        F.col("best_seller_ranks").getItem(0).getField("rank").alias("bsr_primary_rank"),
        F.col("best_seller_ranks").getItem(0).getField("category").alias("bsr_primary_category"),
        F.col("best_seller_ranks").getItem(1).getField("rank").alias("bsr_secondary_rank"),
        F.col("best_seller_ranks").getItem(1).getField("category").alias("bsr_secondary_category"),
        # Rating breakdown
        "rating_weighted_avg", "rating_pct_5", "rating_pct_4",
        "rating_pct_3", "rating_pct_2", "rating_pct_1",
        # Availability
        "in_stock", "buybox_seller", "is_fba",
        # Variation counts
        "variation_count", "size_count", "color_count", "image_count",
        F.current_timestamp().alias("gold_processed_at"),
    )
)
```

---

## 8. mart\_keyword\_daily

**Path:** `s3a://lakehouse/gold/amazon/mart_keyword_daily/`  
**Format:** Delta Lake, partitioned by `ingested_at`  
**Source:** `fact_search_ranking` (same-day partition)  
**Grain:** One row per `(keyword, marketplace, ingested_at)`.

**Purpose:** Provides pre-aggregated keyword-level KPIs for dashboards, search performance monitoring, and competitive analysis.

### 8.1 Schema

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `keyword` | STRING | NO | |
| `marketplace` | STRING | NO | |
| `ingested_at` | DATE | NO | Partition column |
| `total_products` | LONG | NO | Total distinct ASINs ranked for this keyword |
| `sponsored_count` | LONG | NO | ASINs with `is_sponsored = true` |
| `sponsored_pct` | DOUBLE | YES | `sponsored_count / total_products × 100` |
| `prime_count` | LONG | NO | ASINs with `is_prime = true` |
| `best_seller_count` | LONG | NO | ASINs with `is_best_seller = true` |
| `amazons_choice_count` | LONG | NO | ASINs with `is_amazons_choice = true` |
| `coupon_count` | LONG | NO | ASINs with `has_coupon = true` |
| `avg_price` | DOUBLE | YES | Mean `price_amount` across all ranked products |
| `median_price` | DOUBLE | YES | Approximate median using `percentile_approx` |
| `min_price` | DOUBLE | YES | |
| `max_price` | DOUBLE | YES | |
| `avg_rating_value` | DOUBLE | YES | Mean `rating_value` |
| `avg_ratings_count` | DOUBLE | YES | Mean `ratings_count` (social proof depth) |
| `total_monthly_sales_min` | LONG | YES | Sum of `monthly_sales_min` (floor estimate) |
| `gold_processed_at` | TIMESTAMP | NO | |

### 8.2 Transformation

```python
fact_sr = session.read.format("delta").load(GOLD_FACT_SEARCH_RANKING_PATH)\
                 .filter(F.col("ingested_at") == partition_date)

mart = fact_sr.groupBy("keyword", "marketplace", "ingested_at").agg(
    F.countDistinct("asin").alias("total_products"),
    F.sum(F.col("is_sponsored").cast("int")).alias("sponsored_count"),
    F.sum(F.col("is_prime").cast("int")).alias("prime_count"),
    F.sum(F.col("is_best_seller").cast("int")).alias("best_seller_count"),
    F.sum(F.col("is_amazons_choice").cast("int")).alias("amazons_choice_count"),
    F.sum(F.col("has_coupon").cast("int")).alias("coupon_count"),
    F.round(F.avg("price_amount"), 2).alias("avg_price"),
    F.round(F.percentile_approx("price_amount", 0.5), 2).alias("median_price"),
    F.round(F.min("price_amount"), 2).alias("min_price"),
    F.round(F.max("price_amount"), 2).alias("max_price"),
    F.round(F.avg("rating_value"), 2).alias("avg_rating_value"),
    F.round(F.avg("ratings_count"), 0).alias("avg_ratings_count"),
    F.sum("monthly_sales_min").alias("total_monthly_sales_min"),
).withColumn(
    "sponsored_pct",
    F.round(F.col("sponsored_count") / F.col("total_products") * 100, 2),
).withColumn("gold_processed_at", F.current_timestamp())
```

---

## 9. mart\_brand\_competitive

**Path:** `s3a://lakehouse/gold/amazon/mart_brand_competitive/`  
**Format:** Delta Lake, partitioned by `ingested_at`  
**Source:** `fact_product_performance` + `dim_product` (for brand / category_leaf)  
**Grain:** One row per `(brand, category_leaf, marketplace, ingested_at)`.

**Purpose:** Brand-level competitive benchmarking — average BSR, in-stock rate, FBA penetration, and catalogue breadth per category per day.

### 9.1 Schema

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `brand` | STRING | NO | Cleaned brand name (from dim_product) |
| `category_leaf` | STRING | NO | Most specific category node |
| `marketplace` | STRING | NO | |
| `ingested_at` | DATE | NO | Partition column |
| `asin_count` | LONG | NO | Distinct active ASINs tracked today |
| `avg_bsr_primary` | DOUBLE | YES | Mean of `bsr_primary_rank` across ASINs |
| `min_bsr_primary` | INTEGER | YES | Best (lowest number) BSR — brand's highest-ranked product |
| `avg_rating_weighted` | DOUBLE | YES | Mean `rating_weighted_avg` across brand's ASINs |
| `in_stock_rate` | DOUBLE | YES | `in_stock ASINs / asin_count × 100` |
| `fba_count` | LONG | NO | ASINs fulfilled by Amazon |
| `fba_pct` | DOUBLE | YES | `fba_count / asin_count × 100` |
| `avg_variation_count` | DOUBLE | YES | Mean variation breadth per ASIN |
| `avg_color_count` | DOUBLE | YES | |
| `avg_size_count` | DOUBLE | YES | |
| `gold_processed_at` | TIMESTAMP | NO | |

### 9.2 Transformation

```python
perf = session.read.format("delta").load(GOLD_FACT_PRODUCT_PERFORMANCE_PATH)\
              .filter(F.col("ingested_at") == partition_date)

dim_current = session.read.format("delta").load(GOLD_DIM_PRODUCT_PATH)\
                     .filter(F.col("is_current") == True)\
                     .select("product_sk", "brand", "category_leaf")

mart = (
    perf.join(dim_current, on="product_sk", how="left")
        .groupBy("brand", "category_leaf", "marketplace", "ingested_at")
        .agg(
            F.countDistinct("asin").alias("asin_count"),
            F.round(F.avg("bsr_primary_rank"), 0).alias("avg_bsr_primary"),
            F.min("bsr_primary_rank").alias("min_bsr_primary"),
            F.round(F.avg("rating_weighted_avg"), 2).alias("avg_rating_weighted"),
            F.round(F.avg(F.col("in_stock").cast("int")) * 100, 2).alias("in_stock_rate"),
            F.sum(F.col("is_fba").cast("int")).alias("fba_count"),
            F.round(F.avg("variation_count"), 1).alias("avg_variation_count"),
            F.round(F.avg("color_count"), 1).alias("avg_color_count"),
            F.round(F.avg("size_count"), 1).alias("avg_size_count"),
        )
        .withColumn(
            "fba_pct",
            F.round(F.col("fba_count") / F.col("asin_count") * 100, 2),
        )
        .withColumn("gold_processed_at", F.current_timestamp())
        .filter(F.col("brand").isNotNull())  # exclude scraped rows with null brand
)
```

---

## 10. Lineage & Dependency Graph

```
bronze/amazon/search_results
    └─► silver/amazon/search_results
            ├─► gold/amazon/fact_search_ranking
            │       └─► gold/amazon/mart_keyword_daily
            └─► gold/amazon/fact_price_history

bronze/amazon/product_details
    └─► silver/amazon/product_details
            ├─► gold/amazon/dim_product  ◄──────────────────────────────┐
            │       (provides product_sk, brand, category_leaf)          │
            │                                                             │
            ├─► gold/amazon/fact_product_performance ────────────────────┤
            │       └─► gold/amazon/mart_brand_competitive ◄─────────────┘
            │
            └─► (product_sk joined into fact_search_ranking,
                 fact_price_history at write time)
```

**Dagster `AssetKey` dependency declarations:**

| Gold Asset | Upstream `deps` |
|---|---|
| `dim_product` | `["silver", "amazon", "product_details"]` |
| `fact_search_ranking` | `["silver", "amazon", "search_results"]`, `["gold", "amazon", "dim_product"]` |
| `fact_price_history` | `["silver", "amazon", "search_results"]`, `["gold", "amazon", "dim_product"]` |
| `fact_product_performance` | `["silver", "amazon", "product_details"]`, `["gold", "amazon", "dim_product"]` |
| `mart_keyword_daily` | `["gold", "amazon", "fact_search_ranking"]` |
| `mart_brand_competitive` | `["gold", "amazon", "fact_product_performance"]`, `["gold", "amazon", "dim_product"]` |

---

## 11. Surrogate Key Strategy

`product_sk` is a deterministic, collision-resistant string key generated as:

```python
F.sha2(
    F.concat_ws("~", F.col("asin"), F.col("marketplace"), F.col("effective_date").cast("string")),
    256
)
```

| Property | Approach |
|---|---|
| **Determinism** | Re-running the same partition produces the same key — safe for `replaceWhere` idempotency |
| **Cross-day joins** | All fact tables join dimension on `is_current = true`, not on `product_sk`, so key rotations on attribute changes do not break existing fact rows |
| **No auto-increment** | Avoids Spark `monotonically_increasing_id()` which is non-deterministic across shuffles and cannot be replicated across runs |
| **Collision risk** | SHA-256 of a 3-field composite has negligible collision probability for any foreseeable product catalogue size |

---

## 12. Folder Structure

```
elt_pipeline/
└── assets/
    ├── bronze/
    │   └── amazon.py                          # Bronze assets (Polars + delta-rs)
    ├── silver/
    │   ├── __init__.py                        # Re-exports silver assets
    │   ├── search_results.py
    │   └── product_details.py
    └── gold/                                  # ← NEW
        ├── __init__.py                        # Re-exports all gold assets
        ├── dim_product.py                     # SCD2 dimension
        ├── fact_search_ranking.py             # Daily search-result facts
        ├── fact_price_history.py              # Daily ASIN-level price time-series
        ├── fact_product_performance.py        # Daily BSR + review metrics
        ├── mart_keyword_daily.py              # Pre-aggregated keyword KPIs
        └── mart_brand_competitive.py          # Pre-aggregated brand benchmarks
```

**`gold/__init__.py` pattern** (mirrors silver):

```python
from .dim_product              import gold_amazon_dim_product
from .fact_search_ranking      import gold_amazon_fact_search_ranking
from .fact_price_history       import gold_amazon_fact_price_history
from .fact_product_performance import gold_amazon_fact_product_performance
from .mart_keyword_daily       import gold_amazon_mart_keyword_daily
from .mart_brand_competitive   import gold_amazon_mart_brand_competitive

__all__ = [
    "gold_amazon_dim_product",
    "gold_amazon_fact_search_ranking",
    "gold_amazon_fact_price_history",
    "gold_amazon_fact_product_performance",
    "gold_amazon_mart_keyword_daily",
    "gold_amazon_mart_brand_competitive",
]
```

**Adding a new gold asset:** create a module in `gold/`, implement the `@dg.asset` function, and re-export it in `gold/__init__.py`.

---

## 13. Verification Checklist

```bash
# 1. Ensure silver layers are already materialised
dagster asset materialize -m elt_pipeline \
  --select "silver/amazon/search_results silver/amazon/product_details" \
  --partition 2026-03-04

# 2. Materialise gold in dependency order

## 2a. Dimension first
dagster asset materialize -m elt_pipeline \
  --select "gold/amazon/dim_product" \
  --partition 2026-03-04

## 2b. Facts (can run in parallel after dim is ready)
dagster asset materialize -m elt_pipeline \
  --select "gold/amazon/fact_search_ranking gold/amazon/fact_price_history gold/amazon/fact_product_performance" \
  --partition 2026-03-04

## 2c. Marts (depend on facts above)
dagster asset materialize -m elt_pipeline \
  --select "gold/amazon/mart_keyword_daily gold/amazon/mart_brand_competitive" \
  --partition 2026-03-04

# 3. Inspect MinIO
#    Expected paths (all contain *.parquet + _delta_log/):
#      lakehouse/gold/amazon/dim_product/
#      lakehouse/gold/amazon/fact_search_ranking/ingested_at=2026-03-04/
#      lakehouse/gold/amazon/fact_price_history/ingested_at=2026-03-04/
#      lakehouse/gold/amazon/fact_product_performance/ingested_at=2026-03-04/
#      lakehouse/gold/amazon/mart_keyword_daily/ingested_at=2026-03-04/
#      lakehouse/gold/amazon/mart_brand_competitive/ingested_at=2026-03-04/

# 4. Schema spot-check (Python REPL)
from pyspark.sql import SparkSession
# ... configure SparkSession with Delta + S3A (same as silver) ...

dim = spark.read.format("delta").load("s3a://lakehouse/gold/amazon/dim_product/")
assert "product_sk" in dim.columns
assert "is_current"  in dim.columns
assert dim.filter("is_current = true").groupBy("asin","marketplace").count().filter("count > 1").count() == 0, \
    "dim_product: duplicate current rows found for same (asin, marketplace)"

fact_sr = spark.read.format("delta").load("s3a://lakehouse/gold/amazon/fact_search_ranking/")\
               .filter("ingested_at = '2026-03-04'")
print(f"fact_search_ranking rows: {fact_sr.count()}")
assert fact_sr.filter("asin IS NULL OR keyword IS NULL").count() == 0, "Null business keys in fact"

# 5. Idempotency check
#    Re-run steps 2a–2c with the same partition.
#    All row counts must be identical to the first run.

# 6. SCD2 change detection test (manual)
#    Modify a product's title in the bronze/silver layer for a later date,
#    materialise that date, and verify:
#      - Old row: is_current=false, expiry_date=<new_date - 1>
#      - New row: is_current=true,  effective_date=<new_date>
#      - Row count in dim_product for that asin = 2
```

---

## Out-of-Scope & Future Work

| Item | Status | Notes |
|---|---|---|
| `dim_keyword` table | Future | Keyword normalisation, grouping synonyms (e.g. `"sweatshirt"` vs `"sweatshirts"`) |
| Search position tracking | Future | Requires scraper to emit explicit `search_position` column per product card; positional rank analytics cannot be derived retroactively |
| Spark History Server | Future | Add `spark-history-server` service to `docker-compose.yml` for persistent Gold job logs |
| Data quality checks | Future | Column-level null rates and range assertions (`Great Expectations` or Dagster asset checks) at each Gold asset |
| ASIN-level BSR time-series | Future | A dedicated `fact_bsr_history` table tracking BSR rank movements per ASIN per day (`window LAG` to detect rank changes) |
| Gold schema enforcement | Future | Delta `enforceSchema=true` + column-level constraints to prevent schema drift from silver changes |
| Containerised Dagster | Future | Switch `SPARK_MASTER_URL=spark://spark-master:7077` and `SPARK_MINIO_ENDPOINT=http://minio:9000` — no code changes required |
