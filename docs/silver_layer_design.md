# Silver Layer Design — `amazon_search_results`

> **Scope:** This document covers only the silver layer for `amazon_search_results`.  
> `amazon_product_details` silver is out of scope — see §9.

---

## Table of Contents

1. [Medallion Architecture Overview](#1-medallion-architecture-overview)  
2. [Spark Infrastructure Setup](#2-spark-infrastructure-setup)  
3. [Data Flow: Bronze → Silver](#3-data-flow-bronze--silver)  
4. [Silver Schema](#4-silver-schema)  
5. [Transformation Logic](#5-transformation-logic)  
6. [Update Strategy](#6-update-strategy)  
7. [Data Modeling Decision](#7-data-modeling-decision)  
8. [Idempotency & Re-run Safety](#8-idempotency--re-run-safety)  
9. [Out-of-Scope & Future Work](#9-out-of-scope--future-work)  
10. [Verification Checklist](#10-verification-checklist)  

---

## 1. Medallion Architecture Overview

```
Raw JSON Files
     │
     ▼
┌──────────────────────────────────────────────────────────────────── Bronze ──┐
│  Dagster + Polars                                                             │
│  • Schema-on-read from raw JSON                                               │
│  • All columns preserved as-is (including raw strings, nulls)                 │
│  • Minimal casting: null-only columns → String                                │
│  • Partition: ingested_at (YYYY-MM-DD)                                        │
│  • Storage: Delta Lake on MinIO  s3a://lakehouse/bronze/amazon/               │
└───────────────────────────────────────────────────────────────────────────────┘
     │  reads Δ  (partition filter pushdown)
     ▼
┌──────────────────────────────────────────────────────────────────── Silver ──┐
│  Dagster + PySpark + Delta Lake                                               │
│  • Type-safe: prices/ratings/counts cast to native numeric types              │
│  • Clean: whitespace trimmed, nulls preserved as proper NULLs                 │
│  • Parsed: raw strings → structured fields (price_amount, rating_value, …)   │
│  • Deduplicated: 1 row per (asin, keyword) per ingested_at                    │
│  • Partition: ingested_at (same grain as bronze)                              │
│  • Storage: Delta Lake on MinIO  s3a://lakehouse/silver/amazon/               │
└───────────────────────────────────────────────────────────────────────────────┘
     │
     ▼
  Gold / Mart     ← business aggregations, dimensional models, SCD2
  (future)
```

---

## 2. Spark Infrastructure Setup

### 2.1 Components

| Component | Image | Ports | Role |
|---|---|---|---|
| `spark-master` | `bitnami/spark:3.5` | 7077 (RPC), 8080 (UI) | Cluster coordinator |
| `spark-worker` | `bitnami/spark:3.5` | 8081 (UI) | Compute node |

Both containers share the `lakehouse-tntkhai` Docker network with MinIO, so they can reach `minio:9000` internally when Dagster is containerised.

### 2.2 JARs (pre-downloaded, not fetched at runtime)

| JAR | Purpose |
|---|---|
| `delta-spark_2.12-3.2.0.jar` | Delta Lake Spark extension |
| `delta-storage-3.2.0.jar` | Delta log storage abstraction |
| `hadoop-aws-3.3.4.jar` | Hadoop S3A filesystem |
| `aws-java-sdk-bundle-1.12.262.jar` | AWS SDK (S3 client) |

Run once before first start:
```bash
bash spark/download_jars.sh
```

JARs are volume-mounted at `/opt/bitnami/spark/jars/extra/` inside containers and added to `spark.driver.extraClassPath` / `spark.executor.extraClassPath` via `spark/conf/spark-defaults.conf`.

### 2.3 Networking: local[*] vs cluster mode

| Dagster deployment | `SPARK_MASTER_URL` | `SPARK_MINIO_ENDPOINT` |
|---|---|---|
| **Local** (current) | `local[*]` | `http://localhost:9000` |
| Containerised (future) | `spark://spark-master:7077` | `http://minio:9000` |

**Why `local[*]` today:**  
When Dagster runs on the host machine, the Spark driver (in-process) must reach MinIO via `localhost:9000`. If the driver ran inside the Spark container, it would correctly use `minio:9000`. Mixing the two — driver on host, executors in Docker — creates asymmetric network routing that is fragile and hard to debug.

The `SparkResource` reads `SPARK_MASTER_URL` and `SPARK_MINIO_ENDPOINT` from the environment, so switching later requires only two `.env` changes, no code changes.

---

## 3. Data Flow: Bronze → Silver

```
Dagster triggers silver asset materialisation for partition YYYY-MM-DD
│
├─ 1. SparkResource.get_session()
│      Creates or reuses SparkSession (local[*] + Delta + S3A configured)
│
├─ 2. Read bronze partition
│      spark.read.format("delta")
│        .load("s3a://lakehouse/bronze/amazon/search_results")
│        .filter(ingested_at == 'YYYY-MM-DD')
│
├─ 3. Transform  (see §5)
│      parse_price, parse_rating, parse_ratings_count,
│      parse_monthly_sales, parse_coupon, derive discount_pct, dedup
│
├─ 4. Write silver partition  (see §6)
│      .format("delta")
│      .mode("overwrite")
│      .option("replaceWhere", "ingested_at = 'YYYY-MM-DD'")
│      .partitionBy("ingested_at")
│      .save("s3a://lakehouse/silver/amazon/search_results")
│
└─ 5. Emit Dagster metadata
       bronze_row_count, silver_row_count, silver_path, write_mode
```

---

## 4. Silver Schema

> Target: `s3a://lakehouse/silver/amazon/search_results/`  
> Delta table partitioned by `ingested_at`.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `asin` | STRING | NO | Amazon product identifier |
| `title` | STRING | YES | Whitespace-trimmed |
| `price_raw` | STRING | YES | Original price string (audit) |
| `price_currency` | STRING | YES | Extracted currency prefix, e.g. `"VND"`, `"$"` |
| `price_amount` | DOUBLE | YES | Numeric price |
| `original_price_amount` | DOUBLE | YES | Numeric original/crossed-out price |
| `discount_pct` | DOUBLE | YES | `(original - price) / original * 100`, rounded to 2 dp |
| `rating_value` | DOUBLE | YES | Numeric rating (0–5) |
| `ratings_count` | LONG | YES | Number of reviews; K/M suffixes expanded |
| `monthly_sales_min` | LONG | YES | Lower bound of monthly sales badge; null if no badge |
| `is_sponsored` | BOOLEAN | YES | |
| `is_prime` | BOOLEAN | YES | |
| `is_best_seller` | BOOLEAN | YES | |
| `is_amazons_choice` | BOOLEAN | YES | |
| `has_coupon` | BOOLEAN | NO | `true` whenever a coupon string is present |
| `coupon_discount_pct` | DOUBLE | YES | Extracted `%` from coupon text |
| `delivery` | STRING | YES | Raw delivery text; not parsed (low analytical value) |
| `url` | STRING | YES | Product page URL |
| `image_url` | STRING | YES | Product image URL |
| `keyword` | STRING | NO | Search keyword that returned this product |
| `marketplace` | STRING | NO | Marketplace code, e.g. `"US"` |
| `source_extracted_at` | TIMESTAMP | YES | When the scraper collected this row |
| `ingested_at` | DATE | NO | Partition column — date data was written to bronze |
| `silver_processed_at` | TIMESTAMP | NO | When this silver write ran |

**Intentionally dropped from bronze:**
- `source_file` — internal ingest detail; no analytical value beyond bronze.

---

## 5. Transformation Logic

### 5.1 Price parsing
Input: `"VND 340,600"`, `"$12.99"`, `null`  
Steps:
1. Extract leading non-digit characters → `price_currency`
2. Extract all digit/comma/period sequences → strip commas → cast `DOUBLE` → `price_amount`
3. Keep original string as `price_raw` for audit

### 5.2 Discount percentage
```
discount_pct = ROUND((original_price_amount - price_amount) / original_price_amount * 100, 2)
```
Returns `null` when either price is null or `original_price_amount = 0`.

### 5.3 Rating value
Input: `"4.5 out of 5 stars"`  
Regex: `^([\d.]+)` → `DOUBLE`

### 5.4 Ratings count
Input: `"(130.9K)"`, `"(3.4K)"`, `"(22)"`  
Steps:
1. Strip parentheses
2. Split numeric part and suffix (K → ×1,000 / M → ×1,000,000)
3. Cast to `LONG`

### 5.5 Monthly sales lower bound
Input: `"1K+ bought in past month"`, `"300+ bought in past month"`, `null`  
Steps: same K/M expansion as ratings_count → `LONG`, null-safe

### 5.6 Coupon
Input: `"Save 15%with coupon (some sizes/colors)"`, `null`  
- `has_coupon = col IS NOT NULL`
- `coupon_discount_pct`: regex `Save\s*([\d.]+)\s*%` → `DOUBLE`, null when no `%` found

### 5.7 Deduplication
Within each `(asin, keyword, ingested_at)` group, keep the row with the latest
`source_extracted_at`:
```python
Window.partitionBy("asin", "keyword", "ingested_at")
      .orderBy(F.col("source_extracted_at").desc())
# ROW_NUMBER() == 1 → keep
```
This handles scrapers that run multiple times per day or multi-page results
where the same ASIN appears in overlapping keyword pages.

---

## 6. Update Strategy

**Chosen: Partition-level atomic overwrite (`replaceWhere`)**

```python
df.write.format("delta")
  .mode("overwrite")
  .option("replaceWhere", f"ingested_at = '{partition_date}'")
  .partitionBy("ingested_at")
  .save(SILVER_PATH)
```

### Why not `MERGE` (UPSERT)?

| Concern | replaceWhere | MERGE |
|---|---|---|
| Re-run safety | Replace full partition deterministically | Same, but requires a match key |
| Complexity | Low | Higher — requires stable business key for ON clause |
| Performance | Rewrites only target partition files | Scans all existing files for matching rows |
| Semantics fit | Perfect — we always reprocess entire day | MERGE designed for row-level streaming upserts |
| When best | Batch, partition-aligned updates | Streaming / gold layer SCD2 |

At the silver granularity `(asin, keyword, ingested_at)`, an entire day of data is rebuilt from scratch during each run. The `replaceWhere` predicate makes this:
1. **Atomic** — other partitions are untouched, the target partition is replaced in a single Delta transaction
2. **Idempotent** — running twice produces the same result
3. **Auditable** — Delta's transaction log retains the previous version for time-travel queries

`MERGE` will be reconsidered at the **gold layer** when we need row-level upserts on product dimension tables that span multiple days.

---

## 7. Data Modeling Decision

**Decision: No dimensional modeling at the silver layer.**

### Rationale

Silver's responsibility is to be a **clean, type-safe, query-ready mirror** of the bronze data — not to encode business logic. Introducing dimensional modeling (surrogate keys, fact/dim separation, SCD2) at this layer would:

1. **Create tight coupling.** Any schema evolution in silver (e.g., adding a new parsed field) would require cascading changes to all downstream gold tables that join on surrogate keys.

2. **Prevent independent reprocessing.** If silver dim tables have their own slowly-changing history, re-running a bronze partition becomes non-trivial — you must reconcile existing SCD2 rows before writing new silver rows.

3. **Obscure the data lineage.** A senior analyst debugging a gold metric should be able to open a silver row and trace it directly back to a bronze row (same `asin + keyword + ingested_at`). Surrogate keys break this 1:1 traceability.

4. **Prematurely lock in grain.** `amazon_search_results` today is a daily snapshot. In the future, if scraping moves to hourly, the silver schema should just grow a `scraped_at` timestamp column — no star-schema restructure needed.

### What silver *does* provide for downstream analytics

- Stable, named columns (`price_amount`, `rating_value`, …) that will not change format
- A `ingested_at` date partition that enables natural time-series queries across multiple days
- A `source_extracted_at` timestamp for intra-day ordering and dedup at the gold layer if needed
- All boolean badges (`is_best_seller`, `is_sponsored`, …) correctly typed

### Where dimensional modeling belongs

**Gold / Mart layer:**
- `dim_product` (SCD2 — slow-changing attributes like title, brand category)  
- `fact_search_ranking` (fact table: ASIN rank per keyword per day, joinable to dim_product)  
- `fact_price_history` (time-series of `price_amount`, `discount_pct` per ASIN per day)

---

## 8. Idempotency & Re-run Safety

| Scenario | Behaviour |
|---|---|
| First run for a new partition | Creates Delta table if absent, writes partition |
| Re-run for same partition | `replaceWhere` atomically replaces — result identical |
| Bronze partition is empty | Asset returns early, logs warning, writes nothing |
| Bronze data changes between runs | Silver partition reflects the latest bronze state after re-run |
| Adding a new silver column | Delta schema evolution handles additional columns automatically |

---

## 9. Out-of-Scope & Future Work

| Item | Status | Notes |
|---|---|---|
| `amazon_product_details` silver | Future | Requires different parsing (nested `variations`, `best_seller_rank` array). Design separately. |
| Gold / Mart layer | Future | `fact_search_ranking`, `fact_price_history`, `dim_product` (SCD2) |
| Spark cluster mode | Future | Change `SPARK_MASTER_URL` + `SPARK_MINIO_ENDPOINT` in `.env` when Dagster is containerised |
| Data quality checks | Future | Column-level null rates, value range assertions (Great Expectations or Dagster checks) |
| Schema enforcement | Future | Delta `columnMapping` + `enforceSchema` for stricter contract at write time |
| Spark History Server | Future | Add `spark-history-server` service to docker-compose for persistent Spark job logs |

---

## 10. Verification Checklist

```bash
# 1. Download JARs (once)
bash spark/download_jars.sh

# 2. Start infrastructure
docker compose up minio spark-master spark-worker -d

# 3. Verify Spark UI
open http://localhost:8080   # → 1 worker listed as ALIVE

# 4. Materialize bronze (if not already done)
dagster asset materialize -m elt_pipeline --select bronze/amazon/search_results --partition 2026-03-04

# 5. Materialize silver
dagster asset materialize -m elt_pipeline --select silver/amazon/search_results --partition 2026-03-04

# 6. Inspect MinIO
#    → lakehouse/silver/amazon/search_results/ingested_at=2026-03-04/
#      should contain *.parquet files and _delta_log/

# 7. Quick schema check (Python REPL)
import polars as pl
df = pl.read_delta(
    "s3://lakehouse/silver/amazon/search_results",
    storage_options={"endpoint_url": "http://localhost:9000", ...},
).filter(pl.col("ingested_at") == "2026-03-04")
print(df.schema)
# Expected: price_amount → Float64, rating_value → Float64,
#           ratings_count → Int64, source_file column absent

# 8. Idempotency check
dagster asset materialize ...  # same command as step 5
# Row count must be equal to previous run
```
