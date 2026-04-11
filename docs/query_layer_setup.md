# Query Layer — DuckDB + Superset Setup Guide

> **Scope:** Everything between MinIO (Gold Delta tables) and the BI/API consumers.  
> **Stack:** DuckDB 1.2 · duckdb-engine 0.15 · Apache Superset 4.1

---

## 1. Architecture

```
MinIO — Gold Delta Tables
  s3://lakehouse/gold/amazon/
    ├── dim_product/
    ├── fact_search_ranking/
    ├── fact_price_snapshot/
    ├── fact_product_performance/
    ├── mart_keyword_daily/
    └── mart_brand_competitive/
          │
          │  delta_scan() via httpfs + S3 secret
          │
    ┌─────┴──────────────────────────────────────┐
    │                                             │
┌───▼────────────────────┐    ┌──────────────────▼──────┐
│  superset_views.duckdb │    │  In-memory DuckDB        │
│  (file on host, ro     │    │  (per-thread, FastAPI)   │
│   mounted in container)│    │                          │
│                        │    │  get_thread_connection() │
│  6 VIEWs → delta_scan()│    │  + setup_views()         │
└───────────┬────────────┘    └──────────────────────────┘
            │
    ┌───────▼────────┐
    │    Superset    │  http://localhost:8088
    │  (Docker)      │
    └────────────────┘
```

**Key design decision — DuckDB, not Trino:**  
Trino requires 3–4 additional containers (coordinator, worker, Hive Metastore, backend DB).  
For this data volume (millions of rows, single analyst), DuckDB embeds directly into each  
consumer process with no server overhead. Migration path to Trino exists when needed  
(see Section 7).

---

## 2. Project layout

```
.
├── query_layer/                  ← Python package (import by FastAPI)
│   ├── __init__.py
│   ├── config.py                 ← Settings (reads .env)
│   ├── connection.py             ← DuckDB connection factory
│   └── catalog.py                ← View definitions + setup_views()
│
├── scripts/
│   └── setup_duckdb_catalog.py   ← One-time setup CLI (run on host)
│
├── superset/
│   ├── Dockerfile                ← Extends apache/superset + duckdb-engine
│   ├── superset_config.py        ← Flask/Superset overrides (mounted ro)
│   └── init.sh                   ← First-run bootstrap (db migrate, create admin)
│
├── duckdb_data/                  ← Generated; GITIGNORE
│   └── .duckdb/
│       └── stored_secrets/
│           └── minio_s3.duckdb_secret   ← Persistent S3 credential
│
└── superset_views.duckdb         ← Generated; GITIGNORE
```

---

## 3. How the S3 secret flow works

```
Host (setup script)
  duckdb.connect(config={"home_directory": "./duckdb_data"})
  → CREATE PERSISTENT SECRET minio_s3 (TYPE s3, ...)
  → stored at: ./duckdb_data/.duckdb/stored_secrets/minio_s3.duckdb_secret

docker-compose volume mount:
  ./duckdb_data  →  /app/duckdb_data  (read-only in container)

Superset container (SQLAlchemy)
  duckdb.connect("superset_views.duckdb", config={"home_directory": "/app/duckdb_data"})
  → DuckDB auto-loads persistent secret from /app/duckdb_data/.duckdb/stored_secrets/
  → Views execute delta_scan() with S3 credentials  ✓
```

No credential is ever hard-coded in the catalog file or the container.

---

## 4. First-time setup

### 4.1 Prerequisites

- Docker + Docker Compose running (`docker compose ps` shows minio healthy)
- Gold layer materialised at least once (Dagster ran the gold assets)
- Python environment active (`uv sync` or `pip install -e .`)

### 4.2 Run the setup script

```bash
cd /path/to/amz-market-intelligence-lakehouse-platform
python scripts/setup_duckdb_catalog.py
```

Expected output:

```
DuckDB Catalog Setup
==================================================
  → DuckDB home   : /project/duckdb_data
  → Catalog file  : /project/superset_views.duckdb
  → MinIO endpoint: localhost:9000
  → Gold base path: s3://lakehouse/gold/amazon

  ✓ Directory ready: /project/duckdb_data

[1/3] Opening catalog file and installing extensions...
  ✓ Extensions installed and loaded: delta, httpfs

[2/3] Persistent S3 secret...
  ✓ Secret 'minio_s3' written to .../stored_secrets

[3/3] Creating Gold layer views...
  ✓ dim_product
  ✓ fact_search_ranking
  ✓ fact_price_snapshot
  ✓ fact_product_performance
  ✓ mart_keyword_daily
  ✓ mart_brand_competitive
```

### 4.3 Start Superset

```bash
docker compose build superset    # first time only
docker compose up -d superset
```

Watch the init log:

```bash
docker compose logs -f superset
```

Bootstrap takes ~30 seconds. When you see `Starting web server...` it's ready.

### 4.4 Connect DuckDB in the Superset UI

1. Open http://localhost:8088  
2. Log in with `admin` / `admin`  
3. **Settings → Database Connections → + Database**
4. Select **DuckDB**
5. **SQLAlchemy URI:**

   ```
   duckdb:////app/superset_views.duckdb
   ```

6. Open **Advanced → Other → Engine Parameters** and paste:

   ```json
   {
     "connect_args": {
       "config": {
         "home_directory": "/app/duckdb_data",
         "autoinstall_known_extensions": true,
         "autoload_known_extensions": true
       }
     }
   }
   ```

   | Parameter | Purpose |
   |---|---|
   | `home_directory` | Where DuckDB finds the persistent S3 secret |
   | `autoinstall_known_extensions` | Installs delta/httpfs if not already cached |
   | `autoload_known_extensions` | Auto-loads delta/httpfs when views call `delta_scan()` |

7. Click **Test Connection** → should show success.
8. **Save**.

After connecting, all 6 views appear as tables under the `main` schema.

---

## 5. Querying from Python (FastAPI / notebooks)

```python
from query_layer.connection import get_thread_connection
from query_layer.catalog import setup_views

# In FastAPI startup:
con = get_thread_connection()
setup_views(con)   # registers views on this in-memory connection

# Route example:
rows = con.execute("""
    SELECT keyword, marketplace, total_products, avg_price, sponsored_pct
    FROM mart_keyword_daily
    WHERE ingested_at = ?
    ORDER BY total_products DESC
    LIMIT 50
""", ["2026-03-04"]).fetchall()
```

Or query Delta tables directly without views:

```python
con = get_thread_connection()
df = con.execute("""
    SELECT asin, brand, avg_price, min_price
    FROM delta_scan('s3://lakehouse/gold/amazon/fact_price_snapshot/')
    WHERE ingested_at = '2026-03-04'
      AND marketplace = 'US'
""").df()   # returns pandas DataFrame
```

---

## 6. Day-to-day operations

### New Gold table added?
1. Add a `_ViewDef` entry to `query_layer/catalog.py`.
2. Re-run `python scripts/setup_duckdb_catalog.py` (idempotent).
3. Restart Superset (`docker compose restart superset`) so it picks up the new view.

### MinIO credentials changed?
1. Update `.env`.
2. Re-run `python scripts/setup_duckdb_catalog.py` — rewrites the persistent secret.
3. Restart Superset.

### Superset admin password forgotten?
```bash
docker compose exec superset \
  superset fab reset-password --username admin --password newpassword
```

### Rebuild Superset image (after duckdb version bump)?
```bash
docker compose build --no-cache superset
docker compose up -d superset
```

---

## 7. Migration to Trino (future)

Trigger when **any** of these conditions is met:
- Gold layer scans consistently > 10 s in DuckDB
- Gold layer data > 50 GB total
- > 10 concurrent Superset users

Steps at that point:
1. Add `hive-metastore`, `trino-coordinator`, `trino-worker` services to `docker-compose.yml`.
2. Register the Delta tables in Hive Metastore.
3. Update Superset connection string from `duckdb://` to `trino://`.
4. Update `query_layer/connection.py` to use `trino-python-client` instead of `duckdb`.

The Dagster pipeline and all Gold Delta tables on MinIO require **zero changes**.

---

## 8. .gitignore additions

Add these lines to `.gitignore`:

```gitignore
# DuckDB generated files
superset_views.duckdb
duckdb_data/

# Superset metadata (SQLite, Docker volume not tracked)
superset_home/
```
