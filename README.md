# E-commerce PySpark ETL Pipeline

End-to-end PySpark pipeline that ingests four raw CSV files (`customers`, `orders`, `order_items`, `returns`), cleans and enriches them, runs window-function aggregations and return analysis, and writes the results to disk.

## Setup

Requires Python 3.10 and Java 8/11 (for Spark).

```bash
# run the following commands in your terminal
# clone the repo 
cd <your-project-target-directory>
git clone https://github.com/lynwainaina/E-commerce-Pyspark-ETL-pipeline.git
cd E-commerce-Pyspark-ETL-pipeline

# create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# install src as a package so import functions work
pip3 install -e .
```

Place the four source CSVs in `./data/` (`customers.csv`, `orders.csv`,`order_items.csv`, `returns.csv`).

## How to run

```bash
python3 pipeline.py
```

`pipeline.py` is the only entry point. It starts a local SparkSession, runs extract -> transform -> load, and stops Spark in a `finally` block.

## Project layout

```
pipeline.py                       # entry point
src/extract/data.py               # Task 1 - schema enforcement, rejected rows
src/transform/transformations.py  # Tasks 2->4 - cleaning, joins, window aggs
src/load/results.py               # Tasks 5->6 - return analysis + writes
data/                             # input CSVs and pipeline outputs
```

## Where outputs land

All outputs are written under `./data/` with `mode("overwrite")` so re-runs are idempotent:

| Path | Contents |
| --- | --- |
| `data/orders_enriched/` | Final enriched dataset, Parquet, partitioned by `order_year`/`order_month` |
| `data/orphan_items/` | CSV of `order_items` whose `order_id` has no matching order |
| `data/summary/customer_country_rank/` | Customers ranked by lifetime net spend within each country |
| `data/summary/rolling_7d_orders/` | 7-day rolling order count per customer |
| `data/summary/category_revenue_share/` | Each category's share of monthly revenue |
| `data/summary/return_rate_by_category/` | Return rate per product category |
| `data/summary/return_rate_by_tier/` | Return rate per `customer_tier` |
| `data/summary/top_refund_customers/` | Top 10 customers by total refund amount |

## Assumptions

- Date columns may be in `YYYY-MM-DD` or `DD/MM/YYYY`; anything else parses to NULL.
- Rows whose non-string columns fail to cast are routed to a `rejected` DataFrame at ingestion (Task 01) rather than dropped silently.
- A return is only meaningful when its parent order exists in the cleaned order set, so `enrich_returns` uses an inner join (orphan returns are dropped).
- Return rate per category is computed at the order grain using distinct `(order_id, category)` pairs so multi-line orders are not double-counted.
- "Revenue" for the monthly category share uses item-level `quantity * unit_price` (pre-discount) since `discount_pct` is an order-level attribute.

## Design decisions

- **Modular package structure** (`extract`, `transform`, `load`) so each stage is independently testable; `pipeline.py` only orchestrates.
- **Native Spark functions** throughout - no Python UDFs and no `.collect()`/`.toPandas()` in the pipeline logic.
- **Left join for orders -> customers** to surface orphaned customer references rather than silently dropping orders.
- **Left anti-join** isolates orphaned `order_items` (Task 03) into a separate output instead of hiding them.
- **Persist on hot intermediates**: `orders_enriched` and `items_with_orders` are each consumed by several writes. They are `persist()`ed once and `unpersist()`ed in `finally` to avoid recomputing the cleaning/join lineage on every write.

## Limitations

- Runs in `local[*]` mode only; no cluster configuration is provided.
- Summary CSVs are written as Spark directories (`part-*.csv` files) - this scales but means,if a single file is needed, you have to concatenate.
- No automated tests yet (Bonus B1 not implemented).
- No data quality gate before the final write (Bonus B3 not implemented).
