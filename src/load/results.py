"""
This script outputs the results of the ETL pipeline.

Tasks:
    - Return analysis (join returns to enriched orders, return rate per category and per customer_tier, top 10 refund customers, flag refunds
      that exceed the order net amount).
    - Output & partitioning (write enriched dataset to Parquet partitioned by
      order year & month, write summary tables to csv, all with overwrite mode).

Reads its inputs directly from src.transform.transformations.run_transformations
so the pipeline flows end-to-end.
"""

from pathlib import Path
from typing import Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.transform.transformations import run_transformations


# initialize output directories
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
ENRICHED_DIR = DATA_DIR / "orders_enriched"
SUMMARY_DIR = DATA_DIR / "summary"
ORPHANS_DIR = DATA_DIR / "orphan_items"


# Task 05 - Return analysis

# Join returns to enriched orders and add refund_exceeds_order flag
def enrich_returns(returns: DataFrame, orders_enriched: DataFrame) -> DataFrame:
    """
    Inner join returns with enriched orders to add customer_tier and net_amount
    Flag refunds that exceed the order's net amount.
    """
    joined = returns.join(orders_enriched.select("order_id", "customer_id", "customer_tier", "net_amount"),
        on="order_id",
        how="inner")
    return joined.withColumn("refund_exceeds_order", F.col("refund_amount") > F.col("net_amount"))


# Return rate per category = returned orders / total orders, grouped by category.
def return_rate_by_category(items_with_orders: DataFrame, returns: DataFrame) -> DataFrame:
    # drop duplicates so multi-line orders are not double-counted
    order_category = items_with_orders.select("order_id", "category").dropDuplicates()
    returned = returns.select("order_id").dropDuplicates().withColumn("is_returned", F.lit(1))
    flagged = order_category.join(returned, on="order_id", how="left")
    return (flagged.groupBy("category").agg(
            F.count("order_id").alias("total_orders"),
            F.sum(F.coalesce(F.col("is_returned"), F.lit(0))).alias("returned_orders"),
        )
        .withColumn("return_rate", F.col("returned_orders") / F.col("total_orders")))


# Return rate per customer_tier = returned orders / total orders, grouped by tier.
def return_rate_by_tier(orders_enriched: DataFrame, returns: DataFrame) -> DataFrame:
    returned = returns.select("order_id").dropDuplicates().withColumn("is_returned", F.lit(1))
    flagged = orders_enriched.select("order_id", "customer_tier").join(returned, on="order_id", how="left")
    return (flagged.groupBy("customer_tier").agg(
            F.count("order_id").alias("total_orders"),
            F.sum(F.coalesce(F.col("is_returned"), F.lit(0))).alias("returned_orders"))
        .withColumn("return_rate", F.col("returned_orders") / F.col("total_orders")))


# Top 10 customers by total refund amount
def top_refund_customers(returns_enriched: DataFrame, n: int = 10) -> DataFrame:
    return (returns_enriched.groupBy("customer_id")
        .agg(F.sum("refund_amount").alias("total_refund_amount"))
        .orderBy(F.col("total_refund_amount").desc())
        .limit(n))


# Task 06 - Output & partitioning

# Writes enriched orders to Parquet, partitioned by order year and order month.
def write_enriched_parquet(orders_enriched: DataFrame, path: Path = ENRICHED_DIR) -> None:
    """
    add order_year and order_month columns from order_date so we can
    partition the parquet on disk.
    """
    out = (
        orders_enriched
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date")))
    (
        out.write
        .mode("overwrite")
        .partitionBy("order_year", "order_month")
        .parquet(str(path))
    )


# Write a single csv summary table
def write_csv(df: DataFrame, name: str, base_dir: Path = SUMMARY_DIR) -> None:
    """Write a CSV directory with header. Spark may produce multiple part files."""
    (
        df.write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(base_dir / name))
    )


# Persist the enriched dataset and all summary tables to disk.
def write_all_outputs(outputs: Dict[str, DataFrame]) -> None:
    """
    outputs dict comes from run_transformations + Task 05 functions in this file.

    orders_enriched and items_with_orders are each consumed by several downstream
    writes, so we persist them once to avoid recomputing the cleaning/join
    lineage on every write. Unpersist in finally so Spark releases the memory
    even if a write fails.
    """
    orders_enriched   = outputs["orders_enriched"].persist()
    items_with_orders = outputs["items_with_orders"].persist()

    try:
        write_enriched_parquet(orders_enriched)

        # orphan items go straight to ORPHANS_DIR (data/orphan_items)
        (
            outputs["orphan_items"]
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(str(ORPHANS_DIR))
        )

        summaries = {
            "customer_country_rank":   outputs["customer_country_rank"],
            "rolling_7d_orders":       outputs["rolling_7d_orders"],
            "category_revenue_share":  outputs["category_revenue_share"],
            "return_rate_by_category": outputs["return_rate_by_category"],
            "return_rate_by_tier":     outputs["return_rate_by_tier"],
            "top_refund_customers":    outputs["top_refund_customers"],
        }
        for name, df in summaries.items():
            write_csv(df, name)
    finally:
        orders_enriched.unpersist()
        items_with_orders.unpersist()


def build_outputs(spark: SparkSession) -> Dict[str, DataFrame]:
    outputs = run_transformations(spark)
    orders_enriched   = outputs["orders_enriched"]
    items_with_orders = outputs["items_with_orders"]
    returns_clean     = outputs["returns_clean"]

    returns_enriched = enrich_returns(returns_clean, orders_enriched)
    outputs["returns_enriched"]         = returns_enriched
    outputs["return_rate_by_category"]  = return_rate_by_category(items_with_orders, returns_clean)
    outputs["return_rate_by_tier"]      = return_rate_by_tier(orders_enriched, returns_clean)
    outputs["top_refund_customers"]     = top_refund_customers(returns_enriched)
    return outputs


