"""
Script to transform raw data for the e-commerce ETL pipeline.

Tasks:
    - Data quality & cleaning (dedup, date normalisation, lowercase, drop NULL keys, flag negative amounts).
    - Joins & enrichment (orders & customers, & order items, orphan items isolated via left anti-join, net_amount derived).
    - Window aggregations (country spend rank, 7-day rolling order count, monthly category revenue share).

Reads input data directly from src.extract.data.ingest_all function
"""

from typing import Dict
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from src.extract.data import ingest_all


# Task 02 - Data quality & cleaning
# Normalise all date columns to ISO format (YYYY-MM-DD)
def normalize_date(col: F.Column) -> F.Column:
    """
    Parse a date string that may be either YYYY-MM-DD or DD/MM/YYYY.
    Returns:
        DateType column where unparseable values become NULL.
    """
    return F.coalesce(F.to_date(col, "yyyy-MM-dd"), F.to_date(col, "dd/MM/yyyy"))

# Standardise customer_tier to lowercase.
def clean_customers(df: DataFrame) -> DataFrame:
    return (df.dropDuplicates()
          .withColumn("signup_date", normalize_date(F.col("signup_date")))
          .withColumn("customer_tier", F.lower(F.col("customer_tier"))))

# Add a boolean column is_negative_amount to flag (not drop) orders with total_amount < 0.
def clean_orders(df: DataFrame) -> DataFrame:
    return (df.dropDuplicates().withColumn("order_date", normalize_date(F.col("order_date")))
          # Drop rows where order_id or customer_id is NULL.
          .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
          # flag negative amounts but keep the rows.
          .withColumn("is_negative_amount", F.col("total_amount") < 0)
    )


def clean_order_items(df: DataFrame) -> DataFrame:
    # order_items has no date column, just drop exact duplicates.
    return df.dropDuplicates()


def clean_returns(df: DataFrame) -> DataFrame:
    return (df.dropDuplicates().withColumn("return_date", normalize_date(F.col("return_date"))))


def clean_all(ingested: Dict[str, Dict[str, DataFrame]]) -> Dict[str, DataFrame]:
    """
    Apply Data quality & cleaning to each ingested DataFrame
    Returns:
        Dictionary of dataframes that have been cleaned
    """
    return {
        "customers":   clean_customers(ingested["customers"]["clean"]),
        "orders":      clean_orders(ingested["orders"]["clean"]),
        "order_items": clean_order_items(ingested["order_items"]["clean"]),
        "returns":     clean_returns(ingested["returns"]["clean"]),
    }


# Task 03 - Joins & enrichment

# Join orders data to customers data after cleaning
def enrich_orders_with_customers(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Left join to keep every cleaned order even if its customer_id is not present
    in the customers table (orphaned customer reference)
    
    Add net_amount.
    """
    enriched = orders.join(customers, on="customer_id", how="left")
    return enriched.withColumn("net_amount", 
                               F.col("total_amount") * (F.lit(1) - F.col("discount_pct") / F.lit(100)))

# isolate orphaned order_items
def find_orphan_items(order_items: DataFrame, orders: DataFrame) -> DataFrame:
    "Left anti-join isolates items whose order_id is not in orders"
    return order_items.join(orders.select("order_id"), on="order_id", how="left_anti")

# join orders to order_items 
def join_items_to_orders(order_items: DataFrame, orders_enriched: DataFrame) -> DataFrame:
    """
    Inner join: orphaned items are excluded here 
    """
    return order_items.join(orders_enriched, on="order_id", how="inner")


# Task 04 - Aggregations & window functions

# Customers ranked by lifetime net spend within each country
def rank_customers_by_country_spend(orders_enriched: DataFrame) -> DataFrame:
    """
    Using PySpark window functions to rank  Customers by lifetime 
    net spend within each country
    """
    customer_spend = (
        orders_enriched
        .groupBy("country", "customer_id")
        .agg(F.sum("net_amount").alias("lifetime_net_spend"))
    )
    win = Window.partitionBy("country").orderBy(F.col("lifetime_net_spend").desc())
    return customer_spend.withColumn("spend_rank", F.rank().over(win))


def rolling_7day_order_count(orders_enriched: DataFrame) -> DataFrame:
    """
    7-day rolling order count per customer.
    Casting a DateType to long gives days-since-epoch, 
    a rangeBetween of -6 -> 0 covers the current order's day plus the previous six days.
    """
    win = (
        Window.partitionBy("customer_id")
        .orderBy(F.col("order_date").cast("long"))
        .rangeBetween(-6, 0)
    )
    return orders_enriched.withColumn(
        "rolling_7d_order_count", F.count("order_id").over(win)
    )

# Each product category’s share of total revenue per calendar month
def category_revenue_share(items_with_orders: DataFrame) -> DataFrame:
    """
    Each product category's share of total revenue per calendar month.
    Line revenue = quantity * unit_price (item-level, before order discount).
    """
    monthly = (
        items_with_orders
        .withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
        .withColumn("line_revenue", F.col("quantity") * F.col("unit_price"))
        .groupBy("order_month", "category")
        .agg(F.sum("line_revenue").alias("category_revenue"))
    )
    month_win = Window.partitionBy("order_month")
    return monthly.withColumn(
        "monthly_revenue_share",
        F.col("category_revenue") / F.sum("category_revenue").over(month_win))


def run_transformations(spark: SparkSession) -> Dict[str, DataFrame]:
    """
    Run transformations and return all DataFrames downstream tasks will need
    """
    ingested = ingest_all(spark)
    cleaned = clean_all(ingested)

    orders_enriched   = enrich_orders_with_customers(cleaned["orders"], cleaned["customers"])
    orphan_items      = find_orphan_items(cleaned["order_items"], cleaned["orders"])
    items_with_orders = join_items_to_orders(cleaned["order_items"], orders_enriched)

    return {
        # cleaned tables
        "customers_clean":     cleaned["customers"],
        "orders_clean":        cleaned["orders"],
        "order_items_clean":   cleaned["order_items"],
        "returns_clean":       cleaned["returns"],
        # enriched / joined
        "orders_enriched":     orders_enriched,
        "orphan_items":        orphan_items,
        "items_with_orders":   items_with_orders,
        # window aggregations
        "customer_country_rank": rank_customers_by_country_spend(orders_enriched),
        "rolling_7d_orders":     rolling_7day_order_count(orders_enriched),
        "category_revenue_share": category_revenue_share(items_with_orders),
    }
