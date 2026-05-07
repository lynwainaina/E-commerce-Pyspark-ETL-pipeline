"""
This script ingests the data & enforces the schema by loading the four raw CSV 
files (customers, orders, order_items, returns) using explicit StructType schemas. 
Each column is cast to its declared type; 
rows whose casts fail are routed to a separate rejected DataFrame
"""


from pathlib import Path
from typing import Dict, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# establish the local root to read the data
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"

# define the schema of the DataFrame for each dataset
# dates have been set to string at ingestion because we need to perform 
# transformations later and we should not silently drop the rows
# numeric values are expected to be numeric at ingestion
# at the ingestion stage the nulls should not be dropped, hence set nullable true
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=True),
    StructField("signup_date", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("customer_tier", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("order_date", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=True),
    StructField("discount_pct", DoubleType(), nullable=True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("item_id", StringType(), nullable=True),
    StructField("order_id", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("unit_price", DoubleType(), nullable=True),
    StructField("category", StringType(), nullable=True),
])

RETURNS_SCHEMA = StructType([
    StructField("return_id", StringType(), nullable=True),
    StructField("order_id", StringType(), nullable=True),
    StructField("return_date", StringType(), nullable=True),
    StructField("reason", StringType(), nullable=True),
    StructField("refund_amount", DoubleType(), nullable=True),
])


SOURCE_TABLES: Dict[str, Tuple[str, StructType]] = {
    "customers":   ("customers.csv",   CUSTOMERS_SCHEMA),
    "orders":      ("orders.csv",      ORDERS_SCHEMA),
    "order_items": ("order_items.csv", ORDER_ITEMS_SCHEMA),
    "returns":     ("returns.csv",     RETURNS_SCHEMA),
}

# initialize spark session and set master - 'local' to run spark locally
def create_spark_session(app_name: str = "ecommerce-etl") -> SparkSession:
    """Build a local SparkSession. The pipeline must run without an external cluster."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )

# helper function to see the original data after the read, avoiding data being silently dropped as nulls
def _read_as_strings(spark: SparkSession, path: Path, columns: list) -> DataFrame:
    """
    Read a csv with a string-only schema so every value is read
    """
    string_schema = StructType([StructField(c, StringType(), True) for c in columns])
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(string_schema)
        .csv(str(path))
    )


def load_with_schema(spark: SparkSession, path: Path, target_schema: StructType) -> Tuple[DataFrame, DataFrame]:
    """
    Load a csv and cast each column to the respective type
    
    Returns:
        A tuple (clean_df, rejected_df). 
        A row is rejected if any non-string column had a non-empty raw value that produced NULL after .cast(...).
    """
    columns = [f.name for f in target_schema.fields]
    raw = _read_as_strings(spark, path, columns)

    cast_exprs = []
    cast_failed_flags = []
    for field in target_schema.fields:
        raw_col = F.col(field.name)
        if isinstance(field.dataType, StringType):
            cast_exprs.append(raw_col.alias(field.name))
            continue
        casted = raw_col.cast(field.dataType)
        cast_exprs.append(casted.alias(field.name))
        cast_failed_flags.append(
            (
                raw_col.isNotNull()
                & (F.trim(raw_col) != "")
                & casted.isNull()
            )
        )

    if cast_failed_flags:
        any_failed = cast_failed_flags[0]
        for flag in cast_failed_flags[1:]:
            any_failed = any_failed | flag
    else:
        any_failed = F.lit(False)

    raw_aliased = [F.col(c).alias(f"_raw_{c}") for c in columns]
    annotated = raw.select(*raw_aliased, *cast_exprs, any_failed.alias("_cast_failed"))

    clean_df = annotated.where(~F.col("_cast_failed")).select(*columns)
    rejected_df = annotated.where(F.col("_cast_failed")).select(
        *[F.col(f"_raw_{c}").alias(c) for c in columns]
    )
    return clean_df, rejected_df

# runs load_with_schema for each of the four sources and returns dictionary of dataframes
def ingest_all(spark: SparkSession, data_dir: Path = DATA_DIR) -> Dict[str, Dict[str, DataFrame]]:
    """
    Ingest all four source tables. 
    
    Returns:
        dict{name: {'clean': df, 'rejected': df}}
    """
    out: Dict[str, Dict[str, DataFrame]] = {}
    for name, (filename, schema) in SOURCE_TABLES.items():
        clean, rejected = load_with_schema(spark, data_dir / filename, schema)
        out[name] = {"clean": clean, "rejected": rejected}
    return out

