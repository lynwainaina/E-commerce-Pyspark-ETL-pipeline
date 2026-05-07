"""
Entry point for the e-commerce ETL pipeline.

Run with:
    python pipeline.py or python3 pipeline.py depending with the os

Stages (each lives in its own module):
    1. Extract  - src.extract.data -> (load raw input data & enforce schema)
    2. Transform - src.transform.transformations -> (clean, join, enrich, window aggs)
    3. Load - src.load.results -> (return analysis + write outputs)
"""

from src.extract.data import create_spark_session
from src.load.results import build_outputs, write_all_outputs, ENRICHED_DIR, SUMMARY_DIR


def main() -> None:
    spark = create_spark_session()
    try:
        outputs = build_outputs(spark)
        write_all_outputs(outputs)
        print(f"Pipeline completed.")
        print(f"Enriched parquet -> {ENRICHED_DIR}")
        print(f"Summary CSVs     -> {SUMMARY_DIR}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
