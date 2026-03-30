"""
Entropy baseline management for Silver tables.

Captures, stores, and retrieves per-column entropy baselines
that serve as the reference point for drift detection.

Author: Anthony Johnson | EthereaLogic LLC
"""

from __future__ import annotations

from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from entropy_governed_medallion.entropy.shannon import (
    entropy_summary_to_df,
    table_entropy_profile,
)

BASELINE_TABLE = "silver_quality.entropy_baselines"
AUDIT_TABLE = "silver_quality.entropy_audit_log"


def capture_baseline(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    exclude_columns: Optional[List[str]] = None,
    catalog: str = "demo_dev",
) -> DataFrame:
    """
    Compute and persist an entropy baseline for a Silver table.

    This should be run once when a Silver table is first populated
    with trusted data, then updated only when the baseline is
    intentionally recalibrated.
    """
    exclude = exclude_columns or ["source_system", "source_file_path", "ingest_ts"]
    profile = table_entropy_profile(df, exclude_columns=exclude)

    baseline_df = entropy_summary_to_df(spark, profile, table_name)
    baseline_df = baseline_df.withColumn("baseline_version", F.lit(1))

    target = f"{catalog}.{BASELINE_TABLE}"
    baseline_df.write.format("delta").mode("append").saveAsTable(target)

    return baseline_df


def get_latest_baseline(
    spark: SparkSession,
    table_name: str,
    catalog: str = "demo_dev",
) -> List[Dict]:
    """
    Retrieve the most recent baseline for a given table.

    Returns:
        List of dicts representing the per-column baseline profile.
        Empty list if no baseline exists.
    """
    target = f"{catalog}.{BASELINE_TABLE}"

    baseline_df = (
        spark.read.table(target)
        .filter(F.col("table_name") == table_name)
        .orderBy(F.col("measurement_ts").desc())
    )

    latest_ts = baseline_df.select(F.max("measurement_ts")).collect()[0][0]
    if latest_ts is None:
        return []

    latest = baseline_df.filter(F.col("measurement_ts") == latest_ts)
    return [row.asDict() for row in latest.collect()]


def log_measurement(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    exclude_columns: Optional[List[str]] = None,
    catalog: str = "demo_dev",
) -> DataFrame:
    """
    Compute and log a new entropy measurement for audit purposes.

    Every load cycle should call this so the audit trail captures
    the entropy trajectory over time — enabling trend analysis
    and post-incident forensics.
    """
    exclude = exclude_columns or ["source_system", "source_file_path", "ingest_ts"]
    profile = table_entropy_profile(df, exclude_columns=exclude)

    measurement_df = entropy_summary_to_df(spark, profile, table_name)

    target = f"{catalog}.{AUDIT_TABLE}"
    measurement_df.write.format("delta").mode("append").saveAsTable(target)

    return measurement_df
