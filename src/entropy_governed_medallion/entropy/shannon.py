"""
Shannon Entropy computation for PySpark DataFrames.

Uses information-theoretic measurement to quantify the distribution
characteristics of data columns. This enables detection of quality
issues that rule-based validation cannot catch: silent source failures,
distribution drift, cardinality collapse, and freshness decay.

Theory:
    H(X) = -Σ p(xᵢ) × log₂(p(xᵢ))

    Where p(xᵢ) is the observed probability of each distinct value.
    - H = 0 means the column is constant (zero information).
    - H = log₂(n) is the theoretical maximum for n distinct values.
    - Normalized entropy H/log₂(n) gives a 0-1 scale independent of cardinality.

Author: Anthony Johnson | EthereaLogic LLC
"""

import math
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def column_entropy(df: DataFrame, column_name: str) -> float:
    """
    Compute Shannon Entropy H(X) for a single column.

    Args:
        df: PySpark DataFrame
        column_name: Name of the column to measure

    Returns:
        Shannon entropy in bits. Returns 0.0 for empty or single-value columns.
    """
    total_count = df.count()
    if total_count == 0:
        return 0.0

    # Compute value frequencies, treating nulls as a distinct category
    value_counts = (
        df.select(F.col(column_name).cast("string").alias("val"))
        .fillna({"val": "__NULL__"})
        .groupBy("val")
        .agg(F.count("*").alias("freq"))
        .collect()
    )

    entropy = 0.0
    for row in value_counts:
        p = row["freq"] / total_count
        if p > 0:
            entropy -= p * math.log2(p)

    return round(entropy, 6)


def normalized_entropy(df: DataFrame, column_name: str) -> float:
    """
    Compute normalized Shannon Entropy H(X) / log₂(n).

    Scales entropy to [0, 1] regardless of cardinality.
    - 0.0 = perfectly uniform (constant column)
    - 1.0 = maximum entropy (all values equally likely)

    Args:
        df: PySpark DataFrame
        column_name: Name of the column to measure

    Returns:
        Normalized entropy between 0.0 and 1.0.
    """
    h = column_entropy(df, column_name)
    n_distinct = df.select(column_name).distinct().count()

    if n_distinct <= 1:
        return 0.0

    h_max = math.log2(n_distinct)
    return round(h / h_max, 6) if h_max > 0 else 0.0


def table_entropy_profile(
    df: DataFrame,
    columns: Optional[List[str]] = None,
    exclude_columns: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Compute entropy profile for all (or selected) columns in a DataFrame.

    Args:
        df: PySpark DataFrame to profile
        columns: Specific columns to measure (default: all)
        exclude_columns: Columns to skip (e.g., ingestion metadata)

    Returns:
        List of dicts with column_name, entropy, normalized_entropy,
        distinct_count, null_count, total_count, and entropy_class.
    """
    exclude = set(exclude_columns or [])
    target_columns = columns or [c for c in df.columns if c not in exclude]
    total_count = df.count()

    profile = []
    for col_name in target_columns:
        if col_name in exclude:
            continue

        h = column_entropy(df, col_name)
        h_norm = normalized_entropy(df, col_name)
        n_distinct = df.select(col_name).distinct().count()
        n_null = df.filter(F.col(col_name).isNull()).count()

        # Classify entropy level for human-readable interpretation
        entropy_class = _classify_entropy(h, h_norm, n_distinct, total_count)

        profile.append({
            "column_name": col_name,
            "entropy": h,
            "normalized_entropy": h_norm,
            "distinct_count": n_distinct,
            "null_count": n_null,
            "null_ratio": round(n_null / total_count, 4) if total_count > 0 else 0.0,
            "total_count": total_count,
            "entropy_class": entropy_class,
        })

    return profile


def _classify_entropy(
    h: float, h_norm: float, n_distinct: int, total_count: int
) -> str:
    """
    Classify a column's entropy into a human-readable quality signal.

    This classification helps non-technical stakeholders understand
    what the entropy measurement means for their data.
    """
    if n_distinct <= 1 or h == 0.0:
        return "CONSTANT"
    elif h_norm < 0.15:
        return "VERY_LOW"
    elif h_norm < 0.40:
        return "LOW"
    elif h_norm < 0.70:
        return "MODERATE"
    elif h_norm < 0.90:
        return "HIGH"
    else:
        return "VERY_HIGH"


def entropy_summary_to_df(
    spark: SparkSession, profile: List[Dict], table_name: str
) -> DataFrame:
    """
    Convert an entropy profile into a PySpark DataFrame for persistence.

    Adds table_name and measurement_ts for audit trail storage.
    """
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType([
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), False),
        StructField("entropy", DoubleType(), False),
        StructField("normalized_entropy", DoubleType(), False),
        StructField("distinct_count", LongType(), False),
        StructField("null_count", LongType(), False),
        StructField("null_ratio", DoubleType(), False),
        StructField("total_count", LongType(), False),
        StructField("entropy_class", StringType(), False),
    ])

    rows = []
    for p in profile:
        rows.append((
            table_name,
            p["column_name"],
            p["entropy"],
            p["normalized_entropy"],
            p["distinct_count"],
            p["null_count"],
            p["null_ratio"],
            p["total_count"],
            p["entropy_class"],
        ))

    df = spark.createDataFrame(rows, schema)
    return df.withColumn("measurement_ts", F.current_timestamp())
