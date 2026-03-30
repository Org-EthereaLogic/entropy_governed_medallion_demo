# Databricks notebook source
# MAGIC %md
# MAGIC # Entropy-Governed Data Quality Deep Dive
# MAGIC
# MAGIC This notebook demonstrates how Shannon Entropy can be used as a
# MAGIC **governing data quality signal** in a Databricks medallion pipeline.
# MAGIC
# MAGIC Traditional quality checks catch presence problems (nulls, type errors).
# MAGIC Entropy catches **distribution problems** that rule-based checks miss:
# MAGIC - Silent source failures (column collapses to one value)
# MAGIC - Schema drift (new categories injected)
# MAGIC - Cardinality anomalies (join keys clustering)
# MAGIC - Freshness decay (timestamps repeating)
# MAGIC
# MAGIC **Author:** Anthony Johnson | EthereaLogic LLC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup — Import the Entropy Framework

# COMMAND ----------

import math
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Core entropy computation
def column_entropy(df, column_name):
    """Compute Shannon Entropy H(X) for a single column."""
    total_count = df.count()
    if total_count == 0:
        return 0.0
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

def normalized_entropy(df, column_name):
    """Compute normalized entropy H(X)/log2(n) scaled to [0,1]."""
    h = column_entropy(df, column_name)
    n_distinct = df.select(column_name).distinct().count()
    if n_distinct <= 1:
        return 0.0
    h_max = math.log2(n_distinct)
    return round(h / h_max, 6) if h_max > 0 else 0.0

print("Entropy framework loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load NYC Taxi Data as Our Silver Table

# COMMAND ----------

# Use the built-in NYC Taxi dataset (available in every Databricks workspace)
taxi_df = spark.read.table("samples.nyctaxi.trips")
print(f"Total rows: {taxi_df.count():,}")
display(taxi_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Compute Entropy Baseline for Key Columns
# MAGIC
# MAGIC This is what happens at first Silver load — we measure the
# MAGIC information content of each column and store it as baseline.

# COMMAND ----------

columns_to_monitor = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip",
]

print("=" * 70)
print(f"{'Column':<28} {'H(X)':<10} {'H_norm':<10} {'Distinct':<10} {'Class'}")
print("=" * 70)

baseline = {}
for col in columns_to_monitor:
    h = column_entropy(taxi_df, col)
    h_norm = normalized_entropy(taxi_df, col)
    n_dist = taxi_df.select(col).distinct().count()

    # Classify
    if h == 0.0:
        cls = "CONSTANT"
    elif h_norm < 0.15:
        cls = "VERY_LOW"
    elif h_norm < 0.40:
        cls = "LOW"
    elif h_norm < 0.70:
        cls = "MODERATE"
    elif h_norm < 0.90:
        cls = "HIGH"
    else:
        cls = "VERY_HIGH"

    baseline[col] = {"entropy": h, "normalized_entropy": h_norm, "class": cls}
    print(f"{col:<28} {h:<10.4f} {h_norm:<10.4f} {n_dist:<10} {cls}")

print("=" * 70)
print("\nBaseline captured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Simulate Distribution Drift
# MAGIC
# MAGIC Now we simulate what happens when a source system silently breaks.
# MAGIC We'll create a "drifted" version of the data where:
# MAGIC - `pickup_zip` collapses to a single value (source system defaulting)
# MAGIC - `fare_amount` loses variation (flat-rate override)
# MAGIC - Other columns remain stable

# COMMAND ----------

# Create drifted data — simulates a silent source failure
drifted_df = taxi_df.withColumn(
    "pickup_zip",
    F.lit("10001")  # All pickup zips collapse to one value
).withColumn(
    "fare_amount",
    F.when(F.rand() < 0.95, F.lit(25.0)).otherwise(F.col("fare_amount"))
    # 95% of fares become $25.00 — flat-rate override
)

print("Drifted dataset created.")
print(f"  pickup_zip distinct values: {drifted_df.select('pickup_zip').distinct().count()}")
print(f"  fare_amount near-constant: {drifted_df.filter(F.col('fare_amount') == 25.0).count()} / {drifted_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Detect Drift Using Entropy Comparison
# MAGIC
# MAGIC This is the core innovation. We compare current entropy against
# MAGIC baseline and flag columns where the distribution has shifted
# MAGIC beyond our threshold.

# COMMAND ----------

ENTROPY_DROP_THRESHOLD = 0.50  # Flag if entropy drops > 50%

print("=" * 80)
print(f"{'Column':<28} {'Baseline H':<12} {'Current H':<12} {'Change %':<12} {'Verdict'}")
print("=" * 80)

health_scores = []
for col in columns_to_monitor:
    h_baseline = baseline[col]["entropy"]
    h_current = column_entropy(drifted_df, col)

    if h_baseline == 0.0:
        pct_change = 0.0 if h_current == 0.0 else 1.0
    else:
        pct_change = (h_current - h_baseline) / h_baseline

    if pct_change < -ENTROPY_DROP_THRESHOLD:
        verdict = "🔴 COLLAPSE"
        stability = max(0.0, 1.0 - abs(pct_change))
    elif pct_change > ENTROPY_DROP_THRESHOLD:
        verdict = "🟡 SPIKE"
        stability = max(0.0, 1.0 - abs(pct_change))
    else:
        verdict = "🟢 STABLE"
        stability = max(0.0, 1.0 - abs(pct_change))

    health_scores.append(stability)
    print(f"{col:<28} {h_baseline:<12.4f} {h_current:<12.4f} {pct_change:<+12.2%} {verdict}")

print("=" * 80)

# Composite health score
composite_health = sum(health_scores) / len(health_scores)
gate_threshold = 0.70

print(f"\nComposite Health Score: {composite_health:.4f}")
print(f"Gate Threshold:        {gate_threshold}")
print(f"Gold Refresh:          {'✅ ALLOWED' if composite_health >= gate_threshold else '🚫 BLOCKED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Why This Matters
# MAGIC
# MAGIC The drifted data would **pass every traditional quality check**:
# MAGIC - No nulls in pickup_zip ✅
# MAGIC - No nulls in fare_amount ✅
# MAGIC - All values are valid types ✅
# MAGIC - No duplicates ✅
# MAGIC
# MAGIC But the **entropy-based check caught the problem**:
# MAGIC - pickup_zip entropy collapsed → silent source default
# MAGIC - fare_amount entropy collapsed → flat-rate override
# MAGIC
# MAGIC Without entropy monitoring, these corrupted distributions would
# MAGIC flow into Gold KPIs and produce misleading executive dashboards.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify: Traditional Checks Miss the Problem

# COMMAND ----------

# Traditional null check — passes on drifted data
null_pickup = drifted_df.filter(F.col("pickup_zip").isNull()).count()
null_fare = drifted_df.filter(F.col("fare_amount").isNull()).count()

print(f"Null check on pickup_zip: {null_pickup} nulls → {'PASS' if null_pickup == 0 else 'FAIL'}")
print(f"Null check on fare_amount: {null_fare} nulls → {'PASS' if null_fare == 0 else 'FAIL'}")
print(f"\nTraditional checks say everything is fine.")
print(f"Entropy check caught the silent corruption.")
print(f"\nThat is the value of information-theoretic quality governance.")
