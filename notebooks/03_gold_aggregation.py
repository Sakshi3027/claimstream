"""
ClaimStream — Gold Layer
Aggregated analytics for business intelligence using PySpark + Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import os

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

builder = SparkSession.builder \
    .appName("ClaimStream-Gold") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:4.3.1") \
    .config("spark.driver.bindAddress", "127.0.0.1")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark session started")

silver_path = "data/delta/silver"
gold_path_payer = "data/delta/gold/denial_by_payer"
gold_path_cpt = "data/delta/gold/denial_by_cpt"
gold_path_reason = "data/delta/gold/denial_by_reason"
gold_path_monthly = "data/delta/gold/monthly_trends"

# Read Silver
print(f"\n📖 Reading Silver layer...")
silver_df = spark.read.format("delta").load(silver_path)
print(f"   Silver records: {silver_df.count()}")

# ─────────────────────────────────────────
# GOLD TABLE 1 — Denial rate by payer
# ─────────────────────────────────────────
print("\n🥇 Building Gold: Denial by Payer...")
denial_by_payer = silver_df \
    .groupBy("payer") \
    .agg(
        count("*").alias("total_claims"),
        sum(when(col("is_denied"), 1).otherwise(0)).alias("denied_claims"),
        sum(when(~col("is_denied"), 1).otherwise(0)).alias("paid_claims"),
        avg("billed_amount").alias("avg_billed"),
        avg("paid_amount").alias("avg_paid"),
        sum("billed_amount").alias("total_billed"),
        sum("paid_amount").alias("total_paid")
    ) \
    .withColumn("denial_rate_pct", (col("denied_claims") / col("total_claims") * 100)) \
    .withColumn("gold_created_at", current_timestamp()) \
    .orderBy(desc("denial_rate_pct"))

denial_by_payer.show(truncate=False)
denial_by_payer.write.format("delta").mode("overwrite").save(gold_path_payer)

# ─────────────────────────────────────────
# GOLD TABLE 2 — Denial rate by CPT code
# ─────────────────────────────────────────
print("\n🥇 Building Gold: Denial by CPT Code...")
denial_by_cpt = silver_df \
    .groupBy("cpt_code", "cpt_description") \
    .agg(
        count("*").alias("total_claims"),
        sum(when(col("is_denied"), 1).otherwise(0)).alias("denied_claims"),
        avg("billed_amount").alias("avg_billed")
    ) \
    .withColumn("denial_rate_pct", (col("denied_claims") / col("total_claims") * 100)) \
    .withColumn("gold_created_at", current_timestamp()) \
    .orderBy(desc("denial_rate_pct"))

denial_by_cpt.show(truncate=False)
denial_by_cpt.write.format("delta").mode("overwrite").save(gold_path_cpt)

# ─────────────────────────────────────────
# GOLD TABLE 3 — Top denial reasons
# ─────────────────────────────────────────
print("\n🥇 Building Gold: Denial Reasons...")
denial_by_reason = silver_df \
    .filter(col("is_denied") == True) \
    .groupBy("denial_reason") \
    .agg(
        count("*").alias("total_denials"),
        avg("billed_amount").alias("avg_billed_amount"),
        count_distinct("payer").alias("payers_affected")
    ) \
    .withColumn("gold_created_at", current_timestamp()) \
    .orderBy(desc("total_denials"))

denial_by_reason.show(truncate=False)
denial_by_reason.write.format("delta").mode("overwrite").save(gold_path_reason)

# ─────────────────────────────────────────
# GOLD TABLE 4 — Monthly trends
# ─────────────────────────────────────────
print("\n🥇 Building Gold: Monthly Trends...")
monthly_trends = silver_df \
    .groupBy("claim_month") \
    .agg(
        count("*").alias("total_claims"),
        sum(when(col("is_denied"), 1).otherwise(0)).alias("denied_claims"),
        sum("billed_amount").alias("total_billed"),
        sum("paid_amount").alias("total_paid")
    ) \
    .withColumn("denial_rate_pct", (col("denied_claims") / col("total_claims") * 100)) \
    .withColumn("gold_created_at", current_timestamp()) \
    .orderBy("claim_month")

monthly_trends.show(truncate=False)
monthly_trends.write.format("delta").mode("overwrite").save(gold_path_monthly)

print("\n✅ Gold layer complete!")
print(f"   Gold tables created: 4")
print(f"   - denial_by_payer")
print(f"   - denial_by_cpt")
print(f"   - denial_by_reason")
print(f"   - monthly_trends")

spark.stop()
