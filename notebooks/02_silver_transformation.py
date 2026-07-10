"""
ClaimStream — Silver Layer
Data cleaning, validation and enrichment using PySpark + Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import os

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

builder = SparkSession.builder \
    .appName("ClaimStream-Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:4.3.1")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark session started")

bronze_path = "data/delta/bronze"
silver_path = "data/delta/silver"

# Read Bronze
print(f"\n📖 Reading Bronze layer...")
bronze_df = spark.read.format("delta").load(bronze_path)
print(f"   Bronze records: {bronze_df.count()}")

# ─────────────────────────────────────────
# SILVER TRANSFORMATIONS
# ─────────────────────────────────────────

silver_df = bronze_df \
    .filter(col("claim_id").isNotNull()) \
    .filter(col("payer").isNotNull()) \
    .filter(col("billed_amount") > 0) \
    .filter(col("status").isin("PAID", "DENIED")) \
    .withColumn("date_of_service", to_date(col("date_of_service"), "yyyy-MM-dd")) \
    .withColumn("billed_amount", col("billed_amount").cast(DoubleType())) \
    .withColumn("allowed_amount", col("allowed_amount").cast(DoubleType())) \
    .withColumn("paid_amount", col("paid_amount").cast(DoubleType())) \
    .withColumn("is_denied", when(col("status") == "DENIED", lit(True)).otherwise(lit(False))) \
    .withColumn("denial_reason", when(col("denial_reason").isNull(), lit("N/A")).otherwise(col("denial_reason"))) \
    .withColumn("payment_ratio", when(col("billed_amount") > 0, round(col("paid_amount") / col("billed_amount"), 4)).otherwise(lit(0.0))) \
    .withColumn("claim_month", date_format(col("date_of_service"), "yyyy-MM")) \
    .withColumn("silver_processed_at", current_timestamp()) \
    .withColumn("pipeline_layer", lit("silver")) \
    .drop("source_file", "bronze_ingested_at")

# Validation report
total = silver_df.count()
denied = silver_df.filter(col("is_denied") == True).count()
paid = total - denied

print(f"\n📊 Silver Validation Report:")
print(f"   Total valid records: {total}")
print(f"   Paid claims:         {paid}")
print(f"   Denied claims:       {denied}")
denial_pct = denied/total*100
print(f"   Denial rate:         {denial_pct:.1f}%")

# Payer distribution
print(f"\n📈 Denial rate by payer:")
silver_df.groupBy("payer") \
    .agg(
        count("*").alias("total"),
        sum(when(col("is_denied"), 1).otherwise(0)).alias("denied"),
        round(sum(when(col("is_denied"), 1).otherwise(0)) / count("*") * 100, 1).alias("denial_rate_pct")
    ) \
    .orderBy(desc("denial_rate_pct")) \
    .show()

# Write Silver Delta
print(f"\n📦 Writing to Silver Delta Lake: {silver_path}")
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(silver_path)

silver_count = spark.read.format("delta").load(silver_path).count()
print(f"\n✅ Silver layer complete!")
print(f"   Records in Silver: {silver_count}")

spark.stop()
