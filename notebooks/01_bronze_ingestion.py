"""
ClaimStream — Bronze Layer
Raw claims ingestion using PySpark Structured Streaming + Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta import *
import os

# Initialize Spark with Delta Lake
builder = SparkSession.builder \
    .appName("ClaimStream-Bronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:4.3.1")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark session started")
print(f"   Spark version: {spark.version}")

# Define schema for incoming claims
claims_schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("date_of_service", StringType(), True),
    StructField("cpt_code", StringType(), True),
    StructField("cpt_description", StringType(), True),
    StructField("payer", StringType(), True),
    StructField("billed_amount", DoubleType(), True),
    StructField("allowed_amount", DoubleType(), True),
    StructField("paid_amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("denial_reason", StringType(), True),
    StructField("provider_npi", StringType(), True),
    StructField("diagnosis_code", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True)
])

# Read streaming data from JSON files
stream_path = "data/stream"
bronze_path = "data/delta/bronze"

print(f"\n📥 Reading stream from: {stream_path}")

streaming_df = spark.readStream \
    .format("json") \
    .schema(claims_schema) \
    .option("maxFilesPerTrigger", 10) \
    .load(stream_path)

# Add bronze metadata
from pyspark.sql.functions import current_timestamp, input_file_name, lit

bronze_df = streaming_df \
    .withColumn("bronze_ingested_at", current_timestamp()) \
    .withColumn("source_file", input_file_name()) \
    .withColumn("pipeline_layer", lit("bronze"))

# Write to Delta Lake - Bronze
print(f"📦 Writing to Bronze Delta Lake: {bronze_path}")

bronze_query = bronze_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "data/checkpoints/bronze") \
    .option("mergeSchema", "true") \
    .start(bronze_path)

# Wait for completion
bronze_query.awaitTermination(timeout=30)
bronze_query.stop()

# Verify
bronze_count = spark.read.format("delta").load(bronze_path).count()
print(f"\n✅ Bronze layer complete!")
print(f"   Records in Bronze: {bronze_count}")

spark.stop()
