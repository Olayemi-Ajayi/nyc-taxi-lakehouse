# Databricks notebook source
# Phase 1, step 2: ingest landed JSON files into a bronze Delta table
# with Auto Loader. Re-running only processes NEW files (checkpoint tracks them).

from pyspark.sql import functions as F

CATALOG = "workspace"
BASE = f"/Volumes/{CATALOG}/raw/landing"
SOURCE_PATH = f"{BASE}/trips"
SCHEMA_PATH = f"{BASE}/_schemas/trips"            # Auto Loader stores inferred schema here
CHECKPOINT_PATH = f"{BASE}/_checkpoints/bronze_trips"  # progress tracking (exactly-once)
TARGET_TABLE = f"{CATALOG}.bronze.trips"

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(SOURCE_PATH)
    # Audit columns: where each row came from and when it arrived
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

query = (
    stream.writeStream
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)   # process everything new, then stop
    .toTable(TARGET_TABLE)
)
query.awaitTermination()

# Checks
print("Rows in bronze:", spark.table(TARGET_TABLE).count())
display(spark.table(TARGET_TABLE).limit(10))
display(spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}"))
