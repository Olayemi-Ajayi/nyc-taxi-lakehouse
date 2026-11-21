# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "workspace"
BASE = f"/Volumes/{CATALOG}/raw/landing"
SOURCE_PATH = f"{BASE}/trips"
SCHEMA_PATH = f"{BASE}/_schemas/trips"
CHECKPOINT_PATH = f"{BASE}/_checkpoints/bronze_trips"
TARGET_TABLE = f"{CATALOG}.bronze.trips"

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(SOURCE_PATH)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

query = (
    stream.writeStream
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)
query.awaitTermination()

print("Rows in bronze:", spark.table(TARGET_TABLE).count())
display(spark.table(TARGET_TABLE).limit(10))
display(spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}"))