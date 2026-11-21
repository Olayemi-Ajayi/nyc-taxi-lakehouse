# Databricks notebook source
# Phase 1, step 1: simulate "arriving" taxi trip files.
# Each run writes a few small JSON batches into the landing Volume,
# so Auto Loader has new files to pick up.

import time
from pyspark.sql import functions as F

CATALOG = "workspace"
LANDING = f"/Volumes/{CATALOG}/raw/landing/trips"

BATCHES = 5        # how many files-folders to write per run
ROWS_PER_BATCH = 2000
PAUSE_SECONDS = 5  # gap between batches, to feel like a stream

# Built-in sample data (check it exists: SELECT * FROM samples.nyctaxi.trips LIMIT 5)
source = spark.table("samples.nyctaxi.trips")

for i in range(BATCHES):
    batch = (
        source.orderBy(F.rand())
        .limit(ROWS_PER_BATCH)
        .withColumn("trip_id", F.expr("uuid()"))
        .withColumn("event_ts", F.current_timestamp())
        # Make ~1% of fares negative on purpose.
        # In Phase 2 your quality rules will catch these.
        .withColumn(
            "fare_amount",
            F.when(F.rand() < 0.01, -F.col("fare_amount")).otherwise(F.col("fare_amount")),
        )
    )
    path = f"{LANDING}/batch_{int(time.time())}_{i}"
    batch.coalesce(1).write.mode("overwrite").json(path)
    print(f"Wrote {ROWS_PER_BATCH} rows to {path}")
    time.sleep(PAUSE_SECONDS)

# Check: list what landed
display(dbutils.fs.ls(LANDING))
