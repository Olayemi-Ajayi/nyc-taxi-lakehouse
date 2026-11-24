# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "workspace"
SILVER = f"{CATALOG}.silver.trips"
QUAR = f"{CATALOG}.silver.trips_quarantine"

# 1. Find what we already processed
last_ts = spark.sql(f"""
    SELECT COALESCE(MAX(ts), TIMESTAMP '1970-01-01 00:00:00') AS last_ts FROM (
        SELECT MAX(_ingested_at) AS ts FROM {SILVER}
        UNION ALL
        SELECT MAX(_ingested_at) FROM {QUAR}
    )
""").first()["last_ts"]
print("Processing bronze rows ingested after:", last_ts)

# 2. Take only NEW bronze rows, then type, dedup and check them
new = (
    spark.table(f"{CATALOG}.bronze.trips")
    .filter(F.col("_ingested_at") > F.lit(last_ts))
    .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
    .withColumn("event_ts", F.col("event_ts").cast("timestamp"))
    .dropDuplicates(["trip_id"])
    .select("trip_id", "pickup_ts", "dropoff_ts", "trip_distance", "fare_amount",
            "pickup_zip", "dropoff_zip", "event_ts", "_source_file", "_ingested_at")
)

reason = (
    F.when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(), "bad_timestamp")
     .when(F.col("fare_amount") <= 0, "non_positive_fare")
     .when(F.col("dropoff_ts") <= F.col("pickup_ts"), "dropoff_not_after_pickup")
     .when(F.col("pickup_zip").isNull(), "missing_pickup_zip")
)
checked = new.withColumn("reject_reason", reason).cache()
new_valid = checked.filter("reject_reason IS NULL").drop("reject_reason")
new_bad = checked.filter("reject_reason IS NOT NULL")
print("New rows to process:", checked.count())

# 3. MERGE: insert new trips, update trips we already have
new_valid.createOrReplaceTempView("new_valid")
new_bad.createOrReplaceTempView("new_bad")

spark.sql(f"""
    MERGE INTO {SILVER} t USING new_valid s ON t.trip_id = s.trip_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
spark.sql(f"""
    MERGE INTO {QUAR} t USING new_bad s ON t.trip_id = s.trip_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# 4. Refresh gold from silver
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.daily_zone_revenue AS
    SELECT to_date(pickup_ts) AS trip_date, pickup_zip,
           COUNT(*) AS trips, ROUND(SUM(fare_amount), 2) AS total_fare,
           ROUND(AVG(trip_distance), 2) AS avg_distance
    FROM {SILVER} GROUP BY 1, 2
""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.hourly_demand AS
    SELECT hour(pickup_ts) AS pickup_hour, COUNT(*) AS trips
    FROM {SILVER} GROUP BY 1
""")

# 5. Report
print("silver.trips rows:", spark.table(SILVER).count())
print("silver.trips_quarantine rows:", spark.table(QUAR).count())
display(spark.sql(f"DESCRIBE HISTORY {SILVER}").select(
    "version", "timestamp", "operation", "operationMetrics"))
