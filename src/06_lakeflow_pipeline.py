import dlt
from pyspark.sql import functions as F

BRONZE = "workspace.bronze.trips"


def typed(df):
    return df.select(
        "trip_id",
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        "trip_distance",
        "fare_amount",
        "pickup_zip",
        "dropoff_zip",
        F.col("event_ts").cast("timestamp").alias("event_ts"),
        "_source_file",
        "_ingested_at",
    )


# Silver: typed trips, with data quality rules (expectations)
@dlt.table(name="silver_trips_lf", comment="Cleaned taxi trips with quality rules")
@dlt.expect_or_drop("valid_fare", "fare_amount > 0")
@dlt.expect_or_drop("valid_times", "dropoff_ts > pickup_ts")
@dlt.expect_or_drop("has_pickup_zip", "pickup_zip IS NOT NULL")
def silver_trips_lf():
    return typed(spark.readStream.table(BRONZE))


# Quarantine: the rows that fail the same rules, with a reason
@dlt.table(name="quarantine_trips_lf", comment="Rows that failed quality rules")
def quarantine_trips_lf():
    df = typed(spark.readStream.table(BRONZE))
    reason = (
        F.when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(), "bad_times")
        .when(F.col("fare_amount").isNull() | (F.col("fare_amount") <= 0), "non_positive_fare")
        .when(F.col("dropoff_ts") <= F.col("pickup_ts"), "dropoff_not_after_pickup")
        .when(F.col("pickup_zip").isNull(), "missing_pickup_zip")
    )
    return (
        df.withColumn("reject_reason", reason)
        .where(F.col("reject_reason").isNotNull())
        .select("trip_id", "pickup_ts", "dropoff_ts", "fare_amount", "pickup_zip", "reject_reason")
    )


# Gold: daily revenue by pickup zip
@dlt.table(name="daily_zone_revenue_lf", comment="Daily trips and fares per pickup zip")
def daily_zone_revenue_lf():
    return (
        dlt.read("silver_trips_lf")
        .groupBy(F.to_date("pickup_ts").alias("trip_date"), "pickup_zip")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        )
    )