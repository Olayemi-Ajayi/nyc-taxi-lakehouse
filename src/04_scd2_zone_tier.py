# Databricks notebook source
# Phase 4: SCD Type 2 history of zone demand tier, with Change Data Feed

CATALOG = "workspace"
DIM = f"{CATALOG}.gold.dim_zone_tier"
SILVER = f"{CATALOG}.silver.trips"

# 1. History table, with Change Data Feed switched on
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DIM} (
        pickup_zip BIGINT,
        zone_tier STRING,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN
    )
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# 2. Today's tier for every zone, based on trip volume
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW zone_source AS
    SELECT pickup_zip,
           CASE NTILE(3) OVER (ORDER BY COUNT(*) DESC, pickup_zip)
                WHEN 1 THEN 'high' WHEN 2 THEN 'medium' ELSE 'low' END AS zone_tier
    FROM {SILVER}
    WHERE pickup_zip IS NOT NULL
    GROUP BY pickup_zip
""")

# 3a. Close the old version of any zone whose tier changed
spark.sql(f"""
    MERGE INTO {DIM} t USING zone_source s
    ON t.pickup_zip = s.pickup_zip AND t.is_current = true
    WHEN MATCHED AND t.zone_tier <> s.zone_tier THEN
      UPDATE SET effective_to = current_timestamp(), is_current = false
""")

# 3b. Add a new current version for new zones and changed zones
spark.sql(f"""
    INSERT INTO {DIM}
    SELECT s.pickup_zip, s.zone_tier, current_timestamp(),
           CAST(NULL AS TIMESTAMP), true
    FROM zone_source s
    LEFT JOIN {DIM} t
      ON t.pickup_zip = s.pickup_zip AND t.is_current = true
    WHERE t.pickup_zip IS NULL
""")

# 4. Report
print("Current rows:", spark.sql(f"SELECT COUNT(*) FROM {DIM} WHERE is_current").first()[0])
print("History rows:", spark.sql(f"SELECT COUNT(*) FROM {DIM} WHERE NOT is_current").first()[0])

display(spark.sql(f"""
    SELECT pickup_zip, COUNT(*) AS versions
    FROM {DIM} GROUP BY pickup_zip HAVING COUNT(*) > 1
    ORDER BY versions DESC LIMIT 10
"""))

display(spark.sql(f"""
    SELECT _change_type, COUNT(*) AS n
    FROM table_changes('{DIM}', 1) GROUP BY _change_type
"""))
