# Databricks notebook source
# Phase 6: Unity Catalog governance: comments, tags, secure view, grants

CATALOG = "workspace"
SILVER = f"{CATALOG}.silver.trips"
QUAR = f"{CATALOG}.silver.trips_quarantine"
DIM = f"{CATALOG}.gold.dim_zone_tier"
VIEW = f"{CATALOG}.gold.trips_secure"


def run(sql):
    try:
        spark.sql(sql)
        print("OK:", sql.strip().splitlines()[0][:90])
    except Exception as e:
        print("SKIPPED:", sql.strip().splitlines()[0][:90])
        print("  reason:", str(e)[:200])


# 1. Document the tables (data discovery)
run(f"COMMENT ON TABLE {SILVER} IS 'Cleaned taxi trips: typed, de-duplicated, quality-checked'")
run(f"COMMENT ON TABLE {QUAR} IS 'Rows that failed quality rules, with the reject reason'")
run(f"COMMENT ON TABLE {DIM} IS 'SCD Type 2 history of zone demand tier'")

# 2. Tag a sensitive column
run(f"ALTER TABLE {SILVER} ALTER COLUMN fare_amount SET TAGS ('sensitivity' = 'financial')")

# 3. Secure view: column masking + row filtering for anyone outside finance_analysts
run(f"""
CREATE OR REPLACE VIEW {VIEW} AS
SELECT trip_id, pickup_ts, dropoff_ts, trip_distance,
       CASE WHEN is_account_group_member('finance_analysts')
            THEN fare_amount ELSE NULL END AS fare_amount,
       pickup_zip, dropoff_zip
FROM {SILVER}
WHERE is_account_group_member('finance_analysts')
   OR pickup_zip BETWEEN 10001 AND 10099
""")

# 4. Grants: read the view only, never the underlying table
run(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `account users`")
run(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO `account users`")
run(f"GRANT SELECT ON VIEW {VIEW} TO `account users`")
run(f"REVOKE SELECT ON TABLE {SILVER} FROM `account users`")

# 5. Prove it
print("Base table rows:", spark.table(SILVER).count())
print("Secure view rows:", spark.table(VIEW).count())
print("Fares visible in view:", spark.sql(f"SELECT COUNT(fare_amount) FROM {VIEW}").first()[0])
display(spark.sql(f"SELECT trip_id, pickup_zip, fare_amount FROM {SILVER} LIMIT 5"))
display(spark.sql(f"SELECT trip_id, pickup_zip, fare_amount FROM {VIEW} LIMIT 5"))
display(spark.sql(f"SHOW GRANTS ON VIEW {VIEW}"))
