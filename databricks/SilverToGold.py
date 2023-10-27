from pyspark.sql.functions import col, when, monotonically_increasing_id
from pyspark.sql.types import StringType

storage_account_name = "datacohortworkspacelabs"
storage_access_key = "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ=="
silver_container = "silver-phanai"
gold_container = "gold-phanai"

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_access_key)

silver_path = f"abfss://{silver_container}@{storage_account_name}.dfs.core.windows.net/"
gold_path = f"abfss://{gold_container}@{storage_account_name}.dfs.core.windows.net/"

df_silver = spark.read.format("delta").load(silver_path)

dim_time = df_silver.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
)

dim_time.write.format("delta").mode("overwrite").save(gold_path + "DimTime")

dim_location = df_silver.select(
    "PULocationID",
    "DOLocationID",   
)

pu_conditions = [
    (col("PULocationID").between(1, 66), "West Side"),
    (col("PULocationID").between(67, 133), "Lower Manhattan"),
    (col("PULocationID").between(134, 200), "Midtown Manhattan"),
    (col("PULocationID").between(201, 265), "Upper Manhattan")
]

dim_location = dim_location.withColumn(
    "PULocationName",
    when(pu_conditions[0][0], pu_conditions[0][1])
    .when(pu_conditions[1][0], pu_conditions[1][1])
    .when(pu_conditions[2][0], pu_conditions[2][1])
    .when(pu_conditions[3][0], pu_conditions[3][1])
    .otherwise(None)
    .cast(StringType())
)

dim_location.write.format("delta").mode("overwrite").save(gold_path + "DimLocation")

dim_vendor = df_silver.select(
    "VendorID",
)

vendor_conditions = [
    (col("VendorID") == 1, "CMT"),
    (col("VendorID") == 2, "VTS"),
    (col("VendorID") == 4, "DDS")
]

dim_vendor = dim_vendor.withColumn(
    "vendor_name",
    when(vendor_conditions[0][0], vendor_conditions[0][1])
    .when(vendor_conditions[1][0], vendor_conditions[1][1])
    .when(vendor_conditions[2][0], vendor_conditions[2][1])
    .otherwise(None)
    .cast(StringType())
)

dim_vendor.write.format("delta").mode("overwrite").save(gold_path + "DimVendor")

fact_table = df_silver.select(
    "VendorID",
    "PULocationID",
    "DOLocationID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge"
)

fact_table = fact_table.withColumn("TripID", monotonically_increasing_id())

fact_table.write.format("delta").mode("overwrite").save(gold_path + "FactTrip")