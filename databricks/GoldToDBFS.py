storage_account_name = "datacohortworkspacelabs"
storage_access_key = "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ=="
silver_container = "silver-phanai"
gold_container = "gold-phanai"

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_access_key)

dim_time = spark.read.format("delta").load("abfss://gold-phanai@datacohortworkspacelabs.dfs.core.windows.net/DimTime")
dim_location = spark.read.format("delta").load("abfss://gold-phanai@datacohortworkspacelabs.dfs.core.windows.net/DimLocation")
dim_vendor = spark.read.format("delta").load("abfss://gold-phanai@datacohortworkspacelabs.dfs.core.windows.net/DimVendor")
fact_table = spark.read.format("delta").load("abfss://gold-phanai@datacohortworkspacelabs.dfs.core.windows.net/FactTrip")

"""
%sql
CREATE DATABASE IF NOT EXISTS database_pp;
"""

dim_time.write.mode("overwrite").saveAsTable("database_pp.dim_time")
dim_location.write.mode("overwrite").saveAsTable("database_pp.dim_location")
dim_vendor.write.mode("overwrite").saveAsTable("database_pp.dim_vendor")
fact_table.write.mode("overwrite").saveAsTable("database_pp.fact_table")