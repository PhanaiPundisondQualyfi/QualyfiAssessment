from pyspark.sql.functions import col

storage_account_name = "datacohortworkspacelabs"
storage_access_key = "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ=="
bronze_container = "bronze-phanai"
silver_container = "silver-phanai"

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_access_key)

bronze_path_1 = f"abfss://{bronze_container}@{storage_account_name}.dfs.core.windows.net/2019/VendorID=1/"
bronze_path_2 = f"abfss://{bronze_container}@{storage_account_name}.dfs.core.windows.net/2019/VendorID=2/"
bronze_path_4 = f"abfss://{bronze_container}@{storage_account_name}.dfs.core.windows.net/2019/VendorID=4/"
silver_path = f"abfss://{silver_container}@{storage_account_name}.dfs.core.windows.net/"

df1 = spark.read.format("delta").load(bronze_path_1)
df2 = spark.read.format("delta").load(bronze_path_2)
df4 = spark.read.format("delta").load(bronze_path_4)

df1 = df1.filter((col("total_amount").isNotNull()) & (col("total_amount") > 0))
df1 = df1.filter(col("trip_distance") > 0)

df2 = df2.filter((col("total_amount").isNotNull()) & (col("total_amount") > 0))
df2 = df2.filter(col("trip_distance") > 0)

df4 = df4.filter((col("total_amount").isNotNull()) & (col("total_amount") > 0))
df4 = df4.filter(col("trip_distance") > 0)

df1.write.partitionBy("PULocationID").format("delta").mode("overwrite").save(silver_path)
df2.write.partitionBy("PULocationID").format("delta").mode("append").save(silver_path)
df4.write.partitionBy("PULocationID").format("delta").mode("append").save(silver_path)