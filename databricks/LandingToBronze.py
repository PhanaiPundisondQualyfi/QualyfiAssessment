from pyspark.sql.functions import col, input_file_name, current_timestamp
import os
storage_account_name = "datacohortworkspacelabs"
storage_access_key = "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ=="
landing_container = "landing-phanai"
bronze_container = "bronze-phanai"

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_access_key)

landing_path_2019 = f"abfss://{landing_container}@{storage_account_name}.dfs.core.windows.net/yellow_tripdata_2019-*/"
landing_path_2014 = f"abfss://{landing_container}@{storage_account_name}.dfs.core.windows.net/yellow_tripdata_2014-*/"
landing_path_2014 = f"abfss://{landing_container}@{storage_account_name}.dfs.core.windows.net/yellow_tripdata_2010-*/"
bronze_path = f"abfss://{bronze_container}@{storage_account_name}.dfs.core.windows.net/"

df_2019 = spark.read.csv(landing_path_2019, header=True, inferSchema=True)
df_2014 = spark.read.csv(landing_path_2014, header=True, inferSchema=True)
df_2010 = spark.read.csv(landing_path_2014, header=True, inferSchema=True)

df_2019 = df_2019.withColumn("FileName", input_file_name()) \
    .withColumn("CreatedOn", current_timestamp())

df_2014 = df_2014.withColumn("FileName", input_file_name()) \
    .withColumn("CreatedOn", current_timestamp())

df_2010 = df_2010.withColumn("FileName", input_file_name()) \
    .withColumn("CreatedOn", current_timestamp())

df_2014 = df_2014.withColumnRenamed("vendor_id", "VendorID")
df_2010 = df_2010.withColumnRenamed("vendor_id", "VendorID")

df_2019.write.partitionBy("VendorID").format("delta").mode("overwrite").save(bronze_path + "2019")
df_2014.write.partitionBy("VendorID").format("delta").mode("overwrite").save(bronze_path + "2014")
df_2010.write.partitionBy("VendorID").format("delta").mode("overwrite").save(bronze_path + "2010")