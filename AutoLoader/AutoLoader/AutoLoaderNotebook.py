# Databricks notebook source
# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id":"6e69693c-3fdf-474c-aa45-3eb4430b425f",
#           "fs.azure.account.oauth2.client.secret": ".rs8Q~2ct-6PhjdKBhJJX1cHhdorTeTApjU6Hca4",
#           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/3935a805-f370-4e65-aca5-abad6880ec86/oauth2/token"}

# # Optionally, you can add <directory-name> to the source URI of your mount point.
# dbutils.fs.mount(
#   source = "abfss://new-container@storagefordemotesting.dfs.core.windows.net/",
#   mount_point = "/mnt/point",
#   extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
   source="wasbs://new-container@storagefordemotesting.blob.core.windows.net/",
  mount_point = "/mnt/raw",
  extra_configs = {"fs.azure.account.key.storagefordemotesting.blob.core.windows.net":"sSZKGfj41fiWoE70aPC3xYj2WRJdSqJfbHkRQGXVIbOXcw9RFUe4XdMathGrgXJhzIiZ2AW+fw6e+AStkz6Cig=="}
)

# COMMAND ----------

dbutils.fs.ls("/mnt/raw/input")


# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# Define variables used in code below
# file_path = "dbfs:/mnt/raw/input"
# username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = table_demo"
checkpoint_path = checkpoint/demo"

# COMMAND ----------



# COMMAND ----------

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

file_path = "dbfs:/mnt/raw/sqlfolder5"

# COMMAND ----------

table_name = "table_12"
checkpoint_path = "dbfs:/mnt/raw/output16/checkpoint"

# COMMAND ----------

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("spark.sql.streaming.schemaInference", "true")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .writeStream
  .format("delta")
  .partitionBy("YearColumn")
  .option("checkpointLocation", checkpoint_path)
  .option("path","dbfs:/mnt/raw/output16/outputdata")
  .toTable(table_name))

# COMMAND ----------

dbutils.fs.mount(
   source="wasbs://new-container@storagefordemotesting.blob.core.windows.net/",
  mount_point = "/mnt/raw",
  extra_configs = {"fs.azure.account.key.storagefordemotesting.blob.core.windows.net":"sSZKGfj41fiWoE70aPC3xYj2WRJdSqJfbHkRQGXVIbOXcw9RFUe4XdMathGrgXJhzIiZ2AW+fw6e+AStkz6Cig=="}
)

# COMMAND ----------

input_file="dbfs:/mnt/raw/input"
output_file="dbfs:/mnt/raw/output"

# COMMAND ----------



# COMMAND ----------

#correct code all option implemented..
(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
# .option("cloudFiles.allowOverwrites",True)
.option("cloudFiles.schemaEvolutionMode","rescue")
.option("cloudFiles.schemaLocation",f"{output_file}/deepak/autoloader2/schemaLocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation",f"{output_file}/deepak/autoloader2/checkpoint")
.option("path",f"{output_file}/deepak/autoloader2/output")
# .option("")
.option("mergeSchema",True)
.table("autoloader2"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from autoloader1;

# COMMAND ----------

