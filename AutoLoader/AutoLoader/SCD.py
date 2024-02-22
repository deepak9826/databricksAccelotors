# Databricks notebook source
import dlt
from pyspark.sql.functions import concat_ws, md5, col, current_timestamp

# COMMAND ----------

dbutils.fs.ls("/mnt/raw")

# COMMAND ----------



@dlt.table
def bronze_customers():
  return (
    spark.readStream.format("cloudFiles") \
      .option("cloudFiles.format", "csv") \
      .option("cloudFiles.inferColumnTypes", "true") \
      .option("header", "true") \
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
      .load("dbfs:/mnt/raw/newData/") \
      .withColumn("data_hash", \
        md5(concat_ws('-', col("cust_id"),col("cust_name"),col("cust_location")))) \
      .withColumn("file_name", col("_metadata.file_name")) \
      .withColumn("insert_timestamp", current_timestamp())
)


# COMMAND ----------

@dlt.table(name="silver_customers")
@dlt.expect_or_drop("valid_cust_id", "cust_id > 0")
def silver_customers():
  return (
    dlt.read_stream("bronze_customers")
      .select("*")
  )

# COMMAND ----------

@dlt.view
def gold_customers_intra_dedup():
  return (
    dlt.read_stream("silver_customers")
      .select("*")
      .dropDuplicates(["data_hash"])
  )

# COMMAND ----------

dlt.create_streaming_table("gold_customers")

dlt.apply_changes(
  target = "gold_customers",
  source = "gold_customers_intra_dedup",
  keys = ["cust_id"],
  sequence_by = col("cust_last_updt_ts"),
  stored_as_scd_type = "2",
  track_history_column_list = ["data_hash"]
)

# COMMAND ----------

