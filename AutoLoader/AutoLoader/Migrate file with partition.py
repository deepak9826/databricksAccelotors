# Databricks notebook source
input_folder ='dbfs:/mnt/raw/sqlfolder5/'
# dbutils.fs.ls("/mnt/raw/")

# COMMAND ----------

output_folder='dbfs:/mnt/raw/output5'

# COMMAND ----------

# Read CSV files into a Spark DataFrame
df1 = spark.read.option("header", "true").csv(input_folder)

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.write.format("delta").saveAsTable("default.table4")


# COMMAND ----------

# Show the DataFrame
df1.display()

# COMMAND ----------

df.createOrReplaceTempView("table1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table1;

# COMMAND ----------

dbutils.fs.ls('/mnt/raw/table_folder/')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table new_table1;

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table partition_new6
# MAGIC location 'abfss://new-container@storagefordemotesting.dfs.core.windows.net/new-folder5'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED table_12;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions default.table_12;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.partition_new6    ;

# COMMAND ----------

