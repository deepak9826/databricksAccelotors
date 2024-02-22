# Databricks notebook source
# MAGIC %sql
# MAGIC create table my_table;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC COPY INTO my_table
# MAGIC FROM 'dbfs:/mnt/raw/input/'
# MAGIC FILEFORMAT = CSV
# MAGIC format_options('header'='true')
# MAGIC copy_options('mergeSchema'='true')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_table;