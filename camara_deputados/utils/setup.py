# Databricks notebook source
# DBTITLE 1,Cell 1
# Create a Volume for data storage
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.camara_deputados")

# Create subdirectories within the Volume
dbutils.fs.mkdirs("/Volumes/workspace/default/camara_deputados/bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.metadata;
