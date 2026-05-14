# Databricks notebook source
# DBTITLE 1,Cell 1
# Create a Volume for data storage
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.camara_deputados")

# Create subdirectories within the Volume
dbutils.fs.mkdirs("/Volumes/workspace/default/camara_deputados/bronze")
dbutils.fs.mkdirs("/Volumes/workspace/default/camara_deputados/silver")
dbutils.fs.mkdirs("/Volumes/workspace/default/camara_deputados/gold")
dbutils.fs.mkdirs("/Volumes/workspace/default/camara_deputados/checkpoints")
dbutils.fs.mkdirs("/Volumes/workspace/default/camara_deputados/logs")
