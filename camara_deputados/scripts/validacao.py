# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.default

# COMMAND ----------

# MAGIC %md
# MAGIC consulta tabela de log

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from
# MAGIC workspace.metadata.ingestion_control
# MAGIC order by execution_time desc

# COMMAND ----------

# MAGIC %md
# MAGIC volume by SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.`/Volumes/workspace/default/camara_deputados/bronze/eventos`
# MAGIC where 0=0
# MAGIC --and EXISTS (orgaos, x -> x.codTipoOrgao = 20) -- cpmi
# MAGIC and EXISTS (orgaos, x -> x['codTipoOrgao'] = 4)

# COMMAND ----------

# MAGIC %md
# MAGIC volume by PYTHON (SPARK)

# COMMAND ----------

spark.read.format("delta").load(
    "/Volumes/workspace/default/camara_deputados/bronze/eventos"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC tabela by PYTHON (SPARK)

# COMMAND ----------

spark.sql("""

SELECT *
FROM workspace.default.bronze_partidos
limite 100

""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC tabela by SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM workspace.default.bronze_partidos

# COMMAND ----------

# MAGIC %md
# MAGIC apaga VOLUME

# COMMAND ----------

dbutils.fs.rm(
    "/Volumes/workspace/default/camara_deputados/logs/metadata_ingestion_control",
    recurse=True
)
