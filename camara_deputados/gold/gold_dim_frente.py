# Databricks notebook source
df = spark.table(
    "workspace.silver.frentes"
)

display(df)

# COMMAND ----------

from pyspark.sql import functions as F

df_gold = (

    df

    .select(
        F.col("id").alias("sk_frente"),
        F.col("titulo").alias("nm_frente"),
        F.col("idLegislatura").alias("id_legislatura"),
        F.col("uri").alias("url_api"),
        F.col("dt_ingestao"),
        F.col("dt_processamento")
    )

    .dropDuplicates()

)

# COMMAND ----------

(
    df_gold
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.gold.dim_frente"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.gold.dim_frente
# MAGIC LIMIT 20
